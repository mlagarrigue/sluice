package quic_test

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// Listener's whole point is one address serving many real peers at once,
// which dgram.Pair (strictly one-to-one) cannot model — so these use real
// loopback UDP. That would have been the flaky choice when this package had
// no loss recovery (see internal/dgram's own doc comment); it isn't now: a
// dropped loopback datagram is retransmitted, same as any other loss.
func newUDPListener(t *testing.T, lcfg quic.ListenerConfig) (*quic.Listener, net.Addr) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(pc, serverTLS(t), quic.DefaultParameters(), lcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, pc.LocalAddr()
}

func dialUDP(t *testing.T, serverAddr net.Addr, params quic.TransportParameters) *quic.Conn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := quic.Dial(pc, serverAddr, clientTLS(), params)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Two real clients, one shared server socket: both handshakes complete
// independently and both connections work, which is the entire reason a
// Listener exists rather than one net.PacketConn per connection.
func TestListenerServesTwoConnectionsOnOneSocket(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{})

	const n = 2
	type accepted struct {
		conn *quic.Conn
		err  error
	}
	results := make(chan accepted, n)
	go func() {
		for range n {
			c, err := l.Accept()
			results <- accepted{c, err}
			if err != nil {
				return
			}
			go echoOneStream(t, c)
		}
	}()

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			c := dialUDP(t, addr, quic.DefaultParameters())
			s, err := c.OpenStream()
			if err != nil {
				t.Errorf("client %d OpenStream: %v", i, err)
				return
			}
			msg := fmt.Sprintf("hello from client %d", i)
			if _, err := s.Write([]byte(msg)); err != nil {
				t.Errorf("client %d Write: %v", i, err)
				return
			}
			if err := s.CloseWrite(); err != nil {
				t.Errorf("client %d CloseWrite: %v", i, err)
				return
			}
			got, err := io.ReadAll(s)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Errorf("client %d read: %v", i, err)
				return
			}
			if string(got) != msg {
				t.Errorf("client %d echo = %q, want %q", i, got, msg)
			}
		})
	}

	for range n {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("Accept: %v", r.err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("Accept did not return for both clients")
		}
	}
	wg.Wait()
}

func echoOneStream(t *testing.T, c *quic.Conn) {
	t.Helper()
	s, err := c.AcceptStream()
	if err != nil {
		return
	}
	body, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		return
	}
	_, _ = s.Write(body)
	_ = s.CloseWrite()
}

// A garbage datagram and one addressed to an identifier nobody issued must
// not crash the listener or block the client that is actually handshaking
// at the same time.
func TestListenerDropsUnroutableDatagramsWithoutWedgingOthers(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{})

	noise, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer noise.Close()

	// A short-header packet (fixed bit set, header form clear) naming a
	// connection identifier this listener never issued.
	garbage := append([]byte{0x40}, make([]byte, listenerCIDLenForTest+4)...)
	for range 20 {
		if _, err := noise.WriteTo(garbage, addr); err != nil {
			t.Fatal(err)
		}
	}
	// Pure zero bytes too short to be anything.
	if _, err := noise.WriteTo([]byte{0, 0, 0}, addr); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		done <- err
	}()

	c := dialUDP(t, addr, quic.DefaultParameters())
	defer c.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Accept: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the real handshake never completed — noise appears to have wedged the listener")
	}

	if n := l.DiscardedPackets(); n == 0 {
		t.Error("DiscardedPackets = 0, want the noise counted")
	}
}

// listenerCIDLenForTest mirrors the package-private listenerCIDLen (8),
// restated here since external tests cannot reach it — used only to size a
// plausible-looking short header for the noise test above.
const listenerCIDLenForTest = 8

// Listener.Close ends a connection still mid-handshake and unblocks a
// parked Accept — but must not touch a connection already handed out.
func TestListenerCloseEndsOnlyPendingConnections(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{})

	accepted := make(chan *quic.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	c := dialUDP(t, addr, quic.DefaultParameters())

	var live *quic.Conn
	select {
	case live = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("Accept: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}

	// Prove the handed-out connection genuinely works — echo data over a
	// stream — before Close. Afterwards no data can flow at all: the socket
	// is the listener's, and Close closes it. So "untouched" below can only
	// mean "not torn down", and this echo is what makes that worth saying.
	go echoOneStream(t, live)
	s, err := c.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	const msg = "a working connection"
	if _, err := s.Write([]byte(msg)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	if string(got) != msg {
		t.Errorf("echo = %q, want %q", got, msg)
	}

	blocked := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		blocked <- err
	}()

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-blocked:
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("the pending Accept returned %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock a pending Accept")
	}

	// The connection that just echoed was not torn down by Close. Its
	// transport is gone with the listener's socket, so carrying more data is
	// impossible by construction; what Close must not have done is end the
	// connection's own state, and Done() firing is how that would show.
	select {
	case <-live.Done():
		t.Error("the already-accepted connection was closed by Listener.Close")
	default:
	}
	_ = c.Close()
}

// AlwaysRetry round-trips a real token through a real second Initial and
// still produces a working connection.
func TestListenerAlwaysRetryStillCompletesTheHandshake(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{AlwaysRetry: true})

	done := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		go echoOneStream(t, c)
		done <- nil
	}()

	c := dialUDP(t, addr, quic.DefaultParameters())
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("past the retry")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Accept: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed with AlwaysRetry on")
	}

	got, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if string(got) != "past the retry" {
		t.Errorf("echo = %q", got)
	}
}

// The RFC 9000 §8.1 anti-amplification limit lets the server send only three
// times what the client's address has sent it — about 3.6KB against the
// client's padded first Initial. A realistic certificate chain overflows
// that, so the tail of the server's first flight is withheld. This is the
// regression test for those bytes being queued rather than dropped: handshake
// bytes exist nowhere else — crypto/tls does not re-emit them — so dropping
// them killed every handshake whose chain outgrew the budget, which small
// test certificates never exercised.
func TestListenerHandshakeSurvivesAnAmplificationLimitedFirstFlight(t *testing.T) {
	cfg := serverTLS(t)
	// Inflate the chain far past three times the client's first datagram.
	// The same self-signed certificate repeated parses fine on a client
	// that skips verification; only its wire size matters here.
	chain := cfg.Certificates[0]
	der := chain.Certificate[0]
	for total := len(der); total < 12_000; total += len(der) {
		chain.Certificate = append(chain.Certificate, der)
	}
	cfg.Certificates = []tls.Certificate{chain}

	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(pc, cfg, quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		echoOneStream(t, c)
	}()

	c := dialUDP(t, pc.LocalAddr(), quic.DefaultParameters())
	s, err := c.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := s.Write([]byte("across the limit")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	got, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "across the limit" {
		t.Fatalf("echo = %q", got)
	}
}
