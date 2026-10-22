package quic_test

import (
	"context"
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
	conn, err := quic.Dial(context.Background(), pc, serverAddr, clientTLS(), params)
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
			s, err := c.OpenStream(t.Context())
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
	s, err := c.AcceptStream(t.Context())
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

// Listener.Close unblocks a parked Accept, and ends a connection already
// handed out — on both ends, since the connection has no socket of its own
// to outlive the listener on.
func TestListenerCloseUnblocksAcceptAndEndsAcceptedConnections(t *testing.T) {
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
	// stream — before Close: what ends below is a live connection, not one
	// that was already failing.
	go echoOneStream(t, live)
	s, err := c.OpenStream(t.Context())
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

	// The connection that just echoed is ended by Close on both ends: its
	// transport is gone with the listener's socket, so left alone it would
	// only have waited out its idle timeout, and so would its peer.
	for _, end := range []struct {
		name string
		conn *quic.Conn
	}{{"server", live}, {"client", c}} {
		select {
		case <-end.conn.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("the %s end of the accepted connection survived Listener.Close", end.name)
		}
	}
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
	s, err := c.OpenStream(t.Context())
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
	s, err := c.OpenStream(t.Context())
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

// ListenerConfig.PreferredAddress, from outside the package: a listener on
// 127.0.0.1 announces a second socket on 127.0.0.2, the client moves there
// on its own after the handshake, the server follows onto that socket, and
// data flows both ways afterwards. Only the public surface is read: each
// side counts exactly one migration. Skipped where 127.0.0.2 is not a
// local address (macOS configures only 127.0.0.1).
func TestListenerAnnouncesItsPreferredAddress(t *testing.T) {
	pref, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2)})
	if err != nil {
		t.Skipf("no 127.0.0.2 on this host: %v", err)
	}
	l, addr := newUDPListener(t, quic.ListenerConfig{PreferredAddress: pref})

	accepted := make(chan *quic.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	client := dialUDP(t, addr, quic.DefaultParameters())
	var server *quic.Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
	defer server.Close()

	// Traffic in both directions is what proves the move: the client's
	// first stream reaches the server, which by then has followed onto
	// the preferred socket, and its answer comes back from there.
	received := make(chan []byte, 8)
	server.OnStreamFrames(quic.CreditOnDelivery, func(fs []quic.Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				received <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	echo := make(chan []byte, 8)
	client.OnStreamFrames(quic.CreditOnDelivery, func(fs []quic.Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				echo <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})

	deadline := time.Now().Add(10 * time.Second)
	for client.Migrations() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the client never moved to the preferred address")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("to the preferred address")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-received:
		if string(data) != "to the preferred address" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent to the preferred address never arrived")
	}
	for server.Migrations() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the server never followed onto its preferred socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	back, err := server.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := back.Write([]byte("from the preferred address")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-echo:
		if string(data) != "from the preferred address" {
			t.Errorf("the client read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent from the preferred address never arrived")
	}
	if n := client.Migrations(); n != 1 {
		t.Errorf("client migrations = %d, want 1", n)
	}
	if n := server.Migrations(); n != 1 {
		t.Errorf("server migrations = %d, want 1", n)
	}
}

// handshakeLength finds the long-header Handshake packets among a
// datagram's coalesced packets and reads the largest Length field, the size
// of what follows it; ok is false when the datagram carries none.
func handshakeLength(b []byte) (n uint64, ok bool) {
	varint := func(b []byte) (uint64, int) {
		if len(b) == 0 || len(b) < 1<<(b[0]>>6) {
			return 0, 0
		}
		l := 1 << (b[0] >> 6)
		v := uint64(b[0] & 0x3f)
		for i := 1; i < l; i++ {
			v = v<<8 | uint64(b[i])
		}
		return v, l
	}
	for len(b) >= 7 && b[0]&0x80 != 0 {
		typ := (b[0] >> 4) & 3
		off := 5
		off += 1 + int(b[off]) // destination connection ID
		if off >= len(b) {
			return 0, false
		}
		off += 1 + int(b[off]) // source connection ID
		if typ == 0 {          // an Initial's token
			tl, w := varint(b[off:])
			if w == 0 {
				return 0, false
			}
			off += w + int(tl)
		}
		length, w := varint(b[off:])
		if w == 0 {
			return 0, false
		}
		if typ == 2 {
			n, ok = max(n, length), true
		}
		end := off + w + int(length)
		if end > len(b) {
			return n, ok
		}
		b = b[end:]
	}
	return n, ok
}

// A client whose Finished is lost has proved its address with the
// acknowledgements that do arrive, and its handshake is alive: the server
// waits for the Finished past HandshakeTimeout, up to the idle timeout
// counted from the start. Here every Handshake packet of the client large
// enough to carry CRYPTO is dropped for 700 ms, its acknowledgements pass,
// and the server's HandshakeTimeout is 300 ms — under a fixed deadline the
// server closed before the Finished came through (runner handshakeloss
// against ngtcp2 and quinn).
func TestALostFinishedOutlivesTheHandshakeTimeout(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{HandshakeTimeout: 300 * time.Millisecond})
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client := &hookedConn{PacketConn: udp}
	var dropUntil time.Time
	dropped, passed := 0, 0
	client.filterWrite = func(b []byte) [][]byte {
		n, ok := handshakeLength(b)
		if !ok {
			return [][]byte{b}
		}
		if n < 40 { // an ACK alone
			passed++
			return [][]byte{b}
		}
		if dropUntil.IsZero() {
			dropUntil = time.Now().Add(700 * time.Millisecond)
		}
		if time.Now().Before(dropUntil) {
			dropped++
			return nil
		}
		return [][]byte{b}
	}
	conn, err := quic.Dial(context.Background(), client, addr, clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	got := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			_ = c.Close()
		}
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		client.mu.Lock()
		d, p := dropped, passed
		client.mu.Unlock()
		t.Fatalf("the server never accepted the connection (%d Finished packets dropped, %d acknowledgements passed)", d, p)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if dropped == 0 || passed == 0 {
		t.Fatalf("dropped %d Finished packets and passed %d acknowledgements; the test did not set up what it checks", dropped, passed)
	}
}
