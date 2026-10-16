package quic_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9000 §10.2.1: after sending its CONNECTION_CLOSE, an endpoint stays
// in the closing state and answers any packet it still attributes to the
// connection with the close again. Here the server's close is lost on the
// way out; the client, unaware, sends on, and must hear the application
// code — not the stateless reset a listener that had already forgotten the
// connection would answer with, and not its idle timeout.
func TestClosingRepeatsALostConnectionClose(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	server := &hookedConn{PacketConn: udp}
	l, err := quic.NewListener(server, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	client := dialUDP(t, l.Addr(), quic.DefaultParameters())
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// Let the handshake's tail (HANDSHAKE_DONE, the last acknowledgements)
	// leave before the filter arms, so that the first 1-RTT datagram it
	// sees is the close.
	time.Sleep(50 * time.Millisecond)
	dropped := 0
	server.mu.Lock()
	server.filterWrite = func(b []byte) [][]byte {
		if dropped == 0 && b[0]&0x80 == 0 {
			dropped++
			return nil
		}
		return [][]byte{b}
	}
	server.mu.Unlock()

	if err := accepted.CloseWithError(0x100); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	lost := dropped
	server.mu.Unlock()
	if lost != 1 {
		t.Fatalf("the close was not the datagram dropped (dropped %d)", lost)
	}

	// The client has heard nothing and talks on: this packet is what the
	// closing state answers.
	s, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("still here")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the client never heard the repeated close")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](client.Err())
	if !ok || !ce.Application || ce.Code != 0x100 {
		t.Fatalf("client Err = %v, want a ConnectionError with application code 0x100", client.Err())
	}
}

// delayedListener is a Listener whose every datagram is held delay on the
// way out: the round trip on loopback becomes delay, and with it the probe
// timeout both ends compute and their closing or draining period (three of
// them, RFC 9000 §10.2) — hundreds of milliseconds instead of the ~80 ms a
// bare loopback gives, which leaves a test the time to act inside the
// period under any scheduler load.
func delayedListener(t *testing.T, delay time.Duration) *quic.Listener {
	t.Helper()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(quic.NewDelayedPC(udp, delay), serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// The closing state on a socket the connection owns, when asked for
// (SetClosingPeriod): the client's close is lost, the server talks on, and
// the client — whose read loop would otherwise have left the socket the
// moment Close returned — answers with the close again, so the server
// hears the code instead of waiting out its idle timeout. Wait then
// reports the socket free once the period is over.
func TestClosingPeriodOnAnOwnedSocketRepeatsALostClose(t *testing.T) {
	l := delayedListener(t, 100*time.Millisecond)
	own, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	hooked := &hookedConn{PacketConn: own}
	client, err := quic.Dial(hooked, l.Addr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	client.SetClosingPeriod(true)
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the handshake's tail leaves first
	dropped := 0
	hooked.mu.Lock()
	hooked.filterWrite = func(b []byte) [][]byte {
		if dropped == 0 && b[0]&0x80 == 0 {
			dropped++
			return nil
		}
		return [][]byte{b}
	}
	hooked.mu.Unlock()

	if err := client.CloseWithError(0x100); err != nil {
		t.Fatal(err)
	}
	hooked.mu.Lock()
	lost := dropped
	hooked.mu.Unlock()
	if lost != 1 {
		t.Fatalf("the close was not the datagram dropped (dropped %d)", lost)
	}

	s, err := server.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("still here")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the server never heard the repeated close")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](server.Err())
	if !ok || !ce.Application || ce.Code != 0x100 {
		t.Fatalf("server Err = %v, want a ConnectionError with application code 0x100", server.Err())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Wait(ctx); err != nil {
		t.Fatalf("Wait = %v, want nil once the closing period ends", err)
	}
}

// The draining state (RFC 9000 §10.2.2) on a Listener's connection: once
// the peer has closed, a late packet of the same connection reaching the
// listener is absorbed — no CONNECTION_CLOSE (a draining endpoint sends
// nothing) and no stateless reset, which a listener that had already
// forgotten the connection would send to its identifier.
func TestDrainingAbsorbsALatePacketWithoutAReset(t *testing.T) {
	l := delayedListener(t, 200*time.Millisecond)
	addr := l.Addr()
	own, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	hooked := &hookedConn{PacketConn: own}
	var late []byte
	recording := true
	hooked.filterWrite = func(b []byte) [][]byte {
		if recording && b[0]&0x80 == 0 && len(b) > len(late) {
			late = b // the largest 1-RTT datagram, replayed after the close
		}
		return [][]byte{b}
	}
	client, err := quic.Dial(hooked, addr, clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	s, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rs, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	hooked.mu.Lock()
	recording = false
	replay := late
	hooked.mu.Unlock()

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the server never heard the client's close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Whatever the server sent before it read the close (an
	// acknowledgement, a credit update) may still be on its way or sit in
	// the socket's buffer, unread since the client closed: drained until
	// the path has been quiet longer than its delay, so that only an
	// answer to the late packet can be read below.
	buf := make([]byte, 2048)
	for {
		if err := own.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := own.ReadFrom(buf); err != nil {
			break
		}
	}
	if _, err := own.WriteTo(replay, addr); err != nil {
		t.Fatal(err)
	}
	if err := own.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, _, err := own.ReadFrom(buf)
	if err == nil {
		t.Fatalf("a late packet after the peer's close earned a %d-byte reply, want silence", n)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
}

// Wait under a context that has ended cuts the closing period short rather
// than leaving it running behind the caller: on return the connection has
// released its socket — a second Wait finds it so at once — on a socket the
// connection owns as on a Listener's, though both periods (a 300 ms path)
// would last over a second.
func TestWaitEndsTheClosingPeriodWhenTheContextEnds(t *testing.T) {
	l := delayedListener(t, 300*time.Millisecond)
	own, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	client, err := quic.Dial(own, l.Addr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	client.SetClosingPeriod(true)
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		name string
		conn *quic.Conn
	}{{"owned socket", client}, {"listener", server}} {
		if err := c.conn.Close(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := c.conn.Wait(ended); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: Wait = %v, want context.Canceled", c.name, err)
		}
		if took := time.Since(start); took > 400*time.Millisecond {
			t.Fatalf("%s: Wait took %v under an ended context, the period was not cut", c.name, took)
		}
		if err := c.conn.Wait(ended); err != nil {
			t.Fatalf("%s: second Wait = %v, want nil: the socket was not released", c.name, err)
		}
	}
}
