package quic_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// hookedConn wraps one end of a pair so a test can drop, reorder or inject
// datagrams. This is deliberately test-local machinery: internal/dgram is a
// reliable transport on purpose, and the tests that need unreliability are
// exactly the ones for the loss recovery that answers it.
type hookedConn struct {
	net.PacketConn
	mu sync.Mutex
	// filterWrite decides what actually leaves for each datagram written:
	// nil drops it, any number of datagrams (this one, held ones) go out in
	// order. Nil means pass through.
	filterWrite func(b []byte) [][]byte
	// injected datagrams are returned by ReadFrom before real traffic,
	// carrying whatever sender address the test chose.
	injected []injectedDatagram
}

type injectedDatagram struct {
	data []byte
	from net.Addr
}

func (h *hookedConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	// The filter runs under mu: its closed-over state is then safe both
	// against concurrent writers and against the test reading it under the
	// same lock.
	h.mu.Lock()
	filter := h.filterWrite
	var out [][]byte
	if filter != nil {
		out = filter(append([]byte(nil), b...))
	}
	h.mu.Unlock()
	if filter == nil {
		return h.PacketConn.WriteTo(b, addr)
	}
	for _, d := range out {
		if _, err := h.PacketConn.WriteTo(d, addr); err != nil {
			return 0, err
		}
	}
	return len(b), nil
}

func (h *hookedConn) ReadFrom(p []byte) (int, net.Addr, error) {
	h.mu.Lock()
	if len(h.injected) > 0 {
		d := h.injected[0]
		h.injected = h.injected[1:]
		h.mu.Unlock()
		return copy(p, d.data), d.from, nil
	}
	h.mu.Unlock()
	return h.PacketConn.ReadFrom(p)
}

func (h *hookedConn) inject(data []byte, from net.Addr) {
	h.mu.Lock()
	h.injected = append(h.injected, injectedDatagram{data: data, from: from})
	h.mu.Unlock()
}

type fakeAddr string

func (a fakeAddr) Network() string { return "dgram" }
func (a fakeAddr) String() string  { return string(a) }

// startEchoServer accepts one connection on pc and echoes every stream the
// peer opens: read to EOF, write it back, close. Errors surface on the
// returned channel.
func startEchoServer(t *testing.T, pc net.PacketConn) (<-chan *quic.Conn, <-chan error) {
	t.Helper()
	return startEchoServerTLS(t, pc, serverTLS(t))
}

// startEchoServerTLS is startEchoServer with the server's TLS configuration
// chosen by the test.
func startEchoServerTLS(t *testing.T, pc net.PacketConn, cfg *tls.Config) (<-chan *quic.Conn, <-chan error) {
	t.Helper()
	connCh := make(chan *quic.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := pc.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			errCh <- err
			return
		}
		n, peer, err := pc.ReadFrom(buf)
		if err != nil {
			errCh <- err
			return
		}
		conn, err := quic.Accept(pc, peer, buf[:n], cfg, quic.DefaultParameters())
		if err != nil {
			errCh <- err
			return
		}
		connCh <- conn
		for {
			s, err := conn.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			body, err := io.ReadAll(s)
			if err != nil {
				errCh <- fmt.Errorf("server read: %w (conn: %v)", err, conn.Err()) //nolint:errorlint // conn.Err() is context and may be nil
				return
			}
			if _, err := s.Write(body); err != nil {
				errCh <- fmt.Errorf("server write: %w", err)
				return
			}
			if err := s.CloseWrite(); err != nil {
				errCh <- err
				return
			}
		}
	}()
	return connCh, errCh
}

func echoOnce(t *testing.T, conn *quic.Conn, payload string) error {
	t.Helper()
	s, err := conn.OpenStream()
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte(payload)); err != nil {
		return err
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	got, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("reading the echo: %w", err)
	}
	if string(got) != payload {
		return fmt.Errorf("echo = %q, want %q", got, payload)
	}
	return nil
}

// One hostile or stray datagram used to kill an established connection: any
// parse or authentication failure in the read loop was fatal. Now it is
// counted and dropped — from garbage, from a valid-looking header with a bad
// seal, and from a sender that is not the peer at all (RFC 9000 §5.2, §12.2).
func TestHostileDatagramsDoNotKillTheConnection(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()
	hooked := &hookedConn{PacketConn: serverPC}

	connCh, errCh := startEchoServer(t, hooked)
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	var serverConn *quic.Conn
	select {
	case serverConn = <-connCh:
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
	defer serverConn.Close()

	if err := echoOnce(t, conn, "before the hostility"); err != nil {
		t.Fatal(err)
	}

	peer := clientPC.LocalAddr()
	hostile := [][]byte{
		{0xff, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00}, // an unknown version
		bytes.Repeat([]byte{0xa5}, 64),             // garbage with the short-header bit
		append([]byte{0x40}, make([]byte, 40)...),  // a short header that will not authenticate
		make([]byte, 32),                           // zeros: datagram padding, silently fine
		{0x80, 0x00, 0x00, 0x00, 0x01, 0x08},       // a truncated long header
	}
	for _, d := range hostile {
		hooked.inject(d, peer)
	}
	// And a well-formed datagram from an address that is not the peer's.
	hooked.inject(bytes.Repeat([]byte{0x40}, 48), fakeAddr("dgram:intruder"))

	// The connection still serves.
	if err := echoOnce(t, conn, "after the hostility"); err != nil {
		t.Fatalf("the connection did not survive: %v (server err: %v)", err, serverConn.Err())
	}
	if serverConn.Err() != nil {
		t.Fatalf("the server connection recorded a failure: %v", serverConn.Err())
	}
	if n := serverConn.Stats().DiscardedPackets; n == 0 {
		t.Error("nothing was counted as discarded")
	}
}

// The padding branch, forced: with P-256 the ClientHello is small, so the
// first flight must be padded to 1200 bytes *inside* the packet. The old
// code appended zeros after the sealed packet, the receiver parsed them as a
// next packet and died — so this handshake could not complete, and the bug
// hid behind ML-KEM's large ClientHello.
func TestInitialPaddingWithASmallClientHello(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	firstLen := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			done <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		firstLen <- n
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		s, err := conn.AcceptStream()
		if err != nil {
			done <- err
			return
		}
		body, err := io.ReadAll(s)
		if err != nil {
			done <- err
			return
		}
		if string(body) != "small hello" {
			done <- fmt.Errorf("read %q", body)
			return
		}
		done <- nil
	}()

	cfg := clientTLS()
	cfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial with a small ClientHello: %v", err)
	}
	defer conn.Close()

	if n := <-firstLen; n < 1200 {
		t.Errorf("the first client datagram was %d bytes; §14.1 requires 1200", n)
	}

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("small hello")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the server: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the exchange did not finish")
	}
}

// Stream arithmetic must not kill a long-lived connection: the accept queue
// used to overflow fatally at 65 streams and the stream map died at 256.
// Completed streams are retired now and the slots granted back with
// MAX_STREAMS, so a connection serves an unbounded sequence of requests
// through a bounded window.
func TestManyStreamsSurvive(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	var serverConn *quic.Conn
	select {
	case serverConn = <-connCh:
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
	defer serverConn.Close()

	// Well past both former death sentences: the 64-slot accept queue and
	// the 256-stream map.
	const streams = 300
	for i := range streams {
		if err := echoOnce(t, conn, fmt.Sprintf("request %d", i)); err != nil {
			select {
			case serr := <-errCh:
				t.Fatalf("stream %d: %v (server: %v)", i, err, serr)
			default:
				t.Fatalf("stream %d: %v (server conn: %v)", i, err, serverConn.Err())
			}
		}
	}
	if err := serverConn.Err(); err != nil {
		t.Fatalf("the server connection failed along the way: %v", err)
	}
}

// A dropped datagram is retransmitted: the probe timeout notices the silence
// and resends. This is the property none of the loopback tests could see,
// and the one that separates a transport from a demo.
func TestLostDatagramIsRetransmitted(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	hooked := &hookedConn{PacketConn: clientPC}
	var dropped bool
	hooked.mu.Lock()
	hooked.filterWrite = func(b []byte) [][]byte {
		// Drop the client's first 1-RTT packet, once. That is the request
		// itself: without retransmission the server never sees it.
		if !dropped && b[0]&0x80 == 0 {
			dropped = true
			return nil
		}
		return [][]byte{b}
	}
	hooked.mu.Unlock()

	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(hooked, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	select {
	case sc := <-connCh:
		defer sc.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}

	if err := echoOnce(t, conn, "worth resending"); err != nil {
		t.Fatalf("the lost datagram was never repaired: %v", err)
	}
	hooked.mu.Lock()
	didDrop := dropped
	hooked.mu.Unlock()
	if !didDrop {
		t.Fatal("the test dropped nothing; it proved nothing")
	}
}

// The deferred-key path, exercised directly: a 1-RTT packet that arrives
// before the server's application read keys are installed — because the
// datagram carrying the client's Finished was reordered behind it — is held
// and replayed once the keys exist. This was one of the two suspects for the
// flaky HTTP/3 test.
func TestReorderedFinishedDefersTheRequest(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	hooked := &hookedConn{PacketConn: clientPC}
	var held [][]byte
	releaseAfterShort := true
	hooked.mu.Lock()
	hooked.filterWrite = func(b []byte) [][]byte {
		isHandshake := b[0]&0x80 != 0 && (b[0]>>4)&0x03 == 0x02
		if releaseAfterShort && isHandshake {
			held = append(held, b)
			return nil // hold the Finished until a 1-RTT packet passes
		}
		if releaseAfterShort && b[0]&0x80 == 0 {
			releaseAfterShort = false
			out := append([][]byte{b}, held...)
			held = nil
			return out
		}
		return [][]byte{b}
	}
	hooked.mu.Unlock()

	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(hooked, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// The client is done; the server is still waiting for the held Finished.
	// This write is the 1-RTT packet that releases it — and reaches the
	// server before the keys that can open it.
	if err := echoOnce(t, conn, "early bird"); err != nil {
		t.Fatalf("the deferred packet was lost, not held: %v", err)
	}
	select {
	case sc := <-connCh:
		defer sc.Close()
		if err := sc.Err(); err != nil {
			t.Fatalf("the server connection: %v", err)
		}
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
}

// Writing after this end reset the stream is refused: the peer was told the
// final size, and bytes past it would contradict an authenticated statement.
func TestWriteAfterResetIsRefused(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	select {
	case sc := <-connCh:
		defer sc.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("half a request")); err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(0x10c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("more")); !errors.Is(err, quic.ErrQUIC) {
		t.Fatalf("a write after Reset returned %v, want a refusal", err)
	}
}

// STOP_SENDING from the peer abandons this end's writes: the writer is told
// — and released if it was parked on credit — rather than left producing
// bytes nobody will read.
func TestStopSendingAbandonsTheWriter(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	stopped := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
			stopped <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			stopped <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		if err != nil {
			stopped <- err
			return
		}
		defer conn.Close()
		s, err := conn.AcceptStream()
		if err != nil {
			stopped <- err
			return
		}
		stopped <- s.StopSending(0x77)
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("the first of many")); err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("the server could not send STOP_SENDING: %v", err)
	}

	// The refusal races the next writes; keep writing until it lands.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := s.Write([]byte("chunk"))
		if err != nil {
			if !errors.Is(err, quic.ErrQUIC) {
				t.Fatalf("the write failed with %v, want the stream refusal", err)
			}
			if code, reset := s.WasReset(); !reset || code != 0x77 {
				t.Fatalf("WasReset = %#x, %v; want the peer's 0x77", code, reset)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("STOP_SENDING never reached the writer")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A connection's goroutines — the read loop and the timer loop, for both
// ends — are gone once it closes. Two per connection is the design; any more
// after teardown is a leak this test exists to catch.
func TestConnectionsLeaveNoGoroutines(t *testing.T) {
	// Let stragglers from earlier tests finish first, so the baseline is not
	// inflated — an inflated baseline would let `after <= before` pass while
	// this test's own goroutines leaked. Even settled, this stays a smoke
	// check: it counts goroutines, it cannot attribute them.
	before := runtime.NumGoroutine()
	for settle, steady := time.Now().Add(time.Second), 0; steady < 3 && time.Now().Before(settle); {
		time.Sleep(10 * time.Millisecond)
		if n := runtime.NumGoroutine(); n < before {
			before, steady = n, 0
		} else {
			steady++
		}
	}
	for range 3 {
		serverPC, clientPC := dgram.Pair()
		connCh, errCh := startEchoServer(t, serverPC)
		conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		var sc *quic.Conn
		select {
		case sc = <-connCh:
		case err := <-errCh:
			t.Fatalf("the server: %v", err)
		}
		if err := echoOnce(t, conn, "hello"); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		sc.Close()
		serverPC.Close()
		clientPC.Close()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= before {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines: %d before, %d after\n%s",
				before, runtime.NumGoroutine(), buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Err stays callable from any goroutine while the connection tears down —
// the race detector is the assertion here.
func TestErrIsSafeDuringTeardown(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()
	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case sc := <-connCh:
		defer sc.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			_ = conn.Err()
			_ = conn.Stats().DiscardedPackets
		}
	}()
	conn.Close()
	<-done
	if err := conn.Err(); err != nil {
		t.Fatalf("a clean close recorded %v", err)
	}
}

// Connection-level credit is replenished in batch mode too. The batch
// callback is the consumer there — no Read ever runs — so the consumption
// that earns MAX_DATA has to be accounted where the callback returns.
// It was not: only the per-stream level was settled, so a batch-mode server
// served at most InitialMaxData cumulative bytes over the connection's whole
// life, then a conformant client stalled forever on a MAX_DATA that never
// came. The 300-stream test never saw it because 300 echoes weigh a few
// kilobytes; this one pushes four times the connection window.
func TestBatchModeReplenishesConnectionCredit(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	small := quic.DefaultParameters()
	small.InitialMaxData = 16 << 10
	// Per-stream credit must not be the constraint: the connection level is
	// what this test isolates.
	small.InitialMaxStreamDataBidiRemote = 1 << 20

	const total = 64 << 10
	gotAll := make(chan struct{})
	serverUp := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), small)
		if err != nil {
			serverUp <- err
			return
		}
		defer conn.Close()
		var received int
		done := false
		conn.OnStreamFrames(func(frames []quic.Frame) error {
			for _, f := range frames {
				if f.Type == quic.FrameStream {
					received += len(f.Data)
				}
			}
			if received >= total && !done {
				done = true
				close(gotAll)
			}
			return nil
		})
		serverUp <- nil
		<-conn.Done()
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := s.Write(bytes.Repeat([]byte("x"), total))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("writing through the connection window: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the write wedged: batch-mode consumption never earned a MAX_DATA")
	}
	select {
	case <-gotAll:
	case <-time.After(10 * time.Second):
		t.Fatal("the server never saw the full payload")
	}
}

// Flow control blocks the writer at the peer's grant and credit arrives as
// the reader consumes: 64 KiB crosses windows an eighth of its size, which
// only works if MAX_DATA and MAX_STREAM_DATA actually flow back.
func TestFlowControlBlocksAndResumes(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	small := quic.DefaultParameters()
	small.InitialMaxData = 8 << 10
	small.InitialMaxStreamDataBidiRemote = 4 << 10

	payload := bytes.Repeat([]byte("0123456789abcdef"), 4096) // 64 KiB

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			done <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), small)
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		s, err := conn.AcceptStream()
		if err != nil {
			done <- err
			return
		}
		body, err := io.ReadAll(s)
		if err != nil {
			done <- fmt.Errorf("server read: %w (conn: %v)", err, conn.Err()) //nolint:errorlint // conn.Err() is context and may be nil
			return
		}
		if !bytes.Equal(body, payload) {
			done <- fmt.Errorf("the body arrived wrong: %d bytes of %d", len(body), len(payload))
			return
		}
		done <- nil
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("writing through the small window: %v", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the server: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the transfer wedged — credit is not flowing back")
	}
}

// A server whose whole first flight is lost — the ServerHello spans two
// Initial packets since the post-quantum key shares, the second coalesced
// with the Handshake flight — must resend *both* on its probe timeout. The
// probe used to copy only the oldest unacknowledged packet, and the copy was
// anonymous to the flight record: with the client's acknowledgements lost
// too, the original stayed the oldest, every doubled timeout re-sent the
// first half, and the client, holding half a ServerHello, never got the
// rest. The QUIC Interop Runner's handshakeloss case against quic-go is
// where it showed. Here the client's own datagrams are held back once the
// server's flight is dropped, until the client proves it read the whole
// ServerHello by sending at the Handshake level, so the only thing that can
// complete the handshake is the server's probe carrying both halves.
func TestServerProbeResendsItsWholeInitialFlight(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	serverHooked := &hookedConn{PacketConn: serverPC}
	serverDatagrams := 0
	serverHooked.mu.Lock()
	serverHooked.filterWrite = func(b []byte) [][]byte {
		// The server's first datagram acknowledges the ClientHello; the
		// second and third carry the ServerHello and the Handshake flight.
		serverDatagrams++
		if serverDatagrams == 2 || serverDatagrams == 3 {
			return nil
		}
		return [][]byte{b}
	}
	serverHooked.mu.Unlock()

	clientHooked := &hookedConn{PacketConn: clientPC}
	clientDatagrams := 0
	clientHeldBack := 0
	clientHooked.mu.Lock()
	clientHooked.filterWrite = func(b []byte) [][]byte {
		clientDatagrams++
		isHandshake := b[0]&0x80 != 0 && (b[0]>>4)&0x03 == 0x02
		if clientDatagrams <= 2 || isHandshake {
			return [][]byte{b} // the ClientHello, then whatever follows the full ServerHello
		}
		clientHeldBack++
		return nil
	}
	clientHooked.mu.Unlock()

	connCh, errCh := startEchoServer(t, serverHooked)
	// The server has no round-trip sample: its first probe fires at the
	// 1-second initial PTO (RFC 9002 §6.2.2), the next two seconds later.
	// One probe must be enough; the deadline leaves no room for a second.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	conn, err := quic.DialContext(ctx, clientHooked, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("the handshake did not complete on the server's first probe: %v", err)
	}
	defer conn.Close()
	select {
	case sc := <-connCh:
		defer sc.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
	clientHooked.mu.Lock()
	held := clientHeldBack
	clientHooked.mu.Unlock()
	if held == 0 {
		t.Fatal("no client datagram was held back; the test proved nothing")
	}
}

// delayedConn delivers every datagram written through it one fixed delay
// later: a one-way latency, so that a round trip and the probe timeout it
// sets are measurable amounts of time rather than loopback noise.
type delayedConn struct {
	net.PacketConn
	delay time.Duration
}

func (d *delayedConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	data := append([]byte(nil), b...)
	time.AfterFunc(d.delay, func() { _, _ = d.PacketConn.WriteTo(data, addr) })
	return len(b), nil
}

// longChainTLS is serverTLS with a certificate chain of nine, padded past
// 7500 bytes: the runner's amplificationlimit server, whose first flight
// cannot fit in three times one client Initial.
func longChainTLS(t *testing.T) *tls.Config {
	t.Helper()
	cfg := serverTLS(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	total := len(cfg.Certificates[0].Certificate[0])
	for i := range 8 {
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageCertSign,
			ExtraExtensions: []pkix.Extension{{
				Id:    asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 99999, 1},
				Value: make([]byte, 800),
			}},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Certificates[0].Certificate = append(cfg.Certificates[0].Certificate, der)
		total += len(der)
	}
	if total < 7500 {
		t.Fatalf("the chain is %d bytes; the runner's server sends at least 7500", total)
	}
	return cfg
}

// The runner's amplificationlimit case, client side: the ClientHello spans
// two Initial datagrams and the network drops client datagrams 2 to 7. The
// server, allowed three times what it received, has answered the first
// with an acknowledgement and can do nothing more until the client sends
// again — and the server gives up five seconds after that. Only the client
// can raise the budget, and every probe timeout doubles the wait: at one
// datagram per probe the eighth leaves after 63 intervals, at two after 7
// (RFC 9002 §6.2.4). A 40 ms one-way latency makes the interval 120 ms,
// which puts the two strategies on either side of the deadline.
func TestClientProbesUnblockAnAmplificationLimitedServer(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	clientHooked := &hookedConn{PacketConn: clientPC}
	clientDatagrams := 0
	dropped := 0
	var first, eighth time.Time
	clientHooked.mu.Lock()
	clientHooked.filterWrite = func(b []byte) [][]byte {
		clientDatagrams++
		switch {
		case clientDatagrams == 1:
			first = time.Now()
		case clientDatagrams <= 7:
			dropped++
			return nil
		case clientDatagrams == 8:
			eighth = time.Now()
		}
		return [][]byte{b}
	}
	clientHooked.mu.Unlock()

	connCh, errCh := startEchoServerTLS(t, &delayedConn{PacketConn: serverPC, delay: 40 * time.Millisecond}, longChainTLS(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialContext(ctx, clientHooked, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		clientHooked.mu.Lock()
		n, dr := clientDatagrams, dropped
		clientHooked.mu.Unlock()
		t.Fatalf("the handshake did not complete in 5 s (%d client datagrams, %d dropped): %v", n, dr, err)
	}
	defer conn.Close()
	select {
	case sc := <-connCh:
		defer sc.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}
	clientHooked.mu.Lock()
	dr, gap := dropped, eighth.Sub(first)
	clientHooked.mu.Unlock()
	if dr != 6 {
		t.Fatalf("%d client datagrams were dropped, want 6; the test proved nothing", dr)
	}
	// Seven intervals of 120 ms, with room for the scheduler.
	if gap > 2*time.Second {
		t.Errorf("the eighth client datagram left %v after the first; two probes per timeout put it under a second", gap)
	}
}
