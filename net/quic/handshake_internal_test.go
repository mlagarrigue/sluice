package quic

import (
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// failFirstReadPC fails its first ReadFrom the way a forged ICMP surfaces
// on a UDP socket, then reads normally.
type failFirstReadPC struct {
	net.PacketConn
	failed atomic.Bool
}

func (p *failFirstReadPC) ReadFrom(b []byte) (int, net.Addr, error) {
	if p.failed.CompareAndSwap(false, true) {
		return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: syscall.ECONNRESET}
	}
	return p.PacketConn.ReadFrom(b)
}

// A transient read error under the handshake — a forged ICMP, as
// WSAECONNRESET on Windows or ECONNREFUSED on a connected Linux socket —
// used to fail Dial outright. It is now read past, and the peer's answer
// completes the handshake.
func TestHandshakeSurvivesATransientReadError(t *testing.T) {
	cli, srv := newLoopbackPair(t)
	l, err := NewListener(srv, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		if c, err := l.Accept(); err == nil {
			<-c.Done()
		}
	}()
	pc := &failFirstReadPC{PacketConn: cli}
	c, err := Dial(pc, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatalf("Dial = %v; one transient read error ended the handshake", err)
	}
	defer c.Close()
	if !pc.failed.Load() {
		t.Fatal("precondition: the injected read error was never returned")
	}
	if got := c.Stats().DiscardedPackets; got < 1 {
		t.Errorf("DiscardedPackets = %d; the read error was not counted", got)
	}
}

// connectedPC adapts a connected UDP socket to Dial, which writes with
// WriteTo — refused on a connected socket — so the kernel reports the ICMP
// port-unreachable a closed port answers with.
type connectedPC struct{ *net.UDPConn }

func (p connectedPC) WriteTo(b []byte, _ net.Addr) (int, error) { return p.Write(b) }

// Dial to a closed port still fails with the refusal, and well before the
// handshake timeout: refusals with nothing ever heard from the peer end
// the handshake once handshakeRefusalLimit of them arrived.
func TestDialToAClosedPortFailsWithTheRefusal(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ICMP-to-read-error reporting on connected UDP sockets is asserted on Linux")
	}
	closed, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	raddr := closed.LocalAddr().(*net.UDPAddr)
	_ = closed.Close()
	uc, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()

	start := time.Now()
	c, err := Dial(connectedPC{uc}, raddr, ClientTLSForTest(), DefaultParameters())
	took := time.Since(start)
	if c != nil {
		_ = c.Close()
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("Dial = %v, want connection refused", err)
	}
	// The first refusal is taken by a send, the second by the read or send
	// around the first probe at ~1 s; counting only reads waited to ~3 s.
	if took > 2*time.Second {
		t.Errorf("Dial took %v to report the refusal; the handshake timeout was nearly waited out", took)
	}
}

// The refusal limit applies only while nothing has been heard:
// handshakeRefusalLimit read errors straight away end the handshake with the socket's error.
func TestHandshakeRefusalLimitWithNothingHeard(t *testing.T) {
	refused := &net.OpError{Op: "read", Net: "udp", Err: syscall.ECONNREFUSED}
	script := make([]scriptedRead, handshakeRefusalLimit)
	for i := range script {
		script[i] = scriptedRead{err: refused}
	}
	pc := newScriptedPC(script...)
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	start := time.Now()
	c, err := Dial(pc, peer, ClientTLSForTest(), DefaultParameters())
	if c != nil {
		_ = c.Close()
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("Dial = %v, want the refusal", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("Dial took %v; the limit should end it at once", took)
	}
}

// A completed connection waiting for room in a full accept queue keeps its
// pending slot: with MaxPendingConns 1, one connection queued and one
// parked behind it, a third attempt is refused — the application has
// stopped accepting, and the listener pushes back rather than holding
// unbounded connections for it. One Accept moves the parked connection into
// the queue, frees the slot, and the next attempt is admitted.
func TestParkedConnectionHoldsItsPendingSlot(t *testing.T) {
	srv, _ := newLoopbackPair(t)
	l, err := NewListener(srv, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{MaxPendingConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	dial := func() (*Conn, error) {
		cli, _ := newLoopbackPair(t)
		c, err := Dial(cli, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
		if c != nil {
			t.Cleanup(func() { _ = c.Close() })
		}
		return c, err
	}
	// waitPending waits for the pending table to hold want connections,
	// all past their handshake.
	waitPending := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			// Collected first: a connection takes l.mu under its own mu
			// (identifier issuance), so the two are never held this way.
			l.mu.Lock()
			conns := make([]*Conn, 0, len(l.pendingConns))
			for c := range l.pendingConns {
				conns = append(conns, c)
			}
			l.mu.Unlock()
			n, done := len(conns), 0
			for _, c := range conns {
				c.mu.Lock()
				if c.handshakeConfirmed {
					done++
				}
				c.mu.Unlock()
			}
			if n == want && done == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("pending table: %d connections, %d completed; want %d completed", n, done, want)
			}
			runtime.Gosched()
		}
	}

	if _, err := dial(); err != nil {
		t.Fatalf("first Dial: %v", err)
	}
	waitPending(0) // queued
	if _, err := dial(); err != nil {
		t.Fatalf("second Dial: %v", err)
	}
	waitPending(1) // parked behind the full queue
	if _, err := dial(); !errors.Is(err, ErrQUIC) || !strings.Contains(err.Error(), "code 0x2") {
		t.Fatalf("third Dial = %v, want CONNECTION_REFUSED while the slot is held", err)
	}

	if _, err := l.Accept(); err != nil {
		t.Fatal(err)
	}
	waitPending(0) // the parked connection moved into the queue
	if _, err := dial(); err != nil {
		t.Fatalf("Dial after Accept freed the slot: %v", err)
	}
}

// An event kind this package has no case for — what a newer crypto/tls
// adding one looks like — is counted, not fatal, and the count is
// readable from another goroutine while the connection bumps it.
func TestUnknownTLSEventIsCounted(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	c := newConn(newScriptedPC(), peer, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	const n = 100
	read := make(chan struct{})
	go func() {
		defer close(read)
		for c.Stats().UnhandledTLSEvents < n {
			runtime.Gosched()
		}
	}()
	for range n {
		done, err := c.handleTLSEvent(tls.QUICEvent{Kind: tls.QUICEventKind(250)})
		if done || err != nil {
			t.Fatalf("handleTLSEvent = %v, %v; want the unknown kind passed over", done, err)
		}
	}
	<-read
	if got := c.Stats().UnhandledTLSEvents; got != n {
		t.Errorf("UnhandledTLSEvents = %d, want %d", got, n)
	}
}
