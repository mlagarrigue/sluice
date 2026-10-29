package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"runtime"
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
	c, err := Dial(context.Background(), pc, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
	c, err := Dial(context.Background(), connectedPC{uc}, raddr, ClientTLSForTest(), DefaultParameters())
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
	c, err := Dial(context.Background(), pc, peer, ClientTLSForTest(), DefaultParameters())
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
		c, err := Dial(context.Background(), cli, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
	if _, err := dial(); !isRefusedClose(err) {
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

// A handshake whose context is cancelled ends at once with the context's
// cause, not with the socket's timeout: the cancellation sets the read
// deadline to now, and the deadline that wakes the read is the context's.
func TestDialCancelledEndsWithTheContextsCause(t *testing.T) {
	cause := errors.New("the caller gave up")
	ctx, cancel := context.WithCancelCause(context.Background())
	pc := newScriptedPC() // the peer never answers
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	time.AfterFunc(50*time.Millisecond, func() { cancel(cause) })
	done := make(chan error, 1)
	go func() {
		c, err := Dial(ctx, pc, peer, ClientTLSForTest(), DefaultParameters())
		if c != nil {
			_ = c.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, cause) {
			t.Errorf("Dial = %v, want the context's cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dial still running 5 s after its context was cancelled")
	}
}

// refusingWritesPC is a scriptedPC whose every send is refused the way a
// closed port's ICMP surfaces on a connected socket's next send.
type refusingWritesPC struct{ *scriptedPC }

func (p refusingWritesPC) WriteTo(b []byte, addr net.Addr) (int, error) {
	_, _ = p.scriptedPC.WriteTo(b, addr)
	return 0, &net.OpError{Op: "write", Net: "udp", Err: platformRefusal}
}

// Send-side refusals count toward handshakeRefusalLimit as read-side ones
// do: a socket that refuses every send, with nothing ever read, ends Dial
// with that refusal — not with a timeout, and not with no error at all.
// Each refusal wakes the read so the count is re-taken at once; the wake is
// not the deadline, and is read past while the context is alive.
func TestSendRefusalsEndTheHandshakeWithTheRefusal(t *testing.T) {
	if !isRefusal(platformRefusal) {
		t.Skip("no send refusal is recognised on this platform")
	}
	pc := refusingWritesPC{newScriptedPC()}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	done := make(chan error, 1)
	go func() {
		c, err := Dial(context.Background(), pc, peer, ClientTLSForTest(), DefaultParameters())
		if c != nil {
			_ = c.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, platformRefusal) {
			t.Errorf("Dial = %v, want the send refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dial still running 5 s into a socket that refuses every send")
	}
}

// heardThenRefusedPC delivers one datagram, then refuses every read, as a
// socket does once the peer that answered went away.
type heardThenRefusedPC struct {
	*scriptedPC
	reads atomic.Int64
}

func (p *heardThenRefusedPC) ReadFrom(b []byte) (int, net.Addr, error) {
	if p.reads.Add(1) == 1 {
		return copy(b, []byte{0x40, 1, 2, 3}), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}, nil
	}
	return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: syscall.ECONNREFUSED}
}

// Once something was heard, refusals no longer end the handshake (only its
// deadline does), and consecutive ones are read past with a backoff: a
// socket that refuses every read must not spin the handshake's goroutine.
// Over a 300 ms handshake the backoff allows a handful of reads where a
// loop without it makes hundreds of thousands.
func TestRefusedReadsAfterADatagramAreBackedOff(t *testing.T) {
	pc := &heardThenRefusedPC{scriptedPC: newScriptedPC()}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c, err := Dial(ctx, pc, peer, ClientTLSForTest(), DefaultParameters())
	if c != nil {
		_ = c.Close()
	}
	if err == nil {
		t.Fatal("Dial succeeded against a peer that never answered")
	}
	if n := pc.reads.Load(); n > 1000 {
		t.Errorf("%d reads in a 300 ms handshake of refused reads: the backoff did not pace them", n)
	}
}

// refuseFirstWritePC refuses its first send only.
type refuseFirstWritePC struct {
	*scriptedPC
	refused atomic.Bool
}

func (p *refuseFirstWritePC) WriteTo(b []byte, addr net.Addr) (int, error) {
	_, _ = p.scriptedPC.WriteTo(b, addr)
	if p.refused.CompareAndSwap(false, true) {
		return 0, &net.OpError{Op: "write", Net: "udp", Err: platformRefusal}
	}
	return len(b), nil
}

// The wake a single send refusal gives the read is not the deadline: with
// the context alive and the deadline ahead, the read is taken again, and
// the handshake ends at its context's deadline, not at the wake.
func TestASendRefusalWakeIsNotTheDeadline(t *testing.T) {
	if !isRefusal(platformRefusal) {
		t.Skip("no send refusal is recognised on this platform")
	}
	pc := &refuseFirstWritePC{scriptedPC: newScriptedPC()}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	c, err := Dial(ctx, pc, peer, ClientTLSForTest(), DefaultParameters())
	if c != nil {
		_ = c.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Dial = %v after %v, want the context's deadline", err, time.Since(start))
	}
	if !pc.refused.Load() {
		t.Fatal("precondition: no send was refused")
	}
}
