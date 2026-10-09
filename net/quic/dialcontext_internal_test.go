package quic

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

// signalWritePC announces its first send, so a test can act once a
// handshake is underway rather than after a guessed sleep.
type signalWritePC struct {
	net.PacketConn
	once    sync.Once
	written chan struct{}
}

func (p *signalWritePC) WriteTo(b []byte, to net.Addr) (int, error) {
	p.once.Do(func() { close(p.written) })
	return p.PacketConn.WriteTo(b, to)
}

// A cancelled context ends a handshake that would otherwise wait out its
// whole timeout against a peer that never answers, and says why.
func TestDialContextCancelEndsTheHandshake(t *testing.T) {
	cli, silent := newLoopbackPair(t) // silent is bound and never read
	pc := &signalWritePC{PacketConn: cli, written: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		c   *Conn
		err error
	}
	got := make(chan result, 1)
	go func() {
		c, err := DialContext(ctx, pc, silent.LocalAddr(), ClientTLSForTest(), DefaultParameters())
		got <- result{c, err}
	}()
	<-pc.written // the first Initial has left: the handshake is underway
	cancel()
	select {
	case r := <-got:
		if r.c != nil {
			_ = r.c.Close()
			t.Fatal("DialContext returned a connection after its context was cancelled")
		}
		if !errors.Is(r.err, context.Canceled) || !errors.Is(r.err, ErrQUIC) {
			t.Fatalf("DialContext = %v, want an ErrQUIC wrapping context.Canceled", r.err)
		}
	case <-time.After(DefaultHandshakeTimeout / 2):
		t.Fatal("DialContext ignored the cancellation and kept handshaking")
	}
}

// A context deadline replaces the default handshake timeout.
func TestDialContextDeadlineReplacesTheDefault(t *testing.T) {
	cli, silent := newLoopbackPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	c, err := DialContext(ctx, cli, silent.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if c != nil {
		_ = c.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DialContext = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > DefaultHandshakeTimeout/2 {
		t.Errorf("DialContext took %v under a 100ms deadline", took)
	}
}

// An already-cancelled context sends nothing at all.
func TestDialContextAlreadyCancelled(t *testing.T) {
	cli, silent := newLoopbackPair(t)
	pc := &signalWritePC{PacketConn: cli, written: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, err := DialContext(ctx, pc, silent.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if c != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("DialContext = %v, %v; want no connection and context.Canceled", c, err)
	}
	select {
	case <-pc.written:
		t.Error("a datagram left under an already-cancelled context")
	default:
	}
}

// The context governs the handshake only: cancelling it once DialContext
// has returned leaves the connection alone.
func TestDialContextCancelAfterReturnKeepsTheConnection(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	c, err := DialContext(ctx, cli, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer c.Close()
	cancel()

	if _, err := c.OpenStream(); err != nil {
		t.Fatalf("OpenStream after cancel: %v", err)
	}
	if isClosed(c) {
		t.Fatalf("cancelling the dial context closed the established connection: %v", c.Err())
	}
}

// failingWritePC refuses every send, the way a socket whose route went
// away does.
type failingWritePC struct{ fuzzSink }

func (p *failingWritePC) WriteTo([]byte, net.Addr) (int, error) {
	return 0, &net.OpError{Op: "write", Net: "udp", Err: syscall.ENETUNREACH}
}

// Sends are best effort and their errors stop at the send path; the
// counter is what still reports them.
func TestWriteFailuresCountsFailedSends(t *testing.T) {
	c := newConn(&failingWritePC{fuzzSink{addr: "wf:self"}}, fakeFuzzAddr("wf:peer"),
		[]byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats().WriteFailures; got != 0 {
		t.Fatalf("WriteFailures = %d before any send", got)
	}
	c.mu.Lock()
	for range 3 {
		_ = c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{})
	}
	c.mu.Unlock()
	if got := c.Stats().WriteFailures; got != 3 {
		t.Errorf("WriteFailures = %d after three refused sends, want 3", got)
	}
}
