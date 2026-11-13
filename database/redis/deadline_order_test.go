package redis

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// heldDeadlineConn is a net.Conn whose replies are scripted and whose
// SetDeadline can be held: the one call that sets the deadline to the past
// — the function Pipeline hands to context.AfterFunc — waits at the door
// until the test lets it in, which is the schedule a loaded machine can
// produce.
type heldDeadlineConn struct {
	mu       sync.Mutex
	deadline time.Time
	replies  chan []byte
	pending  []byte

	entered chan struct{} // closed when the late SetDeadline has been called
	release chan struct{} // closed to let it through
	done    chan struct{} // closed when it has returned
	onWrite func()
	onRead  func()
}

func (c *heldDeadlineConn) Read(p []byte) (int, error) {
	if c.onRead != nil {
		c.onRead()
	}
	c.mu.Lock()
	dl := c.deadline
	c.mu.Unlock()
	if !dl.IsZero() && !time.Now().Before(dl) {
		return 0, os.ErrDeadlineExceeded
	}
	if len(c.pending) == 0 {
		c.pending = <-c.replies
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *heldDeadlineConn) Write(p []byte) (int, error) {
	if c.onWrite != nil {
		c.onWrite()
	}
	return len(p), nil
}

func (c *heldDeadlineConn) SetDeadline(t time.Time) error {
	late := !t.IsZero() && t.Before(time.Unix(2, 0))
	if late {
		close(c.entered)
		<-c.release
	}
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	if late {
		close(c.done)
	}
	return nil
}

func (c *heldDeadlineConn) Close() error                     { return nil }
func (c *heldDeadlineConn) LocalAddr() net.Addr              { return nil }
func (c *heldDeadlineConn) RemoteAddr() net.Addr             { return nil }
func (c *heldDeadlineConn) SetReadDeadline(time.Time) error  { return nil }
func (c *heldDeadlineConn) SetWriteDeadline(time.Time) error { return nil }

// Pipeline arms context.AfterFunc(ctx, SetDeadline(past)) and stops it on
// return. A context that ends as the call completes may have started that
// function already, and the stop does not wait for it by itself: its
// deadline in the past then landed on the next call, whose context is
// alive, which failed and broke the connection. The stop waits for a
// started function now. The connection holds that late SetDeadline until
// call B has written, or for two seconds: call A, which waits for it,
// returns only once it is through.
func TestADeadlineOfAFinishedCallDoesNotLandOnTheNext(t *testing.T) {
	nc := &heldDeadlineConn{
		replies: make(chan []byte, 4),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	c := &Conn{nc: nc, max: DefaultMaxReplyBytes}
	c.r.br = bufio.NewReader(nc)

	var once sync.Once
	let := func() { once.Do(func() { close(nc.release) }) }
	time.AfterFunc(2*time.Second, let)
	// Call A: its context ends while its reply is on the way; the reply is
	// read whole, the call succeeds.
	ctxA, cancelA := context.WithCancel(t.Context())
	nc.replies <- []byte("+OK\r\n")
	nc.onRead = func() {
		nc.onRead = nil
		cancelA()
		<-nc.entered // AfterFunc's function has started and stands at SetDeadline
	}
	if _, err := c.Do(ctxA, "SET", "k", "v"); err != nil {
		t.Fatalf("call A: %v", err)
	}

	// Call B, under a live context. The held SetDeadline is let through
	// once B has written, if it is still held.
	nc.onWrite = func() {
		let()
		<-nc.done
	}
	nc.replies <- []byte("+OK\r\n")
	_, err := c.Do(t.Context(), "SET", "k", "v")
	if err != nil {
		t.Errorf("call B, whose context is alive, failed on the deadline call A left behind: %v (deadline exceeded: %v, connection broken: %v)",
			err, errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, ErrConnBroken))
	}
}
