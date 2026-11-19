package nats

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// pastDeadlineConn holds the one SetDeadline that sets a deadline in the
// past — what context.AfterFunc runs when a call's context ends — until
// release is closed, and records the deadline last set.
type pastDeadlineConn struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
	entered  chan struct{}
	release  chan struct{}
}

func (c *pastDeadlineConn) SetDeadline(t time.Time) error {
	if !t.IsZero() && t.Before(time.Unix(2, 0)) {
		close(c.entered)
		<-c.release
	}
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return nil
}

// The stop that bound returns waits for the AfterFunc function that began
// as the call ended (79d7314): its past deadline lands before the next call
// sets its own. Landing after, it failed that call with an i/o timeout on a
// live context and broke the connection. database/redis holds the same
// promise with TestADeadlineOfAFinishedCallDoesNotLandOnTheNext.
func TestBoundsStopWaitsForADeadlineAlreadyStarted(t *testing.T) {
	nc := &pastDeadlineConn{entered: make(chan struct{}), release: make(chan struct{})}
	c := &Conn{nc: nc}
	ctx, cancel := context.WithCancel(t.Context())
	stop := c.bound(ctx)
	cancel()
	<-nc.entered // the function has started and stands at SetDeadline
	// Released later: a stop that does not wait returns before it.
	time.AfterFunc(200*time.Millisecond, func() { close(nc.release) })
	stop()
	nc.mu.Lock()
	landed := !nc.deadline.IsZero() && nc.deadline.Before(time.Unix(2, 0))
	nc.mu.Unlock()
	if !landed {
		t.Error("stop returned before the deadline of the ended call had landed: the next call's deadline would be overwritten by it")
	}
}
