package quic

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// deadlineAfterCloseSink is a socket the caller closes right after the
// connection: once the connection is closed, arming a future read deadline
// fails, as it does on a closed socket. An expired deadline still wakes the
// read loop, which is how Close reaches it.
type deadlineAfterCloseSink struct {
	fuzzSink
	wake   chan struct{}
	closed <-chan struct{}
}

func (s *deadlineAfterCloseSink) ReadFrom([]byte) (int, net.Addr, error) {
	<-s.wake
	return 0, nil, os.ErrDeadlineExceeded
}

func (s *deadlineAfterCloseSink) SetReadDeadline(t time.Time) error {
	if !t.After(time.Now()) {
		select {
		case s.wake <- struct{}{}:
		default:
		}
		return nil
	}
	select {
	case <-s.closed:
		return errors.New("use of closed network connection")
	default:
		return nil
	}
}

// Err is nil after this end closed the connection. The read loop, woken by
// Close, armed its next deadline before looking at the close: on a socket
// the caller had closed meanwhile, the error went to fail and Err reported
// it, after a close that was this end's own.
func TestErrStaysNilWhenTheSocketClosesAfterClose(t *testing.T) {
	sink := &deadlineAfterCloseSink{fuzzSink: fuzzSink{addr: "close:self"}, wake: make(chan struct{}, 1)}
	c := newConn(sink, fakeFuzzAddr("close:peer"), []byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	sink.closed = c.closed
	go c.readLoop()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	limit := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(limit) {
		if err := c.Err(); err != nil {
			t.Fatalf("Err after a local Close = %v, want nil", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
