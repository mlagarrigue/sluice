package quic

import (
	"testing"
	"time"
)

// SetWriteTimeout takes no lock, and each write then starts a deadline of
// its own, d from when it took mu, armed in the deadline heap as one
// SetWriteDeadline sets — the heap is what wakes a write parked past it.
// Turned off, the last deadline stays as it was.
func TestWriteTimeoutStartsWithEachWrite(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("wt:self")}
	peerPC := &recordingPC{addr: fakeFuzzAddr("wt:peer")}
	c, _ := newKeyedConnOn(t, pc, peerPC)
	const sid, d = 1, time.Hour
	c.mu.Lock()
	c.connSend.grant(1 << 20)
	s := newStream(c, sid)
	s.sendQ.grant(1 << 20)
	c.streams[sid] = s
	set := make(chan struct{})
	go func() {
		s.SetWriteTimeout(d) // with mu held elsewhere
		close(set)
	}()
	select {
	case <-set:
	case <-time.After(5 * time.Second):
		t.Fatal("SetWriteTimeout waited for the connection's lock")
	}
	c.mu.Unlock()

	for range 2 {
		before := time.Now()
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		c.mu.Lock()
		dl, armed := s.writeDeadline, s.dlIdx[1] != 0
		c.mu.Unlock()
		if dl.Before(before.Add(d)) || dl.After(after.Add(d)) || !armed {
			t.Fatalf("a write between %v and %v left the deadline at %v (armed %v), want %v after its start",
				before, after, dl, armed, d)
		}
	}

	s.SetWriteTimeout(0)
	c.mu.Lock()
	last := s.writeDeadline
	c.mu.Unlock()
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !s.writeDeadline.Equal(last) {
		t.Errorf("with the timeout off, a write moved the deadline from %v to %v", last, s.writeDeadline)
	}
}
