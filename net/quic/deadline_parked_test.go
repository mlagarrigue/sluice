package quic

import (
	"errors"
	"os"
	"testing"
	"time"
)

// A write deadline set while a Write is parked ends that Write with
// os.ErrDeadlineExceeded, as SetWriteDeadline's godoc promises for both
// waits — flow-control credit and the congestion window. The window's wait
// read a copy of the deadline taken when the frame was built, so the writer
// SetWriteDeadline woke went back to sleep on the stale value. The deadline
// is set in the past, so no timer is needed: SetWriteDeadline's own
// broadcast, and the repeated ones below (what the timer loop would do),
// give the writer every chance to re-read it. The flow-control case is the
// control that keeps the test from proving nothing.
func TestADeadlineSetWhileParkedIsSeen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window bool // park on the congestion window, else on stream credit
	}{
		{"flow-credit-control", false},
		{"congestion-window", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := &recordingPC{addr: fakeFuzzAddr("contra06:self")}
			peerPC := &recordingPC{addr: fakeFuzzAddr("contra06:peer")}
			c, _ := newKeyedConnOn(t, pc, peerPC)

			const sid = 1
			c.mu.Lock()
			c.connSend.grant(1 << 20)
			s := newStream(c, sid)
			c.streams[sid] = s
			if tc.window {
				c.cc = &shutController{congestionController: c.cc}
				s.sendQ.grant(1 << 20)
			} // else: no stream credit at all, the writer parks on it
			c.mu.Unlock()

			type result struct {
				n   int
				err error
			}
			done := make(chan result, 1)
			go func() {
				n, err := s.Write(make([]byte, 4096))
				done <- result{n, err}
			}()

			// Wait until parked: the window case charges credit before
			// asking the window; the credit case sends DATA/STREAM_DATA
			// BLOCKED before waiting.
			parked := false
			for range 20_000 {
				c.mu.Lock()
				var ok bool
				if tc.window {
					ok = c.connSend.sent > 0
				} else {
					ok = s.sendQ.blockedSent
				}
				c.mu.Unlock()
				if ok {
					parked = true
					break
				}
				select {
				case r := <-done:
					t.Fatalf("the write returned (%d, %v) without parking", r.n, r.err)
				default:
				}
				time.Sleep(100 * time.Microsecond)
			}
			if !parked {
				t.Fatal("the writer never parked; the sequence under test was not reached")
			}
			time.Sleep(20 * time.Millisecond) // let it reach cond.Wait

			if err := s.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			guard := time.After(10 * time.Second)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case r := <-done:
					if !errors.Is(r.err, os.ErrDeadlineExceeded) {
						t.Fatalf("Write = %d, %v; want os.ErrDeadlineExceeded", r.n, r.err)
					}
					return
				case <-tick.C:
					c.mu.Lock()
					c.cond.Broadcast() // what the timer loop does when a deadline matures
					c.mu.Unlock()
				case <-guard:
					t.Fatal("a Write parked when the write deadline was moved into the past never returned, despite repeated wakes")
				}
			}
		})
	}
}
