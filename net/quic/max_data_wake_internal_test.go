package quic

import (
	"testing"
	"testing/synctest"
)

// creditWakes parks writers where a writer out of the connection's credit
// alone parks (connCredit), counts their wake-ups,
// and sends the MAX_DATA values given; it returns the wake-ups counted.
// spent starts the connection window used up, as a writer that parked for
// credit leaves it.
func creditWakes(t *testing.T, writers int, spent bool, raise func(base uint64, i int) uint64, frames int) int {
	t.Helper()
	var got int
	synctest.Test(t, func(t *testing.T) {
		c := assembleConn(&fuzzSink{addr: "wake:self"}, fakeFuzzAddr("wake:peer"),
			[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
		wakes, stop := 0, false
		done := make(chan struct{}, writers)
		for range writers {
			go func() {
				c.mu.Lock()
				for !stop {
					c.connCredit.Wait()
					if !stop {
						wakes++
					}
				}
				c.mu.Unlock()
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		c.mu.Lock()
		if spent {
			c.connSend.sent = c.connSend.max
		} else {
			c.connSend.max = c.connSend.sent + 1<<20 // room
		}
		base := c.connSend.max
		c.mu.Unlock()
		for i := range frames {
			c.handleLimitGrant(Frame{Type: frameMaxData, value: raise(base, i)})
			synctest.Wait()
		}
		c.mu.Lock()
		got = wakes
		stop = true
		c.wakeAllLocked()
		c.mu.Unlock()
		for range writers {
			<-done
		}
	})
	return got
}

// MAX_DATA frames that raise nothing wake no one.
func TestAMaxDataThatRaisesNothingWakesNoOne(t *testing.T) {
	if got := creditWakes(t, 4, false, func(base uint64, _ int) uint64 { return base }, 8); got != 0 {
		t.Fatalf("eight MAX_DATA at the current limit woke writers %d times", got)
	}
}

// A writer waits on the connection's credit only once it is spent. Every
// MAX_DATA that raised the limit woke every parked writer, so eight one-byte
// raises against a window with room woke four writers eight times each, for
// three bytes a frame; they wake no one now.
func TestAMaxDataAgainstAWindowWithRoomWakesNoOne(t *testing.T) {
	const writers, frames = 4, 8
	got := creditWakes(t, writers, false, func(base uint64, i int) uint64 { return base + uint64(i) + 1 }, frames)
	if got != 0 {
		t.Fatalf("%d one-byte MAX_DATA raises against a window with room woke %d parked writers %d times, want none",
			frames, writers, got)
	}
}

// The other side of the rule: against a spent window, the first raise wakes
// one parked writer — the one that takes the credit, and wakes the next if
// it leaves some (passCreditLocked) — and the raises after it, the window no
// longer spent, wake no one. It woke every parked writer, all but one to
// park again.
func TestAMaxDataAgainstASpentWindowWakesOneWriter(t *testing.T) {
	const writers, frames = 4, 8
	got := creditWakes(t, writers, true, func(base uint64, i int) uint64 { return base + uint64(i) + 1 }, frames)
	if got != 1 {
		t.Fatalf("%d one-byte MAX_DATA raises against a spent window woke %d parked writers %d times, want once", frames, writers, got)
	}
}
