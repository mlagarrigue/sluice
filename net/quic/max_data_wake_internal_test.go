package quic

import (
	"testing"
	"testing/synctest"
	"time"
)

// creditWakes parks writers where a writer out of the connection's credit
// alone parks (waitForConnCreditLocked), counts their wake-ups,
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
		for i := range writers {
			s := newStream(c, StreamID(4*i))
			go func() {
				c.mu.Lock()
				for !stop {
					_ = c.waitForConnCreditLocked(s, time.Time{})
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

// A write deadline that matures, or a STOP_SENDING, on one stream ends that
// stream's writer and no other. The writers parked on the connection's
// credit shared one cond, which both broadcast: a thousand of them woke for
// each, 20 µs a deadline (BenchmarkStreamDeadlineBesideParkedWriters). Each
// waits on its own stream's cond now, which its stream's events reach; the
// others sleep through them.
func TestAnotherStreamsEndWakesNoCreditWriter(t *testing.T) {
	const writers = 8
	synctest.Test(t, func(t *testing.T) {
		c := assembleConn(&fuzzSink{addr: "wake:self"}, fakeFuzzAddr("wake:peer"),
			[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
		c.mu.Lock()
		c.connSend.sent = c.connSend.max // spent: the writers wait for a MAX_DATA
		other := newStream(c, StreamID(4*writers))
		c.streams[other.id] = other
		c.mu.Unlock()
		wakes, stop := 0, false
		done := make(chan struct{}, writers)
		for i := range writers {
			s := newStream(c, StreamID(4*i))
			go func() {
				c.mu.Lock()
				for !stop {
					_ = c.waitForConnCreditLocked(s, time.Time{})
					if !stop {
						wakes++
					}
				}
				c.mu.Unlock()
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		at := time.Now().Add(time.Second)
		_ = other.SetWriteDeadline(at)
		c.mu.Lock()
		c.streamDeadline = c.expireStreamDeadlinesLocked(at)
		c.mu.Unlock()
		synctest.Wait()
		if _, err := c.handleStopSending(Frame{Type: FrameStopSending, StreamID: other.id, value: 7}, spaceApplication); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		c.mu.Lock()
		got := wakes
		stop = true
		c.wakeAllLocked()
		c.mu.Unlock()
		for range writers {
			<-done
		}
		if got != 0 {
			t.Errorf("a deadline and a STOP_SENDING on another stream woke %d writers parked on the connection's credit %d times, want none", writers, got)
		}
	})
}
