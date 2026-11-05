package quic

import (
	"strconv"
	"testing"
	"time"
)

// blockedConn is a connection with b streams parked at their flow-control
// limit, each STREAM_DATA_BLOCKED already waiting in the control queue —
// the shut window that keeps them there — which is the tick's worst case.
func blockedConn(tb testing.TB, b int) *Conn {
	tb.Helper()
	c := newConn(&fuzzSink{addr: "bench:self"}, fakeFuzzAddr("bench:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
	tb.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range b {
		id := uint64(4 * i)
		s := newStream(c, id)
		s.sendQ = sendQuota{max: 0, blockedSent: true}
		c.streams[id] = s
		c.control = appendFrame(c.control, frameStreamDataBlocked, id, 0)
	}
	return c
}

// One re-announcement tick with b streams blocked and their frames still
// queued: it must queue nothing more, at a cost that grows with b, not b².
func BenchmarkResendBlocked(b *testing.B) {
	for _, n := range []int{16, 256, 4096} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c := blockedConn(b, n)
			c.mu.Lock()
			defer c.mu.Unlock()
			queued := len(c.control)
			now := time.Now()
			b.ResetTimer()
			for b.Loop() {
				c.resendBlockedFramesLocked(now)
			}
			if len(c.control) != queued {
				b.Fatalf("the control queue grew from %d to %d bytes", queued, len(c.control))
			}
		})
	}
}

// A tick queues one STREAM_DATA_BLOCKED per blocked stream whose frame is
// not already waiting, and none for one that is.
func TestResendBlockedQueuesOnePerLimit(t *testing.T) {
	c := blockedConn(t, 3)
	c.mu.Lock()
	defer c.mu.Unlock()
	queued := len(c.control)
	c.resendBlockedFramesLocked(time.Now())
	if len(c.control) != queued {
		t.Fatalf("a tick with every frame waiting grew the queue from %d to %d", queued, len(c.control))
	}
	c.control = appendFrame(nil, frameStreamDataBlocked, 0, 0)
	c.resendBlockedFramesLocked(time.Now())
	want := append(appendFrame(nil, frameStreamDataBlocked, 0, 0), appendFrame(nil, frameStreamDataBlocked, 4, 0)...)
	want = appendFrame(want, frameStreamDataBlocked, 8, 0)
	if len(c.control) != len(want) {
		t.Errorf("after a tick with one frame waiting: % x, want the two missing added", c.control)
	}
}
