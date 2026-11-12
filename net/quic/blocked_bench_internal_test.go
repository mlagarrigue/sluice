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
		id := StreamID(4 * i)
		s := newStream(c, id)
		s.sendQ = sendQuota{max: 0, blockedSent: true}
		c.streams[id] = s
		c.control = appendStreamFrame(c.control, frameStreamDataBlocked, id, 0)
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

// retiredCopyConn is a server connection that retired the peer's sequence
// 1 (a Retire Prior To of 2), with its RETIRE_CONNECTION_ID still queued
// behind b STREAM_DATA_BLOCKED frames — a shut window holds them all — and
// returns one copy of that sequence's NEW_CONNECTION_ID.
func retiredCopyConn(tb testing.TB, b int) (*Conn, []byte) {
	tb.Helper()
	c := newConn(&fuzzSink{addr: "ncidbench:self"}, fakeFuzzAddr("ncidbench:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultConfig())
	tb.Cleanup(func() { _ = c.Close() })
	c.pinPeerSCID([]byte("srv-cid0"))
	c.mu.Lock()
	for i := range b {
		c.control = appendFrame(c.control, frameStreamDataBlocked, uint64(4*i), 0)
	}
	c.mu.Unlock()
	tok := make([]byte, 16)
	one := ncidFrame(1, 0, []byte("srv-cid1"), tok)
	if err := c.frames(spaceApplication, one, time.Now()); err != nil {
		tb.Fatal(err)
	}
	if err := c.frames(spaceApplication, ncidFrame(2, 2, []byte("srv-cid2"), tok), time.Now()); err != nil {
		tb.Fatal(err)
	}
	return c, one
}

// retiresQueued counts the RETIRE_CONNECTION_ID frames in the control
// queue. Callers hold mu.
func retiresQueued(tb testing.TB, c *Conn) int {
	tb.Helper()
	n := 0
	for q := c.control; len(q) > 0; {
		f, rest, err := parseFrame(q)
		if err != nil {
			tb.Fatal(err)
		}
		if f.Type == frameRetireConnectionID {
			n++
		}
		q = rest
	}
	return n
}

// Copies of the NEW_CONNECTION_ID of a retired sequence queue no second
// RETIRE_CONNECTION_ID while the first waits, whatever waits in front of
// it, and the check reads the retirements waiting, not the whole queue.
func TestACopyOfARetiredNewConnectionIDQueuesNoSecondRetirement(t *testing.T) {
	for _, b := range []int{0, 300} {
		t.Run(strconv.Itoa(b)+" blocked frames in front", func(t *testing.T) {
			c, one := retiredCopyConn(t, b)
			c.mu.Lock()
			before := retiresQueued(t, c)
			c.mu.Unlock()
			if before == 0 {
				t.Fatal("setup: no retirement waiting")
			}
			for range 5 {
				if err := c.frames(spaceApplication, one, time.Now()); err != nil {
					t.Fatalf("a copy ended the connection: %v", err)
				}
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			if n := retiresQueued(t, c); n != before || len(c.controlRetires) != before {
				t.Errorf("%d RETIRE_CONNECTION_ID queued after five copies (%d in the books), %d before", n, len(c.controlRetires), before)
			}
		})
	}
}

// One copy of a retired sequence's NEW_CONNECTION_ID with b blocked frames
// in the control queue: the cost must not grow with b. Every copy parsed
// the whole queue, the blocked frames of every stream at its limit
// included.
func BenchmarkRetiredNewConnectionIDCopy(b *testing.B) {
	for _, n := range []int{16, 256, 4096} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c, one := retiredCopyConn(b, n)
			c.mu.Lock()
			queued := len(c.control)
			c.mu.Unlock()
			now := time.Now()
			b.ResetTimer()
			for b.Loop() {
				if err := c.frames(spaceApplication, one, now); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			c.mu.Lock()
			defer c.mu.Unlock()
			if len(c.control) != queued {
				b.Fatalf("the control queue grew from %d to %d bytes", queued, len(c.control))
			}
		})
	}
}
