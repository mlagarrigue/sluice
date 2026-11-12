package quic

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// An acknowledgement with n writers parked for their stream's credit: a peer
// that grants a stream nothing and acknowledges what the connection sends.
// The ACK can release a writer on the congestion window, never one waiting
// for MAX_STREAM_DATA; ns/op flat in n says it leaves those asleep, growing
// with n that it wakes them to take mu and park again.
func BenchmarkAckWithWritersParkedOnStreamCredit(b *testing.B) {
	for _, n := range []int{0, 64, 1024} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c, ss := deadlineConn(b, n)
			c.mu.Lock()
			c.connSend.grant(1 << 30)
			for _, s := range ss {
				s.sendQ = sendQuota{max: 0}
			}
			c.mu.Unlock()
			for _, s := range ss {
				go func() { _, _ = s.Write([]byte{1}) }()
			}
			time.Sleep(100 * time.Millisecond) // every writer out of credit, parked
			if got := parkedWriters(); got != n {
				b.Fatalf("%d writers parked, want %d", got, n)
			}
			sp := &c.spaces[spaceInitial]
			var pn uint64
			for b.Loop() {
				now := time.Now()
				c.mu.Lock()
				sp.sent.record(sentPacket{pn: pn, sentAt: now.Add(-time.Millisecond), size: 1200, ackEliciting: true})
				c.cc.onSent(now, 1200)
				sp.nextPN = pn + 1
				c.mu.Unlock()
				var peer ackTracker
				peer.record(pn, now)
				raw := peer.appendAck(nil, now)
				frames, err := parseFrames(nil, raw)
				if err != nil {
					b.Fatal(err)
				}
				if err := c.handleAck(spaceInitial, frames[0], now); err != nil {
					b.Fatal(err)
				}
				pn++
			}
		})
	}
}

// parkedWriters counts the goroutines waiting on a sync.Cond inside
// Stream.Write.
func parkedWriters() int {
	buf := make([]byte, 64<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "sync.(*Cond).Wait") && strings.Contains(g, "quic.(*Stream).Write(") {
			n++
		}
	}
	return n
}
