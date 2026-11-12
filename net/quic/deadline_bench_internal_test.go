package quic

import (
	"strconv"
	"testing"
	"time"
)

// deadlineConn is a connection with n open streams and no timer loop: the
// benchmark plays the loop itself, so what it measures is the bookkeeping,
// not the scheduler.
func deadlineConn(tb testing.TB, n int) (*Conn, []*Stream) {
	tb.Helper()
	c := newConn(&fuzzSink{addr: "deadline:self"}, fakeFuzzAddr("deadline:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
	tb.Cleanup(func() { _ = c.Close() })
	ss := make([]*Stream, n)
	c.mu.Lock()
	for i := range n {
		id := StreamID(4 * i)
		ss[i] = newStream(c, id)
		c.streams[id] = ss[i]
	}
	c.mu.Unlock()
	return c, ss
}

// The HTTP/3 server sets a fresh write deadline on a stream before each
// write (writeBound.write), so n streams that a peer starves of credit carry
// n deadlines, staggered, that mature one after another. One op is that
// whole round — n SetWriteDeadline calls, then the timer loop's n wake-ups,
// each at the next deadline — and ns/stream is its cost divided by n: flat
// when the bookkeeping is per stream, growing with n when each maturity
// walks every stream.
func BenchmarkStreamDeadlinesMature(b *testing.B) {
	for _, n := range []int{100, 1024, 8192} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c, ss := deadlineConn(b, n)
			base := time.Now().Add(time.Hour)
			for b.Loop() {
				for i, s := range ss {
					_ = s.SetWriteDeadline(base.Add(time.Duration(i+1) * time.Microsecond))
				}
				c.mu.Lock()
				for i := range ss {
					now := base.Add(time.Duration(i+1) * time.Microsecond)
					if !c.streamDeadline.IsZero() && !now.Before(c.streamDeadline) {
						c.streamDeadline = c.expireStreamDeadlinesLocked(now)
					}
				}
				c.mu.Unlock()
				for _, s := range ss {
					_ = s.SetWriteDeadline(time.Time{})
				}
				c.mu.Lock()
				c.streamDeadline = time.Time{}
				c.mu.Unlock()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/stream")
		})
	}
}
