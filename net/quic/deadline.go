package quic

import (
	"container/heap"
	"time"
)

// deadlineEntry is one armed stream deadline: a stream's read deadline, or
// its write deadline.
type deadlineEntry struct {
	at    time.Time
	s     *Stream
	write bool
}

// deadlineHeap orders the armed stream deadlines, earliest first, so that the
// timer loop reaches the ones that matured without walking every stream.
//
// It replaced a pass over the whole stream table per matured deadline. The
// HTTP/3 server sets a fresh write deadline before each write
// (writeBound.write), so n streams a peer starves of credit carry n
// deadlines that mature one after another: n passes of n stream locks, under
// mu, with n up to 65,536. BenchmarkStreamDeadlinesMature measured 1.6 µs,
// 15.9 µs and 147 µs per stream at 100, 1,024 and 8,192 streams — the n² —
// and 0.23 µs, 0.31 µs and 0.36 µs with this heap, a log n per set and per
// maturity (docs/benchmarks.md, "Stream deadlines that mature one by one").
//
// Each stream holds its two places in the heap (Stream.dlIdx), so moving a
// deadline is a heap.Fix and clearing one or closing the stream a
// heap.Remove: no stale entry outlives what it stood for, whatever the rate
// at which a peer opens and closes streams. Under Conn.mu.
type deadlineHeap []deadlineEntry

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }

func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].s.dlIdx[b2i(h[i].write)] = i + 1
	h[j].s.dlIdx[b2i(h[j].write)] = j + 1
}

func (h *deadlineHeap) Push(x any) {
	e := x.(deadlineEntry)
	e.s.dlIdx[b2i(e.write)] = len(*h) + 1
	*h = append(*h, e)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = deadlineEntry{}
	*h = old[:len(old)-1]
	e.s.dlIdx[b2i(e.write)] = 0
	return e
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// setStreamDeadlineLocked records s's read or write deadline t in the heap —
// the zero time removes it — and keeps Conn.streamDeadline on the earliest,
// waking the timer loop when that moved earlier. Callers hold mu.
func (c *Conn) setStreamDeadlineLocked(s *Stream, write bool, t time.Time) {
	i := s.dlIdx[b2i(write)] - 1
	switch {
	case t.IsZero() && i >= 0:
		heap.Remove(&c.deadlines, i)
	case t.IsZero():
	case i >= 0:
		c.deadlines[i].at = t
		heap.Fix(&c.deadlines, i)
	default:
		heap.Push(&c.deadlines, deadlineEntry{at: t, s: s, write: write})
	}
	c.syncStreamDeadlineLocked()
}

// dropStreamDeadlinesLocked takes a stream leaving the table out of the
// heap. Callers hold mu.
func (c *Conn) dropStreamDeadlinesLocked(s *Stream) {
	for k := range s.dlIdx {
		if i := s.dlIdx[k] - 1; i >= 0 {
			heap.Remove(&c.deadlines, i)
		}
	}
	c.syncStreamDeadlineLocked()
}

func (c *Conn) syncStreamDeadlineLocked() {
	var top time.Time
	if len(c.deadlines) > 0 {
		top = c.deadlines[0].at
	}
	if !top.IsZero() && (c.streamDeadline.IsZero() || top.Before(c.streamDeadline)) {
		c.kickTimer()
	}
	c.streamDeadline = top
}

// expireStreamDeadlinesLocked wakes whatever the deadlines matured by now
// hold — the stream's Read, the writers parked on cond — takes them out of
// the heap, and returns the earliest deadline still to come, zero when none
// is armed. A matured deadline stays on its stream, so later calls fail at
// once until it is moved; only its place in the heap goes. Callers hold mu.
func (c *Conn) expireStreamDeadlinesLocked(now time.Time) time.Time {
	wakeWriters := false
	for len(c.deadlines) > 0 && !now.Before(c.deadlines[0].at) {
		e := heap.Pop(&c.deadlines).(deadlineEntry)
		if e.write {
			wakeWriters = true
			e.s.wakeCreditLocked()
		} else {
			e.s.signal()
		}
	}
	if wakeWriters {
		c.wakeWindowLocked() // a writer of a matured stream on the window
	}
	if len(c.deadlines) == 0 {
		return time.Time{}
	}
	return c.deadlines[0].at
}
