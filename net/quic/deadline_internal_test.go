package quic

import (
	"testing"
	"time"
)

// checkDeadlines holds the heap to its two promises: each entry is where its
// stream says it is, and streamDeadline is the earliest one.
func checkDeadlines(t *testing.T, c *Conn) {
	t.Helper()
	for i, e := range c.deadlines {
		if got := e.s.dlIdx[b2i(e.write)]; got != i+1 {
			t.Fatalf("entry %d (stream %d, write %v) is recorded at %d", i, e.s.id, e.write, got-1)
		}
	}
	var top time.Time
	if len(c.deadlines) > 0 {
		top = c.deadlines[0].at
	}
	if !c.streamDeadline.Equal(top) {
		t.Fatalf("streamDeadline %v, the earliest armed is %v", c.streamDeadline, top)
	}
}

// A matured deadline leaves the heap and the next one takes its place; a
// deadline moved, cleared, or on a stream that closes is the heap's to
// follow, so the timer loop never sleeps on one that no longer holds.
func TestStreamDeadlineHeapFollowsEverySet(t *testing.T) {
	c, ss := deadlineConn(t, 4)
	a, b, d, e := ss[0], ss[1], ss[2], ss[3]
	base := time.Now().Add(time.Hour)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }

	_ = a.SetWriteDeadline(at(3))
	_ = b.SetWriteDeadline(at(1))
	_ = d.SetWriteDeadline(at(2))
	_ = d.SetReadDeadline(at(4))
	c.mu.Lock()
	checkDeadlines(t, c)
	if next := c.expireStreamDeadlinesLocked(at(1)); !next.Equal(at(2)) || b.dlIdx != [2]int{} {
		t.Fatalf("after b matured: next %v, b at %v; want %v and out of the heap", next, b.dlIdx, at(2))
	}
	c.streamDeadline = c.deadlines[0].at
	checkDeadlines(t, c)
	c.mu.Unlock()

	_ = d.SetWriteDeadline(at(5)) // moved later: a comes first
	_ = a.SetWriteDeadline(time.Time{})
	c.mu.Lock()
	checkDeadlines(t, c)
	if !c.streamDeadline.Equal(at(4)) || a.dlIdx != [2]int{} {
		t.Fatalf("after a cleared and d moved: earliest %v, want d's read at %v", c.streamDeadline, at(4))
	}
	c.mu.Unlock()

	_ = e.SetReadDeadline(at(1))
	c.mu.Lock()
	c.dropStreamDeadlinesLocked(e) // e leaves the table
	checkDeadlines(t, c)
	if len(c.deadlines) != 2 || !c.streamDeadline.Equal(at(4)) {
		t.Fatalf("after e closed: %d entries, earliest %v; want d's two at %v", len(c.deadlines), c.streamDeadline, at(4))
	}
	if next := c.expireStreamDeadlinesLocked(at(9)); !next.IsZero() || len(c.deadlines) != 0 || d.dlIdx != [2]int{} {
		t.Fatalf("after everything matured: next %v, %d entries left", next, len(c.deadlines))
	}
	c.mu.Unlock()
	// A matured deadline stays on its stream: later calls fail at once.
	if d.writeDeadline.IsZero() {
		t.Fatal("the matured write deadline was cleared from its stream")
	}
}
