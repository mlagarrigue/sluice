package window

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A pane says what it lost: Dropped counts the elements its window refused
// at maxPerWindow under DropNewest, Late the elements that arrived, under
// LateDrop, after their own window was emitted, while it was open. A whole
// pane says zero for both.
func TestWindowPanesCountWhatTheyLost(t *testing.T) {
	at := func(v int) time.Time { return time.Unix(int64(v), 0) }
	// Window [0,10): 0..4 kept, 5..7 dropped at 5 a window. Window [10,20):
	// 10, then 3 and 4, late, then 11. Window [20,30): 20, whole.
	events := []int{0, 1, 2, 3, 4, 5, 6, 7, 10, 3, 4, 11, 20}
	var panes []Pane[int]
	Tumbling(sluice.Of(events, 4), at, 10*time.Second, 5, sluice.DropNewest, LateDrop)(func(b sluice.Batch[Pane[int]]) bool {
		panes = append(panes, b.Items...)
		return true
	})
	want := []struct{ items, dropped, late int }{{5, 3, 0}, {2, 0, 2}, {1, 0, 0}}
	if len(panes) != len(want) {
		t.Fatalf("%d panes, want %d", len(panes), len(want))
	}
	for i, w := range want {
		p := panes[i]
		if p.Len() != w.items || p.Dropped != w.dropped || p.Late != w.late {
			t.Errorf("pane %d [%v, %v): %d items, Dropped %d, Late %d; want %d, %d, %d",
				i, p.Start.Unix(), p.End.Unix(), p.Len(), p.Dropped, p.Late, w.items, w.dropped, w.late)
		}
	}
}
