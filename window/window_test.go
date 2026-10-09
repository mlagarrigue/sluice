package window

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// wRow is the test element: an event time in seconds and a name.
type wRow struct {
	sec  int64
	name string
}

func wAt(r wRow) time.Time { return time.Unix(r.sec, 0).UTC() }

// panes renders each pane as "start:a,b,c" so a table can state the expected
// grouping and its bounds in one readable string.
func panes(s sluice.Stream[Pane[wRow]]) []string {
	var got []string
	s(func(b sluice.Batch[Pane[wRow]]) bool {
		for _, p := range b.Items {
			names := make([]string, len(p.Items))
			for i, r := range p.Items {
				names[i] = r.name
			}
			got = append(got, fmt.Sprintf("%d:%s", p.Start.Unix(), joinNames(names)))
		}
		return true
	})
	return got
}

func joinNames(names []string) string { return strings.Join(names, ",") }

func rows(specs ...wRow) []wRow { return specs }

func TestWindow(t *testing.T) {
	tests := []struct {
		name  string
		src   []wRow
		size  time.Duration
		batch int
		want  []string
	}{
		{
			"one window per group of ten seconds",
			rows(wRow{0, "a"}, wRow{3, "b"}, wRow{10, "c"}, wRow{19, "d"}),
			10 * time.Second, 2,
			[]string{"0:a,b", "10:c,d"},
		},
		{
			"windows are aligned on absolute time, not on the first element",
			// The first element is at 7s: its window still starts at 0, not 7.
			rows(wRow{7, "a"}, wRow{12, "b"}),
			10 * time.Second, 8,
			[]string{"0:a", "10:b"},
		},
		{
			"a gap emits no empty windows",
			rows(wRow{0, "a"}, wRow{100, "b"}),
			10 * time.Second, 8,
			[]string{"0:a", "100:b"},
		},
		{
			"one window spanning several input batches",
			rows(wRow{0, "a"}, wRow{1, "b"}, wRow{2, "c"}, wRow{3, "d"}),
			time.Minute, 1,
			[]string{"0:a,b,c,d"},
		},
		{
			"one input batch spanning several windows",
			rows(wRow{0, "a"}, wRow{10, "b"}, wRow{20, "c"}),
			10 * time.Second, 64,
			[]string{"0:a", "10:b", "20:c"},
		},
		{
			"ties in the same window stay in arrival order",
			rows(wRow{5, "a"}, wRow{5, "b"}, wRow{5, "c"}),
			10 * time.Second, 2,
			[]string{"0:a,b,c"},
		},
		{
			"a single element closes at the end of the stream",
			rows(wRow{42, "a"}),
			10 * time.Second, 8,
			[]string{"40:a"},
		},
		{
			"an empty stream produces nothing",
			nil,
			time.Second, 8,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := panes(Tumbling(sluice.Of(tt.src, tt.batch), wAt, tt.size, 100, sluice.Fail))
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Each pane's End is its Start plus the size, and the bounds are what the
// elements were grouped by.
func TestWindowBounds(t *testing.T) {
	src := rows(wRow{5, "a"}, wRow{25, "b"})
	var got []Pane[wRow]
	Tumbling(sluice.Of(src, 4), wAt, 10*time.Second, 100, sluice.Fail)(func(b sluice.Batch[Pane[wRow]]) bool {
		got = append(got, b.Items...)
		return true
	})

	if len(got) != 2 {
		t.Fatalf("got %d panes, want 2", len(got))
	}
	for _, p := range got {
		if want := p.Start.Add(10 * time.Second); !p.End.Equal(want) {
			t.Errorf("pane at %v: End = %v, want %v", p.Start, p.End, want)
		}
		for _, r := range p.Items {
			if at := wAt(r); at.Before(p.Start) || !at.Before(p.End) {
				t.Errorf("element at %v is outside its pane [%v, %v)", at, p.Start, p.End)
			}
		}
	}
}

// A Pane owns its Items: values kept across further pulls must still read back
// correctly, which is what makes a pane storable and sendable.
func TestWindowPanesOwnTheirItems(t *testing.T) {
	src := make([]wRow, 300)
	for i := range src {
		src[i] = wRow{sec: int64(i), name: fmt.Sprintf("n%d", i)}
	}

	var kept []Pane[wRow]
	Tumbling(sluice.Of(src, 7), wAt, 10*time.Second, 100, sluice.Fail)(func(b sluice.Batch[Pane[wRow]]) bool {
		kept = append(kept, b.Items...)
		return true
	})

	if len(kept) != 30 {
		t.Fatalf("got %d panes, want 30", len(kept))
	}
	// Every pane retained from the very first call must still hold its own
	// ten elements, in order, long after the stream has moved on.
	for i, p := range kept {
		if p.Len() != 10 {
			t.Fatalf("pane %d holds %d elements, want 10 — its buffer was reused", i, p.Len())
		}
		for j, r := range p.Items {
			if want := fmt.Sprintf("n%d", i*10+j); r.name != want {
				t.Fatalf("pane %d element %d is %q, want %q — its buffer was reused", i, j, r.name, want)
			}
		}
	}
}

// Upstream operators emit empty batches to hold the cadence; they must not
// disturb the windowing.
func TestWindowSkipsEmptyBatches(t *testing.T) {
	src := rows(wRow{0, "a"}, wRow{1, "keep"}, wRow{10, "b"}, wRow{11, "keep2"})
	kept := sluice.Filter(sluice.Of(src, 1), func(r wRow) bool { return r.name != "a" && r.name != "b" })
	got := panes(Tumbling(kept, wAt, 10*time.Second, 100, sluice.Fail))
	if want := []string{"0:keep", "10:keep2"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Time going backwards would belong to a window already emitted, so it panics
// rather than being dropped or reopening one.
func TestWindowUnsorted(t *testing.T) {
	tests := []struct {
		name string
		src  []wRow
	}{
		{"backwards inside one window", rows(wRow{5, "a"}, wRow{3, "b"})},
		{"backwards into a closed window", rows(wRow{5, "a"}, wRow{15, "b"}, wRow{7, "c"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); !streamtest.IsErr(r, sluice.ErrUnsorted) {
					t.Errorf("panicked with %v, want ErrUnsorted", r)
				}
			}()
			panes(Tumbling(sluice.Of(tt.src, 4), wAt, 10*time.Second, 100, sluice.Fail))
		})
	}
}

func TestWindowOverflowFail(t *testing.T) {
	defer func() {
		if r := recover(); !streamtest.IsErr(r, sluice.ErrOverflow) {
			t.Errorf("panicked with %v, want ErrOverflow", r)
		}
	}()
	src := rows(wRow{0, "a"}, wRow{1, "b"}, wRow{2, "c"})
	panes(Tumbling(sluice.Of(src, 4), wAt, time.Minute, 2, sluice.Fail))
}

// sluice.DropNewest keeps what the window holds: the pane is short, never wrong, and
// the next window starts clean.
func TestWindowOverflowDropNewest(t *testing.T) {
	src := rows(wRow{0, "a"}, wRow{1, "b"}, wRow{2, "dropped"}, wRow{60, "c"})
	got := panes(Tumbling(sluice.Of(src, 1), wAt, time.Minute, 2, sluice.DropNewest))
	if want := []string{"0:a,b", "60:c"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// An early stop reaches the source promptly, and the window still open when
// the consumer stopped is discarded rather than flushed.
func TestWindowEarlyStopDiscardsOpenWindow(t *testing.T) {
	finalized := false
	src := sluice.Stream[wRow](func(yield func(sluice.Batch[wRow]) bool) {
		defer func() { finalized = true }()
		for i := range 1000 {
			if !yield(sluice.Batch[wRow]{Items: []wRow{{sec: int64(i), name: "x"}}}) {
				return
			}
		}
	})

	n := 0
	Tumbling(src, wAt, 10*time.Second, 100, sluice.Fail)(func(sluice.Batch[Pane[wRow]]) bool {
		n++
		return n < 2
	})

	if !finalized {
		t.Error("the source's deferred cleanup did not run")
	}
	if n != 2 {
		t.Errorf("saw %d panes, want the stop to take effect on the 2nd", n)
	}
}

func TestWindowArgumentValidation(t *testing.T) {
	tests := []struct {
		name string
		f    func()
	}{
		{"nil ts", func() { Tumbling[wRow](sluice.Empty[wRow](), nil, time.Second, 1, sluice.Fail) }},
		{"zero size", func() { Tumbling(sluice.Empty[wRow](), wAt, 0, 1, sluice.Fail) }},
		{"negative size", func() { Tumbling(sluice.Empty[wRow](), wAt, -time.Second, 1, sluice.Fail) }},
		{"zero n", func() { Tumbling(sluice.Empty[wRow](), wAt, time.Second, 0, sluice.Fail) }},
		{"DropOldest rejected", func() { Tumbling(sluice.Empty[wRow](), wAt, time.Second, 1, sluice.DropOldest) }},
		{"undeclared policy", func() { Tumbling(sluice.Empty[wRow](), wAt, time.Second, 1, sluice.Overflow(9)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			tt.f()
		})
	}
}
