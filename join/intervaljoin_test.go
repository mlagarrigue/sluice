package join

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// tRow is the test row: a key, an event time offset in seconds, a payload.
type tRow struct {
	key  string
	sec  int64
	name string
}

func tRows(rows ...tRow) []tRow { return rows }

func at(sec int64) time.Time { return time.Unix(sec, 0) }

func intervalRows(left, right []tRow, lower, upper time.Duration, limit BuildLimit, batch int) []string {
	var got []string
	Interval(
		sluice.Of(left, batch), sluice.Of(right, batch),
		func(r tRow) string { return r.key },
		func(r tRow) string { return r.key },
		func(r tRow) time.Time { return at(r.sec) },
		func(r tRow) time.Time { return at(r.sec) },
		lower, upper,
		func(l, r tRow) string { return l.name + "+" + r.name },
		limit,
	)(func(b sluice.Batch[string]) bool {
		got = append(got, b.Items...)
		return true
	})
	return got
}

func failLimit(n int) BuildLimit { return BuildLimit{MaxEntries: n, OnOverflow: sluice.Fail} }

func TestIntervalJoin(t *testing.T) {
	tests := []struct {
		name         string
		left, right  []tRow
		lower, upper time.Duration
		want         []string
	}{
		{
			"right within window after left",
			tRows(tRow{"k", 10, "l1"}),
			tRows(tRow{"k", 12, "r1"}, tRow{"k", 25, "r2"}),
			0, 5 * time.Second,
			[]string{"l1+r1"},
		},
		{
			"negative lower reaches the right's past",
			tRows(tRow{"k", 10, "l1"}),
			tRows(tRow{"k", 7, "r1"}, tRow{"k", 9, "r2"}),
			-3 * time.Second, 0,
			[]string{"l1+r1", "l1+r2"},
		},
		{
			"window edges are inclusive",
			tRows(tRow{"k", 10, "l1"}),
			tRows(tRow{"k", 7, "out"}, tRow{"k", 8, "rLo"}, tRow{"k", 12, "rHi"}, tRow{"k", 13, "out2"}),
			-2 * time.Second, 2 * time.Second,
			[]string{"l1+rLo", "l1+rHi"},
		},
		{
			"keys partition the matches",
			tRows(tRow{"a", 10, "la"}, tRow{"b", 10, "lb"}),
			tRows(tRow{"a", 10, "ra"}, tRow{"c", 10, "rc"}),
			0, 0,
			[]string{"la+ra"},
		},
		{
			"zero window pairs the same instant only",
			tRows(tRow{"k", 10, "l1"}, tRow{"k", 11, "l2"}),
			tRows(tRow{"k", 10, "r1"}, tRow{"k", 12, "r2"}),
			0, 0,
			[]string{"l1+r1"},
		},
		{
			"several matches on both sides",
			tRows(tRow{"k", 10, "l1"}, tRow{"k", 11, "l2"}),
			tRows(tRow{"k", 10, "r1"}, tRow{"k", 11, "r2"}),
			-1 * time.Second, 1 * time.Second,
			[]string{"l1+r1", "l1+r2", "l2+r1", "l2+r2"},
		},
		{
			"left empty",
			nil,
			tRows(tRow{"k", 10, "r1"}),
			0, time.Second,
			nil,
		},
		{
			"right empty",
			tRows(tRow{"k", 10, "l1"}),
			nil,
			0, time.Second,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intervalRows(tt.left, tt.right, tt.lower, tt.upper, failLimit(100), 2)
			slices.Sort(got)
			want := slices.Clone(tt.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

// Pairs leave in discovery order — the later row of each pair, ties left
// first — deterministically for given inputs.
func TestIntervalJoinDiscoveryOrder(t *testing.T) {
	left := tRows(tRow{"k", 10, "l1"}, tRow{"k", 12, "l2"})
	right := tRows(tRow{"k", 11, "r1"}, tRow{"k", 13, "r2"})
	got := intervalRows(left, right, -2*time.Second, 2*time.Second, failLimit(100), 8)
	// l1@10 buffers; r1@11 probes -> l1+r1; l2@12 probes -> l2+r1;
	// r2@13 probes -> l1 evicted? l1+2s=12 < 13 -> evicted; l2+r2.
	want := []string{"l1+r1", "l2+r1", "l2+r2"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// The reference oracle: a brute-force double loop over deterministic
// pseudo-random rows, compared as multisets. Long streams with a narrow
// window drive eviction and compaction through their paces.
func TestIntervalJoinAgainstBruteForce(t *testing.T) {
	const n = 2000
	gen := func(seed int64, count int) []tRow {
		rows := make([]tRow, count)
		state := uint64(seed)
		clock := int64(0)
		for i := range rows {
			state = state*6364136223846793005 + 1442695040888963407
			clock += int64((state >> 33) % 3) // non-decreasing, frequent ties
			key := fmt.Sprintf("k%d", (state>>21)%17)
			rows[i] = tRow{key: key, sec: clock, name: fmt.Sprintf("s%d-%d", seed, i)}
		}
		return rows
	}
	left, right := gen(1, n), gen(2, n)
	// Windows straddling zero, and windows wholly in the past or the future,
	// which retain rows before their window opens.
	windows := []struct{ lower, upper time.Duration }{
		{-4 * time.Second, 3 * time.Second},
		{-9 * time.Second, -5 * time.Second},
		{5 * time.Second, 9 * time.Second},
		{-7 * time.Second, -7 * time.Second},
	}
	for _, w := range windows {
		var want []string
		for _, l := range left {
			for _, r := range right {
				if l.key != r.key {
					continue
				}
				d := time.Duration(r.sec-l.sec) * time.Second
				if d >= w.lower && d <= w.upper {
					want = append(want, l.name+"+"+r.name)
				}
			}
		}

		got := intervalRows(left, right, w.lower, w.upper, failLimit(100_000), 64)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("[%v, %v]: join disagrees with brute force: got %d pairs, want %d",
				w.lower, w.upper, len(got), len(want))
		}
	}
}

// A row the other side's clock has already passed can never match, so it is
// not retained and does not count against the limit. Here every left closes
// its window before the first right, and every right before the next left:
// nothing is ever worth keeping, and a limit of 2 must not overflow.
func TestIntervalJoinDoesNotRetainRowsDeadAtBirth(t *testing.T) {
	left := tRows(tRow{"k", 1, "l1"}, tRow{"k", 2, "l2"}, tRow{"k", 3, "l3"},
		tRow{"k", 100, "l4"}, tRow{"k", 200, "l5"})
	right := tRows(tRow{"k", 50, "r1"}, tRow{"k", 60, "r2"}, tRow{"k", 70, "r3"},
		tRow{"k", 150, "r4"})
	for _, w := range []struct{ lower, upper time.Duration }{
		{0, 0},
		{-5 * time.Second, 5 * time.Second},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("[%v, %v]: panicked with %v; dead rows were counted against the limit",
						w.lower, w.upper, r)
				}
			}()
			if got := intervalRows(left, right, w.lower, w.upper, failLimit(2), 1); len(got) != 0 {
				t.Errorf("[%v, %v]: got %v, want no pairs", w.lower, w.upper, got)
			}
		}()
	}
}

// Input going backwards in time must panic with sluice.ErrUnsorted, on either side.
func TestIntervalJoinUnsorted(t *testing.T) {
	backwards := tRows(tRow{"k", 10, "a"}, tRow{"k", 5, "b"})
	forward := tRows(tRow{"k", 1, "c"}, tRow{"k", 20, "d"})
	tests := []struct {
		name        string
		left, right []tRow
	}{
		{"left goes backwards", backwards, forward},
		{"right goes backwards", forward, backwards},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); !streamtest.IsErr(r, sluice.ErrUnsorted) {
					t.Errorf("panicked with %v, want ErrUnsorted", r)
				}
			}()
			intervalRows(tt.left, tt.right, 0, time.Minute, failLimit(100), 2)
		})
	}
}

// sluice.Fail panics with sluice.ErrOverflow when the retained rows of both sides together
// reach the limit.
func TestIntervalJoinOverflowFail(t *testing.T) {
	defer func() {
		if r := recover(); !streamtest.IsErr(r, sluice.ErrOverflow) {
			t.Errorf("panicked with %v, want ErrOverflow", r)
		}
	}()
	// A wide window over one key retains everything: the third retention hits
	// the limit of 2.
	left := tRows(tRow{"k", 1, "l1"}, tRow{"k", 2, "l2"}, tRow{"k", 3, "l3"}, tRow{"k", 4, "l4"})
	right := tRows(tRow{"k", 100, "r1"})
	intervalRows(left, right, 0, 200*time.Second, failLimit(2), 1)
}

// sluice.DropNewest stops retaining, keeps probing: pairs against already-buffered
// rows survive, pairs that needed the dropped row buffered are missing, and
// the diagnostic reports once.
func TestIntervalJoinOverflowDropNewest(t *testing.T) {
	diags := diagnostics.NewCollector(4)
	limit := BuildLimit{MaxEntries: 1, OnOverflow: sluice.DropNewest, Report: reportTo(diags)}

	// l1 is retained (1/1). l2 cannot be (dropped). r1@12 matches both in
	// window, but only l1 is buffered.
	left := tRows(tRow{"k", 10, "l1"}, tRow{"k", 11, "l2"})
	right := tRows(tRow{"k", 12, "r1"})
	got := intervalRows(left, right, 0, 5*time.Second, limit, 1)

	if want := []string{"l1+r1"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if diags.Len() != 1 {
		t.Errorf("%d diagnostics, want exactly 1 — reported once, not per drop", diags.Len())
	}
	if sev, ok := diags.Worst(); !ok || sev != diagnostics.Critical {
		t.Errorf("worst severity %v, want diagnostics.Critical", sev)
	}
}

// An early stop must reach both sources promptly and run their deferred
// cleanup (S9 through the cursors, S10 through the refusal).
func TestIntervalJoinEarlyStopReleasesBoth(t *testing.T) {
	mkSrc := func(done *bool) sluice.Stream[tRow] {
		return func(yield func(sluice.Batch[tRow]) bool) {
			defer func() { *done = true }()
			for i := range 10_000 {
				if !yield(sluice.Batch[tRow]{Items: []tRow{{key: "k", sec: int64(i), name: "x"}}}) {
					return
				}
			}
		}
	}
	var lDone, rDone bool
	n := 0
	Interval(
		mkSrc(&lDone), mkSrc(&rDone),
		func(r tRow) string { return r.key }, func(r tRow) string { return r.key },
		func(r tRow) time.Time { return at(r.sec) }, func(r tRow) time.Time { return at(r.sec) },
		-time.Second, time.Second,
		func(l, r tRow) string { return "p" },
		failLimit(100),
	)(func(b sluice.Batch[string]) bool {
		n++
		return n < 2
	})
	if !lDone || !rDone {
		t.Errorf("sources finalized: left %v right %v, want both", lDone, rDone)
	}
}

func TestIntervalJoinArgumentValidation(t *testing.T) {
	key := func(r tRow) string { return r.key }
	ts := func(r tRow) time.Time { return at(r.sec) }
	mrg := func(l, r tRow) string { return "" }
	ok := failLimit(1)

	tests := []struct {
		name string
		f    func()
	}{
		{"nil keyL", func() { Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), nil, key, ts, ts, 0, 0, mrg, ok) }},
		{"nil tsR", func() { Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, nil, 0, 0, mrg, ok) }},
		{"nil merge", func() {
			Interval[tRow, tRow, string, string](sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, ts, 0, 0, nil, ok)
		}},
		{"lower above upper", func() {
			Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, ts, time.Second, 0, mrg, ok)
		}},
		{"unusable limit", func() {
			Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, ts, 0, 0, mrg, BuildLimit{})
		}},
		// The buffers count dead rows as well as live ones — up to twice the
		// live bound before compaction — so a MaxEntries the hash join would
		// accept can overflow the int32 chain indices here. Refused at
		// construction rather than left to corrupt a chain two billion rows in.
		{"limit past what the chain indices can address with a dead prefix", func() {
			Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, ts, 0, 0, mrg,
				BuildLimit{MaxEntries: maxIntervalEntries + 1, OnOverflow: sluice.Fail})
		}},
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

	// And the cap is exact: the largest admissible limit constructs.
	Interval(sluice.Empty[tRow](), sluice.Empty[tRow](), key, key, ts, ts, 0, 0, mrg,
		BuildLimit{MaxEntries: maxIntervalEntries, OnOverflow: sluice.Fail})
}

// benchIntervalJoinOffset joins one key ticking once per millisecond on each
// side through a window that excludes zero, so every row is retained for
// about distance before it can match anything. The probe cost must not grow
// with that distance: rows still waiting for their window to open are not in
// the way of the rows being probed.
func benchIntervalJoinOffset(b *testing.B, distance time.Duration) {
	b.Helper()
	const n = 1 << 14
	rows := make([]int64, n)
	for i := range rows {
		rows[i] = int64(i)
	}
	ts := func(v int64) time.Time { return time.UnixMilli(v) }
	key := func(int64) int { return 0 }
	limit := BuildLimit{MaxEntries: 1 << 20, OnOverflow: sluice.Fail}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var acc int64
		Interval(
			sluice.Of(rows, 1024), sluice.Of(rows, 1024),
			key, key, ts, ts, -distance-time.Millisecond, -distance,
			func(l, r int64) int64 { return l ^ r },
			limit,
		)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		_ = acc
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(2*n), "ns/element")
}

// BenchmarkIntervalJoinOffset compares a window opening 10 ms in the past with
// one opening 1 s in the past: the same pairs per row, a hundred times the
// rows waiting in between. The two figures should be within noise.
func BenchmarkIntervalJoinOffset(b *testing.B) {
	b.Run("10ms", func(b *testing.B) { benchIntervalJoinOffset(b, 10*time.Millisecond) })
	b.Run("1s", func(b *testing.B) { benchIntervalJoinOffset(b, time.Second) })
}

// A side's chain map is claimed by the first row a probe links, not when the
// join opens: an empty side, or one whose rows all die before any window
// reaches them, never needs one.
func TestIntervalBufferClaimsMapOnFirstLink(t *testing.T) {
	b := newIntervalBuffer[string, int]()
	if b.head != nil {
		t.Fatal("newIntervalBuffer claimed its chain map before any row was linked")
	}
	if got := b.probe("k", at(0), at(10)); len(got) != 0 {
		t.Fatalf("probe on an empty buffer = %v, want nothing", got)
	}
	b.put("k", at(1), 7)
	if got := b.probe("k", at(0), at(10)); len(got) != 1 || b.vals[got[0]] != 7 {
		t.Fatalf("probe after put = %v, want the one row", got)
	}
}

func BenchmarkIntervalJoinEmpty(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		intervalRows(nil, nil, 0, time.Second, failLimit(16), 16)
	}
}
