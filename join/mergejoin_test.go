package join

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// joinRows renders a merge-join result compactly: L(x) unmatched left, R(x)
// unmatched right, B(x,y) a matched pair.
func joinRows(s sluice.Stream[EitherOrBoth[int, int]]) []string {
	var out []string
	s(func(b sluice.Batch[EitherOrBoth[int, int]]) bool {
		for _, e := range b.Items {
			switch {
			case e.Both():
				out = append(out, fmt.Sprintf("B(%d,%d)", e.Left, e.Right))
			case e.HasLeft:
				out = append(out, fmt.Sprintf("L(%d)", e.Left))
			default:
				out = append(out, fmt.Sprintf("R(%d)", e.Right))
			}
		}
		return true
	})
	return out
}

func identity(v int) int { return v }

func joinInts(left, right []int, batch int) []string {
	return joinRows(Merge(
		sluice.Of(left, batch), sluice.Of(right, batch),
		identity, identity, cmp.Compare[int],
	))
}

func TestMergeJoinBy(t *testing.T) {
	tests := []struct {
		name        string
		left, right []int
		want        []string
	}{
		{
			"interleaved",
			[]int{1, 3, 5},
			[]int{3, 4, 5, 6},
			[]string{"L(1)", "B(3,3)", "R(4)", "B(5,5)", "R(6)"},
		},
		{
			"disjoint",
			[]int{1, 2},
			[]int{3, 4},
			[]string{"L(1)", "L(2)", "R(3)", "R(4)"},
		},
		{
			"identical",
			[]int{1, 2},
			[]int{1, 2},
			[]string{"B(1,1)", "B(2,2)"},
		},
		{
			"left empty", nil,
			[]int{1, 2},
			[]string{"R(1)", "R(2)"},
		},
		{
			"right empty",
			[]int{1, 2},
			nil,
			[]string{"L(1)", "L(2)"},
		},
		{"both empty", nil, nil, nil},
		{
			"left runs out first",
			[]int{1},
			[]int{1, 2, 3},
			[]string{"B(1,1)", "R(2)", "R(3)"},
		},
		{
			"right runs out first",
			[]int{1, 2, 3},
			[]int{1},
			[]string{"B(1,1)", "L(2)", "L(3)"},
		},
		// Duplicates on one side pair with the single row on the other.
		{
			"duplicate left",
			[]int{2, 2, 2},
			[]int{2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)"},
		},
		{
			"duplicate right",
			[]int{2},
			[]int{2, 2, 2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)"},
		},
		// Duplicates on both sides: the full cross product, as SQL requires.
		{
			"cross product 2x3",
			[]int{2, 2},
			[]int{2, 2, 2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)"},
		},
		{
			"consecutive groups",
			[]int{1, 1, 2},
			[]int{1, 2, 2},
			[]string{"B(1,1)", "B(1,1)", "B(2,2)", "B(2,2)"},
		},
		// A duplicated key with no partner stays unmatched, once per row.
		{
			"unmatched duplicates",
			[]int{2, 2},
			[]int{3},
			[]string{"L(2)", "L(2)", "R(3)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// sluice.Batch size 2 splits runs across batch boundaries, which is where
			// a cursor bug would show.
			if got := joinInts(tt.left, tt.right, 2); !slices.Equal(got, tt.want) {
				t.Errorf("batch=2: got %v, want %v", got, tt.want)
			}
			// sluice.Batch size 1 puts every element in its own batch.
			if got := joinInts(tt.left, tt.right, 1); !slices.Equal(got, tt.want) {
				t.Errorf("batch=1: got %v, want %v", got, tt.want)
			}
		})
	}
}

// Each join semantics is a filter over the same output — that is the claim the
// operator is built on, so it is worth pinning.
func TestMergeJoinBySemantics(t *testing.T) {
	left, right := []int{1, 2, 3}, []int{2, 3, 4}

	rows := func(keep func(EitherOrBoth[int, int]) bool) []string {
		s := Merge(sluice.Of(left, 2), sluice.Of(right, 2), identity, identity, cmp.Compare[int])
		return joinRows(sluice.Filter(s, keep))
	}

	tests := []struct {
		name string
		keep func(EitherOrBoth[int, int]) bool
		want []string
	}{
		{
			"inner", func(e EitherOrBoth[int, int]) bool { return e.Both() },
			[]string{"B(2,2)", "B(3,3)"},
		},
		{
			"left join", func(e EitherOrBoth[int, int]) bool { return e.HasLeft },
			[]string{"L(1)", "B(2,2)", "B(3,3)"},
		},
		{
			"right join", func(e EitherOrBoth[int, int]) bool { return e.HasRight },
			[]string{"B(2,2)", "B(3,3)", "R(4)"},
		},
		{
			"full outer", func(EitherOrBoth[int, int]) bool { return true },
			[]string{"L(1)", "B(2,2)", "B(3,3)", "R(4)"},
		},
		{
			"except left", func(e EitherOrBoth[int, int]) bool { return e.HasLeft && !e.HasRight },
			[]string{"L(1)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rows(tt.keep); !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Unsorted input is the operator's silent failure mode, so it must not be
// silent: it panics rather than returning a plausible wrong answer.
func TestMergeJoinByUnsorted(t *testing.T) {
	tests := []struct {
		name        string
		left, right []int
	}{
		{"left goes backwards", []int{1, 5, 2}, []int{1, 2, 5}},
		{"right goes backwards", []int{1, 2, 5}, []int{1, 5, 2}},
		{"left starts backwards", []int{9, 1}, []int{1, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				switch r := recover(); {
				case streamtest.IsErr(r, sluice.ErrUnsorted):
				case r == nil:
					t.Error("unsorted input produced a result instead of panicking")
				default:
					t.Errorf("panicked with %v, want ErrUnsorted", r)
				}
			}()
			joinInts(tt.left, tt.right, 2)
		})
	}
}

// Equal consecutive keys are sorted, not out of order: they must not trip the
// monotonicity check.
func TestMergeJoinByEqualKeysAreSorted(t *testing.T) {
	got := joinInts([]int{1, 1, 1}, []int{1}, 2)
	want := []string{"B(1,1)", "B(1,1)", "B(1,1)"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMergeJoinByNilFunctions(t *testing.T) {
	tests := []struct {
		name string
		call func()
	}{
		{"nil keyL", func() {
			Merge(sluice.Empty[int](), sluice.Empty[int](), nil, identity, cmp.Compare[int])
		}},
		{"nil keyR", func() {
			Merge(sluice.Empty[int](), sluice.Empty[int](), identity, nil, cmp.Compare[int])
		}},
		{"nil cmp", func() {
			Merge[int, int, int](sluice.Empty[int](), sluice.Empty[int](), identity, identity, nil)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic rather than a later nil dereference")
				}
			}()
			tt.call()
		})
	}
}

// The operator walks both inputs once, so it must work on sources that cannot
// be replayed.
func TestMergeJoinBySinglePass(t *testing.T) {
	left, consumedL := streamtest.SinglePass(t, []int{1, 3, 5}, 2)
	right, consumedR := streamtest.SinglePass(t, []int{3, 5, 7}, 2)

	got := joinRows(Merge(left, right, identity, identity, cmp.Compare[int]))
	want := []string{"L(1)", "B(3,3)", "B(5,5)", "R(7)"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if consumedL() != 3 || consumedR() != 3 {
		t.Errorf("consumed %d and %d, want 3 each", consumedL(), consumedR())
	}
}

func TestMergeJoinByEarlyStopReleasesBoth(t *testing.T) {
	var closedL, closedR bool
	mk := func(vals []int, closed *bool) sluice.Stream[int] {
		return func(yield func(sluice.Batch[int]) bool) {
			defer func() { *closed = true }()
			for _, v := range vals {
				if !yield(sluice.Batch[int]{Items: []int{v}}) {
					return
				}
			}
		}
	}

	n := 0
	s := Merge(mk([]int{5, 5, 5}, &closedL), mk([]int{5, 5, 5}, &closedR),
		identity, identity, cmp.Compare[int])
	s(func(b sluice.Batch[EitherOrBoth[int, int]]) bool {
		n += b.Len()
		return false
	})

	if n == 0 {
		t.Error("no row was emitted: the test proves nothing")
	}
	if !closedL || !closedR {
		t.Errorf("sources not released on early stop: left=%v right=%v", closedL, closedR)
	}
}

// A merge join buffers rows until a batch is full, so an early stop is only
// observable past sluice.DefaultBatchSize. Each case below drives the stop through a
// different branch of the merge — every one of them must propagate it, which is
// the invariant the whole model rests on.
func TestMergeJoinByEarlyStopEveryBranch(t *testing.T) {
	const n = sluice.DefaultBatchSize + 100

	seq := func(start, count int) []int {
		s := make([]int, count)
		for i := range s {
			s[i] = start + i
		}
		return s
	}
	repeat := func(v, count int) []int {
		s := make([]int, count)
		for i := range s {
			s[i] = v
		}
		return s
	}

	tests := []struct {
		name        string
		left, right []int
	}{
		// Right is exhausted at once: every later row takes the !rok branch.
		{"left only", seq(0, n), nil},
		// Left is exhausted at once: the !lok branch.
		{"right only", nil, seq(0, n)},
		// Left keys all sort before the right ones: the c < 0 branch.
		{"left sorts first", seq(0, n), seq(n*2, 10)},
		// Right keys all sort before the left ones: the c > 0 branch.
		{"right sorts first", seq(n*2, 10), seq(0, n)},
		// One key on both sides: the cross-product branch.
		{"cross product", repeat(1, 40), repeat(1, 40)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				closedL, closedR bool
				startedL         bool
			)
			mk := func(vals []int, started, closed *bool) sluice.Stream[int] {
				return func(yield func(sluice.Batch[int]) bool) {
					if started != nil {
						*started = true
					}
					defer func() { *closed = true }()
					for i := 0; i < len(vals); i += 16 {
						end := min(i+16, len(vals))
						if !yield(sluice.Batch[int]{Items: vals[i:end]}) {
							return
						}
					}
				}
			}

			batches := 0
			s := Merge(
				mk(tt.left, &startedL, &closedL),
				mk(tt.right, nil, &closedR),
				identity, identity, cmp.Compare[int],
			)
			s(func(sluice.Batch[EitherOrBoth[int, int]]) bool {
				batches++
				return false // stop on the first full batch
			})

			if batches != 1 {
				t.Errorf("consumer saw %d batches, want 1: the stop did not propagate", batches)
			}
			if !closedL || !closedR {
				t.Errorf("sources not released: left=%v right=%v", closedL, closedR)
			}
		})
	}
}

// Output is batched rather than emitted one row at a time.
func TestMergeJoinByBatchesOutput(t *testing.T) {
	const n = 2500
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	var sizes []int
	total := 0
	s := Merge(sluice.Of(left, 100), sluice.Empty[int](), identity, identity, cmp.Compare[int])
	s(func(b sluice.Batch[EitherOrBoth[int, int]]) bool {
		sizes = append(sizes, b.Len())
		total += b.Len()
		return true
	})

	if total != n {
		t.Errorf("emitted %d rows, want %d", total, n)
	}
	if want := []int{sluice.DefaultBatchSize, sluice.DefaultBatchSize, n - 2*sluice.DefaultBatchSize}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

// sluice.Filter emits empty batches to keep the upstream cadence, so a join fed by one
// must skip them rather than mistake them for exhaustion.
func TestMergeJoinByEmptyBatches(t *testing.T) {
	left := sluice.Filter(sluice.Of([]int{1, 2, 3, 4, 5, 6}, 2), func(v int) bool { return v%3 == 0 })
	right := sluice.Of([]int{3, 6}, 1)

	got := joinRows(Merge(left, right, identity, identity, cmp.Compare[int]))
	if want := []string{"B(3,3)", "B(6,6)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Joining on a field rather than the element itself: the key functions exist so
// the two sides need not share a type.
func TestMergeJoinByDistinctTypes(t *testing.T) {
	type order struct {
		customerID int
		total      int
	}
	type customer struct {
		id   int
		name string
	}

	orders := []order{{1, 100}, {2, 250}, {4, 75}}
	customers := []customer{{1, "ana"}, {3, "bo"}, {4, "cy"}}

	var matched []string
	s := Merge(sluice.Of(orders, 2), sluice.Of(customers, 2),
		func(o order) int { return o.customerID },
		func(c customer) int { return c.id },
		cmp.Compare[int],
	)
	s(func(b sluice.Batch[EitherOrBoth[order, customer]]) bool {
		for _, e := range b.Items {
			if e.Both() {
				matched = append(matched, fmt.Sprintf("%s:%d", e.Right.name, e.Left.total))
			}
		}
		return true
	})

	if want := []string{"ana:100", "cy:75"}; !slices.Equal(matched, want) {
		t.Errorf("matched = %v, want %v", matched, want)
	}
}

// The monotonicity check must survive the batch-wise tail drain: once one side
// is exhausted, the other's elements flow through takeChecked rather than peek,
// and a backwards key there must panic exactly as it does mid-join. The table
// covers both placements — inside the drained batch (takeChecked) and at the
// batch boundary that follows it (peek) — on both sides, and after a match.
func TestMergeJoinByUnsortedInTail(t *testing.T) {
	tests := []struct {
		name        string
		left, right []int
		size        int
	}{
		{"backwards inside a drained batch", []int{1, 5, 2}, nil, 3},
		{"backwards at a drained batch boundary", []int{1, 5, 2}, nil, 2},
		{"tail after a match goes backwards", []int{1, 5, 2}, []int{1}, 3},
		{"right tail goes backwards", nil, []int{4, 3}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				switch r := recover(); {
				case streamtest.IsErr(r, sluice.ErrUnsorted):
				case r == nil:
					t.Error("unsorted tail produced a result instead of panicking")
				default:
					t.Errorf("panicked with %v, want ErrUnsorted", r)
				}
			}()
			joinInts(tt.left, tt.right, tt.size)
		})
	}
}
