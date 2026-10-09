package join

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// zipRows renders a ZipLongest result: B(l,r) a pair, L(l) or R(r) a tail row.
func zipRows(s sluice.Stream[EitherOrBoth[int, string]]) []string {
	var out []string
	s(func(b sluice.Batch[EitherOrBoth[int, string]]) bool {
		for _, e := range b.Items {
			switch {
			case e.Both():
				out = append(out, fmt.Sprintf("B(%d,%s)", e.Left, e.Right))
			case e.HasLeft:
				out = append(out, fmt.Sprintf("L(%d)", e.Left))
			default:
				out = append(out, fmt.Sprintf("R(%s)", e.Right))
			}
		}
		return true
	})
	return out
}

func TestZipLongest(t *testing.T) {
	tests := []struct {
		name  string
		left  []int
		right []string
		want  []string
	}{
		{
			"equal lengths",
			[]int{1, 2},
			[]string{"a", "b"},
			[]string{"B(1,a)", "B(2,b)"},
		},
		{
			"left longer",
			[]int{1, 2, 3},
			[]string{"a"},
			[]string{"B(1,a)", "L(2)", "L(3)"},
		},
		{
			"right longer",
			[]int{1},
			[]string{"a", "b", "c"},
			[]string{"B(1,a)", "R(b)", "R(c)"},
		},
		{
			"left empty", nil,
			[]string{"a", "b"},
			[]string{"R(a)", "R(b)"},
		},
		{
			"right empty",
			[]int{1, 2},
			nil,
			[]string{"L(1)", "L(2)"},
		},
		{"both empty", nil, nil, nil},
		{
			"single pair",
			[]int{1},
			[]string{"a"},
			[]string{"B(1,a)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Different batch sizes on each side: the two inputs are not
			// aligned, which is the case §7.5 says nobody tries to fix.
			got := zipRows(ZipLongest(sluice.Of(tt.left, 2), sluice.Of(tt.right, 3)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("batch 2/3: got %v, want %v", got, tt.want)
			}
			got = zipRows(ZipLongest(sluice.Of(tt.left, 1), sluice.Of(tt.right, 1)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("batch 1/1: got %v, want %v", got, tt.want)
			}
		})
	}
}

// Zip — stopping at the shorter stream — is a filter over ZipLongest rather
// than an operator. That is the claim §7.4 rests on.
func TestZipLongestDerivesZip(t *testing.T) {
	zipped := ZipLongest(sluice.Of([]int{1, 2, 3}, 2), sluice.Of([]string{"a", "b"}, 2))
	got := zipRows(sluice.Filter(zipped, EitherOrBoth[int, string].Both))

	if want := []string{"B(1,a)", "B(2,b)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Once a stream is exhausted it must not be restarted on later rounds.
func TestZipLongestDoesNotRestart(t *testing.T) {
	runs := 0
	short := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		runs++
		yield(sluice.Batch[int]{Items: []int{1}})
	})

	got := zipRows(ZipLongest(short, sluice.Of([]string{"a", "b", "c"}, 1)))

	if want := []string{"B(1,a)", "R(b)", "R(c)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if runs != 1 {
		t.Errorf("exhausted source ran %d times, want 1", runs)
	}
}

func TestZipLongestSinglePass(t *testing.T) {
	left, consumedL := streamtest.SinglePass(t, []int{1, 2, 3}, 2)
	right, consumedR := streamtest.SinglePass(t, []int{10, 20}, 2)

	var got []string
	ZipLongest(left, right)(func(b sluice.Batch[EitherOrBoth[int, int]]) bool {
		for _, e := range b.Items {
			if e.Both() {
				got = append(got, fmt.Sprintf("%d+%d", e.Left, e.Right))
			} else {
				got = append(got, fmt.Sprintf("%d+_", e.Left))
			}
		}
		return true
	})

	if want := []string{"1+10", "2+20", "3+_"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if consumedL() != 3 || consumedR() != 2 {
		t.Errorf("consumed %d and %d, want 3 and 2", consumedL(), consumedR())
	}
}

// sluice.Filter emits empty batches; a zip fed by one must not mistake them for the
// end of the stream.
func TestZipLongestEmptyBatches(t *testing.T) {
	left := sluice.Filter(sluice.Of([]int{1, 2, 3, 4}, 2), func(v int) bool { return v%2 == 0 })
	got := zipRows(ZipLongest(left, sluice.Of([]string{"a", "b"}, 1)))

	if want := []string{"B(2,a)", "B(4,b)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// ZipLongest drains what both sides hold in common before re-testing the batch
// boundary, so batch sizes that do not line up are the case that exercises the
// refill: pairing must stay positional across every boundary, whichever side
// runs out of its current batch first.
func TestZipLongestMisalignedBatches(t *testing.T) {
	tests := []struct {
		name         string
		leftN, right int
		leftSz, rSz  int
	}{
		{"left batches larger", 10, 10, 4, 3},
		{"right batches larger", 10, 10, 3, 4},
		{"coprime sizes", 12, 12, 5, 7},
		{"single-element left", 7, 7, 1, 4},
		{"single-element right", 7, 7, 4, 1},
		{"left longer, misaligned", 11, 4, 3, 2},
		{"right longer, misaligned", 4, 11, 2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := make([]int, tt.leftN)
			for i := range left {
				left[i] = i
			}
			right := make([]string, tt.right)
			for i := range right {
				right[i] = fmt.Sprintf("r%d", i)
			}

			var want []string
			for i := range max(tt.leftN, tt.right) {
				switch {
				case i < tt.leftN && i < tt.right:
					want = append(want, fmt.Sprintf("B(%d,%s)", left[i], right[i]))
				case i < tt.leftN:
					want = append(want, fmt.Sprintf("L(%d)", left[i]))
				default:
					want = append(want, fmt.Sprintf("R(%s)", right[i]))
				}
			}

			got := zipRows(ZipLongest(sluice.Of(left, tt.leftSz), sluice.Of(right, tt.rSz)))
			if !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

// Rows are buffered until a batch is full, so an early stop is observable past
// sluice.DefaultBatchSize. Both sources must be released whichever tail is running.
func TestZipLongestEarlyStopReleasesBoth(t *testing.T) {
	tests := []struct {
		name         string
		leftN, right int
	}{
		{"both running", sluice.DefaultBatchSize + 100, sluice.DefaultBatchSize + 100},
		{"left tail", sluice.DefaultBatchSize + 100, 1},
		{"right tail", 1, sluice.DefaultBatchSize + 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var closedL, closedR bool
			mkL := func(n int) sluice.Stream[int] {
				return func(yield func(sluice.Batch[int]) bool) {
					defer func() { closedL = true }()
					for i := range n {
						if !yield(sluice.Batch[int]{Items: []int{i}}) {
							return
						}
					}
				}
			}
			mkR := func(n int) sluice.Stream[string] {
				return func(yield func(sluice.Batch[string]) bool) {
					defer func() { closedR = true }()
					for i := range n {
						if !yield(sluice.Batch[string]{Items: []string{strconv.Itoa(i)}}) {
							return
						}
					}
				}
			}

			batches := 0
			ZipLongest(mkL(tt.leftN), mkR(tt.right))(func(sluice.Batch[EitherOrBoth[int, string]]) bool {
				batches++
				return false
			})

			if batches != 1 {
				t.Errorf("consumer saw %d batches, want 1", batches)
			}
			if !closedL || !closedR {
				t.Errorf("sources not released: left=%v right=%v", closedL, closedR)
			}
		})
	}
}

func TestZipLongestBatchesOutput(t *testing.T) {
	const n = 2500
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	var sizes []int
	ZipLongest(sluice.Of(left, 100), sluice.Empty[string]())(func(b sluice.Batch[EitherOrBoth[int, string]]) bool {
		sizes = append(sizes, b.Len())
		return true
	})

	if want := []int{sluice.DefaultBatchSize, sluice.DefaultBatchSize, n - 2*sluice.DefaultBatchSize}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}
