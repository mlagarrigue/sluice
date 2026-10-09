package sluice

import (
	"slices"
	"strconv"
	"testing"
)

func TestCollect(t *testing.T) {
	tests := []struct {
		name string
		in   Stream[int]
		want []int
	}{
		{"several batches", Of([]int{1, 2, 3, 4, 5}, 2), []int{1, 2, 3, 4, 5}},
		{"empty stream", Empty[int](), nil},
		{"empty batches only", Filter(Of([]int{1, 2}, 1), func(int) bool { return false }), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Collect(tc.in); !slices.Equal(got, tc.want) {
				t.Errorf("Collect = %v, want %v", got, tc.want)
			}
		})
	}
}

// Collect must copy: batches are reusable buffers, and Filter hands out the
// same one every time. Without the copy the result aliases that buffer and
// every batch reads as the last one.
func TestCollectCopiesReusedBuffers(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6}
	got := Collect(Filter(Of(src, 2), func(v int) bool { return true }))
	if want := src; !slices.Equal(got, want) {
		t.Errorf("Collect over a reused buffer = %v, want %v", got, want)
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		name string
		in   Stream[int]
		want int
	}{
		{"several batches", Of([]int{1, 2, 3, 4, 5}, 2), 5},
		{"empty stream", Empty[int](), 0},
		{"filtered to nothing", Filter(Of([]int{1, 2}, 1), func(int) bool { return false }), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Count(tc.in); got != tc.want {
				t.Errorf("Count = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReduce(t *testing.T) {
	sum := Reduce(Of([]int{1, 2, 3, 4}, 3), 0, func(a, v int) int { return a + v })
	if sum != 10 {
		t.Errorf("Reduce sum = %d, want 10", sum)
	}

	// The accumulator type differs from the element type: that is the point of
	// the second type parameter.
	joined := Reduce(Of([]int{1, 2, 3}, 2), "", func(a string, v int) string {
		return a + strconv.Itoa(v)
	})
	if joined != "123" {
		t.Errorf("Reduce concat = %q, want %q", joined, "123")
	}

	if got := Reduce(Empty[int](), 42, func(a, v int) int { return a + v }); got != 42 {
		t.Errorf("Reduce over an empty stream = %d, want the init value 42", got)
	}
}

func TestForEach(t *testing.T) {
	var got []int
	ForEach(Of([]int{1, 2, 3, 4}, 2), func(v int) bool {
		got = append(got, v)
		return true
	})
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("ForEach saw %v, want %v", got, want)
	}
}

// Returning false from ForEach must stop the source, not merely skip the
// remaining elements: the whole point is that the generator unwinds and its
// deferred calls run.
func TestForEachStopsTheSource(t *testing.T) {
	released := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { released = true }()
		for i := 0; i < 100; i += 2 {
			if !yield(Batch[int]{Items: []int{i, i + 1}}) {
				return
			}
		}
	})

	var seen []int
	ForEach(src, func(v int) bool {
		seen = append(seen, v)
		return v < 2
	})

	if want := []int{0, 1, 2}; !slices.Equal(seen, want) {
		t.Errorf("ForEach saw %v, want %v — it must stop at the element, not the batch", seen, want)
	}
	if !released {
		t.Error("ForEach stopped without letting the source unwind: deferred cleanup never ran")
	}
}

func TestFirst(t *testing.T) {
	tests := []struct {
		name     string
		in       Stream[int]
		want     int
		wantOK   bool
		wantSeen int // batches the source produced before First stopped it
	}{
		{"takes the head", Of([]int{7, 8, 9}, 2), 7, true, 1},
		{"empty stream", Empty[int](), 0, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := First(tc.in)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("First = (%d, %v), want (%d, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// A leading empty batch — what Filter emits to hold the cadence — must not be
// read as an empty stream.
func TestFirstSkipsEmptyBatches(t *testing.T) {
	src := Stream[int](func(yield func(Batch[int]) bool) {
		if !yield(Batch[int]{Items: []int{}}) {
			return
		}
		yield(Batch[int]{Items: []int{5, 6}})
	})
	got, ok := First(src)
	if !ok || got != 5 {
		t.Errorf("First = (%d, %v), want (5, true): a leading empty batch is not the end of the stream", got, ok)
	}
}

// First must terminate on an infinite source, which is the reason it stops
// rather than collecting.
func TestFirstStopsInfiniteSource(t *testing.T) {
	released := false
	infinite := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { released = true }()
		for i := 0; ; i++ {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	got, ok := First(infinite)
	if !ok || got != 0 {
		t.Errorf("First = (%d, %v), want (0, true)", got, ok)
	}
	if !released {
		t.Error("First did not let the infinite source unwind")
	}
}

// Collect's doc promises log(n) reallocations, 15 for 100 000 elements at
// DefaultBatchSize. The bound here is loose on purpose — append's growth
// factor is the runtime's to tune — but a per-batch or per-element
// allocation would blow through it by two orders of magnitude.
func TestCollectAllocationsAreLogarithmic(t *testing.T) {
	src := make([]int64, 100_000)
	s := Of(src, DefaultBatchSize)
	got := testing.AllocsPerRun(5, func() { _ = Collect(s) })
	if got > 20 {
		t.Errorf("Collect of 100000 elements made %.0f allocations, want about 15 (log n)", got)
	}
	t.Logf("%.0f allocations", got)
}
