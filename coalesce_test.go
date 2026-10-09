package sluice

import (
	"slices"
	"testing"
)

func TestCoalesce(t *testing.T) {
	// A heavily fragmented source: one batch per element.
	src := []int{1, 2, 3, 4, 5, 6, 7}
	var sizes []int
	Coalesce(Of(src, 1), 3)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{3, 3, 1}; !slices.Equal(sizes, want) {
		t.Errorf("sizes after Coalesce = %v, want %v", sizes, want)
	}

	if got := collect(Coalesce(Of(src, 1), 3)); !slices.Equal(got, src) {
		t.Errorf("Coalesce altered the data: %v, want %v", got, src)
	}
}

func TestCoalesceDefaultSize(t *testing.T) {
	src := make([]int, DefaultBatchSize+1)
	var sizes []int
	Coalesce(Of(src, 1), 0)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{DefaultBatchSize, 1}; !slices.Equal(sizes, want) {
		t.Errorf("size<=0 must mean DefaultBatchSize: got %v, want %v", sizes, want)
	}
}

// Coalesce drops what it has accumulated when the consumer stops: the contract
// says so, and this pins it down.
func TestCoalesceEarlyStopDiscards(t *testing.T) {
	var got [][]int
	Coalesce(Of([]int{1, 2, 3, 4, 5}, 1), 2)(func(b Batch[int]) bool {
		got = append(got, slices.Clone(b.Items))
		return false // stop on the first full batch; 3,4,5 never surface
	})
	if len(got) != 1 || !slices.Equal(got[0], []int{1, 2}) {
		t.Errorf("got %v, want [[1 2]]", got)
	}
}

// Coalesce claims its buffer on the first element rather than up front, so a
// large size costs nothing on a stream that yields nothing. size is often a
// configuration value, and claiming it eagerly turned a large number into a
// large allocation before a single element had flowed.
func TestCoalesceDoesNotPreallocate(t *testing.T) {
	const huge = 1 << 20 // 8 MB if claimed eagerly for int64

	allocs := testing.AllocsPerRun(10, func() {
		Coalesce(Empty[int64](), huge)(func(Batch[int64]) bool { return true })
	})

	if allocs > 0 {
		t.Errorf("an empty stream allocated %.0f times with size=%d: the buffer is claimed up front", allocs, huge)
	}
}

// The other half of the deferred allocation: once an element does arrive, the
// buffer is claimed once at full size rather than grown into it. Letting append
// reach size would cost log(size) reallocations, each copying what is already
// buffered — paid on the nominal path, where data actually flows.
func TestCoalesceClaimsBufferOnce(t *testing.T) {
	const size = 1 << 16

	src := make([]int64, size)
	allocs := testing.AllocsPerRun(10, func() {
		Coalesce(Of(src, 64), size)(func(Batch[int64]) bool { return true })
	})

	// One allocation for the buffer. Growing into the same capacity would take
	// roughly log2(size) of them.
	if allocs > 1 {
		t.Errorf("Coalesce allocated %.0f times for one buffer of %d: it is growing rather than claiming once",
			allocs, size)
	}
}

// Filter emits empty batches by contract, to hold the upstream cadence, so
// Coalesce must skip them rather than treat them as anything. Worth its own
// case: an empty batch is the one input that reaches Coalesce carrying no
// element, and it must neither emit nor claim a buffer.
func TestCoalesceSkipsEmptyBatches(t *testing.T) {
	// Nothing passes the filter: every batch reaching Coalesce is empty.
	none := Filter(Of([]int{1, 2, 3, 4, 5, 6}, 2), func(int) bool { return false })

	var got [][]int
	Coalesce(none, 3)(func(b Batch[int]) bool {
		got = append(got, slices.Clone(b.Items))
		return true
	})
	if len(got) != 0 {
		t.Errorf("Coalesce emitted %v over empty batches, want nothing", got)
	}

	// Empty batches interleaved with real ones must not break the regrouping.
	odd := Filter(Of([]int{1, 2, 3, 4, 5, 6, 7, 8}, 2), func(v int) bool { return v%2 == 1 })
	if out := collect(Coalesce(odd, 3)); !slices.Equal(out, []int{1, 3, 5, 7}) {
		t.Errorf("Coalesce = %v, want [1 3 5 7]", out)
	}
}

// Deferring the allocation must not change what Coalesce emits: the batches it
// produces are still exactly size elements until the tail.
func TestCoalesceFillsToSize(t *testing.T) {
	const size = 300
	src := make([]int, 700)
	for i := range src {
		src[i] = i
	}

	var sizes []int
	got := make([]int, 0, len(src))
	Coalesce(Of(src, 7), size)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		got = append(got, b.Items...)
		return true
	})

	if want := []int{300, 300, 100}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
	if !slices.Equal(got, src) {
		t.Error("Coalesce altered the data while growing its buffer")
	}
}

// Coalesce passes size-element runs through without copying while its buffer is
// empty, and buffers the rest. The table walks the shapes that decide between
// the two paths: oversized input batches, exact fits, remainders carried into
// the next batch, and a buffer emptied mid-batch by a large arrival.
func TestCoalesceBatchShapes(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int // input batching over 1..10
		size      int // Coalesce target
		want      []int
	}{
		{"input larger than size", 7, 3, []int{3, 3, 3, 1}},
		{"input exactly size", 5, 5, []int{5, 5}},
		{"input several sizes worth", 10, 3, []int{3, 3, 3, 1}},
		{"remainder carried across batches", 4, 3, []int{3, 3, 3, 1}},
		{"fragments then flush", 2, 3, []int{3, 3, 3, 1}},
	}
	src := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sizes []int
			var got []int
			Coalesce(Of(src, tt.batchSize), tt.size)(func(b Batch[int]) bool {
				sizes = append(sizes, b.Len())
				got = append(got, b.Items...)
				return true
			})
			if !slices.Equal(sizes, tt.want) {
				t.Errorf("batch sizes = %v, want %v", sizes, tt.want)
			}
			if !slices.Equal(got, src) {
				t.Errorf("data altered: %v, want %v", got, src)
			}
		})
	}
}

// A buffer emptied mid-batch hands the rest of that batch back to the pass-
// through path: 2 buffered elements complete a batch of 3, then the remaining
// 6 elements go out as two uncopied runs, and 1 is buffered for the flush.
func TestCoalescePassThroughResumesMidBatch(t *testing.T) {
	first := []int{1, 2}
	second := []int{3, 4, 5, 6, 7, 8, 9, 10}
	var sizes []int
	var got []int
	Coalesce(Concat(Of(first, 2), Of(second, 8)), 3)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		got = append(got, b.Items...)
		return true
	})
	if want := []int{3, 3, 3, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
	if want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; !slices.Equal(got, want) {
		t.Errorf("data altered: %v, want %v", got, want)
	}
}
