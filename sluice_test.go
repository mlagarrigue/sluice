package sluice

import (
	"slices"
	"testing"
)

// collect gathers every element of a Stream. Batches may reuse a buffer, so
// values are copied as they are consumed.
func collect[T any](s Stream[T]) []T {
	var out []T
	s(func(b Batch[T]) bool {
		out = append(out, b.Items...)
		return true
	})
	return out
}

func TestOf(t *testing.T) {
	src := []int{1, 2, 3, 4, 5}
	got := collect(Of(src, 2))
	if !slices.Equal(got, src) {
		t.Errorf("Of = %v, want %v", got, src)
	}

	var sizes []int
	Of(src, 2)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{2, 2, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

func TestOfDefaultSize(t *testing.T) {
	src := make([]int, DefaultBatchSize+1)
	var n int
	Of(src, 0)(func(b Batch[int]) bool {
		n++
		return true
	})
	if n != 2 {
		t.Errorf("size<=0 must mean DefaultBatchSize: got %d batches, want 2", n)
	}
}

func TestMapFilterConvert(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6}

	got := collect(Map(Of(slices.Clone(src), 4), func(v int) int { return v * 10 }))
	if want := []int{10, 20, 30, 40, 50, 60}; !slices.Equal(got, want) {
		t.Errorf("Map = %v, want %v", got, want)
	}

	got = collect(Filter(Of(src, 4), func(v int) bool { return v%2 == 0 }))
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Errorf("Filter = %v, want %v", got, want)
	}

	strs := collect(Convert(Of(src, 4), func(v int) string {
		return string(rune('a' + v - 1))
	}))
	if want := []string{"a", "b", "c", "d", "e", "f"}; !slices.Equal(strs, want) {
		t.Errorf("Convert = %v, want %v", strs, want)
	}
}

func TestConcat(t *testing.T) {
	got := collect(Concat(Of([]int{1, 2}, 2), Empty[int](), Of([]int{3, 4}, 2)))
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Concat = %v, want %v", got, want)
	}
}

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

// An early stop must halt the source, not just the consumer: this is the
// invariant that guarantees upstream resource release.
func TestEarlyStop(t *testing.T) {
	produced := 0
	src := Stream[int](func(yield func(Batch[int]) bool) {
		for i := range 100 {
			produced++
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	consumed := 0
	Map(src, func(v int) int { return v })(func(b Batch[int]) bool {
		consumed += b.Len()
		return consumed < 3
	})

	if consumed != 3 {
		t.Errorf("consumed = %d, want 3", consumed)
	}
	if produced != 3 {
		t.Errorf("produced = %d, want 3: the source was not stopped", produced)
	}
}

// The generator's deferred call must run as soon as the consumer stops, without
// waiting for the stream to end — this is prompt resource finalization.
func TestPromptFinalization(t *testing.T) {
	closed := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { closed = true }()
		for i := range 100 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	Filter(src, func(int) bool { return true })(func(Batch[int]) bool {
		return false // stop right away
	})

	if !closed {
		t.Error("the source's deferred call did not run on early stop")
	}
}

func TestSplitPartition(t *testing.T) {
	// Each batch goes to a single branch, chosen from its first element.
	src := Of([]int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3, 5}) {
		t.Errorf("odd branch = %v, want [1 3 5]", got)
	}
}

func TestSplitBroadcast(t *testing.T) {
	// With no other branch attached, the consumed branch receives everything.
	src := Of([]int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("broadcast branch 0 = %v, want [1 2 3]", got)
	}
}

func TestSplitBounds(t *testing.T) {
	if got := Split(Empty[int](), 0, nil); got != nil {
		t.Error("Split with n<=0 must return nil")
	}
	// An out-of-range branch index is ignored, without panicking.
	branches := Split(Of([]int{1, 2}, 1), 1, func(Batch[int]) []int {
		return []int{0, 5, -1}
	})
	if got := collect(branches[0]); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("out-of-range indices mishandled: %v", got)
	}
}
