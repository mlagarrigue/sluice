package sluice

import (
	"slices"
	"testing"
)

// collect gathers every element of a Stream. It is [Collect] under the name the
// tests already use; keeping the alias means the tests exercise the exported
// terminal rather than a private copy of it that could drift.
func collect[T any](s Stream[T]) []T { return Collect(s) }

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

func TestMapFilterMap(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6}

	got := collect(Map(Of(slices.Clone(src), 4), func(v int) int { return v * 10 }))
	if want := []int{10, 20, 30, 40, 50, 60}; !slices.Equal(got, want) {
		t.Errorf("Map = %v, want %v", got, want)
	}

	got = collect(Filter(Of(src, 4), func(v int) bool { return v%2 == 0 }))
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Errorf("Filter = %v, want %v", got, want)
	}

	letters := []string{"", "a", "b", "c", "d", "e", "f"}
	strs := collect(Convert(Of(src, 4), func(v int) string {
		return letters[v]
	}))
	if want := []string{"a", "b", "c", "d", "e", "f"}; !slices.Equal(strs, want) {
		t.Errorf("Convert = %v, want %v", strs, want)
	}
}

// Of must stop cutting batches as soon as the consumer refuses one.
func TestOfEarlyStop(t *testing.T) {
	n := 0
	Of([]int{1, 2, 3, 4, 5, 6}, 2)(func(Batch[int]) bool {
		n++
		return false
	})
	if n != 1 {
		t.Errorf("Of yielded %d batches after a stop, want 1", n)
	}
}

// Filter and Map reuse one buffer across batches and let it grow, so a
// source whose batches get bigger must not corrupt or truncate anything. The
// growth path is the one a fixed-size source never exercises.
func TestGrowingBatchesAreNotTruncated(t *testing.T) {
	// Batches of 1, 2, 3, ... elements: every batch is a new high-water mark.
	growing := Stream[int](func(yield func(Batch[int]) bool) {
		n := 1
		for v := 0; v < 60; {
			b := make([]int, 0, n)
			for range n {
				if v >= 60 {
					break
				}
				b = append(b, v)
				v++
			}
			if !yield(Batch[int]{Items: b}) {
				return
			}
			n++
		}
	})

	want := make([]int, 60)
	for i := range want {
		want[i] = i
	}

	if got := collect(Filter(growing, func(int) bool { return true })); !slices.Equal(got, want) {
		t.Errorf("Filter over growing batches = %v, want %v", got, want)
	}

	// Rebuild: the stream above is single-pass by construction.
	growing2 := Stream[int](func(yield func(Batch[int]) bool) {
		n := 1
		for v := 0; v < 60; {
			b := make([]int, 0, n)
			for range n {
				if v >= 60 {
					break
				}
				b = append(b, v)
				v++
			}
			if !yield(Batch[int]{Items: b}) {
				return
			}
			n++
		}
	})
	if got := collect(Map(growing2, func(v int) int { return v })); !slices.Equal(got, want) {
		t.Errorf("Map over growing batches = %v, want %v", got, want)
	}
}

// The operators that own a buffer reuse it between batches. That is the one
// core rule a caller can break silently — retaining Items without copying — so
// it is pinned here in both directions: a change either way is a contract
// change, and must not slip through unnoticed.
func TestBufferReuseContract(t *testing.T) {
	tests := []struct {
		name  string
		build func() Stream[int]
	}{
		{"Filter", func() Stream[int] {
			return Filter(Of([]int{1, 2, 3, 4}, 2), func(int) bool { return true })
		}},
		{"Convert", func() Stream[int] {
			return Convert(Of([]int{1, 2, 3, 4}, 2), func(v int) int { return v })
		}},
		{"Coalesce", func() Stream[int] {
			return Coalesce(Of([]int{1, 2, 3, 4}, 1), 2)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var kept [][]int
			tt.build()(func(b Batch[int]) bool {
				kept = append(kept, b.Items) // deliberately not copied
				return true
			})
			if len(kept) < 2 {
				t.Fatalf("need at least 2 batches to observe reuse, got %d", len(kept))
			}
			if &kept[0][0] != &kept[1][0] {
				t.Error("batches no longer share a buffer: the documented contract changed")
			}
		})
	}
}
