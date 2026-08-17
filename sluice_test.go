package sluice

import (
	"iter"
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

	letters := []string{"", "a", "b", "c", "d", "e", "f"}
	strs := collect(Convert(Of(src, 4), func(v int) string {
		return letters[v]
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

// Every operator must propagate an early stop to its source, and let the
// source's deferred calls run at once. This is the invariant the whole model
// rests on: swallowing the false breaks upstream resource release silently.
//
// The table covers every operator rather than a representative one, because the
// bug this guards against is per-operator — one forgotten return is enough.
func TestEarlyStopPropagates(t *testing.T) {
	// wrap builds the operator under test on top of the instrumented source.
	tests := []struct {
		name string
		wrap func(Stream[int]) Stream[int]
	}{
		{"Map", func(s Stream[int]) Stream[int] {
			return Map(s, func(v int) int { return v })
		}},
		{"Filter", func(s Stream[int]) Stream[int] {
			return Filter(s, func(int) bool { return true })
		}},
		{"Convert", func(s Stream[int]) Stream[int] {
			return Convert(s, func(v int) int { return v })
		}},
		{"Coalesce", func(s Stream[int]) Stream[int] {
			return Coalesce(s, 1)
		}},
		{"Concat/first", func(s Stream[int]) Stream[int] {
			return Concat(s, Of([]int{97, 98, 99}, 1))
		}},
		{"Concat/only", func(s Stream[int]) Stream[int] {
			return Concat(s)
		}},
		{"Split", func(s Stream[int]) Stream[int] {
			return Split(s, 1, func(Batch[int]) []int { return []int{0} })[0]
		}},
		{"identity", func(s Stream[int]) Stream[int] { return s }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				produced int
				closed   bool
			)
			src := Stream[int](func(yield func(Batch[int]) bool) {
				defer func() { closed = true }()
				for i := range 100 {
					produced++
					if !yield(Batch[int]{Items: []int{i}}) {
						return
					}
				}
			})

			consumed := 0
			tt.wrap(src)(func(b Batch[int]) bool {
				consumed += b.Len()
				return consumed < 3
			})

			if consumed != 3 {
				t.Errorf("consumed = %d, want 3", consumed)
			}
			// The source may run one batch ahead: what must not happen is it
			// running to completion.
			if produced > 4 {
				t.Errorf("produced = %d, want <= 4: the source was not stopped", produced)
			}
			if !closed {
				t.Error("the source's deferred call did not run on early stop")
			}
		})
	}
}

// Concat must not start the next stream once the consumer has stopped.
func TestConcatEarlyStopSkipsRest(t *testing.T) {
	started := false
	second := Stream[int](func(yield func(Batch[int]) bool) {
		started = true
		yield(Batch[int]{Items: []int{9}})
	})

	Concat(Of([]int{1, 2, 3}, 1), second)(func(Batch[int]) bool {
		return false // stop on the very first batch
	})

	if started {
		t.Error("Concat consumed the next stream after an early stop")
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

// Of shares the caller's slice and Map writes in place, so composing them
// overwrites the input. Documented, and pinned here because it surprises.
func TestMapWritesThroughToCallerSlice(t *testing.T) {
	src := []int{1, 2, 3}
	collect(Map(Of(src, 3), func(v int) int { return v * 10 }))

	if !slices.Equal(src, []int{10, 20, 30}) {
		t.Errorf("caller slice = %v, want [10 20 30]", src)
	}
}

// pullBranch drives a branch one batch at a time, which is how concurrent
// branches must be consumed.
func pullBranch[T any](s Stream[T]) (next func() (Batch[T], bool), stop func()) {
	return iter.Pull(iter.Seq[Batch[T]](s))
}

// alternate consumes two branches in lock-step and returns what each received.
func alternate(a, b Stream[int]) (gotA, gotB []int) {
	nextA, stopA := pullBranch(a)
	defer stopA()
	nextB, stopB := pullBranch(b)
	defer stopB()

	for {
		ba, okA := nextA()
		if okA {
			gotA = append(gotA, ba.Items...)
		}
		bb, okB := nextB()
		if okB {
			gotB = append(gotB, bb.Items...)
		}
		if !okA && !okB {
			return gotA, gotB
		}
	}
}

func TestSplitPartition(t *testing.T) {
	// Each batch goes to a single branch, chosen from its first element.
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	odd, even := alternate(branches[0], branches[1])
	if !slices.Equal(odd, []int{1, 3, 5}) {
		t.Errorf("odd branch = %v, want [1 3 5]", odd)
	}
	if !slices.Equal(even, []int{2, 4, 6}) {
		t.Errorf("even branch = %v, want [2 4 6]", even)
	}
}

// A partition may be consumed one branch at a time: batches routed to a branch
// nobody is reading are dropped rather than stalling the pipeline.
func TestSplitPartitionSingleBranch(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
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

// The point of the rewrite: broadcast works on a source that cannot be
// replayed, and the source is traversed exactly once.
func TestSplitBroadcastSinglePass(t *testing.T) {
	src, consumed := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	got0, got1 := alternate(branches[0], branches[1])
	want := []int{1, 2, 3}
	if !slices.Equal(got0, want) {
		t.Errorf("branch 0 = %v, want %v", got0, want)
	}
	if !slices.Equal(got1, want) {
		t.Errorf("branch 1 = %v, want %v", got1, want)
	}
	if consumed() != 3 {
		t.Errorf("source produced %d elements, want 3: it was traversed more than once", consumed())
	}
}

// Draining one branch while another is mid-consumption cannot work with a
// one-batch slot. It must fail loudly rather than yield a silent prefix.
func TestSplitStalls(t *testing.T) {
	defer func() {
		switch r := recover(); r {
		case ErrSplitStalled:
		case nil:
			t.Error("draining branches sequentially should have panicked")
		default:
			t.Errorf("panicked with %v, want ErrSplitStalled", r)
		}
	}()

	src, _ := singlePass(t, []int{1, 2, 3, 4}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	next1, stop1 := pullBranch(branches[1])
	defer stop1()
	next1() // branch 1 is now live, holding nothing

	collect(branches[0]) // drains branch 0 while branch 1 lags: stalls
}

// A branch is single-pass, like the stream it comes from.
func TestSplitBranchNotReplayable(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 1, func(Batch[int]) []int { return []int{0} })

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("first pass = %v, want [1 2 3]", got)
	}
	if got := collect(branches[0]); len(got) != 0 {
		t.Errorf("second pass = %v, want nothing: a branch is single-pass", got)
	}
}

// The shared traversal must be stopped exactly once, however many branches
// finish, and only once the last live branch is gone.
func TestSplitReleasesSourceOnce(t *testing.T) {
	stops := 0
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { stops++ }()
		for i := range 4 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })
	got0, got1 := alternate(branches[0], branches[1])

	if len(got0) != 4 || len(got1) != 4 {
		t.Fatalf("branches got %d and %d batches, want 4 each", len(got0), len(got1))
	}
	if stops != 1 {
		t.Errorf("source finalized %d times, want exactly 1", stops)
	}
}

// A branch that stops early must not cut off a branch that has not started
// yet: the caller is allowed to finish with one branch before opening the next.
func TestSplitEarlyStopKeepsSiblingUsable(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	n := 0
	branches[0](func(Batch[int]) bool {
		n++
		return false // take one batch, then stop
	})

	if got := collect(branches[1]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("sibling branch = %v, want [1 2 3]", got)
	}
}

// Abandoning a branch must still let the source be finalized, otherwise the
// pull coroutine outlives the pipeline.
func TestSplitAbandonedBranchReleasesSource(t *testing.T) {
	finalized := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { finalized = true }()
		for i := range 100 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })
	n := 0
	branches[0](func(Batch[int]) bool {
		n++
		return n < 2
	})
	// branches[1] is never consumed.

	if !finalized {
		t.Error("the source was not finalized after every live branch stopped")
	}
}

// route is the caller's function and may carry state — round-robin, the mode
// the documentation recommends, is exactly that. Calling it more than once per
// batch would advance that state twice and misroute the data.
func TestSplitRoutesOncePerBatch(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)

	calls := 0
	branches := Split(src, 2, func(Batch[int]) []int {
		calls++
		return []int{calls % 2} // round-robin: relies on being called once
	})

	got0, got1 := alternate(branches[0], branches[1])

	if calls != 6 {
		t.Errorf("route called %d times for 6 batches, want 6", calls)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got0, want) {
		t.Errorf("branch 0 = %v, want %v", got0, want)
	}
	if want := []int{1, 3, 5}; !slices.Equal(got1, want) {
		t.Errorf("branch 1 = %v, want %v", got1, want)
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

func TestSplitNilRoute(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Split with a nil route must panic rather than dereference it")
		}
	}()
	Split(Of([]int{1}, 1), 1, nil)
}

// A batch routed nowhere stops nothing: the remaining batches still flow.
func TestSplitRouteToNothing(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4}, 1)
	branches := Split(src, 1, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return nil // drop even batches
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3}) {
		t.Errorf("got %v, want [1 3]", got)
	}
}
