package sluice

import (
	"slices"
	"testing"
)

func TestConcat(t *testing.T) {
	got := collect(Concat(Of([]int{1, 2}, 2), Empty[int](), Of([]int{3, 4}, 2)))
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Concat = %v, want %v", got, want)
	}
}

// Concat must not start the next stream once the consumer has stopped.
func TestMerge(t *testing.T) {
	tests := []struct {
		name string
		done Completion
		a, b []int
		want []int
	}{
		{"all/even lengths", WhenAll, []int{1, 2}, []int{10, 20}, []int{1, 10, 2, 20}},
		{"all/left shorter", WhenAll, []int{1}, []int{10, 20, 30}, []int{1, 10, 20, 30}},
		{"all/right shorter", WhenAll, []int{1, 2, 3}, []int{10}, []int{1, 10, 2, 3}},
		{"all/left empty", WhenAll, nil, []int{10, 20}, []int{10, 20}},
		{"all/right empty", WhenAll, []int{1, 2}, nil, []int{1, 2}},
		{"all/both empty", WhenAll, nil, nil, nil},
		// WhenAny stops at the first exhausted source, but batches already
		// pulled in the same round are still emitted.
		{"any/even lengths", WhenAny, []int{1, 2}, []int{10, 20}, []int{1, 10, 2, 20}},
		{"any/left shorter", WhenAny, []int{1}, []int{10, 20, 30}, []int{1, 10}},
		{"any/right shorter", WhenAny, []int{1, 2, 3}, []int{10}, []int{1, 10, 2}},
		{"any/left empty", WhenAny, nil, []int{10, 20}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collect(Merge(tt.done, Of(tt.a, 1), Of(tt.b, 1)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("Merge = %v, want %v", got, tt.want)
			}
		})
	}
}

// Merge holds one iter.Pull per source and no buffer, so it must work on
// sources that cannot be replayed — the case the operator exists for.
func TestMergeSinglePass(t *testing.T) {
	a, consumedA := singlePass(t, []int{1, 2, 3}, 1)
	b, consumedB := singlePass(t, []int{10, 20, 30}, 1)

	got := collect(Merge(WhenAll, a, b))

	if want := []int{1, 10, 2, 20, 3, 30}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
	if consumedA() != 3 || consumedB() != 3 {
		t.Errorf("consumed %d and %d elements, want 3 each", consumedA(), consumedB())
	}
}

// An early stop must release every source that was started, not just the one
// being read when the consumer stopped. A source that was never pulled from has
// not run at all, so there is nothing to release — that is the Stream contract,
// not a leak.
func TestMergeEarlyStopReleasesAll(t *testing.T) {
	var started, closed [3]bool
	mk := func(i, base int) Stream[int] {
		return func(yield func(Batch[int]) bool) {
			started[i] = true
			defer func() { closed[i] = true }()
			for n := range 100 {
				if !yield(Batch[int]{Items: []int{base + n}}) {
					return
				}
			}
		}
	}

	n := 0
	Merge(WhenAll, mk(0, 0), mk(1, 100), mk(2, 200))(func(Batch[int]) bool {
		n++
		return n < 5 // far enough in that every source has been pulled from
	})

	for i := range started {
		if !started[i] {
			t.Errorf("source %d was never started: the test does not prove anything", i)
			continue
		}
		if !closed[i] {
			t.Errorf("source %d was not released on early stop", i)
		}
	}
}

// Merge is lazy per source: a stop before a source's turn means that source
// never runs at all. Nothing is read and thrown away.
func TestMergeStopsBeforeUntouchedSource(t *testing.T) {
	started := false
	late := Stream[int](func(yield func(Batch[int]) bool) {
		started = true
		yield(Batch[int]{Items: []int{99}})
	})

	Merge(WhenAll, Of([]int{1, 2, 3}, 1), late)(func(Batch[int]) bool {
		return false // stop on the very first batch, before late's turn
	})

	if started {
		t.Error("Merge started a source the consumer never reached")
	}
}

// A panic crossing the consumer must not strand the sources: the deferred
// release runs on the way out, whatever the reason for leaving.
func TestMergeReleasesOnPanic(t *testing.T) {
	var closed [2]bool
	mk := func(i, base int) Stream[int] {
		return func(yield func(Batch[int]) bool) {
			defer func() { closed[i] = true }()
			for n := range 100 {
				if !yield(Batch[int]{Items: []int{base + n}}) {
					return
				}
			}
		}
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic did not propagate")
			}
		}()
		n := 0
		Merge(WhenAll, mk(0, 0), mk(1, 100))(func(Batch[int]) bool {
			n++
			if n == 3 {
				panic("consumer failure")
			}
			return true
		})
	}()

	for i, c := range closed {
		if !c {
			t.Errorf("source %d was not released when the consumer panicked", i)
		}
	}
}

func TestMergeNoStreams(t *testing.T) {
	for _, done := range []Completion{WhenAll, WhenAny} {
		if got := collect(Merge[int](done)); len(got) != 0 {
			t.Errorf("Merge(%v) with no source = %v, want nothing", done, got)
		}
	}
}

func TestMergeOrdersRoundRobin(t *testing.T) {
	// Three sources, one batch each per round: the interleaving is visible.
	got := collect(Merge(WhenAll,
		Of([]int{1, 2}, 1),
		Of([]int{10, 20}, 1),
		Of([]int{100, 200}, 1),
	))
	if want := []int{1, 10, 100, 2, 20, 200}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

// An undeclared Completion must not read as WhenAll in silence: Merge refuses
// it at construction, before any stream is touched, with or without sources.
func TestMergeRejectsUndeclaredCompletion(t *testing.T) {
	for _, done := range []Completion{Completion(255), Completion(2), Completion(7)} {
		for _, n := range []int{0, 2} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("Merge(%v) with %d sources did not panic", done, n)
					}
				}()
				streams := make([]Stream[int], n)
				for i := range streams {
					streams[i] = Of([]int{i}, 1)
				}
				_ = Merge(done, streams...)
			}()
		}
	}
}

func TestCompletionString(t *testing.T) {
	tests := []struct {
		in   Completion
		want string
	}{
		{WhenAll, "WhenAll"},
		{WhenAny, "WhenAny"},
		{Completion(7), "Completion(7)"},
	}
	for _, tt := range tests {
		if got := tt.in.String(); got != tt.want {
			t.Errorf("Completion(%d).String() = %q, want %q", tt.in, got, tt.want)
		}
	}
}

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
