package distinct

import (
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

func identityKey(v int) int { return v }

func TestDistinct(t *testing.T) {
	tests := []struct {
		name   string
		src    []int
		size   int
		window int
		want   []int
	}{
		{
			"window large enough for every key",
			[]int{1, 2, 1, 3, 2, 4},
			2, 10,
			[]int{1, 2, 3, 4},
		},
		{
			"no duplicates at all",
			[]int{1, 2, 3},
			2, 10,
			[]int{1, 2, 3},
		},
		{
			"everything is the same key",
			[]int{7, 7, 7, 7},
			2, 10,
			[]int{7},
		},
		{
			"duplicates span a batch boundary",
			[]int{1, 2, 2, 1},
			2, 10,
			[]int{1, 2},
		},
		{
			"empty stream",
			nil, 2, 10,
			nil,
		},
		{
			// A window of zero remembers nothing, so nothing is ever a
			// duplicate. Passing everything through is the honest reading.
			"window of zero passes everything",
			[]int{1, 1, 1},
			2, 0,
			[]int{1, 1, 1},
		},
		{
			"negative window passes everything",
			[]int{1, 1},
			2, -1,
			[]int{1, 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sluice.Collect(By(sluice.Of(tc.src, tc.size), tc.window, identityKey, sluice.DropOldest))
			if !slices.Equal(got, tc.want) {
				t.Errorf("By = %v, want %v", got, tc.want)
			}
		})
	}
}

// The operator is a window, not a global deduplication, and the difference is
// the trap its documentation exists to close. A key that left the window passes
// again — asserting that keeps the contract honest rather than aspirational.
func TestDistinctIsAWindowNotGlobal(t *testing.T) {
	// Window of 2. Keys: 1,2 fill it; 3 evicts 1; 1 is no longer remembered and
	// passes a second time.
	got := sluice.Collect(By(sluice.Of([]int{1, 2, 3, 1}, 4), 2, identityKey, sluice.DropOldest))
	if want := []int{1, 2, 3, 1}; !slices.Equal(got, want) {
		t.Errorf("By = %v, want %v: a key evicted from the window must pass again", got, want)
	}

	// The same stream with room for every key deduplicates it exactly.
	got = sluice.Collect(By(sluice.Of([]int{1, 2, 3, 1}, 4), 3, identityKey, sluice.DropOldest))
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("By = %v, want %v", got, want)
	}
}

func TestDistinctOverflowPolicies(t *testing.T) {
	// Window of 2 over 1,2,3,1: the policies differ on what happens at 3.
	src := []int{1, 2, 3, 1}

	t.Run("DropOldest evicts and tracks the new key", func(t *testing.T) {
		got := sluice.Collect(By(sluice.Of(src, 4), 2, identityKey, sluice.DropOldest))
		if want := []int{1, 2, 3, 1}; !slices.Equal(got, want) {
			t.Errorf("By = %v, want %v", got, want)
		}
	})

	t.Run("DropNewest keeps the window and lets the key through untracked", func(t *testing.T) {
		// 1,2 fill the window. 3 arrives, is not tracked, and passes. 1 is
		// still in the window, so it is dropped as a duplicate.
		got := sluice.Collect(By(sluice.Of(src, 4), 2, identityKey, sluice.DropNewest))
		if want := []int{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("By = %v, want %v", got, want)
		}
	})

	t.Run("Fail panics with ErrOverflow", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected a panic under the Fail policy")
			}
			if !streamtest.IsErr(r, sluice.ErrOverflow) {
				t.Errorf("panicked with %v, want ErrOverflow", r)
			}
		}()
		sluice.Collect(By(sluice.Of(src, 4), 2, identityKey, sluice.Fail))
	})
}

// A key repeated beyond the window must not trigger the sluice.Fail policy: the bound
// is on distinct keys held, not on elements seen.
func TestDistinctFailIgnoresDuplicates(t *testing.T) {
	got := sluice.Collect(By(sluice.Of([]int{1, 1, 1, 2, 2}, 5), 2, identityKey, sluice.Fail))
	if want := []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("By = %v, want %v: duplicates must not count against the bound", got, want)
	}
}

func TestDistinctRejectsBadArguments(t *testing.T) {
	tests := []struct {
		name string
		call func()
	}{
		{"nil key", func() { By[int, int](sluice.Empty[int](), 4, nil, sluice.DropOldest) }},
		{"undeclared policy", func() { By(sluice.Empty[int](), 4, identityKey, sluice.Overflow(99)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic at construction rather than a later surprise")
				}
			}()
			tc.call()
		})
	}
}

// A stream of unique keys far longer than the window loses nothing: eviction
// forgets keys, never elements. This does not assert the memory bound itself —
// that is structural (eviction fires at len(seen) == n) and is pinned
// behaviourally by TestRing and FuzzDistinctMatchesReference.
func TestDistinctLongUniqueStreamPassesEverything(t *testing.T) {
	const window = 8

	src := make([]int, 10_000)
	for i := range src {
		src[i] = i // every key distinct
	}

	got := sluice.Count(By(sluice.Of(src, 256), window, identityKey, sluice.DropOldest))
	if got != len(src) {
		t.Errorf("Count = %d, want %d: every key is unique, so none should be dropped", got, len(src))
	}
	// Memory is not observable from here; what is observable is that the ring
	// never grew past its capacity, which the ring test below covers directly.
}

func TestDistinctStringKeys(t *testing.T) {
	type order struct {
		ref  string
		line int
	}
	src := []order{{"A", 1}, {"B", 1}, {"A", 2}, {"C", 1}}

	got := sluice.Collect(By(sluice.Of(src, 2), 10, func(o order) string { return o.ref }, sluice.DropOldest))
	want := []order{{"A", 1}, {"B", 1}, {"C", 1}}
	if !slices.Equal(got, want) {
		t.Errorf("By = %v, want %v", got, want)
	}
}

func TestRing(t *testing.T) {
	t.Run("FIFO order", func(t *testing.T) {
		r := newRing[int](5)
		for i := range 5 {
			r.push(i)
		}
		got := make([]int, 0, 5)
		for range 5 {
			got = append(got, r.pop())
		}
		if want := []int{0, 1, 2, 3, 4}; !slices.Equal(got, want) {
			t.Errorf("pop order = %v, want %v", got, want)
		}
	})

	t.Run("wraps in place", func(t *testing.T) {
		r := newRing[int](3)
		for i := range 3 {
			r.push(i)
		}
		r.pop()
		r.pop()
		r.push(10)
		r.push(11)

		got := make([]int, 0, 3)
		for range 3 {
			got = append(got, r.pop())
		}
		if want := []int{2, 10, 11}; !slices.Equal(got, want) {
			t.Errorf("after wrapping, pop order = %v, want %v", got, want)
		}
	})

	t.Run("never grows past capacity", func(t *testing.T) {
		const capacity = 4
		r := newRing[int](capacity)
		for i := range 1000 {
			if r.size == capacity {
				r.pop()
			}
			r.push(i)
			if len(r.items) > capacity {
				t.Fatalf("backing array grew to %d, past the capacity of %d", len(r.items), capacity)
			}
		}
	})

	// An evicted key must not stay live in the backing array: for a string or
	// pointer key that is retention for as long as the stream runs.
	t.Run("pop clears the slot", func(t *testing.T) {
		r := newRing[string](2)
		r.push("held")
		r.push("other")
		r.pop()

		for _, v := range r.items {
			if v == "held" {
				t.Error("pop left the evicted key in the backing array")
			}
		}
	})
}

// The ring grows into its capacity rather than claiming it on the first push,
// so a window sized from configuration costs nothing on a stream that yields
// nothing. Growing has to preserve FIFO order across the copy — the entries may
// be wrapped when it happens, so a straight copy of the backing array would
// reorder them.
func TestRingGrowsPreservingOrder(t *testing.T) {
	const capacity = initialDistinct * 4

	r := newRing[int](capacity)
	if len(r.items) != 0 {
		t.Errorf("a new ring claimed %d entries, want 0", len(r.items))
	}

	// Wrap before growing: fill past the first allocation, drain part of it,
	// then push enough to force a copy while head is not at zero.
	for i := range initialDistinct {
		r.push(i)
	}
	for range initialDistinct / 2 {
		r.pop()
	}
	for i := initialDistinct; i < initialDistinct*2; i++ {
		r.push(i)
	}

	want := make([]int, 0, r.size)
	for i := initialDistinct / 2; i < initialDistinct*2; i++ {
		want = append(want, i)
	}

	got := make([]int, 0, len(want))
	for range want {
		got = append(got, r.pop())
	}
	if !slices.Equal(got, want) {
		t.Errorf("after growing, pop order = %v, want %v", got, want)
	}
	if len(r.items) > capacity {
		t.Errorf("backing array grew to %d, past the capacity of %d", len(r.items), capacity)
	}
}

// A window larger than the initial reservation must still deduplicate exactly
// across the growth, which is the behaviour the growth could quietly break.
func TestDistinctLargeWindow(t *testing.T) {
	const window = initialDistinct * 4

	src := make([]int, 0, window*2)
	for i := range window {
		src = append(src, i)
	}
	src = append(src, src[:window]...) // every key repeated once

	got := sluice.Count(By(sluice.Of(src, 64), window, identityKey, sluice.DropOldest))
	if got != window {
		t.Errorf("Count = %d, want %d: a window of %d must catch every repeat", got, window, window)
	}
}
