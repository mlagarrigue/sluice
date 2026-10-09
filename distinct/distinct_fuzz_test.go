package distinct

import (
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// distinctReference is the specification written as the simplest thing that
// could work: keep an ordered list of the keys currently remembered, evict the
// oldest when it is full. It is O(n) per element and would be unacceptable in
// the operator, which is exactly why it is a useful oracle — the real
// implementation trades a map and a ring for that simplicity, and the trade is
// where a subtle divergence would hide.
func distinctReference(src []int, window int, policy sluice.Overflow) []int {
	if window <= 0 {
		return slices.Clone(src)
	}
	var out []int
	var seen []int // oldest first

	for _, v := range src {
		if slices.Contains(seen, v) {
			continue
		}
		if len(seen) == window {
			switch policy {
			case sluice.DropOldest:
				seen = seen[1:]
			case sluice.DropNewest:
				out = append(out, v) // passes, untracked
				continue
			case sluice.Fail:
				panic(sluice.ErrOverflow)
			}
		}
		seen = append(seen, v)
		out = append(out, v)
	}
	return out
}

// FuzzDistinctMatchesReference checks the operator against that oracle for any
// input and any window.
//
// A sliding-window deduplication is easy to describe and easy to get subtly
// wrong: evicting on a duplicate rather than only on an insert, letting the
// ring drift out of step with the map, counting duplicates against the bound.
// Each of those passes the hand-written cases and fails somewhere a fuzzer
// reaches.
func FuzzDistinctMatchesReference(f *testing.F) {
	f.Add(1, 2, 1, 3, 2)
	f.Add(0, 0, 0, 0, 0)
	f.Add(1, 2, 3, 4, 5)
	f.Add(5, 4, 3, 2, 1)

	f.Fuzz(func(t *testing.T, a, b, c, d, e int) {
		src := []int{a, b, c, d, e}

		// Windows around the input size, so eviction happens in some runs and
		// not others, plus the degenerate ones.
		for _, window := range []int{-1, 0, 1, 2, 3, 5, 8} {
			for _, policy := range []sluice.Overflow{sluice.DropOldest, sluice.DropNewest} {
				// sluice.Batch size 2 so duplicates straddle batch boundaries: the
				// window has to survive them, which is the state a per-batch
				// implementation would lose.
				got := sluice.Collect(By(sluice.Of(slices.Clone(src), 2), window, identityKey, policy))
				want := distinctReference(src, window, policy)

				if !slices.Equal(got, want) {
					t.Errorf("By(%v, window=%d, %v) = %v, want %v",
						src, window, policy, got, want)
				}
			}
		}
	})
}

// FuzzDistinctWindowOfOneSuppressesOnlyRuns pins the bound for the smallest
// window: with a window of one, a key can only be suppressed if it repeats
// immediately, so any other suppression proves the operator remembered more
// than one key. It is the S1 bound observed through behaviour at window=1 —
// the general case, every window against the oracle, is
// FuzzDistinctMatchesReference above.
func FuzzDistinctWindowOfOneSuppressesOnlyRuns(f *testing.F) {
	f.Add(1, 2, 3)
	f.Add(1, 1, 1)
	f.Add(0, 1, 0)

	f.Fuzz(func(t *testing.T, a, b, c int) {
		src := []int{a, b, c}

		got := sluice.Collect(By(sluice.Of(slices.Clone(src), 1), 1, identityKey, sluice.DropOldest))

		// With a window of one, exactly the runs collapse. Written as a fold
		// over the previous value rather than an index lookup: the short-circuit
		// on i == 0 is obvious to a reader and not to a static analyser.
		want := make([]int, 0, len(src))
		for _, v := range src {
			if len(want) == 0 || want[len(want)-1] != v {
				want = append(want, v)
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("By(%v, window=1) = %v, want %v: only immediate repeats "+
				"can be suppressed by a window of one", src, got, want)
		}
	})
}
