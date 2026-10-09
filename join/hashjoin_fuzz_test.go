package join

import (
	"slices"
	"strconv"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// buildRow carries a key and a value that is distinct from it.
//
// Keying on the value itself would make the fuzz blind to exactly what needs
// checking: with int keys, two build rows under the same key produce identical
// output, so a reversed chain reads the same as a correct one. The position
// field is what makes the ordering observable.
type buildRow struct {
	key int
	pos int // distinct per row, so duplicate keys are still distinguishable
}

// joinReference is the specification written as a nested loop: for each probe
// row, walk the whole build side and emit a result per match. It is O(n·m) and
// would be unacceptable in the operator, which is what makes it a useful oracle
// — the real implementation trades a hash table and an index chain for that
// simplicity, and the trade is where a divergence would hide.
//
// The truncation matches the operator's sluice.DropNewest behaviour: the table holds
// the first limit rows and nothing after them.
func joinReference(probe []int, build []buildRow, limit int) []string {
	if len(build) > limit {
		build = build[:limit]
	}
	var out []string
	for _, p := range probe {
		for _, b := range build {
			if p == b.key {
				out = append(out, strconv.Itoa(p)+"/"+strconv.Itoa(b.pos))
			}
		}
	}
	return out
}

// FuzzJoinMatchesNestedLoop checks the hash join against that oracle.
//
// The index chain is the part worth fuzzing: it stores values newest-first and
// reverses them on read, so an off-by-one in the walk, a stale head after a
// key repeats, or a scratch buffer not reset would each produce a plausible
// wrong answer. Duplicate keys on both sides are the case that exercises all
// three at once, and the seeds lean on them.
func FuzzJoinMatchesNestedLoop(f *testing.F) {
	f.Add(1, 2, 3, 1, 2, 3)
	f.Add(1, 1, 1, 1, 1, 1)
	f.Add(1, 2, 3, 4, 5, 6)
	f.Add(0, 0, 1, 1, 0, 0)

	f.Fuzz(func(t *testing.T, p1, p2, p3, b1, b2, b3 int) {
		probe := []int{p1, p2, p3}
		// pos is the row's position, so two rows under one key stay
		// distinguishable in the output and a reordered chain is visible.
		build := []buildRow{{b1, 0}, {b2, 1}, {b3, 2}}

		// Limits around the build size, so truncation happens in some runs and
		// not others.
		for _, limit := range []int{1, 2, 3, 8} {
			// sluice.Batch size 2 so a key's rows straddle a batch boundary: the chain
			// has to survive that, which a per-batch implementation would not.
			got := sluice.Collect(StreamTable(
				sluice.Of(slices.Clone(probe), 2), sluice.Of(slices.Clone(build), 2),
				func(v int) int { return v },
				func(r buildRow) int { return r.key },
				func(a int, r buildRow) string {
					return strconv.Itoa(a) + "/" + strconv.Itoa(r.pos)
				},
				BuildLimit{MaxEntries: limit, OnOverflow: sluice.DropNewest}))

			want := joinReference(probe, build, limit)

			if !slices.Equal(got, want) {
				t.Errorf("join(probe=%v, build=%v, limit=%d) = %v, want %v",
					probe, build, limit, got, want)
			}
		}
	})
}

// FuzzMapTableRoundTrip asserts the table's own contract independently of the
// join: values come back under their key, in insertion order, and Len counts
// values rather than keys.
//
// Insertion order is the part the chain can quietly get wrong, since it stores
// newest-first and reverses on read. A reversed result still looks like a
// plausible join output, which is why this is asserted rather than assumed.
func FuzzMapTableRoundTrip(f *testing.F) {
	f.Add(1, 1, 2)
	f.Add(0, 0, 0)
	f.Add(-1, 5, -1)

	f.Fuzz(func(t *testing.T, k1, k2, k3 int) {
		tbl := newMapTable[int, string](4)

		keys := []int{k1, k2, k3}
		vals := []string{"a", "b", "c"}
		for i, k := range keys {
			tbl.Put(k, vals[i])
		}

		if tbl.Len() != len(keys) {
			t.Errorf("Len = %d, want %d: the table counts values, not keys", tbl.Len(), len(keys))
		}

		for _, k := range keys {
			var want []string
			for i, kk := range keys {
				if kk == k {
					want = append(want, vals[i])
				}
			}
			// Get returns a reused buffer, so compare before the next call.
			if got := tbl.Get(k); !slices.Equal(got, want) {
				t.Errorf("Get(%d) = %v, want %v in insertion order", k, got, want)
			}
		}

		// A key never inserted must report nothing rather than a stale buffer.
		missing := k1 - 1
		if !slices.Contains(keys, missing) {
			if got := tbl.Get(missing); got != nil {
				t.Errorf("Get on a missing key = %v, want nil", got)
			}
		}
	})
}
