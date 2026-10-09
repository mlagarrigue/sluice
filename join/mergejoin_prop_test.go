package join

import (
	"cmp"
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// mjRow is a join row: the key it is sorted by, and an identity so every row of
// the output can be traced back to the input row it came from.
type mjRow struct{ key, id int }

func mjKey(r mjRow) int { return r.key }

// mjReference is the join written as the specification: keys ascending; for a
// key present on both sides, every (left, right) pair found by a nested loop,
// left-major; otherwise each row alone. It is quadratic per key, which is the
// point — nothing in it can lose a row by streaming.
func mjReference(left, right []mjRow) []EitherOrBoth[mjRow, mjRow] {
	var keys []int
	for _, r := range left {
		keys = append(keys, r.key)
	}
	for _, r := range right {
		keys = append(keys, r.key)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)

	var out []EitherOrBoth[mjRow, mjRow]
	for _, k := range keys {
		matched := false
		for _, l := range left {
			for _, r := range right {
				if l.key == k && r.key == k {
					out = append(out, EitherOrBoth[mjRow, mjRow]{Left: l, Right: r, HasLeft: true, HasRight: true})
					matched = true
				}
			}
		}
		if matched {
			continue
		}
		for _, l := range left {
			if l.key == k {
				out = append(out, EitherOrBoth[mjRow, mjRow]{Left: l, HasLeft: true})
			}
		}
		for _, r := range right {
			if r.key == k {
				out = append(out, EitherOrBoth[mjRow, mjRow]{Right: r, HasRight: true})
			}
		}
	}
	return out
}

// mjSorted draws n sorted rows over a small key range, so runs of equal keys —
// on one side, on both, of length one or many — are the common case.
func mjSorted(rng *streamtest.Rand, n, keys, idBase int) []mjRow {
	rows := make([]mjRow, n)
	for i := range rows {
		rows[i] = mjRow{key: rng.Intn(keys), id: idBase + i}
	}
	slices.SortStableFunc(rows, func(a, b mjRow) int { return cmp.Compare(a.key, b.key) })
	for i := range rows {
		rows[i].id = idBase + i
	}
	return rows
}

// FuzzMergeJoinByMatchesNestedLoop checks the full outer join, row for row and
// in order, against the nested-loop reference. sluice.Batch sizes are small and
// irregular, empty batches included, so equal-key runs straddle batch
// boundaries on both sides: one-to-one at a boundary, one-to-many streaming a
// run across several batches, and many-to-many buffering one.
func FuzzMergeJoinByMatchesNestedLoop(f *testing.F) {
	f.Add(uint64(1), uint8(20), uint8(20), uint8(4), uint8(3))
	f.Add(uint64(2), uint8(1), uint8(64), uint8(1), uint8(2))    // one-to-many
	f.Add(uint64(3), uint8(64), uint8(1), uint8(1), uint8(2))    // many-to-one
	f.Add(uint64(4), uint8(30), uint8(30), uint8(2), uint8(1))   // many-to-many, batch of one
	f.Add(uint64(5), uint8(0), uint8(10), uint8(5), uint8(3))    // left empty
	f.Add(uint64(6), uint8(40), uint8(40), uint8(200), uint8(5)) // mostly unique

	f.Fuzz(func(t *testing.T, seed uint64, nl, nr, keys, maxBatch uint8) {
		rng := streamtest.NewRand(seed)
		k := int(keys%64) + 1
		left := mjSorted(rng, int(nl), k, 0)
		right := mjSorted(rng, int(nr), k, 1000)
		mb := int(maxBatch%8) + 1

		got := sluice.Collect(Merge(
			streamtest.Chunks(left, mb, rng), streamtest.Chunks(right, mb, rng),
			mjKey, mjKey, cmp.Compare[int],
		))
		want := mjReference(left, right)
		if !slices.Equal(got, want) {
			t.Fatalf("Merge(%v, %v)\n got %v\nwant %v", left, right, got, want)
		}
	})
}

// mjCounted streams run rows under key 0 followed by one row under key 1,
// generated on demand in batches of size, and counts the rows handed out. A
// run of a million rows is never allocated: the count is what proves whether
// the operator materialised it.
func mjCounted(run, size int, pulled *int) sluice.Stream[mjRow] {
	return func(yield func(sluice.Batch[mjRow]) bool) {
		buf := make([]mjRow, 0, size)
		for i := 0; i <= run; i++ {
			key := 0
			if i == run {
				key = 1
			}
			buf = append(buf, mjRow{key: key, id: i})
			if len(buf) == size || i == run {
				*pulled += len(buf)
				if !yield(sluice.Batch[mjRow]{Items: buf}) {
					return
				}
				buf = buf[:0]
			}
		}
	}
}

// A one-to-many join streams the "many" run against the single row rather than
// buffering it: the first output batch arrives after a bounded number of rows
// has been pulled, whichever side holds the run. Before the fix the whole run
// of 1<<20 rows was pulled, and held, before the first row came out.
func TestMergeJoinByOneToManyStreams(t *testing.T) {
	const (
		run  = 1 << 20
		size = 256
		// One output batch is sluice.DefaultBatchSize rows, each consuming one row of
		// the run, plus at most the source batch the cursor is reading.
		bound = sluice.DefaultBatchSize + 2*size
	)
	single := []mjRow{{key: 0, id: -1}, {key: 1, id: -2}}

	tests := []struct {
		name string
		join func(many sluice.Stream[mjRow]) sluice.Stream[EitherOrBoth[mjRow, mjRow]]
	}{
		{"many on the right", func(many sluice.Stream[mjRow]) sluice.Stream[EitherOrBoth[mjRow, mjRow]] {
			return Merge(sluice.Of(single, 1), many, mjKey, mjKey, cmp.Compare[int])
		}},
		{"many on the left", func(many sluice.Stream[mjRow]) sluice.Stream[EitherOrBoth[mjRow, mjRow]] {
			return Merge(many, sluice.Of(single, 1), mjKey, mjKey, cmp.Compare[int])
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pulled := 0
			var first []EitherOrBoth[mjRow, mjRow]
			tt.join(mjCounted(run, size, &pulled))(func(b sluice.Batch[EitherOrBoth[mjRow, mjRow]]) bool {
				first = slices.Clone(b.Items)
				return false
			})
			if pulled > bound {
				t.Fatalf("pulled %d rows before the first output batch, want <= %d: the run was materialised", pulled, bound)
			}
			if len(first) != sluice.DefaultBatchSize {
				t.Fatalf("first batch holds %d rows, want %d", len(first), sluice.DefaultBatchSize)
			}
			for i, e := range first {
				if !e.Both() || e.Left.key != 0 || e.Right.key != 0 {
					t.Fatalf("row %d = %+v, want a key-0 match", i, e)
				}
			}
		})
	}
}

// Draining the whole one-to-many join still yields every pair, then the key-1
// match, with nothing lost at the run's batch boundaries.
func TestMergeJoinByOneToManyComplete(t *testing.T) {
	const run = 5000
	pulled := 0
	got := sluice.Collect(Merge(
		sluice.Of([]mjRow{{key: 0, id: -1}, {key: 1, id: -2}}, 1),
		mjCounted(run, 7, &pulled),
		mjKey, mjKey, cmp.Compare[int],
	))
	if len(got) != run+1 {
		t.Fatalf("got %d rows, want %d", len(got), run+1)
	}
	for i, e := range got[:run] {
		if !e.Both() || e.Left.id != -1 || e.Right.id != i {
			t.Fatalf("row %d = %+v, want left -1 paired with right %d", i, e, i)
		}
	}
	if last := got[run]; !last.Both() || last.Left.key != 1 || last.Right.key != 1 {
		t.Fatalf("last row = %+v, want the key-1 match", last)
	}
}
