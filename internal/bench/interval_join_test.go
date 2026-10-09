package bench

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
)

// IntervalJoin walks two time-ordered streams with a bounded buffer per the
// match window. The figures establish what the temporal predicate costs
// against the merge join's plain equality — the same walk, plus the window
// arithmetic, the buffer bookkeeping and the eviction frontier.

// intervalRow spreads N elements over 64 keys with one row per second per
// stream, so a ±2s window pairs each row with a handful of partners.
func intervalRows(n int) []int64 {
	rows := make([]int64, n)
	for i := range rows {
		rows[i] = int64(i)
	}
	return rows
}

func benchIntervalJoin(b *testing.B, lower, upper time.Duration) {
	b.Helper()
	// Two identical slices rather than one shared: with a single backing
	// array both sides would walk the same cache lines, and the figure would
	// flatter the join with locality a real pair of streams does not have.
	left := intervalRows(N / 16)
	right := intervalRows(N / 16)
	key := func(v int64) int64 { return v % 64 }
	ts := func(v int64) time.Time { return time.Unix(v/64, 0) }
	limit := join.BuildLimit{MaxEntries: 1 << 20, OnOverflow: sluice.Fail}

	b.SetBytes(int64(len(left)) * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		join.Interval(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
			key, key, ts, ts, lower, upper,
			func(l, r int64) int64 { return l + r },
			limit,
		)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, len(left))
}

// BenchmarkIntervalJoinNarrow: a zero-width window — every row pairs exactly
// with its same-instant partner on the other side. The floor of the operator.
func BenchmarkIntervalJoinNarrow(b *testing.B) {
	benchIntervalJoin(b, 0, 0)
}

// BenchmarkIntervalJoinWide: a ±2s window — each row buffers longer, evicts
// later, and pairs with several partners. The figure moves with the output
// cardinality, which is the point: O(throughput × interval) is paid in rows.
func BenchmarkIntervalJoinWide(b *testing.B) {
	benchIntervalJoin(b, -2*time.Second, 2*time.Second)
}
