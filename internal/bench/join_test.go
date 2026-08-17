package bench

import (
	"cmp"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// MergeJoinBy walks both inputs once in O(1) memory. These benchmarks establish
// what that costs against the ceiling, and — the point that was arbitrated
// against the spec — what the always-on sort check costs.

func idInt64(v int64) int64 { return v }

// evens builds a sorted slice of n values stepping by 2, offset by start.
// Joining two of them with different offsets controls the match rate.
func evens(n int, start int64) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = start + int64(i)*2
	}
	return s
}

func drainJoin(b *testing.B, s sluice.Stream[sluice.EitherOrBoth[int64, int64]]) {
	b.Helper()
	var acc int64
	s(func(bt sluice.Batch[sluice.EitherOrBoth[int64, int64]]) bool {
		for _, e := range bt.Items {
			if e.HasLeft {
				acc += e.Left
			}
			if e.HasRight {
				acc += e.Right
			}
		}
		return true
	})
	sink = acc
}

// BenchmarkJoinFullMatch: every key matches, one row on each side. This is the
// common case for a join on an identifier, and the one that must stay O(1).
func BenchmarkJoinFullMatch(b *testing.B) {
	left, right := evens(N/2, 0), evens(N/2, 0)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		drainJoin(b, sluice.MergeJoinBy(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
			idInt64, idInt64, cmp.Compare[int64],
		))
	}
	reportPerElem(b, N)
}

// BenchmarkJoinNoMatch: the two sides interleave without ever matching, so
// every row is emitted unmatched. Same volume, opposite branch.
func BenchmarkJoinNoMatch(b *testing.B) {
	left, right := evens(N/2, 0), evens(N/2, 1)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		drainJoin(b, sluice.MergeJoinBy(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
			idInt64, idInt64, cmp.Compare[int64],
		))
	}
	reportPerElem(b, N)
}

// BenchmarkJoinSortCheck isolates what the always-on monotonicity check costs.
// The spec wanted it behind a debug flag; this is the measurement that decides
// whether that complexity is worth it.
//
// The comparison is against a merge that does the same work without the check —
// Merge over the same two sorted inputs — so the difference is the check plus
// the join's own bookkeeping.
func BenchmarkJoinSortCheckBaseline(b *testing.B) {
	left, right := evens(N/2, 0), evens(N/2, 1)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.Merge(sluice.WhenAll,
			sluice.Of(left, 1024), sluice.Of(right, 1024),
		)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkJoinCrossProduct: keys repeated on both sides. This is the one path
// that allocates — the runs are buffered to be paired — so it is worth knowing
// what a duplicated key costs.
func BenchmarkJoinCrossProduct(b *testing.B) {
	// 8 rows per key on each side: 64 output rows per key.
	const dup = 8
	left := make([]int64, N/2)
	right := make([]int64, N/2)
	for i := range left {
		left[i] = int64(i / dup)
		right[i] = int64(i / dup)
	}
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		drainJoin(b, sluice.MergeJoinBy(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
			idInt64, idInt64, cmp.Compare[int64],
		))
	}
	reportPerElem(b, N)
}
