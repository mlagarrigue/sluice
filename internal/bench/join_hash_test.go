package bench

import (
	"cmp"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
)

// The hash join materialises one side, so what it costs is a map: one insert
// per build row and one lookup per probe row. The per-stage budget does not
// apply — it bounds stateless operators, where the only cost is the indirect
// call — but the comparison against MergeJoinBy does, because §9.2 claims the
// sorted-merge strategy does the same work in O(1) and that claim should carry
// a number.

const joinRows = 1 << 16

func joinData(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = int64(i)
	}
	return s
}

// BenchmarkJoinHash is the hash join over keys that all match: every probe row
// finds exactly one build row, so nothing is skipped and the figure is the
// operator rather than the selectivity.
func BenchmarkJoinHash(b *testing.B) {
	probe := joinData(joinRows)
	build := joinData(joinRows)

	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		join.StreamTable(
			sluice.Of(probe, 1024), sluice.Of(build, 1024),
			func(v int64) int64 { return v },
			func(v int64) int64 { return v },
			func(a, bv int64) int64 { return a + bv },
			join.BuildLimit{MaxEntries: joinRows, OnOverflow: sluice.Fail},
		)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, joinRows)
}

// BenchmarkJoinMerge is the same join over the same data through MergeJoinBy,
// which needs both inputs sorted — they are — and holds O(1) memory. §9.2 says
// this is the strategy to prefer when the inputs allow it; this is by how much.
func BenchmarkJoinMerge(b *testing.B) {
	left := joinData(joinRows)
	right := joinData(joinRows)

	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		join.Merge(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
			func(v int64) int64 { return v },
			func(v int64) int64 { return v },
			cmp.Compare[int64],
		)(func(bt sluice.Batch[join.EitherOrBoth[int64, int64]]) bool {
			for _, e := range bt.Items {
				if e.Both() {
					acc += e.Left + e.Right
				}
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, joinRows)
}

// BenchmarkJoinHashBuildOnly isolates the materialisation: a probe side of one
// batch against a full build side, so almost all the time is the table being
// filled.
func BenchmarkJoinHashBuildOnly(b *testing.B) {
	probe := joinData(1024)
	build := joinData(joinRows)

	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		join.StreamTable(
			sluice.Of(probe, 1024), sluice.Of(build, 1024),
			func(v int64) int64 { return v },
			func(v int64) int64 { return v },
			func(a, bv int64) int64 { return a + bv },
			join.BuildLimit{MaxEntries: joinRows, OnOverflow: sluice.Fail},
		)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}
