package bench

import (
	"iter"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// Split drives one shared traversal and hands each branch a slot of one batch
// (ARCHITECTURE.md §6.3, ADR 0001). The O(1) backlog was stated but never
// measured — BENCHMARK-STEP-0.md lists it as an open item. These benchmarks
// close it.
//
// The reference point is a plain batched pipeline over the same data: whatever
// Split adds over that is the price of the fan-out.

// BenchmarkSplitBaseline is the denominator for the two below: the same
// traversal and the same summing, with no Split in the way.
func BenchmarkSplitBaseline(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.Of(src, 1024)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkSplitPartition routes each batch to one of two branches, consuming a
// single branch. This is the cheapest mode: no alternation, half the data.
func BenchmarkSplitPartition(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n := 0
		branches := sluice.Split(sluice.Of(src, 1024), 2, func(sluice.Batch[int64]) []int {
			n++
			return []int{n % 2}
		})
		var acc int64
		branches[0](func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkSplitBroadcast is the demanding case: two branches read at once,
// advanced in alternation through iter.Pull. Every element crosses the fan-out
// twice, so the per-element figure covers 2N element-visits over N elements of
// source.
func BenchmarkSplitBroadcast(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		branches := sluice.Split(sluice.Of(src, 1024), 2, func(sluice.Batch[int64]) []int {
			return []int{0, 1}
		})
		next0, stop0 := iter.Pull(iter.Seq[sluice.Batch[int64]](branches[0]))
		next1, stop1 := iter.Pull(iter.Seq[sluice.Batch[int64]](branches[1]))

		var acc int64
		for {
			b0, ok0 := next0()
			if ok0 {
				for _, v := range b0.Items {
					acc += v
				}
			}
			b1, ok1 := next1()
			if ok1 {
				for _, v := range b1.Items {
					acc += v
				}
			}
			if !ok0 && !ok1 {
				break
			}
		}
		stop0()
		stop1()
		sink = acc
	}
	reportPerElem(b, N)
}
