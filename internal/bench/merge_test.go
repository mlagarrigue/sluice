package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// §7.3 predicts a batched merge at ~0.39 ns/element, measured on a hand-rolled
// harness in step 0. These benchmarks check the real operator against that
// figure, and against a plain traversal of the same total volume.

// BenchmarkMergeOperatorBaseline traverses the two halves back to back, with no
// merging: the denominator for the two below.
func BenchmarkMergeOperatorBaseline(b *testing.B) {
	left, right := data(N/2), data(N/2)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		for _, src := range [][]int64{left, right} {
			sluice.Of(src, 1024)(func(bt sluice.Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
		}
		sink = acc
	}
	reportPerElem(b, N)
}

func BenchmarkMergeOperator(b *testing.B) {
	left, right := data(N/2), data(N/2)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s := sluice.Merge(sluice.WhenAll,
			sluice.Of(left, 1024),
			sluice.Of(right, 1024),
		)
		var acc int64
		s(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// Merging eight sources instead of two: the per-source cost is one iter.Pull,
// so the figure should stay flat rather than scale with the source count.
func BenchmarkMergeOperatorEight(b *testing.B) {
	const sources = 8
	parts := make([][]int64, sources)
	for i := range parts {
		parts[i] = data(N / sources)
	}
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		streams := make([]sluice.Stream[int64], sources)
		for i, p := range parts {
			streams[i] = sluice.Of(p, 1024)
		}
		var acc int64
		sluice.Merge(sluice.WhenAll, streams...)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}
