package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// The O(1) operators of the architecture's taxonomy, measured against the ~1.5 ns per
// stage per element budget. Convention says every core operator carries a
// number rather than a claim.
//
// Each of these adds one stage to the same traversal BenchmarkCoalesceBaseline
// measures, so the figure to read is the gap to that baseline: it is the cost
// of the stage, which is what the budget bounds.
//
// Interleave is the exception and is measured separately: it pulls, so it is
// budgeted against iter.Pull rather than against a Map.

// BenchmarkPeek is the cheapest possible per-element stage: a call that
// transforms nothing. It isolates the cost of the indirect call itself, which
// every element-wise operator here pays on top of its own work.
func BenchmarkPeek(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Peek(sluice.Of(src, 1024), func(v int64) { acc += v })(func(bt sluice.Batch[int64]) bool {
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkScan writes one output element per input element into a reused
// buffer. It must not allocate per batch: the buffer is grown once and refilled.
func BenchmarkScan(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Scan(sluice.Of(src, 1024), int64(0), func(a, v int64) int64 { return a + v })(
			func(bt sluice.Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkFlatMap expands one element into one, the shape that isolates the
// operator's own cost from the expansion's. A scratch slice is returned so the
// benchmark measures FlatMap's copy rather than an allocation per element.
//
// It was once over the per-stage budget, at ~2.3 ns/element; it now sits
// inside it (figures in docs/benchmarks.md, which this comment does not
// repeat). The buffer is reserved once per stream: reserving it cut the
// allocations from 12 per run to 1.
func BenchmarkFlatMap(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		scratch := make([]int64, 1)
		var acc int64
		sluice.FlatMap(sluice.Of(src, 1024), func(v int64) []int64 {
			scratch[0] = v
			return scratch
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkDrop skips nothing and passes every batch through. Drop slices
// rather than copies, so the per-element cost should be indistinguishable from
// the baseline: this benchmark is what makes that claim falsifiable.
func BenchmarkDrop(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Drop(sluice.Of(src, 1024), 0)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkTake bounds the stream at its full length, so every batch flows and
// the operator pays its per-batch bookkeeping without ever truncating. Like
// Drop it slices rather than copies.
func BenchmarkTake(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Take(sluice.Of(src, 1024), N)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkTakeWhile is the per-element counterpart of Take: the predicate runs
// on every element, so this measures a call per element where Take pays one per
// batch. The gap between the two is the price of an element-grained bound.
func BenchmarkTakeWhile(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.TakeWhile(sluice.Of(src, 1024), func(int64) bool { return true })(
			func(bt sluice.Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkDropWhile drops the first element and then stops testing. Once the
// predicate has failed once, the operator is a pass-through, so this measures
// that steady state rather than the predicate.
func BenchmarkDropWhile(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.DropWhile(sluice.Of(src, 1024), func(v int64) bool { return v == 0 })(
			func(bt sluice.Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkInterleave2 pulls from two sources, alternating a batch at a time.
// It costs one iter.Pull resumption per batch — 68.6 ns amortised over 1024
// elements — which is the same trade Merge makes and the reason the operator is
// viable at batch grain and unthinkable at element grain.
func BenchmarkInterleave2(b *testing.B) {
	// Two independent halves rather than the same slice twice: identical
	// arguments read as an accident, and a shared backing array would let the
	// second traversal run on a cache already warmed by the first.
	src := data(N)
	left, right := src[:N/2], src[N/2:]
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Merge(sluice.WhenAll, sluice.Of(left, 1024), sluice.Of(right, 1024))(
			func(bt sluice.Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkInterleave8 checks how the cost scales with the number of sources.
// The rotation is a loop over a slice, so the per-element cost should be flat:
// the pull count per element does not change with the source count.
func BenchmarkInterleave8(b *testing.B) {
	const sources = 8

	// Eight independent slices, not one slice eight times: sharing a backing
	// array shrinks the working set eight-fold and re-reads warm cache lines,
	// which is the exact confound BenchmarkInterleave2 documents avoiding — a
	// flat per-element figure could then be cache warmth rather than the flat
	// rotation cost this benchmark claims to check.
	parts := make([][]int64, sources)
	for i := range parts {
		parts[i] = data(N / sources)
	}
	streams := make([]sluice.Stream[int64], sources)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		for i := range streams {
			streams[i] = sluice.Of(parts[i], 1024)
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
