package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// Async's two claims, each against its own control: the hand-off overhead is
// amortized to noise by the batch, and a producer-shaped stage overlapping a
// consumer-shaped one tends the wall clock toward the slower of the two
// instead of their sum.

// spinBatch stands in for CPU-shaped work attached to a batch — a decode on
// the producer side, an aggregation on the consumer side. The work is
// dependent so the compiler cannot elide it.
func spinBatch(seed int64, rounds int) int64 {
	acc := seed
	for range rounds {
		acc = acc*2654435761 + 1
	}
	return acc
}

// BenchmarkAsyncBaseline is the control: the same pipeline with no Async.
func BenchmarkAsyncBaseline(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
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

// BenchmarkAsyncPassThrough measures the operator's own cost: one goroutine
// hand-off and one batch copy per 1024 elements, on a pipeline doing nothing
// else.
func BenchmarkAsyncPassThrough(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Async(sluice.Of(src, 1024), 4)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// asyncWorkload is a source that works per batch, feeding a consumer that
// works per batch: the serial pipeline pays produce + consume in sequence,
// the decoupled one overlaps them.
func asyncWorkload(b *testing.B, wrap func(sluice.Stream[int64]) sluice.Stream[int64]) {
	b.Helper()
	const batches, rounds = 64, 1 << 16

	src := sluice.Stream[int64](func(yield func(sluice.Batch[int64]) bool) {
		buf := make([]int64, 1024)
		for i := range batches {
			buf[0] = spinBatch(int64(i), rounds) // the producer's work
			if !yield(sluice.Batch[int64]{Items: buf}) {
				return
			}
		}
	})

	var acc int64
	wrap(src)(func(bt sluice.Batch[int64]) bool {
		acc += spinBatch(bt.Items[0], rounds) // the consumer's work
		return true
	})
	sink = acc
}

// BenchmarkAsyncOverlapSerial: produce then consume, strictly in sequence.
func BenchmarkAsyncOverlapSerial(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		asyncWorkload(b, func(s sluice.Stream[int64]) sluice.Stream[int64] { return s })
	}
}

// BenchmarkAsyncOverlap: the same work with an Async between the two. Equal
// work on both sides is the best case; the figure to read is the ratio to the
// serial form, ideally approaching 2.
func BenchmarkAsyncOverlap(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		asyncWorkload(b, func(s sluice.Stream[int64]) sluice.Stream[int64] {
			return parallel.Async(s, 4)
		})
	}
}
