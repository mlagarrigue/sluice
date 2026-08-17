package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// Coalesce regroups sparse batches back to a target size (ARCHITECTURE.md §7.5).
// BENCHMARK-STEP-0.md measured the case it exists for — a 1% filter — but never
// the operator itself: §7.5 concludes Coalesce "is not urgent" from the filter
// figures alone. These benchmarks put a number on the operator.
//
// They also pin the allocation behaviour the buffer strategy turns on. Coalesce
// claims its buffer when the first element arrives rather than up front, so
// size can be a large configuration value without costing anything on a stream
// that yields nothing. What that must not do is trade one allocation for the
// log(size) an append-grown buffer would take, on the nominal path where data
// does flow.

// BenchmarkCoalesceBaseline is the denominator: the same traversal and summing
// over already-full batches, with no Coalesce in the way.
func BenchmarkCoalesceBaseline(b *testing.B) {
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

// BenchmarkCoalescePassThrough feeds Coalesce batches that are already the
// target size. Nothing needs regrouping, so this is the floor: the cost of
// having the operator in the pipeline at all.
func BenchmarkCoalescePassThrough(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.Coalesce(sluice.Of(src, 1024), 1024)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkCoalesceFragmented is the case the operator exists for: a source
// yielding 8-element batches, recompacted to 1024. Every element is copied once
// into the buffer, which is the price being measured.
func BenchmarkCoalesceFragmented(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.Coalesce(sluice.Of(src, 8), 1024)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkCoalesceLargeSize uses a target size far above what a single
// pipeline would pick, standing in for a size that came from configuration.
// The allocation count is the figure of interest: one buffer, not log(size)
// reallocations copying what is already buffered.
func BenchmarkCoalesceLargeSize(b *testing.B) {
	const size = 1 << 16

	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.Coalesce(sluice.Of(src, 1024), size)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkCoalesceEmptyStream is the case the deferred allocation was made
// for: a large configured size on a stream that yields nothing at all. It must
// allocate nothing for the buffer, however large size is.
func BenchmarkCoalesceEmptyStream(b *testing.B) {
	const size = 1 << 20 // 8 MB if claimed eagerly for int64

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sluice.Coalesce(sluice.Empty[int64](), size)(func(sluice.Batch[int64]) bool {
			return true
		})
	}
}
