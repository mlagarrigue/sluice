// Package bench establishes the performance ceiling of Go 1.26 iteration
// primitives, a prerequisite to any architecture decision (see docs/ARCHITECTURE.md §13,
// step 0).
//
// Four questions:
//  1. What is the ceiling? (native loop = denominator of every measurement)
//  2. How much does an operator stage cost, and is the cost linear?
//  3. How much does iter.Pull really cost per value?
//  4. Does all-batch keep its promise against tuple-at-a-time?
package bench

import (
	"fmt"
	"iter"
	"testing"
)

// N is the dataset size for all fixed-volume benchmarks.
// Large enough to fall out of the L2 cache and measure a realistic regime.
const N = 1 << 20 // 1,048,576 elements

func data(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = int64(i)
	}
	return s
}

// sink prevents the compiler from eliminating the measured computations.
var sink int64

// ---------------------------------------------------------------------------
// Q1 — The ceiling: the native loop.
// Every other measurement is expressed as a percentage of this one.
// ---------------------------------------------------------------------------

func BenchmarkBaselineLoop(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		for _, v := range src {
			acc += v
		}
		sink = acc
	}
	reportPerElem(b, N)
}

// ---------------------------------------------------------------------------
// Q2 — Cost of a stage: element-wise iter.Seq, 0 to 4 Map stages.
// ---------------------------------------------------------------------------

type Seq[T any] iter.Seq[T]

func seqOf[T any](s []T) Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

func mapSeq[A, B any](s Seq[A], f func(A) B) Seq[B] {
	return func(yield func(B) bool) {
		s(func(a A) bool { return yield(f(a)) })
	}
}

func filterSeq[T any](s Seq[T], pred func(T) bool) Seq[T] {
	return func(yield func(T) bool) {
		s(func(v T) bool {
			if !pred(v) {
				return true
			}
			return yield(v)
		})
	}
}

func inc(v int64) int64 { return v + 1 }

// BenchmarkSeqStages measures the marginal overhead of each added stage.
// The slope of the line is the cost of one stage; the y-intercept
// is the cost of entering iter.Seq.
func BenchmarkSeqStages(b *testing.B) {
	src := data(N)
	for _, stages := range []int{0, 1, 2, 4} {
		b.Run(fmt.Sprintf("stages=%d", stages), func(b *testing.B) {
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := seqOf(src)
				for range stages {
					s = mapSeq(s, inc)
				}
				var acc int64
				s(func(v int64) bool {
					acc += v
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// ---------------------------------------------------------------------------
// Q4 — The heart of the matter: batch against element, at identical useful work.
// ---------------------------------------------------------------------------

type Batch[T any] struct {
	Items []T
}

type BatchSeq[T any] iter.Seq[Batch[T]]

func batchesOf[T any](s []T, size int) BatchSeq[T] {
	return func(yield func(Batch[T]) bool) {
		for i := 0; i < len(s); i += size {
			end := min(i+size, len(s))
			if !yield(Batch[T]{Items: s[i:end]}) {
				return
			}
		}
	}
}

// mapBatch applies f to the whole batch. This is the key point of the model:
// a single closure indirection per batch, and a tight inner loop
// that the compiler can optimize.
func mapBatch[T any](s BatchSeq[T], f func(T) T) BatchSeq[T] {
	return func(yield func(Batch[T]) bool) {
		s(func(b Batch[T]) bool {
			items := b.Items
			for i := range items {
				items[i] = f(items[i])
			}
			return yield(b)
		})
	}
}

// BenchmarkBatchVsElement compares the two models at an equal number of stages.
// This is the measurement that validates or invalidates §2.1 of the spec.
func BenchmarkBatchVsElement(b *testing.B) {
	for _, stages := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("element/stages=%d", stages), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := seqOf(src)
				for range stages {
					s = mapSeq(s, inc)
				}
				var acc int64
				s(func(v int64) bool {
					acc += v
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})

		b.Run(fmt.Sprintf("batch1024/stages=%d", stages), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := batchesOf(src, 1024)
				for range stages {
					s = mapBatch(s, inc)
				}
				var acc int64
				s(func(bt Batch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// BenchmarkBatchSize sweeps the batch size to locate the optimal plateau.
// The spec settles on 1024; DuckDB uses 2048, DataFusion 8192.
func BenchmarkBatchSize(b *testing.B) {
	sizes := []int{1, 8, 64, 256, 512, 1024, 2048, 4096, 8192, 65536, N}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := batchesOf(src, size)
				s = mapBatch(s, inc)
				s = mapBatch(s, inc)
				var acc int64
				s(func(bt Batch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// ---------------------------------------------------------------------------
// Q3 — iter.Pull: the real cost per value, and its amortization per batch.
// The spec quotes ~20ns/next() from a community source, undocumented by Go.
// ---------------------------------------------------------------------------

func BenchmarkPullPerElement(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, stop := iter.Pull(iter.Seq[int64](seqOf(src)))
		var acc int64
		for {
			v, ok := next()
			if !ok {
				break
			}
			acc += v
		}
		stop()
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkPullPerBatch: the same Pull, but pulled in batches.
// This is the argument of §7.3 — the coroutine cost amortized over 1024.
func BenchmarkPullPerBatch(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, stop := iter.Pull(iter.Seq[Batch[int64]](batchesOf(src, 1024)))
		var acc int64
		for {
			bt, ok := next()
			if !ok {
				break
			}
			for _, v := range bt.Items {
				acc += v
			}
		}
		stop()
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkMergeTwoStreams measures the real cost of an N->1 operator in both
// models. This is the direct validation of §7.3: is the batch merge
// really negligible?
func BenchmarkMergeTwoStreams(b *testing.B) {
	b.Run("element", func(b *testing.B) {
		a, c := data(N/2), data(N/2)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nextA, stopA := iter.Pull(iter.Seq[int64](seqOf(a)))
			nextC, stopC := iter.Pull(iter.Seq[int64](seqOf(c)))
			var acc int64
			okA, okC := true, true
			for okA || okC {
				if okA {
					var v int64
					if v, okA = nextA(); okA {
						acc += v
					}
				}
				if okC {
					var v int64
					if v, okC = nextC(); okC {
						acc += v
					}
				}
			}
			stopA()
			stopC()
			sink = acc
		}
		reportPerElem(b, N)
	})

	b.Run("batch1024", func(b *testing.B) {
		a, c := data(N/2), data(N/2)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nextA, stopA := iter.Pull(iter.Seq[Batch[int64]](batchesOf(a, 1024)))
			nextC, stopC := iter.Pull(iter.Seq[Batch[int64]](batchesOf(c, 1024)))
			var acc int64
			okA, okC := true, true
			for okA || okC {
				if okA {
					var bt Batch[int64]
					if bt, okA = nextA(); okA {
						for _, v := range bt.Items {
							acc += v
						}
					}
				}
				if okC {
					var bt Batch[int64]
					if bt, okC = nextC(); okC {
						for _, v := range bt.Items {
							acc += v
						}
					}
				}
			}
			stopA()
			stopC()
			sink = acc
		}
		reportPerElem(b, N)
	})
}

// ---------------------------------------------------------------------------
// Realistic case: selective filter followed by a transformation.
// Checks that the batch model holds up when batches empty out (see Coalesce, §7.5).
// ---------------------------------------------------------------------------

func BenchmarkSelectiveFilter(b *testing.B) {
	// keep=1 in 100: the case that motivates Coalesce.
	keep := func(v int64) bool { return v%100 == 0 }

	b.Run("element", func(b *testing.B) {
		src := data(N)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			s := filterSeq(seqOf(src), keep)
			s = mapSeq(s, inc)
			var acc int64
			s(func(v int64) bool {
				acc += v
				return true
			})
			sink = acc
		}
		reportPerElem(b, N)
	})

	b.Run("batch1024_nocoalesce", func(b *testing.B) {
		src := data(N)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Batch filter: produces sparse batches (10 elements out of 1024).
			out := make([]int64, 0, 1024)
			var acc int64
			batchesOf(src, 1024)(func(bt Batch[int64]) bool {
				out = out[:0]
				for _, v := range bt.Items {
					if keep(v) {
						out = append(out, v+1)
					}
				}
				for _, v := range out {
					acc += v
				}
				return true
			})
			sink = acc
		}
		reportPerElem(b, N)
	})
}

// reportPerElem expresses the result in nanoseconds per element — the unit
// that allows comparing benchmarks of different volumes.
func reportPerElem(b *testing.B, elems int) {
	b.Helper()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*elems), "ns/elem")
}
