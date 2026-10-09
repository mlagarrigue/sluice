package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/distinct"
	"github.com/mlagarrigue/sluice/parallel"
)

// Distinct costs a map lookup per element, so it is measured against the
// per-stage budget like any other O(1)-per-element operator. Parallel is not:
// it trades coordination for concurrency, and the figure that matters is where
// that trade starts paying rather than how it compares to a Map.

// BenchmarkDistinctAllUnique is the worst case for the map: every key misses,
// so every element inserts and the window churns.
func BenchmarkDistinctAllUnique(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		distinct.By(sluice.Of(src, 1024), 1024,
			func(v int64) int64 { return v }, sluice.DropOldest)(
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

// BenchmarkDistinctAllDuplicates is the other end: every key hits, so nothing
// is inserted or evicted after the first window fills.
func BenchmarkDistinctAllDuplicates(b *testing.B) {
	src := make([]int64, N) // every element is zero
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var n int
		distinct.By(sluice.Of(src, 1024), 1024,
			func(v int64) int64 { return v }, sluice.DropOldest)(
			func(bt sluice.Batch[int64]) bool {
				n += bt.Len()
				return true
			})
		sink = int64(n)
	}
	reportPerElem(b, N)
}

// parallelWork is a stand-in for the kind of f that justifies fanning out: a
// per-element cost large enough that coordination is not the dominant term.
func parallelWork(b sluice.Batch[int64], rounds int) sluice.Batch[int64] {
	out := make([]int64, len(b.Items))
	for i, v := range b.Items {
		acc := v
		for range rounds {
			acc = acc*2654435761 + 1
		}
		out[i] = acc
	}
	return sluice.Batch[int64]{Items: out}
}

// BenchmarkParallelSerialBaseline is the denominator for the fan-out figures:
// the same work, no concurrency, through the documented no-op path.
func BenchmarkParallelSerialBaseline(b *testing.B) {
	const rounds = 64

	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Ordered(sluice.Of(src, 1024), 1, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			return parallelWork(bt, rounds)
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// BenchmarkParallelHeavyWork is the case the operator exists for: f expensive
// enough that four goroutines beat one. Against the serial baseline above, this
// is what the fan-out actually buys.
func BenchmarkParallelHeavyWork(b *testing.B) {
	const rounds = 64

	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Ordered(sluice.Of(src, 1024), 4, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			return parallelWork(bt, rounds)
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// BenchmarkParallelTrivialWork is the case it does not: f cheap enough that
// coordination dominates. Documented so that "add Parallel to make it faster"
// is a claim with a number against it rather than an assumption.
func BenchmarkParallelTrivialWork(b *testing.B) {
	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Ordered(sluice.Of(src, 1024), 4, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			return bt
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// BenchmarkParallelTrivialSerial is that same trivial f with no fan-out, so the
// coordination overhead is readable as the gap between the two.
func BenchmarkParallelTrivialSerial(b *testing.B) {
	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Ordered(sluice.Of(src, 1024), 1, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			return bt
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// scratchWork stands for an f that needs a workspace per batch — a decoder's
// table, a staging buffer — sized to the batch. Under Parallel the workspace
// must be allocated per call; under ParallelInit it lives in the worker state
// and is reused. The output is fresh either way, as the output rule requires.
func scratchWork(scratch, in []int64) (reused, out []int64) {
	scratch = append(scratch[:0], in...)
	for i, v := range scratch {
		scratch[i] = v*2654435761 + 1
	}
	out = make([]int64, len(scratch))
	copy(out, scratch)
	return scratch, out
}

// BenchmarkParallelFreshScratch is the state ParallelInit exists to remove:
// the workspace is allocated on every batch because f may not keep one.
func BenchmarkParallelFreshScratch(b *testing.B) {
	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Ordered(sluice.Of(src, 1024), 4, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			_, out := scratchWork(nil, bt.Items)
			return sluice.Batch[int64]{Items: out}
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// BenchmarkParallelInitReusedScratch is the same work with the workspace owned
// by the worker: the allocation count is the figure being claimed.
func BenchmarkParallelInitReusedScratch(b *testing.B) {
	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.WithState(sluice.Of(src, 1024), 4,
			func() (*[]int64, error) { s := make([]int64, 0, 1024); return &s, nil },
			func(w *[]int64, bt sluice.Batch[int64]) sluice.Batch[int64] {
				var out []int64
				*w, out = scratchWork(*w, bt.Items)
				return sluice.Batch[int64]{Items: out}
			})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}

// skewedRounds makes every 4th batch 8x heavier: the shape that separates
// ordered from unordered fan-out. Under ordered emission the window stalls
// behind each heavy batch while the light ones sit finished; unordered lets
// them leave.
func skewedRounds(batch int) int {
	const base = 64
	if batch%4 == 0 {
		return 8 * base
	}
	return base
}

func runSkewed(wrap func(sluice.Stream[int64], func(sluice.Batch[int64]) sluice.Batch[int64]) sluice.Stream[int64]) int64 {
	src := data(N / 16)
	f := func(bt sluice.Batch[int64]) sluice.Batch[int64] {
		// The weight is derived from the data rather than from an arrival
		// counter, which workers would race on under the parallel forms. Of
		// slices src sequentially, so bt.Items[0] identifies the batch
		// deterministically whatever the interleaving.
		idx := int(bt.Items[0]) / 1024
		return parallelWork(bt, skewedRounds(idx))
	}
	var acc int64
	wrap(sluice.Of(src, 1024), f)(func(bt sluice.Batch[int64]) bool {
		for _, v := range bt.Items {
			acc += v
		}
		return true
	})
	return acc
}

// BenchmarkParallelSkewedSerial is the denominator for the skewed figures.
func BenchmarkParallelSkewedSerial(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sink = runSkewed(func(s sluice.Stream[int64], f func(sluice.Batch[int64]) sluice.Batch[int64]) sluice.Stream[int64] {
			return parallel.Ordered(s, 1, f)
		})
	}
}

// BenchmarkParallelSkewed: ordered fan-out on skewed work — the case §6.4's
// trade gives up.
func BenchmarkParallelSkewed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sink = runSkewed(func(s sluice.Stream[int64], f func(sluice.Batch[int64]) sluice.Batch[int64]) sluice.Stream[int64] {
			return parallel.Ordered(s, 4, f)
		})
	}
}

// BenchmarkParallelUnorderedSkewed: the same work with emission freed from
// input order — the case ParallelUnordered exists for.
func BenchmarkParallelUnorderedSkewed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sink = runSkewed(func(s sluice.Stream[int64], f func(sluice.Batch[int64]) sluice.Batch[int64]) sluice.Stream[int64] {
			return parallel.Unordered(s, 4, f)
		})
	}
}

// BenchmarkParallelUnorderedHeavyWork is the honesty pair: uniform work, where
// unordered emission has nothing to win over ordered. Compare against
// BenchmarkParallelHeavyWork in the same run.
func BenchmarkParallelUnorderedHeavyWork(b *testing.B) {
	const rounds = 64

	src := data(N / 16)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		parallel.Unordered(sluice.Of(src, 1024), 4, func(bt sluice.Batch[int64]) sluice.Batch[int64] {
			return parallelWork(bt, rounds)
		})(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
}
