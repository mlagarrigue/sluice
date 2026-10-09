package bench

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/probe"
)

// The cost of stacking real sluice stages, as opposed to the model harness in
// BenchmarkBatchVsElement — that one measures locally defined operators and
// answers "is all-batch worth it", which is a different question from "what do
// the shipped operators cost".
//
// The figure to read is the slope: the marginal cost of one more stage, which
// is what the ~1.5 ns per-stage budget bounds. The intercept is the cost of
// entering a Stream at all, paid once however deep the pipeline.

// BenchmarkPipelineDepth stacks Map stages over one traversal. Map is the
// cheapest transforming operator — it writes in place and allocates nothing —
// so the slope isolates the machinery from the useful work.
func BenchmarkPipelineDepth(b *testing.B) {
	for _, stages := range []int{0, 1, 2, 4, 8} {
		b.Run(fmt.Sprintf("stages=%d", stages), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			for b.Loop() {
				s := sluice.Of(src, 1024)
				for range stages {
					s = sluice.Map(s, inc)
				}
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
		})
	}
}

// BenchmarkPipelineMixed stacks the operators a real pipeline actually mixes:
// a transform, a type change, a selection, a recompaction. Depth alone can hide
// a cost that only appears when a stage reads from a buffer another stage
// wrote — which is every stage but the first.
func BenchmarkPipelineMixed(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		s := sluice.Of(src, 1024)
		s = sluice.Map(s, inc)
		s = sluice.Filter(s, func(v int64) bool { return v&1 == 0 })
		c := sluice.Convert(s, func(v int64) int64 { return v >> 1 })
		var acc int64
		c(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkStreamEntry measures entering a Stream and doing nothing else: the
// intercept of the depth curve above. Of yields batches that are slices of the
// caller's array, so this is the floor for any sluice pipeline.
func BenchmarkStreamEntry(b *testing.B) {
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

// BenchmarkProbe answers the obligation probe.Probe's doc makes: two atomic
// adds per 1024-element batch must be indistinguishable from the pipeline
// without the probe. Compare against BenchmarkProbeBaseline in the same run.
func BenchmarkProbeBaseline(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		sluice.Map(sluice.Of(src, 1024), inc)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

func BenchmarkProbe(b *testing.B) {
	src := data(N)
	var c probe.Counter
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		probe.Probe(sluice.Map(sluice.Of(src, 1024), inc), &c)(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}
