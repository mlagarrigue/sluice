package bench

import "github.com/mlagarrigue/sluice"

// lend is the source the operator benchmarks read from: batches that are
// slices of src itself, as sluice.Of made them until 2026-11-19. Of now
// copies src into a buffer of its own, so that Map(Of(xs)) leaves xs intact;
// that copy is the source's cost, a few hundredths of a nanosecond per
// element, and an operator's figure is not the place to carry it. What
// entering a Stream through Of costs is BenchmarkStreamEntry's and
// BenchmarkPipelineDepth's, which keep Of.
func lend[T any](src []T, size int) sluice.Stream[T] {
	if size == 0 {
		size = sluice.DefaultBatchSize
	}
	return func(yield func(sluice.Batch[T]) bool) {
		for i := 0; i < len(src); i += size {
			if !yield(sluice.Batch[T]{Items: src[i:min(i+size, len(src))]}) {
				return
			}
		}
	}
}
