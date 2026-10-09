// Package probe counts what flows through the points of a pipeline: how many
// batches and elements each point has delivered downstream.
//
//	var in, out probe.Counter
//	s := probe.Probe(source, &in)
//	s = expensiveStage(s)
//	s = probe.Probe(s, &out)
//
// # What the difference between two probes says
//
// The lag between two probes — in.Elems() minus out.Elems(), read at any
// moment by any goroutine — is the number of elements that entered the stage
// between them and have not come out of it. What that number means depends on
// whether the stage crosses a goroutine boundary.
//
// In a synchronous pull pipeline it is volume, not time. Each batch travels
// the whole chain before the next is pulled, so every stage runs at exactly
// the same pace and none holds work up for another: the lag is what the stage
// consumed without emitting — the rows a [sluice.Filter] dropped, the elements
// a [sluice.Coalesce] is accumulating, the input an aggregate reduced. A large
// lag there is a selective or buffering stage, not a slow one. Which stage
// costs the time is a question for a CPU profile, or for timing the stages
// apart; no counter on a single goroutine can answer it.
//
// Across a boundary where the two sides run on different goroutines — an
// [parallel.Async] — the lag also counts the elements queued in transit, and
// there it does speak to pace: a lag that sits at the boundary's depth means
// the side after it is the slower one, a lag near zero means the side before
// it is. [parallel.Ordered] is not such a boundary: its source runs on the
// consumer's goroutine and its reordering ring is kept full whatever the
// pace, so the lag across it is up to its worker count in batches.
//
// The subtraction is the whole method; rendering it is the application's
// business, not this package's.
//
// This package lives outside the core deliberately. Apply the dependency
// test: remove it and nothing stops compiling — so it must not be in the
// core, whose rule is that it never knows its extensions.
package probe

import (
	"sync/atomic"

	"github.com/mlagarrigue/sluice"
)

// Counter accumulates what passed a probe point. The zero value is ready to
// use.
//
// Reads and writes are atomic: the pipeline increments from its consuming
// goroutine while any other goroutine — a ticker, a metrics scrape — reads.
// A read taken while a batch is mid-flight may be one batch stale, which is
// the accepted price: an observability counter is not worth a lock on the hot
// path.
type Counter struct {
	batches atomic.Int64
	elems   atomic.Int64
}

// Batches reports how many batches have been accepted downstream of the probe.
func (c *Counter) Batches() int64 { return c.batches.Load() }

// Elems reports how many elements have been accepted downstream of the probe.
func (c *Counter) Elems() int64 { return c.elems.Load() }

// Probe counts the batches and elements passing a point in the pipeline. It
// is a pass-through: the stream it returns yields exactly what s yields, in
// the same batches, with the same validity.
//
// The counters increment after the downstream yield returns true, so a count
// means "accepted downstream", not "offered downstream". That is what makes
// the lag arithmetic in the package documentation hold: a refused batch — the
// consumer stopping — is not counted, and the two probes around a stage can
// be subtracted without a phantom batch between them.
//
// The cost is two atomic adds per batch. Against the ~1.5 ns/element budget
// at 1024-element batches that is measured as indistinguishable from the
// pipeline without the probe (see BenchmarkProbe in internal/bench).
//
// Probe panics if c is nil, at construction rather than at first use: a nil
// counter is a wiring mistake, and the panic should point at the wiring.
func Probe[T any](s sluice.Stream[T], c *Counter) sluice.Stream[T] {
	if c == nil {
		panic("probe: Probe requires a non-nil Counter")
	}
	return func(yield func(sluice.Batch[T]) bool) {
		s(func(b sluice.Batch[T]) bool {
			if !yield(b) {
				return false
			}
			c.batches.Add(1)
			c.elems.Add(int64(b.Len()))
			return true
		})
	}
}
