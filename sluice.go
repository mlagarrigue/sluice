// Package sluice is a dataflow engine: a single data model — the Stream — for
// web serving, database access and ETL.
//
// A sluice regulates flow, and that is what this package does. The consumer
// sets the pace; the producer only produces what is pulled from it. This
// back-pressure is not a mechanism bolted on, it follows from the shape of the
// type.
//
// # The core
//
// Two types, and nothing else:
//
//	Stream[T] — a sequence of batches, pulled by the consumer
//	Batch[T]  — a batch of values, the unit of transport
//
// Everything else in the framework (diagnostics, joins, connectors, HTTP) is
// built on top without the core knowing about it. That is deliberate: a core
// that knows its extensions can no longer evolve.
//
// # Why batches
//
// The batch is the unit of transport, never the element. A function that
// handles N elements efficiently handles 1 without effort; the converse does
// not hold. Measured in this repository: a batched pipeline is ~2x faster than
// an element-wise one for identical useful work, and the gap widens to 179x for
// operators that must pull from several streams at once. See
// docs/BENCHMARK-STEP-0.md.
//
// # Zero dependencies
//
// Production code imports the standard library only.
package sluice

import "iter"

// DefaultBatchSize is the default number of elements per batch.
//
// Measurement (docs/BENCHMARK-STEP-0.md) shows a performance plateau from 8
// elements upward, flat through one million: this setting is safe but not
// critical. A batch thinned to a few dozen elements by a selective filter does
// not degrade throughput.
const DefaultBatchSize = 1024

// Batch is a group of values, the framework's unit of transport.
//
// Items may be a slice borrowed from a reusable buffer, valid only for the
// duration of the call that receives it. An operator that keeps Items beyond
// that call must copy it. This is the one core rule a caller can break
// silently.
type Batch[T any] struct {
	Items []T
}

// Len reports the number of elements in the batch.
func (b Batch[T]) Len() int { return len(b.Items) }

// Stream is a sequence of batches whose pace the consumer controls.
//
// It is an iter.Seq: it works with range and composes with the standard
// library.
//
//	for b := range s {
//	    for _, v := range b.Items {
//	        // ...
//	    }
//	}
//
// Early stop: the consumer returns false, the generator unwinds and its
// deferred calls run immediately. Every operator must propagate that false and
// let the generator return — swallowing it breaks upstream resource release,
// silently.
//
// A Stream cannot be replayed. Consuming it twice runs it twice, or yields
// nothing at all if its source is single-pass. To feed two consumers, use
// [Split].
type Stream[T any] iter.Seq[Batch[T]]

// Of builds a Stream from a slice, cut into batches of size elements.
//
// Batches share src's memory: they stay valid as long as src does. A size of
// zero or less means [DefaultBatchSize].
func Of[T any](src []T, size int) Stream[T] {
	if size <= 0 {
		size = DefaultBatchSize
	}
	return func(yield func(Batch[T]) bool) {
		for i := 0; i < len(src); i += size {
			if !yield(Batch[T]{Items: src[i:min(i+size, len(src))]}) {
				return
			}
		}
	}
}

// Empty is a Stream with no elements.
func Empty[T any]() Stream[T] {
	return func(func(Batch[T]) bool) {}
}

// Map applies f to every element, in place within the batch.
//
// The transformation runs per batch: one closure indirection for all elements,
// then a tight loop the compiler can optimize. This is what makes the batched
// model faster than the element-wise one.
//
// f must not retain a reference to the element beyond the call.
func Map[T any](s Stream[T], f func(T) T) Stream[T] {
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

// Convert turns a Stream[A] into a Stream[B].
//
// Unlike [Map], the type changes, so a new batch must be allocated. The output
// buffer is reused across batches, meaning the produced batch is valid only for
// the duration of the call — retaining it requires a copy.
func Convert[A, B any](s Stream[A], f func(A) B) Stream[B] {
	return func(yield func(Batch[B]) bool) {
		var out []B
		s(func(b Batch[A]) bool {
			if cap(out) < len(b.Items) {
				out = make([]B, len(b.Items))
			}
			out = out[:len(b.Items)]
			for i, v := range b.Items {
				out[i] = f(v)
			}
			return yield(Batch[B]{Items: out})
		})
	}
}

// Filter keeps only the elements satisfying keep.
//
// Output batches may be smaller than input ones, or empty — an empty batch is
// emitted rather than skipped, so downstream operators keep the upstream
// cadence. Use [Coalesce] to recompact after a highly selective filter.
//
// The output batch reuses an internal buffer: it is valid only for the duration
// of the call.
func Filter[T any](s Stream[T], keep func(T) bool) Stream[T] {
	return func(yield func(Batch[T]) bool) {
		var out []T
		s(func(b Batch[T]) bool {
			if cap(out) < len(b.Items) {
				out = make([]T, 0, len(b.Items))
			}
			out = out[:0]
			for _, v := range b.Items {
				if keep(v) {
					out = append(out, v)
				}
			}
			return yield(Batch[T]{Items: out})
		})
	}
}

// Concat chains streams end to end: the first in full, then the next.
//
// An early stop breaks the chain without consuming the remaining streams.
func Concat[T any](streams ...Stream[T]) Stream[T] {
	return func(yield func(Batch[T]) bool) {
		for _, s := range streams {
			stopped := false
			s(func(b Batch[T]) bool {
				if !yield(b) {
					stopped = true
					return false
				}
				return true
			})
			if stopped {
				return
			}
		}
	}
}

// Coalesce regroups batches until they hold size elements.
//
// Place it after a selective operator — [Filter], a join — to keep sparse
// batches from propagating across several stages. A size of zero or less means
// [DefaultBatchSize].
//
// Coalesce accumulates into an internal buffer, so it costs O(size) memory, and
// the batch it produces is valid only for the duration of the call.
func Coalesce[T any](s Stream[T], size int) Stream[T] {
	if size <= 0 {
		size = DefaultBatchSize
	}
	return func(yield func(Batch[T]) bool) {
		buf := make([]T, 0, size)
		stopped := false
		s(func(b Batch[T]) bool {
			for _, v := range b.Items {
				buf = append(buf, v)
				if len(buf) == size {
					if !yield(Batch[T]{Items: buf}) {
						stopped = true
						return false
					}
					buf = buf[:0]
				}
			}
			return true
		})
		if !stopped && len(buf) > 0 {
			yield(Batch[T]{Items: buf})
		}
	}
}

// Split routes each batch to one or more of n branches.
//
// route receives a batch and returns the indices of its destination branches.
// This single primitive covers three uses:
//
//	partition — return one index, chosen from the contents
//	balance   — return one index, round-robin
//	broadcast — return every index
//
// Branches are consumed in lock-step: a batch is pushed to each destination
// before the next one is pulled. No buffering is needed, but a branch that
// stops early does not release the others — the upstream keeps running as long
// as one branch is still consuming.
//
// The batch is shared between branches without copying: a branch that mutates
// it affects the ones after it. Copy when that matters.
func Split[T any](s Stream[T], n int, route func(Batch[T]) []int) []Stream[T] {
	if n <= 0 {
		return nil
	}
	// Branches share a single run of the source: the first one consumed drives
	// it, and the others receive through that same traversal.
	yields := make([]func(Batch[T]) bool, n)
	alive := make([]bool, n)

	out := make([]Stream[T], n)
	for i := range out {
		out[i] = func(yield func(Batch[T]) bool) {
			yields[i] = yield
			alive[i] = true
			defer func() { alive[i] = false }()

			s(func(b Batch[T]) bool {
				for _, dst := range route(b) {
					if dst < 0 || dst >= n || !alive[dst] || yields[dst] == nil {
						continue
					}
					if !yields[dst](b) {
						alive[dst] = false
					}
				}
				// Keep going as long as one attached branch still consumes. A
				// batch not destined for the driving branch therefore stops
				// nothing: that is what makes partition mode work.
				for i := range alive {
					if alive[i] && yields[i] != nil {
						return true
					}
				}
				return false
			})
		}
	}
	return out
}
