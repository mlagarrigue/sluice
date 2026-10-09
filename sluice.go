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
// Everything else in the library (diagnostics, joins, connectors, HTTP) is
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
// docs/benchmarks.md.
//
// # Dependencies
//
// Every production dependency is a recorded decision. Today the graph is the
// standard library plus one module, golang.org/x/crypto, for the
// ChaCha20-Poly1305 cipher that crypto/tls negotiates but does not export
// (the net/quic package needs it to protect packets). CI fails on any module
// outside the recorded list.
package sluice

import (
	"iter"

	"github.com/mlagarrigue/sluice/internal/batch"
)

// DefaultBatchSize is the default number of elements per batch.
//
// Measurement (docs/benchmarks.md) shows a performance plateau from 8
// elements upward, flat through one million: this setting is safe but not
// critical. A batch thinned to a few dozen elements by a selective filter does
// not degrade throughput.
const DefaultBatchSize = 1024

// Batch is a group of values, the library's unit of transport.
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
//
// Map writes in place, and [Of] batches share the caller's slice: composing the
// two therefore overwrites that slice. Pass a copy when the input must survive
// the pipeline.
//
// Map panics if f is nil.
func Map[T any](s Stream[T], f func(T) T) Stream[T] {
	if f == nil {
		panic("sluice: Map requires a non-nil f function")
	}
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
//
// Map and Convert are two functions rather than one Map[A, B] because Go has
// no compile-time test for A == B: a single function must either allocate on
// the same-type path or decide at construction with a runtime type assertion,
// and both were measured — +8% per element and +60% per construction on the
// request-path benchmark. Two names cost nothing.
//
// Convert panics if f is nil.
func Convert[A, B any](s Stream[A], f func(A) B) Stream[B] {
	if f == nil {
		panic("sluice: Convert requires a non-nil f function")
	}
	return func(yield func(Batch[B]) bool) {
		var out []B
		s(func(b Batch[A]) bool {
			// Convert writes by index, so it needs the length rather than
			// append's growth. Grow amortizes it: a source whose batches get
			// bigger reallocates log(n) times instead of once per new maximum.
			out = batch.Grow(out, len(b.Items))
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
//
// Filter panics if keep is nil.
func Filter[T any](s Stream[T], keep func(T) bool) Stream[T] {
	if keep == nil {
		panic("sluice: Filter requires a non-nil keep function")
	}
	return func(yield func(Batch[T]) bool) {
		var out []T
		s(func(b Batch[T]) bool {
			// No explicit sizing: append grows the buffer in amortized time and
			// it is reused across batches, so a source whose batches grow costs
			// log(n) reallocations rather than one per new high-water mark.
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
