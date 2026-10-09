package sluice

// Terminals consume a Stream. Every operator in this package returns a Stream,
// so a pipeline stays inert until one of these functions runs it: they are what
// turns a description of the work into the work.
//
// A terminal is where back-pressure originates. It pulls, the pipeline
// produces, and nothing runs ahead of it.
//
// # Memory
//
// Most terminals here are O(1) and work on an infinite stream. [Collect] is the
// exception: it holds every element, so it is bounded only by the stream. Bound
// it by composition — Collect(Take(s, n)) — rather than by trusting the source.

// ForEach calls f on every element, in order.
//
// It is the general terminal: [Collect], [Count] and the rest are the shapes
// worth naming, this one covers the others. Returning false stops the stream —
// the generator unwinds and its deferred calls run before ForEach returns.
//
// f must not retain its argument beyond the call: batches may reuse a buffer.
//
// ForEach runs per element and pays one indirect call for each. A consumer with
// real work to do will not notice; one that needs the batch loop to stay tight
// should range over the stream directly.
//
// ForEach panics if f is nil.
func ForEach[T any](s Stream[T], f func(T) bool) {
	if f == nil {
		panic("sluice: ForEach requires a non-nil f function")
	}
	s(func(b Batch[T]) bool {
		for _, v := range b.Items {
			if !f(v) {
				return false
			}
		}
		return true
	})
}

// Collect gathers every element into a slice.
//
// The elements are copied, so the result outlives the batches that carried them
// and is safe to retain.
//
// Collect is O(n) in memory and never returns on an infinite stream. That is
// not a flaw to guard against with a mandatory limit — the bound belongs where
// the caller knows it, and composes: Collect(Take(s, 1000)) states it exactly.
//
// The result grows by append, which costs log(n) reallocations — 15 of them for
// a hundred thousand elements at DefaultBatchSize, more with smaller batches
// (TestCollectAllocationsAreLogarithmic) — because the total is not knowable here: a
// Stream does not announce its length, and that is the point of the type. A
// caller who does know it beats this by writing the loop directly:
//
//	out := make([]T, 0, known)
//	s(func(b Batch[T]) bool { out = append(out, b.Items...); return true })
//
// A reservation heuristic was measured and rejected: guessing from the first
// batch cut the time but left the allocation count and the peak memory
// unchanged, so it bought a magic number rather than a property.
func Collect[T any](s Stream[T]) []T {
	var out []T
	s(func(b Batch[T]) bool {
		out = append(out, b.Items...)
		return true
	})
	return out
}

// Count reports how many elements the stream yields, consuming it.
//
// It counts per batch rather than per element, so it costs one addition per
// batch whatever the batch holds.
func Count[T any](s Stream[T]) int {
	var n int
	s(func(b Batch[T]) bool {
		n += len(b.Items)
		return true
	})
	return n
}

// Reduce folds the stream into a single value, left to right.
//
// The accumulator starts at init and threads through f. Reduce is O(1) in
// memory but consumes the whole stream: it does not return on an infinite one.
//
// Reduce panics if f is nil.
func Reduce[T, A any](s Stream[T], init A, f func(A, T) A) A {
	if f == nil {
		panic("sluice: Reduce requires a non-nil f function")
	}
	acc := init
	s(func(b Batch[T]) bool {
		for _, v := range b.Items {
			acc = f(acc, v)
		}
		return true
	})
	return acc
}

// First returns the first element, and whether there was one.
//
// It stops the stream as soon as it has that element, so it terminates on an
// infinite source and releases the pipeline's resources on the way out.
//
// The element is a copy, valid after the batch that carried it is gone.
func First[T any](s Stream[T]) (elem T, ok bool) {
	var out T
	var found bool
	s(func(b Batch[T]) bool {
		if len(b.Items) == 0 {
			return true // Filter emits these to hold the cadence
		}
		out, found = b.Items[0], true
		return false
	})
	return out, found
}
