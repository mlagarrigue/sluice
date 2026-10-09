package sluice

// Coalesce regroups batches until they hold size elements.
//
// Place it after a selective operator — [Filter], a join — to keep sparse
// batches from propagating across several stages. A size of zero or less means
// [DefaultBatchSize].
//
// Coalesce accumulates into an internal buffer, so it costs O(size) memory, and
// the batch it produces is valid only for the duration of the call. The buffer
// is claimed when the first element arrives rather than up front — a large size
// on a stream that yields nothing costs nothing — and is then refilled rather
// than reallocated, so two batches retained without copying may alias each
// other. An input batch already holding size elements or more is passed through
// in size-element slices without copying while nothing is buffered, so a
// Coalesce whose upstream already produces full batches costs the boundary
// test and nothing else — measured at 0.54 ns/element, the cost of the bare
// pipeline without it. Refilling copies by chunk rather than by element, worth
// −20% on 8-element fragments and −49% when size is far above the incoming
// batches (paired rounds, sign test).
//
// An early stop discards the elements accumulated since the last emitted batch
// — up to size-1 of them. They are not flushed, because the consumer asked to
// stop.
func Coalesce[T any](s Stream[T], size int) Stream[T] {
	if size <= 0 {
		size = DefaultBatchSize
	}
	return func(yield func(Batch[T]) bool) {
		// The buffer is claimed on the first element rather than up front. size
		// is the caller's number and often comes from configuration, so
		// claiming it eagerly turns a large value into a large allocation
		// before a single element has flowed — on a stream that may yield
		// nothing at all.
		//
		// Deferred rather than grown: letting append reach size costs log(size)
		// reallocations, each copying what is already buffered, on the nominal
		// path of a stream that does produce data. One allocation on first use
		// costs nothing on an empty stream and nothing extra on a full one.
		//
		// The empty batch returns early rather than guarding the claim with a
		// combined condition. Both are correct, and the early return is also
		// the clearer of the two: it says "nothing to regroup here" where
		// `buf == nil && len(b.Items) > 0` says it by implication.
		//
		// An earlier note claimed the shape was worth 0.92 against 1.21
		// ns/element. That figure does not reproduce — re-measured, the two
		// shapes are within a few percent of each other, well inside the noise
		// this benchmark carries. The early return is kept for readability, not
		// for speed, and no test guards it because there is nothing to guard.
		var buf []T
		stopped := false
		s(func(b Batch[T]) bool {
			items := b.Items
			if len(items) == 0 {
				return true // Filter emits these to hold the cadence
			}
			for len(items) > 0 {
				// A size-sized run needs no copy while nothing is buffered: it
				// goes out as a slice of the input. This serves the Coalesce
				// placed defensively in a pipeline whose batches are already
				// full — which becomes free — and the tail of any batch large
				// enough to leave the buffer empty behind it.
				if len(buf) == 0 && len(items) >= size {
					if !yield(Batch[T]{Items: items[:size:size]}) {
						stopped = true
						return false
					}
					items = items[size:]
					continue
				}
				if buf == nil {
					buf = make([]T, 0, size)
				}
				// Refill by chunk rather than by element: the boundary test
				// runs once per chunk and the copy is one memmove, which is
				// what the batch is for.
				n := min(size-len(buf), len(items))
				buf = append(buf, items[:n]...)
				items = items[n:]
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
