package sluice

import "github.com/mlagarrigue/sluice/internal/batch"

// The O(1) operators of the taxonomy: each holds a fixed amount of state, so
// they compose indefinitely and run on an infinite stream. See
// docs/design/architecture.md, "Operator taxonomy".

// FlatMap expands every element into zero or more elements of another type.
//
// f appends the results for one element to dst and returns the extended
// slice, the shape of strconv.AppendInt: returning dst unchanged drops the
// element. dst is the output batch being built, so the results land where the
// consumer reads them, with no copy and no slice of their own. A batch of 1000
// elements each expanding to 3 yields one batch of 3000 rather than 1000 small
// ones — the expansion does not fragment the stream. Use [Coalesce] the other
// way round, when f mostly appends nothing.
//
// f must only append: the slice it returns must begin with dst, whose
// elements are the batch's earlier results. A function that already returns a
// slice composes by appending it:
//
//	sluice.FlatMap(lines, func(dst []string, line string) []string {
//	    return append(dst, strings.Fields(line)...)
//	})
//
// Returning a slice of f's own instead would drop those earlier results
// without a word, so FlatMap checks once per batch and panics when the batch
// did not end on the buffer it reserved while it would have fitted there —
// the trace such a slice leaves. The check is per batch, not per element: in
// the loop it cost the 1-to-1 shape a fifth of its time.
//
// The output batch reuses an internal buffer and is valid only for the duration
// of the call. f must not retain its argument or dst beyond the call.
//
// The appending form is what the measurement chose. When f returned its own
// slice, the natural body — return []B{x, y} — allocated on every element as
// soon as f did not inline: 13.6 ns and one allocation per element against
// 1.4 ns and none, for two results per element. Inlined, the returned form
// cost 1.6 to 3 times as much as appending, for the copy out of each slice.
//
// Reach for [Map] when f yields exactly one element for every input: it
// writes by index into a buffer it owns and pays for no append at all.
// FlatMap earns its cost when the count actually varies.
//
// FlatMap panics if f is nil.
func FlatMap[A, B any](s Stream[A], f func(dst []B, a A) []B) Stream[B] {
	if f == nil {
		panic("sluice: FlatMap requires a non-nil f function")
	}
	return func(yield func(Batch[B]) bool) {
		var buf []B
		s(func(b Batch[A]) bool {
			// The output is at least the input's length whenever f is 1-to-1 —
			// the common case — so without this reservation the buffer regrows
			// from empty on every batch and the operator allocates per batch
			// instead of once. Expansions beyond it still fall back to append's
			// growth.
			reserved := batch.Reserve(buf, len(b.Items))
			out := reserved
			for _, v := range b.Items {
				out = f(out, v)
			}
			// append moves the array only when it is full, so a batch that
			// fits in what was reserved and does not start there went through
			// a slice of f's own, which dropped the results before it. One
			// check per batch rather than per element: a guard in the loop
			// cost the 1-to-1 shape a fifth of its time.
			if len(out) > 0 && len(out) <= cap(reserved) && &out[0] != &reserved[:1][0] {
				panic("sluice: FlatMap's f must return dst with its results appended")
			}
			buf = out
			if cap(out) < cap(reserved) {
				buf = reserved
			}
			return yield(Batch[B]{Items: out})
		})
	}
}

// Peek calls f on every element and passes the batch through unchanged.
//
// It is the operator for effects that are not transformations — logging,
// metrics, a counter. f sees the element but the stream does not change shape,
// which is what separates it from [Map].
//
// f must not retain its argument beyond the call. Peek does not copy: mutating
// through a pointer element mutates what flows downstream.
//
// Peek panics if f is nil.
func Peek[T any](s Stream[T], f func(T)) Stream[T] {
	if f == nil {
		panic("sluice: Peek requires a non-nil f function")
	}
	return func(yield func(Batch[T]) bool) {
		s(func(b Batch[T]) bool {
			for _, v := range b.Items {
				f(v)
			}
			return yield(b)
		})
	}
}

// Scan emits the running accumulation: one output element per input element,
// each the fold of every element up to and including it.
//
// Where [Reduce] returns only the final value, Scan shows the intermediate
// ones — a running total, a cumulative maximum, a state machine's successive
// states.
//
// The accumulator starts at init. State is a single value, so Scan is O(1) and
// runs on an infinite stream.
//
// The output batch reuses an internal buffer and is valid only for the duration
// of the call.
//
// Scan panics if f is nil.
func Scan[T, A any](s Stream[T], init A, f func(A, T) A) Stream[A] {
	if f == nil {
		panic("sluice: Scan requires a non-nil f function")
	}
	return func(yield func(Batch[A]) bool) {
		acc := init
		var out []A
		s(func(b Batch[T]) bool {
			// Scan writes by index, so it needs the length rather than append's
			// growth — same reasoning as Convert.
			out = batch.Grow(out, len(b.Items))
			for i, v := range b.Items {
				acc = f(acc, v)
				out[i] = acc
			}
			return yield(Batch[A]{Items: out})
		})
	}
}

// Take emits at most the first n elements, then stops the stream.
//
// It counts elements, not batches: asking for 10 elements of a stream batched
// by 1024 stops inside the first batch rather than yielding 1024. The last
// batch is a slice of the upstream one — no copy — so it obeys the same rule as
// any other batch and must be copied to be retained.
//
// Take stops the source as soon as it has n elements, so it terminates on an
// infinite stream and is the bound to put in front of [Collect]. An n of zero
// consumes nothing at all: the source is never touched. A negative n panics.
func Take[T any](s Stream[T], n int) Stream[T] {
	if n < 0 {
		panic(negativeArg{"Take n", n, "nothing"})
	}
	return func(yield func(Batch[T]) bool) {
		if n == 0 {
			return
		}
		left := n
		s(func(b Batch[T]) bool {
			items := b.Items
			if len(items) > left {
				items = items[:left]
			}
			left -= len(items)
			// The batch that reaches n is emitted before stopping, and the
			// refusal below it is returned as is: a consumer that stops early
			// must not be overridden by Take's own bound.
			if !yield(Batch[T]{Items: items}) {
				return false
			}
			return left > 0
		})
	}
}

// Drop skips the first n elements and emits the rest.
//
// Like [Take] it counts elements, so it can start mid-batch; the first emitted
// batch is then a slice of the upstream one. An n of zero passes everything
// through; a negative n panics.
//
// The skipped elements are still produced by the source — dropping is not
// pushdown: the source produces them and this operator discards them. Where
// the source can be told to skip instead, [github.com/mlagarrigue/sluice/pushdown]
// is the mechanism — a demand the source consults once per batch, which is
// sideways information passing (a later stage telling an earlier one what it
// will not need), measured at ×6.9 on a scan that can
// act on it.
func Drop[T any](s Stream[T], n int) Stream[T] {
	if n < 0 {
		panic(negativeArg{"Drop n", n, "nothing dropped"})
	}
	return func(yield func(Batch[T]) bool) {
		left := n
		s(func(b Batch[T]) bool {
			items := b.Items
			if left > 0 {
				if len(items) <= left {
					left -= len(items)
					// Nothing survives this batch. The empty batch is still
					// emitted, so downstream keeps the upstream cadence — the
					// same contract as Filter.
					return yield(Batch[T]{Items: items[:0]})
				}
				items = items[left:]
				left = 0
			}
			return yield(Batch[T]{Items: items})
		})
	}
}

// TakeWhile emits elements until keep returns false, then stops the stream.
//
// The element that fails the test is not emitted, and keep is not called again.
// The batch it belongs to is truncated before it and emitted, so the stop takes
// effect at the element rather than at the next batch boundary. When the
// failing element is the first of its batch, nothing is emitted for that batch:
// the stream ends there, so an empty batch would hold a cadence that no longer
// exists. Like [Take], TakeWhile ends without a trailing empty batch.
//
// TakeWhile stops the source, so it terminates on an infinite stream.
//
// TakeWhile panics if keep is nil.
func TakeWhile[T any](s Stream[T], keep func(T) bool) Stream[T] {
	if keep == nil {
		panic("sluice: TakeWhile requires a non-nil keep function")
	}
	return func(yield func(Batch[T]) bool) {
		s(func(b Batch[T]) bool {
			for i, v := range b.Items {
				if !keep(v) {
					// Emit the prefix, then stop. The refusal of this last
					// batch is not distinguished from the bound: either way
					// there is nothing more to send.
					//
					// An empty prefix is not emitted at all. Filter and Drop
					// emit empty batches to hold the cadence — "this batch
					// was processed, nothing survived it" — but the stream
					// ends on the next line, so there is no cadence left to
					// hold and the batch would carry no information. Take
					// ends without a trailing empty batch; this matches it.
					if i > 0 {
						yield(Batch[T]{Items: b.Items[:i]})
					}
					return false
				}
			}
			return yield(b)
		})
	}
}

// DropWhile skips elements while drop returns true, then emits everything that
// follows.
//
// drop is called only until it first returns false; from there the stream
// passes through untouched, including elements that would satisfy it again.
// That asymmetry with [Filter] is the point of the operator.
//
// Batches before the switch are emitted empty rather than skipped, so
// downstream keeps the upstream cadence.
//
// DropWhile panics if drop is nil.
func DropWhile[T any](s Stream[T], drop func(T) bool) Stream[T] {
	if drop == nil {
		panic("sluice: DropWhile requires a non-nil drop function")
	}
	return func(yield func(Batch[T]) bool) {
		dropping := true
		s(func(b Batch[T]) bool {
			items := b.Items
			if dropping {
				i := 0
				for i < len(items) && drop(items[i]) {
					i++
				}
				if i == len(items) {
					return yield(Batch[T]{Items: items[:0]})
				}
				items = items[i:]
				dropping = false
			}
			return yield(Batch[T]{Items: items})
		})
	}
}
