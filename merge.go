package sluice

import "strconv"

// Completion decides when a merged stream ends. There is no sensible default:
// stopping at the first exhausted source silently drops the rest, and waiting
// for all of them stalls a pipeline whose point was the first result. The
// caller states which one it wants.
type Completion uint8

const (
	// WhenAll ends the merged stream once every source is exhausted. Sources
	// that end early simply stop contributing.
	//
	// A source that never ends never ends the merge: an endless one, or one that
	// keeps yielding empty batches — which [Filter] does by contract, to hold the
	// upstream cadence. Merge then spins without progressing until the consumer
	// stops it. That is the requested semantics, not a defect, but it shows up on
	// a profile as CPU with no allocation and no output; use [WhenAny] when a
	// source running dry is meant to end the merge.
	WhenAll Completion = iota

	// WhenAny ends the merged stream as soon as one source is exhausted, and
	// releases the others. Batches already pulled from the other sources in the
	// same round are still emitted: a source is never read and discarded.
	WhenAny
)

// String implements [fmt.Stringer].
func (c Completion) String() string {
	switch c {
	case WhenAll:
		return "WhenAll"
	case WhenAny:
		return "WhenAny"
	default:
		return "Completion(" + strconv.Itoa(int(c)) + ")"
	}
}

// valid reports whether c is one of the declared constants. Merge checks it at
// construction: an undeclared value would otherwise read as [WhenAll] in
// silence, which is the one choice this type exists to make explicit.
func (c Completion) valid() bool { return c == WhenAll || c == WhenAny }

// Merge interleaves several streams, one batch at a time from each.
//
// Sources are pulled in round-robin from the consumer's goroutine: one batch
// from the first, one from the second, and around again. Nothing runs ahead,
// so "whatever is ready" has no meaning here — a slow source holds the
// rotation until it yields. Order within a batch is preserved, and the order
// between sources is that rotation: with [WhenAll] and sources yielding a, b and
// 1, 2 the output is a, 1, b, 2, a determinism a test can rely on.
//
// done says when the merged stream ends — see [WhenAll] and [WhenAny]. Merging
// no streams yields nothing, whatever done says. Merge panics if done is not a
// declared [Completion].
//
// Cost is O(1) in memory whatever the stream length: one [iter.Pull] per
// source, no buffering. The batch is what makes this affordable — the pull
// machinery costs ~68 ns per call, which a 1024-element batch amortizes to
// ~0.07 ns per element. Element-wise, the same operator would be unusable.
//
// Batches are passed through untouched, so an operator upstream that reuses its
// buffer keeps that contract here: retaining Items requires a copy.
func Merge[T any](done Completion, streams ...Stream[T]) Stream[T] {
	if !done.valid() {
		panic("sluice: Merge requires a declared Completion (WhenAll or WhenAny)")
	}
	// No early return of Empty for zero streams, and no helper for the body:
	// the rotation already yields nothing then, and one closure holding the
	// whole loop keeps the call direct once Merge is inlined — measured, the
	// caller's yield and the closure then stay off the heap. Merge
	// delegates here, so this is the one rotation of both.
	return func(yield func(Batch[T]) bool) {
		type source struct {
			next func() (Batch[T], bool)
			stop func()
		}
		srcs := make([]source, len(streams))
		// One deferred call for the whole set rather than one per source: the
		// sources must be released together however this function returns —
		// exhaustion, early stop, or a panic crossing the yield (S9). A source
		// not yet pulled has a nil stop; iter.Pull's stop is idempotent, so one
		// already released below is released again at no cost.
		defer func() {
			for _, src := range srcs {
				if src.stop != nil {
					src.stop()
				}
			}
		}()
		for i, s := range streams {
			srcs[i].next, srcs[i].stop = pull(s)
		}

		live := len(srcs)
		for live > 0 {
			for i := range srcs {
				if srcs[i].next == nil {
					continue
				}
				b, ok := srcs[i].next()
				if !ok {
					// Released at once: a dry source has nothing left to hold.
					srcs[i].next = nil
					srcs[i].stop()
					live--
					if done == WhenAny {
						return
					}
					continue
				}
				if !yield(b) {
					return
				}
			}
		}
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
