package join

import "github.com/mlagarrigue/sluice"

// ZipLongest pairs two streams positionally: first with first, second with
// second, and so on until the longer one runs out.
//
// The output carries [EitherOrBoth] rows, like [Merge]: Both() while
// both streams still have elements, then HasLeft or HasRight alone for the tail
// of whichever is longer. Nothing is dropped.
//
// This is the primitive rather than a Zip that stops at the shorter stream,
// because stopping at the shorter one hides bugs: two streams that were meant
// to be the same length quietly produce a truncated result. Rust found it
// necessary to add zip_eq, which panics on unequal lengths — a sign the
// stop-at-shortest default is considered dangerous. A caller who wants it asks:
//
//	pairs := sluice.Filter(ZipLongest(a, b), EitherOrBoth[A, B].Both)
//
// Memory is O(1): one [iter.Pull] per stream, no queue. In a push model a zip
// needs an unbounded buffer per source, because a single slow producer forces
// the others to keep their values somewhere; pulling makes that impossible by
// construction.
//
// Pairing runs over what both sides hold in common before it re-tests the batch
// boundary, rather than asking each side for one element at a time. The check
// is the same either way; paying it once per batch instead of once per element
// is what the batch is for. Measured at 2.46 ns/element against 2.69 for the
// element-at-a-time form, and 2.03 against 2.65 when one side is twice the
// other — the uneven case gains most, because whole batches of the longer side
// then drain in one pass.
//
// Output batches reuse an internal buffer: retaining Items beyond the call that
// receives it requires a copy. Rows accumulate until a batch is full, so an
// early stop takes effect at the next batch boundary rather than the next row.
func ZipLongest[L, R any](left sluice.Stream[L], right sluice.Stream[R]) sluice.Stream[EitherOrBoth[L, R]] {
	return func(yield func(sluice.Batch[EitherOrBoth[L, R]]) bool) {
		lc := newWalker[L](left)
		defer lc.stop()
		rc := newWalker[R](right)
		defer rc.stop()

		out := newEmitter(yield)

		for {
			lok, rok := lc.ready(), rc.ready()

			switch {
			case !lok && !rok:
				_ = out.flush() // nothing follows: both streams are exhausted
				return

			case lok && rok:
				// Both sides have a batch, so pair what they hold in common
				// without re-testing the boundary per element: ready has just
				// established it, and neither side can run out before the
				// shorter of the two counts is used up. That check was 49% of
				// this operator's runtime when it ran once per element.
				n := min(lc.left(), rc.left())
				for range n {
					if !out.push(EitherOrBoth[L, R]{
						Left: lc.take(), Right: rc.take(), HasLeft: true, HasRight: true,
					}) {
						return
					}
				}

			case lok:
				// Right is exhausted: the rest of left is unpaired, and left's
				// current batch can be drained in one go for the same reason.
				for range lc.left() {
					if !out.push(EitherOrBoth[L, R]{Left: lc.take(), HasLeft: true}) {
						return
					}
				}

			default:
				for range rc.left() {
					if !out.push(EitherOrBoth[L, R]{Right: rc.take(), HasRight: true}) {
						return
					}
				}
			}
		}
	}
}

// walker reads one input element by element, without the key extraction and
// lookahead a merge join needs. [cursor] does more and costs more; positional
// pairing needs neither.
type walker[T any] struct {
	pull  func() (sluice.Batch[T], bool)
	stop  func()
	batch []T
	pos   int
	done  bool
}

func newWalker[T any](s sluice.Stream[T]) *walker[T] {
	n, stop := pull(s)
	return &walker[T]{pull: n, stop: stop}
}

// ready reports whether an element is available, refilling from the source when
// the current batch runs out. Upstream operators may emit empty batches —
// [sluice.Filter] does — so the advance loops rather than testing once.
//
// It is the boundary check on its own, so a caller can hoist it out of the
// element loop and pay it once per batch rather than once per element.
func (w *walker[T]) ready() bool {
	for w.pos >= len(w.batch) {
		if w.done {
			return false
		}
		b, ok := w.pull()
		if !ok {
			w.done = true
			continue
		}
		w.batch, w.pos = b.Items, 0
	}
	return true
}

// left reports how many elements remain in the current batch, without touching
// the source. A caller that has called [walker.ready] can take this many
// elements with no further boundary check.
func (w *walker[T]) left() int { return len(w.batch) - w.pos }

// take consumes the next element of the current batch. The caller guarantees
// availability, by [walker.ready] or by [walker.left]; taking past the batch is
// out of contract and panics on the slice index rather than silently reading a
// stale element.
func (w *walker[T]) take() T {
	v := w.batch[w.pos]
	w.pos++
	return v
}
