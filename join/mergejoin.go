package join

import "github.com/mlagarrigue/sluice"

// EitherOrBoth carries one row of a [Merge] result: a left row, a right
// row, or a matched pair.
//
// Values are held directly rather than behind pointers. A pointer into an
// operator's reused buffer dangles as soon as the next batch arrives, which is
// exactly the mistake the sluice.Batch contract warns about; carrying values keeps
// each row valid on its own terms.
type EitherOrBoth[L, R any] struct {
	Left     L
	Right    R
	HasLeft  bool
	HasRight bool
}

// Both reports whether the row matched on both sides.
func (e EitherOrBoth[L, R]) Both() bool { return e.HasLeft && e.HasRight }

// Merge merges two streams sorted on a common key, in O(1) memory over
// streams of any length — including infinite ones.
//
// keyL and keyR extract the key each side is sorted by, and cmp orders two
// keys: negative if the first sorts before the second, zero if they are equal,
// positive otherwise — the convention of [cmp.Compare] and [strings.Compare].
// Both inputs must be sorted ascending by that key.
//
// The output carries every row exactly once, tagged with what was found. Each
// join semantics is a filter over it rather than a separate operator:
//
//	inner join   keep Both()
//	left join    keep Both() or HasLeft
//	full outer   keep everything
//	intersect    keep Both()
//	except A\B   keep HasLeft && !HasRight
//
// Rows sharing a key on both sides produce their full cross product, as SQL
// requires. That is the one place memory grows: a key repeated n > 1 times on
// the left and m > 1 times on the right buffers the right run to pair it with
// each left row, costing O(m) for that key. A key held once on either side
// costs O(1) whatever the other side's run — the repeated side streams against
// the single row, across batch boundaries — so a one-to-many join on an
// identifier stays O(1) over runs of any length.
//
// The unique case is also taken separately for speed, since buffering a run to
// discover it holds one element is most of what the general path costs: 11.6
// ns/element before, 10.2 after, and the same change is worth 15% on a join of
// 65536 rows. A key repeated on both sides pays ~2% for the test that finds
// this out, which is the trade — the common shape gets the saving, the rare one
// carries the check.
//
// Once one side is exhausted, the other drains batch-wise rather than through
// the per-element peek machinery — nothing re-peeks a tail element, so its
// cache was pure overhead. Measured at 12.0 ns/element against 17.6 for the
// peeked form (−32%, paired rounds); the whole output of an anti-join over
// mostly-disjoint inputs flows through this path. The sortedness check still
// runs on every element.
//
// Output batches reuse an internal buffer, like [sluice.Filter] and [sluice.Convert]:
// retaining Items beyond the call that receives it requires a copy.
//
// Rows accumulate until a batch is full, so the inputs run ahead of what the
// consumer has seen: an early stop takes effect at the next batch boundary, not
// at the next row. That is the cost of emitting full batches rather than
// one-row ones, and it is what makes the operator worth its name.
//
// Merge panics with [sluice.ErrUnsorted] if either input goes backwards, and
// with a plain message if cmp, keyL or keyR is nil.
func Merge[L, R any, K any](
	left sluice.Stream[L],
	right sluice.Stream[R],
	keyL func(L) K,
	keyR func(R) K,
	cmp func(K, K) int,
) sluice.Stream[EitherOrBoth[L, R]] {
	if keyL == nil || keyR == nil {
		panic("sluice: Merge requires non-nil key functions")
	}
	if cmp == nil {
		panic("sluice: Merge requires a non-nil cmp function")
	}

	return func(yield func(sluice.Batch[EitherOrBoth[L, R]]) bool) {
		lc := newCursor[L](left, keyL, cmp)
		defer lc.stop()
		rc := newCursor[R](right, keyR, cmp)
		defer rc.stop()

		out := newEmitter(yield)

		for {
			lv, lk, lok := lc.peek()
			rv, rk, rok := rc.peek()

			switch {
			case !lok && !rok:
				_ = out.flush() // nothing follows: both inputs are exhausted
				return

			case !rok: // right exhausted: the rest of left is unmatched
				// The tail drains batch-wise: nothing re-peeks a tail element,
				// so the peek cache is pure overhead here. takeChecked keeps
				// the monotonicity check — every element is still checked
				// exactly once — and the boundary test is the loop bound.
				lc.next()
				if !out.push(EitherOrBoth[L, R]{Left: lv, HasLeft: true}) {
					return
				}
				for range lc.leftInBatch() {
					if !out.push(EitherOrBoth[L, R]{Left: lc.takeChecked(), HasLeft: true}) {
						return
					}
				}

			case !lok: // left exhausted: the rest of right is unmatched
				rc.next()
				if !out.push(EitherOrBoth[L, R]{Right: rv, HasRight: true}) {
					return
				}
				for range rc.leftInBatch() {
					if !out.push(EitherOrBoth[L, R]{Right: rc.takeChecked(), HasRight: true}) {
						return
					}
				}

			default:
				switch c := cmp(lk, rk); {
				case c < 0:
					lc.next()
					if !out.push(EitherOrBoth[L, R]{Left: lv, HasLeft: true}) {
						return
					}
				case c > 0:
					rc.next()
					if !out.push(EitherOrBoth[L, R]{Right: rv, HasRight: true}) {
						return
					}
				default:
					// Equal keys. The case where each side holds the key exactly
					// once is both the common one — a join on an identifier — and
					// the one that needs none of joinEqual's machinery.
					//
					// Taking it separately is worth a branch because the general
					// path is not cheap: it consumes and re-peeks to learn each
					// side's multiplicity. Buffering both runs, the earlier
					// general path, was measured at 70% of this operator's
					// runtime with every key unique.
					if lc.aloneInBatch(lk) && rc.aloneInBatch(rk) {
						lc.next()
						rc.next()
						if !out.push(EitherOrBoth[L, R]{
							Left: lv, Right: rv, HasLeft: true, HasRight: true,
						}) {
							return
						}
						continue
					}
					if !joinEqual(lc, rc, lv, rv, lk, out) {
						return
					}
				}
			}
		}
	}
}

// joinEqual emits the cross product of the left and right runs under key k,
// whose first rows lv and rv have been peeked but not consumed. It reports
// false once the consumer has stopped.
//
// Only a key repeated on both sides needs a buffer. Each side's multiplicity
// is learned by consuming its first row and peeking the next — across a batch
// boundary if need be — and when either side turns out to hold k once, the
// other side's run streams against that single row, however long it is: a
// one-to-many join over a run of a million rows holds one row, not a million.
// Only many-to-many buffers, and then only the right run, so the left run can
// stream against it in the left-major order the output has always had.
func joinEqual[L, R, K any](
	lc *cursor[L, K], rc *cursor[R, K], lv L, rv R, k K,
	out *emitter[EitherOrBoth[L, R]],
) bool {
	both := func(l L, r R) EitherOrBoth[L, R] {
		return EitherOrBoth[L, R]{Left: l, Right: r, HasLeft: true, HasRight: true}
	}

	lc.next()
	if _, ok := lc.peekKey(k); !ok {
		// Left holds k once: stream the right run against lv.
		for {
			rc.next()
			if !out.push(both(lv, rv)) {
				return false
			}
			v, ok := rc.peekKey(k)
			if !ok {
				return true
			}
			rv = v
		}
	}

	rc.next()
	if _, ok := rc.peekKey(k); !ok {
		// Right holds k once: stream the rest of the left run against rv.
		for {
			if !out.push(both(lv, rv)) {
				return false
			}
			v, ok := lc.peekKey(k)
			if !ok {
				return true
			}
			lv = v
			lc.next()
		}
	}

	// Many-to-many: buffer the right run, stream the left one against it.
	run := append(rc.run[:0], rv)
	for {
		v, ok := rc.peekKey(k)
		if !ok {
			break
		}
		run = append(run, v)
		rc.next()
	}
	rc.run = run
	for {
		for _, r := range run {
			if !out.push(both(lv, r)) {
				return false
			}
		}
		v, ok := lc.peekKey(k)
		if !ok {
			return true
		}
		lv = v
		lc.next()
	}
}
