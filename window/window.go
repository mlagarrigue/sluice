package window

import (
	"time"

	"github.com/mlagarrigue/sluice"
)

// Pane is one closed window: the elements whose event time fell in
// [Start, End), with the bounds that defined it.
//
// The bounds travel with the elements because a window has an identity a batch
// does not: "the 10:03 minute" is a fact about the data, where a batch is a
// unit of transport that [sluice.Coalesce] may regroup at will. Deriving the bounds
// from the elements would also lose them for a pane holding one element at the
// very end of its window, and lie about where the window actually started.
//
// Items is owned by the pane, not borrowed from a reusable buffer: a Pane
// value may be retained, stored, or sent to another goroutine after the call
// that delivered it. That is the one place in this package where a copy is
// paid for by default, and the reason is that a Pane is an element containing
// a slice. Every generic operator here copies elements shallowly — [sluice.Convert]
// writes them by index, [sluice.Collect] appends them — so a borrowed Items would
// survive the copy as a slice header pointing at a buffer already refilled:
// sluice.Collect(Tumbling(...)) would return panes that all read back as the last one,
// with no error anywhere. Documentation cannot prevent that, because the
// operators doing the copying are correct. The copy is one allocation per
// window, amortized over the window's elements.
//
// The batch carrying the pane is a normal batch and follows the normal rule:
// its Items slice is valid for the call only. Copy the Pane values out of it,
// never the slice itself.
type Pane[T any] struct {
	// Start is the inclusive lower bound of the window.
	Start time.Time

	// End is the exclusive upper bound: Start.Add(size).
	End time.Time

	// Items holds the elements of the window, in arrival order.
	Items []T
}

// Len reports how many elements the pane holds.
func (p Pane[T]) Len() int { return len(p.Items) }

// Tumbling groups a time-ordered stream into consecutive windows of size, and
// emits each one as a [Pane] once it can no longer receive elements.
//
//	perMinute := Tumbling(clicks, func(c Click) time.Time { return c.At },
//	    time.Minute, 100_000, sluice.DropNewest)
//	stats := sluice.Convert(perMinute, func(p Pane[Click]) Stat {
//	    return Stat{At: p.Start, N: len(p.Items)}
//	})
//
// Windows are tumbling — consecutive and non-overlapping — and aligned on
// absolute time rather than on the first element, so the same data produces
// the same windows whenever the stream is run and whichever element arrives
// first. Alignment is [time.Time.Truncate]'s: multiples of size counted from
// the zero time, which for every size that divides an hour or a day is the
// alignment a reader expects. A size that does not — 7 days, 90 minutes —
// still tumbles regularly, just from an epoch nobody chose; give those windows
// a size that divides the period they belong to.
//
// # Ordering closes the windows
//
// The input must be non-decreasing on ts, checked as each element is seen and
// panicking with [sluice.ErrUnsorted] otherwise — the same requirement, check and
// sentinel as [IntervalJoin], for the same reason: a window is closed by the
// arrival of an element beyond it, so an element that goes backwards would
// belong to a window already emitted and would be silently dropped or, worse,
// silently reopen it.
//
// That ordering is also what keeps the state at one window: element times
// never decrease, so window indices never decrease either, and a new index
// means every earlier window is complete. Memory is O(elements in one window),
// which is why n bounds that count.
//
// # sluice.Empty windows are not emitted
//
// A window that received no element produces no pane. The alternative would
// let a bounded input produce unbounded output — a one-second window over a
// stream with a one-year gap would emit thirty million empty panes — so the
// operator emits what it saw rather than what it could have seen. A caller who
// needs the gaps filled knows the cadence they expect and can generate it;
// this operator cannot know it.
//
// # Bound and overflow
//
// n bounds the elements held for the open window, per S1, and policy says what
// happens when it is reached: [sluice.Fail] panics with [sluice.ErrOverflow], [sluice.DropNewest]
// discards the arriving element and keeps the window as it stands — a pane
// that is short, never one that is wrong. [sluice.DropOldest] is rejected at
// construction rather than accepted: dropping the oldest element of an open
// window would silently mutilate a pane from its front, which no caller can
// mean, and accepting a policy that behaves like sluice.DropNewest would offer a
// choice that does nothing.
//
// Each closed pane is emitted as its own batch. A pane is already an aggregate
// of many elements, so the per-batch transport cost is amortized by the pane's
// own contents; grouping panes would trade a real emission delay — up to a
// whole batch of windows held back — against an overhead that is already
// negligible. Compose [sluice.Coalesce] when panes are small and numerous.
//
// # Cost
//
// Measured at 4.5 ns/element on 1024-element windows and 7.4 on 8-element
// ones, allocation-free but for one slice per pane. That is above the ~1.5 ns
// per-stage budget, which bounds stateless operators where the only cost is
// the indirect call; here every element also pays a caller callback returning
// a 24-byte [time.Time] and two time comparisons — the order check and the
// window test. What is left is the operator's actual work. The narrow-window
// figure is the same work plus the per-pane copy, which is what buys [Pane]'s
// ownership and shows only when windows are small enough to be numerous.
//
// Tumbling panics if ts is nil, if size is zero or negative, if n is zero or
// negative, or if policy is not [sluice.Fail] or [sluice.DropNewest].
func Tumbling[T any](s sluice.Stream[T], ts func(T) time.Time, size time.Duration, n int, policy sluice.Overflow) sluice.Stream[Pane[T]] {
	if ts == nil {
		panic("sluice: Tumbling requires a non-nil ts function")
	}
	if size <= 0 {
		panic("sluice: Tumbling requires a positive size")
	}
	if n <= 0 {
		panic("sluice: Tumbling requires a positive n")
	}
	if policy != sluice.Fail && policy != sluice.DropNewest {
		panic("sluice: Tumbling requires an Overflow of Fail or DropNewest")
	}

	return func(yield func(sluice.Batch[Pane[T]]) bool) {
		var (
			buf   []T       // the open window's elements
			start time.Time // its lower bound
			end   time.Time // its exclusive upper bound, kept to avoid a divide
			open  bool
			last  time.Time // the previous element's time, for the order check
			seen  bool
		)
		// One reused slot for the single-pane batch: the batch's Items is a
		// normal reusable buffer, while the Pane's own Items is copied out and
		// owned, which is what makes a Pane safe to retain.
		slot := make([]Pane[T], 1)

		emit := func() bool {
			items := make([]T, len(buf))
			copy(items, buf)
			slot[0] = Pane[T]{Start: start, End: end, Items: items}
			open = false
			return yield(sluice.Batch[Pane[T]]{Items: slot})
		}

		stopped := false
		s(func(b sluice.Batch[T]) bool {
			for _, v := range b.Items {
				at := ts(v)
				if seen && at.Compare(last) < 0 {
					panic(sluice.ErrUnsorted)
				}
				last, seen = at, true

				// Truncate is a 64-bit division, and the common element does
				// not need it: times never decrease, so an element below the
				// open window's end is necessarily inside it. One comparison
				// answers for every element but the first of each window,
				// measured at 10.50 -> 4.52 ns/element on 1024-element
				// windows and 13.38 -> 7.36 on 8-element ones.
				if !open || !at.Before(end) {
					if open {
						// The element belongs past the open window, so nothing
						// can be added to it any more.
						if !emit() {
							stopped = true
							return false
						}
					}
					start = at.Truncate(size)
					end = start.Add(size)
					open = true
					buf = buf[:0]
				}
				if len(buf) == n {
					if policy == sluice.Fail {
						panic(sluice.ErrOverflow)
					}
					continue // sluice.DropNewest: the window keeps what it holds
				}
				buf = append(buf, v)
			}
			return true
		})

		// The last window is closed by the end of the stream rather than by a
		// later element. An early stop is not an end: the consumer asked to
		// stop, so the partial window is discarded like sluice.Coalesce's tail.
		if !stopped && open {
			emit()
		}
	}
}
