package window

import (
	"strconv"
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

// Late says what [Tumbling] does with an element that arrives after its own
// window was emitted.
//
// It is a policy and not a default because the two answers are both right and
// the caller is the only one who knows which: a billing rollup cannot drop an
// event, and an ingest fed by clients whose clocks disagree cannot stop. The
// zero value is not one of them, for the reason [sluice.Overflow]'s is not —
// it is the argument nobody passed.
type Late uint8

const (
	// LateFail stops the stream with a panic carrying [sluice.ErrUnsorted].
	// The choice when a missing element would make the result wrong rather
	// than short: the pipeline stops where the data went backwards instead
	// of producing an aggregate nobody can audit.
	LateFail Late = iota + 1

	// LateDrop steps over the element and keeps going. The choice for event
	// time that comes from elsewhere — several producers, unsynchronised
	// clocks, a retry — where one client's clock must not be able to stop
	// everyone's aggregation. The panes stay right; the one whose window had
	// already closed is short by that element, and nothing says so, which is
	// the price of the choice.
	LateDrop
)

// String renders the policy for logs, with the same rule as
// [sluice.Overflow.String]: a value outside the constants shows its number
// rather than a label that would misrepresent it.
func (l Late) String() string {
	switch l {
	case LateFail:
		return "LateFail"
	case LateDrop:
		return "LateDrop"
	default:
		return "Late(" + strconv.Itoa(int(l)) + ")"
	}
}

// Tumbling groups a time-ordered stream into consecutive windows of size, and
// emits each one as a [Pane] once it can no longer receive elements.
//
//	perMinute := Tumbling(clicks, func(c Click) time.Time { return c.At },
//	    time.Minute, 100_000, sluice.DropNewest, LateDrop)
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
// # Arrival closes the windows, and late is late only past the open one
//
// A window is closed by the arrival of an element beyond it, so what the
// operator needs of its input is not that it be sorted but that it never
// reach back into a window already emitted. Those are different
// requirements, and only the second one is checked: an element whose time
// falls inside the open window joins it whatever order it arrived in —
// unsynchronised clocks a few milliseconds apart, a batch the network
// reordered, a retry — and the pane is the same pane either way, since
// [Pane.Items] is in arrival order and never claimed to be sorted.
//
// An element older than the open window's start is late: its window is gone,
// and nothing can place it. There the caller's late policy answers —
// [LateFail] panics with [sluice.ErrUnsorted], [LateDrop] steps over the
// element — because both answers are right for some pipeline and neither is
// right for all. Requiring a non-decreasing input instead, the way
// [github.com/mlagarrigue/sluice/join.Interval] does for its cursor, would
// mean one element from one misconfigured clock can stop an ingest for good:
// measured on the first consumer built on this library, where the panic
// travelled up through a [github.com/mlagarrigue/sluice/gateway] and left
// every later call failing.
//
// What keeps the state at one window: nothing before the open
// window's start is ever buffered, so window indices never decrease and a new
// index means every earlier window is complete. Memory is O(elements in one
// window), which is why maxPerWindow bounds that count.
//
// # Empty windows are not emitted
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
// maxPerWindow bounds the elements held for the open window, per S1, and policy says what
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
// Tumbling panics if ts is nil, if size is zero or negative, if maxPerWindow is zero or
// negative, if policy is not [sluice.Fail] or [sluice.DropNewest], or if late
// is not [LateFail] or [LateDrop].
func Tumbling[T any](s sluice.Stream[T], ts func(T) time.Time, size time.Duration, maxPerWindow int, policy sluice.Overflow, late Late) sluice.Stream[Pane[T]] {
	if ts == nil {
		panic("sluice: Tumbling requires a non-nil ts function")
	}
	if size <= 0 {
		panic("sluice: Tumbling requires a positive size")
	}
	if maxPerWindow <= 0 {
		panic("sluice: Tumbling requires a positive n")
	}
	if policy != sluice.Fail && policy != sluice.DropNewest {
		panic("sluice: Tumbling requires an Overflow of Fail or DropNewest")
	}
	if late != LateFail && late != LateDrop {
		panic("sluice: Tumbling requires a Late of LateFail or LateDrop")
	}

	return func(yield func(sluice.Batch[Pane[T]]) bool) {
		var (
			buf   []T       // the open window's elements
			start time.Time // its lower bound
			end   time.Time // its exclusive upper bound, kept to avoid a divide
			open  bool
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
				// Late only means older than the open window: anything from
				// its start onwards is placeable, whatever order it came in.
				if open && at.Before(start) {
					if late == LateFail {
						panic(sluice.ErrUnsorted)
					}
					continue // LateDrop: the window it belonged to is gone
				}

				// Truncate is a 64-bit division, and the common element does
				// not need it: nothing below the open window's start reaches
				// here, so an element below its end is inside it. One comparison
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
				if len(buf) == maxPerWindow {
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
