package sluice

import (
	"github.com/mlagarrigue/sluice/internal/batch"
)

// ErrSplitStalled reports that a [Split] branch cannot advance because a
// sibling branch is holding an undrained batch.
//
// It signals a wiring mistake, not a runtime condition: branches were consumed
// one after the other instead of in alternation. Split panics with this value
// rather than returning it, because an iter.Seq has nowhere to put an error and
// yielding a silent prefix of the data would be worse. Recover on it only to
// improve the diagnostic.
var ErrSplitStalled = batch.NewSentinel("sluice: Split branch stalled — consume branches in alternation, not one after the other")

// ErrSplitDrained reports that a [Split] branch was consumed after a sibling
// had already drained the source.
//
// The source is single-pass and traversed once, so a branch attached after it
// runs dry has nothing left to receive: it would yield nothing at all. That is
// the silent-prefix failure this operator exists to remove, so it panics here
// too rather than return an empty stream that reads like a legitimate one.
//
// It separates two cases a caller cannot otherwise tell apart: a branch that is
// empty because nothing was ever addressed to it — no panic — and a branch that
// is empty because it arrived too late. Only a branch that was named a
// destination at least once can be late, so a branch route never chose stays
// silent, as does every branch over an empty source. Consume branches in
// alternation, or accept that only the first one consumed receives anything and
// do not attach the others.
//
// Recover on it only to improve the diagnostic. It signals a consumption-order
// mistake, not a runtime condition.
var ErrSplitDrained = batch.NewSentinel("sluice: Split branch consumed after the source was drained — consume branches in alternation, not one after the other")

// Split routes each batch to one or more of n branches.
//
// route receives a batch and returns the indices of its destination branches;
// indices outside [0, n) are ignored. This single primitive covers three uses:
//
//	partition — return one index, chosen from the contents
//	balance   — return one index, round-robin
//	broadcast — return every index
//
// The source is traversed exactly once, whatever the number of branches: Split
// works on single-pass sources — a cursor, a network read — not only on
// replayable ones.
//
// # Branches only receive while they are being consumed
//
// A branch nobody consumes is not a destination: batches routed to it are
// dropped, so one branch of a partition can be read on its own without the
// unread ones stalling the pipeline. Consuming a branch a second time yields
// nothing — a branch is single-pass, like the stream it comes from.
//
// "One branch on its own" means exactly that: the branch you read gets its
// batches, the others get nothing and must not be read afterwards. Reading a
// second branch once the first has run the source dry panics with
// [ErrSplitDrained] rather than yield an empty stream that looks legitimate.
// Two branches that must both receive have to be consumed in alternation, as
// below.
//
// A branch route never chose is not "read afterwards" in that sense: nothing
// was ever addressed to it, so it is empty for the same reason every branch
// over an empty source is, and reading it yields nothing without panicking. A
// partition branch that matches none of the data is the ordinary case of this.
//
// # Consume concurrent branches in alternation
//
// Each live branch holds at most one pending batch. Whichever branch is asked
// for a batch drives the source and deposits the result in every live
// destination; a branch whose slot is still full must be drained before the
// source can advance. Two branches consumed at once must therefore alternate —
// use [iter.Pull] on each, or range over them in lock-step:
//
//	next0, stop0 := iter.Pull(iter.Seq[Batch[T]](branches[0]))
//	next1, stop1 := iter.Pull(iter.Seq[Batch[T]](branches[1]))
//	defer stop0()
//	defer stop1()
//
// Draining one branch to exhaustion while another is mid-consumption panics
// with [ErrSplitStalled]. That is deliberate: the alternative is a silent
// prefix of the data, which is worse.
//
// The bounded slot is what keeps memory at one batch per branch. The cost is
// that concurrent branches are not independent — the trade-off every
// single-goroutine fan-out must make, and the one this package chooses.
//
// # Every branch belongs to the same goroutine
//
// "In alternation" above means interleaved from one goroutine, as
// [ExampleSplit_broadcast] shows. It does not mean in parallel: Split keeps its
// routing state in variables shared by every branch, with no synchronization,
// so consuming two branches from two goroutines is a data race — undefined
// behaviour, not merely a slower path.
//
// This one is not detected, unlike the stall above. The legitimate way to drive
// two branches is [iter.Pull], which suspends each branch inside its yield
// between calls; a branch parked that way cannot be told apart from one running
// in another goroutine, so any check strict enough to catch the mistake also
// rejects correct code. Build with -race to catch it.
//
// To feed goroutines from a Split, consume the branches in one goroutine and
// hand the batches on through channels — copying each batch, since the slot is
// reused.
//
// The batch is shared between branches without copying: a branch that mutates
// it affects the ones after it. Copy when that matters.
//
// It is also valid only until the source advances, and any branch can advance
// it: a next on a sibling can drive the source, which may refill the buffer
// the batch you hold points into. Under [iter.Pull], finish with a batch — or
// copy it — before calling next on another branch.
//
// Like a Stream, a Split that is never consumed runs nothing: the source is not
// touched, and there is nothing to release.
//
// A branch that stops early keeps the source open for its siblings — an
// unconsumed branch still counts as a possible consumer, which is what lets a
// caller finish one branch before starting the next. The cost falls on the
// caller who walks away instead: if the last branch being read stops before the
// source is exhausted and the remaining branches are never read at all, the
// source's coroutine stays parked for the life of the process — the leak S9
// exists to prevent. Consuming any leftover branch, even one route never
// chose, drains or releases the source and costs at most one pass; walking
// away costs a goroutine.
//
// Split panics if route is nil.
func Split[T any](s Stream[T], n int, route func(Batch[T]) []int) []Stream[T] {
	if n <= 0 {
		return nil
	}
	if route == nil {
		panic("sluice: Split requires a non-nil route function")
	}

	// One shared traversal, driven on demand. Pull turns the push-based source
	// into something a branch can advance one batch at a time, which is what
	// lets every branch read the same single traversal.
	next, stop := pull(s)

	var (
		slot    = make([]Batch[T], n) // at most one pending batch per branch
		pending = make([]bool, n)
		started = make([]bool, n)
		done    = make([]bool, n) // branch detached, by exhaustion or early stop
		held    Batch[T]          // batch pulled but not yet placed everywhere
		dests   []int             // held's destinations, routed once when pulled
		holding bool
		drained bool // the source has no batch left

		// named a destination at least once. This is what separates a branch
		// that is empty because it arrived too late from one that is empty
		// because route never chose it — a partition branch that matches
		// nothing is legitimately empty, and reading it must stay silent.
		//
		// Tracked per branch rather than globally: whether the source produced
		// anything says nothing about whether *this* branch was ever a
		// destination.
		routed = make([]bool, n)

		// written off by detach rather than consumed: the branch never ran, and
		// consuming it now would silently yield nothing. Kept apart from done,
		// which a branch also reaches by being consumed normally.
		detached = make([]bool, n)
	)

	// Every variable above is shared by the branches and unsynchronized, so
	// consuming two branches concurrently is a data race. That is documented on
	// Split rather than detected, and deliberately so: the legitimate way to
	// drive two branches is iter.Pull, which runs each branch body on its own
	// coroutine and leaves it suspended inside yield between calls. A branch
	// held that way is indistinguishable — by entry counter or by goroutine
	// identity — from a branch running concurrently in another goroutine. Any
	// detector precise enough to catch the mistake also rejects
	// ExampleSplit_broadcast, so the check would cost more than it buys.
	//
	// The race detector does catch it, which is what the test suite relies on.

	// release ends the shared traversal once no branch can consume from it, so
	// the source's deferred calls run promptly rather than at GC time.
	//
	// A branch that has not been consumed yet counts as a possible consumer: a
	// caller may finish with one branch before starting the next. Stopping the
	// source on the strength of the started branches alone would cut that
	// second branch off.
	//
	// Each branch calls this once, from its deferred close, so the call that
	// finds every branch done is necessarily the last one: stop runs exactly
	// once without needing a guard.
	release := func() {
		for i := range done {
			if !done[i] {
				return // still a branch that could consume
			}
		}
		stop()
	}

	// live reports whether dst can receive a batch. A branch that has finished
	// is out; one that has not started yet still counts, because a caller is
	// allowed to attach branches in any order.
	live := func(dst int) bool {
		return dst >= 0 && dst < n && !done[dst]
	}

	// place deposits the held batch into every live destination, provided none
	// of them still holds an undrained one. Refusing to overwrite a full slot is
	// what bounds memory to one batch per branch without losing data: the batch
	// stays held, and the caller learns it cannot make progress.
	//
	// A branch that has not been consumed yet is served optimistically — it may
	// still be attached. If it never is, the deposit is undone by [detach].
	//
	// It works from dests, decided once when the batch was pulled: place may run
	// several times for one batch, and route is the caller's function — calling
	// it again would re-run its side effects, breaking round-robin routing and
	// costing an allocation per retry.
	place := func() bool {
		for _, dst := range dests {
			if live(dst) && pending[dst] {
				return false // that branch must be drained first
			}
		}
		for _, dst := range dests {
			if !live(dst) {
				continue
			}
			slot[dst] = held
			pending[dst] = true
			routed[dst] = true
		}
		held, holding, dests = Batch[T]{}, false, nil
		return true
	}

	// detach writes off the branches that are blocking progress and have never
	// been consumed. A caller who has started reading and needs another batch
	// has, by that act, shown which branches are in play: whatever is still
	// unattached at that point never will be.
	//
	// This is what lets a partition be consumed one branch at a time without
	// buffering for readers that will never arrive.
	detach := func() bool {
		freed := false
		for i := range done {
			if !started[i] && !done[i] && pending[i] {
				slot[i], pending[i] = Batch[T]{}, false
				done[i], detached[i] = true, true
				freed = true
			}
		}
		return freed
	}

	// advance moves the shared traversal forward by at most one batch. It
	// reports false when no further progress is possible — either the source is
	// exhausted, or a sibling branch is holding up the pipeline.
	//
	// Callers check drained before calling, so the source is only pulled when it
	// may still have something to give.
	advance := func() bool {
		if holding {
			return place()
		}
		b, ok := next()
		if !ok {
			drained = true
			return false
		}
		held, holding, dests = b, true, route(b)
		return place()
	}

	out := make([]Stream[T], n)
	for i := range out {
		out[i] = func(yield func(Batch[T]) bool) {
			// Attached too late: a sibling has already driven the source past
			// this branch, either draining it or forcing this one to be written
			// off. Either way it can only yield nothing, which is the silent
			// prefix this operator exists to remove — so it is reported.
			//
			// routed[i] keeps this apart from the legitimate empty branch, and it
			// is the branch's own history that decides: a branch route never
			// chose is empty because nothing was addressed to it, exactly as
			// every branch over an empty source is. Only a branch that did have
			// batches addressed to it can be late. Checked before done, because
			// detach reaches done by a different route than ordinary consumption.
			if routed[i] && !pending[i] && (detached[i] || (drained && !holding && !started[i] && !done[i])) {
				panic(ErrSplitDrained)
			}
			if done[i] {
				return // already consumed or stopped: a branch is single-pass too
			}
			started[i] = true
			defer func() {
				done[i] = true
				release()
			}()

			for {
				if pending[i] {
					b := slot[i]
					slot[i], pending[i] = Batch[T]{}, false
					if !yield(b) {
						return
					}
					continue
				}
				if drained && !holding {
					return // the source is exhausted: normal end of branch
				}
				// Nothing waiting: drive the source until this branch is served
				// or progress becomes impossible.
				if !advance() {
					if drained && !holding {
						return
					}
					// Blocked. Branches nobody ever consumed are written off
					// first — they are the common case, a partition read one
					// branch at a time.
					if detach() {
						continue
					}
					// Still blocked: a branch that *is* being consumed holds an
					// undrained batch, so the caller is draining branches one
					// after the other instead of alternating. Failing loudly
					// beats yielding a silent prefix of the data.
					panic(ErrSplitStalled)
				}
			}
		}
	}
	return out
}
