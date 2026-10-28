package sluice

import (
	"strconv"

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
// route receives a batch and returns the indices of its destination branches,
// each in [0, n); an index outside panics, naming it. One primitive covers
// three uses:
//
//	partition — return one index, chosen from the contents
//	balance   — return one index, round-robin
//	broadcast — return every index
//
// The source is traversed exactly once, whatever the number of branches, so
// Split works on single-pass sources — a cursor, a network read.
//
// How the branches are read is the contract, and docs/guide/operators.md
// ("Split: reading the branches") spells it out with examples:
//
//   - A branch read on its own gets its batches; the unread ones get nothing
//     and must not be read afterwards — reading one once the source is dry
//     panics with [ErrSplitDrained]. A branch route never chose is simply
//     empty.
//   - Branches that must all receive are read in alternation, from one
//     goroutine ([iter.Pull], or lock-step as the Split broadcast example does):
//     each holds at most one pending batch, which is what keeps memory at one
//     batch per branch, and draining one to the end while another is mid-read
//     panics with [ErrSplitStalled]. Reading two branches from two goroutines
//     is a data race, not detected — build with -race.
//   - The batch is shared by its destinations without copying, and valid until
//     the source advances, which any branch can make it do: copy it to keep
//     it, or before calling next on a sibling.
//   - A branch that stops early keeps the source open for its siblings. If
//     the last one read stops early and the others are never read, the
//     source's coroutine stays parked for the life of the process (S9 in
//     docs/design/architecture.md): read any leftover branch to release it.
//
// Like a Stream, a Split never consumed runs nothing: the source's traversal
// starts at the first read of a branch, so a Split built and abandoned
// holds no coroutine. Split panics if route is nil or n is negative; an n of
// zero returns nil.
func Split[T any](s Stream[T], n int, route func(Batch[T]) []int) []Stream[T] {
	if n < 0 {
		panic(negativeArg{"Split n", n, "no branches"})
	}
	if n == 0 {
		return nil
	}
	if route == nil {
		panic("sluice: Split requires a non-nil route function")
	}
	// One shared traversal, driven on demand and started by the first read
	// (advance): pulling here would start the source's coroutine at
	// construction, and a Split never read would leave it parked with no
	// handle to stop it.
	sp := &splitter[T]{branches: make([]branch[T], n), src: s, route: route}
	out := make([]Stream[T], n)
	for i := range out {
		out[i] = func(yield func(Batch[T]) bool) { sp.serve(i, yield) }
	}
	return out
}

// branch is where one destination of a Split stands.
type branch[T any] struct {
	slot    Batch[T] // at most one pending batch
	pending bool
	started bool
	done    bool // detached, by exhaustion or early stop

	// routed: named a destination at least once. This is what separates a
	// branch that is empty because it arrived too late from one that is empty
	// because route never chose it — a partition branch that matches nothing
	// is legitimately empty, and reading it must stay silent. Per branch:
	// whether the source produced anything says nothing about whether this
	// branch was ever a destination.
	routed bool

	// detached: written off by detach rather than consumed. The branch never
	// ran, and consuming it now would silently yield nothing. Kept apart from
	// done, which a branch also reaches by being consumed normally.
	detached bool
}

// splitter is what the branches of one Split share.
//
// Nothing here is synchronized, so consuming two branches concurrently is a
// data race. That is documented on Split rather than detected, and
// deliberately so: the legitimate way to drive two branches is iter.Pull,
// which runs each branch body on its own coroutine and leaves it suspended
// inside yield between calls. A branch held that way is indistinguishable —
// by entry counter or by goroutine identity — from a branch running
// concurrently in another goroutine. Any detector precise enough to catch the
// mistake also rejects ExampleSplit_broadcast, so the check would cost more
// than it buys. The race detector does catch it, which is what the test suite
// relies on.
type splitter[T any] struct {
	branches []branch[T]
	// src is the source until the first read pulls it: Pull turns the
	// push-based source into something a branch can advance one batch at
	// a time, which is what lets every branch read the same single
	// traversal. next and stop are nil before that.
	src   Stream[T]
	next  func() (Batch[T], bool)
	stop  func()
	route func(Batch[T]) []int

	held    Batch[T] // batch pulled but not yet placed everywhere
	dests   []int    // held's destinations, routed once when pulled
	holding bool
	drained bool // the source has no batch left
}

// serve is branch i's body: yield what is pending, drive the source when
// nothing is.
func (sp *splitter[T]) serve(i int, yield func(Batch[T]) bool) {
	br := &sp.branches[i]
	// Attached too late: a sibling has already driven the source past this
	// branch, either draining it or forcing this one to be written off. Either
	// way it can only yield nothing, which is the silent prefix this operator
	// exists to remove — so it is reported.
	//
	// routed keeps this apart from the legitimate empty branch, and it is the
	// branch's own history that decides: a branch route never chose is empty
	// because nothing was addressed to it, exactly as every branch over an
	// empty source is. Checked before done, because detach reaches done by a
	// different route than ordinary consumption.
	if br.routed && !br.pending && (br.detached || (sp.drained && !sp.holding && !br.started && !br.done)) {
		panic(ErrSplitDrained)
	}
	if br.done {
		return // already consumed or stopped: a branch is single-pass too
	}
	br.started = true
	defer func() {
		br.done = true
		sp.release()
	}()

	for {
		if br.pending {
			b := br.slot
			br.slot, br.pending = Batch[T]{}, false
			if !yield(b) {
				return
			}
			continue
		}
		if sp.drained && !sp.holding {
			return // the source is exhausted: normal end of branch
		}
		// Nothing waiting: drive the source until this branch is served or
		// progress becomes impossible.
		if !sp.advance() {
			if sp.drained && !sp.holding {
				return
			}
			// Blocked. Branches nobody ever consumed are written off first —
			// they are the common case, a partition read one branch at a time.
			if sp.detach() {
				continue
			}
			// Still blocked: a branch that *is* being consumed holds an
			// undrained batch, so the caller is draining branches one after
			// the other instead of alternating. Failing loudly beats yielding
			// a silent prefix of the data.
			panic(ErrSplitStalled)
		}
	}
}

// release ends the shared traversal once no branch can consume from it, so
// the source's deferred calls run promptly rather than at GC time.
//
// A branch that has not been consumed yet counts as a possible consumer: a
// caller may finish with one branch before starting the next. Stopping the
// source on the strength of the started branches alone would cut that second
// branch off.
//
// Each branch calls this once, from its deferred close, so the call that
// finds every branch done is necessarily the last one: stop runs exactly once
// without needing a guard.
func (sp *splitter[T]) release() {
	for i := range sp.branches {
		if !sp.branches[i].done {
			return // still a branch that could consume
		}
	}
	if sp.stop != nil { // nil: no branch ever pulled, nothing started
		sp.stop()
	}
}

// live reports whether dst can receive a batch. A branch that has finished is
// out; one that has not started yet still counts, because a caller is allowed
// to attach branches in any order.
func (sp *splitter[T]) live(dst int) bool {
	return dst >= 0 && dst < len(sp.branches) && !sp.branches[dst].done
}

// place deposits the held batch into every live destination, provided none of
// them still holds an undrained one. Refusing to overwrite a full slot is what
// bounds memory to one batch per branch without losing data: the batch stays
// held, and the caller learns it cannot make progress.
//
// A branch that has not been consumed yet is served optimistically — it may
// still be attached. If it never is, the deposit is undone by detach.
//
// It works from dests, decided once when the batch was pulled: place may run
// several times for one batch, and route is the caller's function — calling it
// again would re-run its side effects, breaking round-robin routing and
// costing an allocation per retry.
func (sp *splitter[T]) place() bool {
	for _, dst := range sp.dests {
		if sp.live(dst) && sp.branches[dst].pending {
			return false // that branch must be drained first
		}
	}
	for _, dst := range sp.dests {
		if !sp.live(dst) {
			continue
		}
		br := &sp.branches[dst]
		br.slot, br.pending, br.routed = sp.held, true, true
	}
	sp.held, sp.holding, sp.dests = Batch[T]{}, false, nil
	return true
}

// detach writes off the branches that are blocking progress and have never
// been consumed. A caller who has started reading and needs another batch has,
// by that act, shown which branches are in play: whatever is still unattached
// at that point never will be.
//
// This is what lets a partition be consumed one branch at a time without
// buffering for readers that will never arrive.
func (sp *splitter[T]) detach() bool {
	freed := false
	for i := range sp.branches {
		br := &sp.branches[i]
		if !br.started && !br.done && br.pending {
			br.slot, br.pending = Batch[T]{}, false
			br.done, br.detached = true, true
			freed = true
		}
	}
	return freed
}

// advance moves the shared traversal forward by at most one batch. It reports
// false when no further progress is possible — either the source is
// exhausted, or a sibling branch is holding up the pipeline.
//
// Callers check drained before calling, so the source is only pulled when it
// may still have something to give.
func (sp *splitter[T]) advance() bool {
	if sp.holding {
		return sp.place()
	}
	if sp.next == nil {
		sp.next, sp.stop = pull(sp.src)
		sp.src = nil
	}
	b, ok := sp.next()
	if !ok {
		sp.drained = true
		return false
	}
	sp.held, sp.holding, sp.dests = b, true, sp.route(b)
	n := len(sp.branches)
	for _, dst := range sp.dests {
		if dst < 0 || dst >= n {
			panic("sluice: Split route returned branch " + strconv.Itoa(dst) + ", outside [0, " + strconv.Itoa(n) + ")")
		}
	}
	return sp.place()
}
