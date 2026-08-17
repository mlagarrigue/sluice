// Package sluice is a dataflow engine: a single data model — the Stream — for
// web serving, database access and ETL.
//
// A sluice regulates flow, and that is what this package does. The consumer
// sets the pace; the producer only produces what is pulled from it. This
// back-pressure is not a mechanism bolted on, it follows from the shape of the
// type.
//
// # The core
//
// Two types, and nothing else:
//
//	Stream[T] — a sequence of batches, pulled by the consumer
//	Batch[T]  — a batch of values, the unit of transport
//
// Everything else in the framework (diagnostics, joins, connectors, HTTP) is
// built on top without the core knowing about it. That is deliberate: a core
// that knows its extensions can no longer evolve.
//
// # Why batches
//
// The batch is the unit of transport, never the element. A function that
// handles N elements efficiently handles 1 without effort; the converse does
// not hold. Measured in this repository: a batched pipeline is ~2x faster than
// an element-wise one for identical useful work, and the gap widens to 179x for
// operators that must pull from several streams at once. See
// docs/BENCHMARK-STEP-0.md.
//
// # Zero dependencies
//
// Production code imports the standard library only.
package sluice

import (
	"errors"
	"iter"
	"slices"
	"strconv"
)

// ErrSplitStalled reports that a [Split] branch cannot advance because a
// sibling branch is holding an undrained batch.
//
// It signals a wiring mistake, not a runtime condition: branches were consumed
// one after the other instead of in alternation. Split panics with this value
// rather than returning it, because an iter.Seq has nowhere to put an error and
// yielding a silent prefix of the data would be worse. Recover on it only to
// improve the diagnostic.
var ErrSplitStalled = errors.New("sluice: Split branch stalled — consume branches in alternation, not one after the other")

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
var ErrSplitDrained = errors.New("sluice: Split branch consumed after the source was drained — consume branches in alternation, not one after the other")

// DefaultBatchSize is the default number of elements per batch.
//
// Measurement (docs/BENCHMARK-STEP-0.md) shows a performance plateau from 8
// elements upward, flat through one million: this setting is safe but not
// critical. A batch thinned to a few dozen elements by a selective filter does
// not degrade throughput.
const DefaultBatchSize = 1024

// Batch is a group of values, the framework's unit of transport.
//
// Items may be a slice borrowed from a reusable buffer, valid only for the
// duration of the call that receives it. An operator that keeps Items beyond
// that call must copy it. This is the one core rule a caller can break
// silently.
type Batch[T any] struct {
	Items []T
}

// Len reports the number of elements in the batch.
func (b Batch[T]) Len() int { return len(b.Items) }

// Stream is a sequence of batches whose pace the consumer controls.
//
// It is an iter.Seq: it works with range and composes with the standard
// library.
//
//	for b := range s {
//	    for _, v := range b.Items {
//	        // ...
//	    }
//	}
//
// Early stop: the consumer returns false, the generator unwinds and its
// deferred calls run immediately. Every operator must propagate that false and
// let the generator return — swallowing it breaks upstream resource release,
// silently.
//
// A Stream cannot be replayed. Consuming it twice runs it twice, or yields
// nothing at all if its source is single-pass. To feed two consumers, use
// [Split].
type Stream[T any] iter.Seq[Batch[T]]

// Of builds a Stream from a slice, cut into batches of size elements.
//
// Batches share src's memory: they stay valid as long as src does. A size of
// zero or less means [DefaultBatchSize].
func Of[T any](src []T, size int) Stream[T] {
	if size <= 0 {
		size = DefaultBatchSize
	}
	return func(yield func(Batch[T]) bool) {
		for i := 0; i < len(src); i += size {
			if !yield(Batch[T]{Items: src[i:min(i+size, len(src))]}) {
				return
			}
		}
	}
}

// Empty is a Stream with no elements.
func Empty[T any]() Stream[T] {
	return func(func(Batch[T]) bool) {}
}

// Map applies f to every element, in place within the batch.
//
// The transformation runs per batch: one closure indirection for all elements,
// then a tight loop the compiler can optimize. This is what makes the batched
// model faster than the element-wise one.
//
// f must not retain a reference to the element beyond the call.
//
// Map writes in place, and [Of] batches share the caller's slice: composing the
// two therefore overwrites that slice. Pass a copy when the input must survive
// the pipeline.
func Map[T any](s Stream[T], f func(T) T) Stream[T] {
	return func(yield func(Batch[T]) bool) {
		s(func(b Batch[T]) bool {
			items := b.Items
			for i := range items {
				items[i] = f(items[i])
			}
			return yield(b)
		})
	}
}

// Convert turns a Stream[A] into a Stream[B].
//
// Unlike [Map], the type changes, so a new batch must be allocated. The output
// buffer is reused across batches, meaning the produced batch is valid only for
// the duration of the call — retaining it requires a copy.
func Convert[A, B any](s Stream[A], f func(A) B) Stream[B] {
	return func(yield func(Batch[B]) bool) {
		var out []B
		s(func(b Batch[A]) bool {
			// Convert writes by index, so it needs the length rather than
			// append's growth. Grow amortizes it: a source whose batches get
			// bigger reallocates log(n) times instead of once per new maximum.
			out = slices.Grow(out[:0], len(b.Items))[:len(b.Items)]
			for i, v := range b.Items {
				out[i] = f(v)
			}
			return yield(Batch[B]{Items: out})
		})
	}
}

// Filter keeps only the elements satisfying keep.
//
// Output batches may be smaller than input ones, or empty — an empty batch is
// emitted rather than skipped, so downstream operators keep the upstream
// cadence. Use [Coalesce] to recompact after a highly selective filter.
//
// The output batch reuses an internal buffer: it is valid only for the duration
// of the call.
func Filter[T any](s Stream[T], keep func(T) bool) Stream[T] {
	return func(yield func(Batch[T]) bool) {
		var out []T
		s(func(b Batch[T]) bool {
			// No explicit sizing: append grows the buffer in amortized time and
			// it is reused across batches, so a source whose batches grow costs
			// log(n) reallocations rather than one per new high-water mark.
			out = out[:0]
			for _, v := range b.Items {
				if keep(v) {
					out = append(out, v)
				}
			}
			return yield(Batch[T]{Items: out})
		})
	}
}

// Completion decides when a merged stream ends. There is no sensible default:
// stopping at the first exhausted source silently drops the rest, and waiting
// for all of them stalls a pipeline whose point was the first result. The
// caller states which one it wants.
type Completion int

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

// Merge interleaves several streams, one batch at a time from each.
//
// Sources are pulled in round-robin: one batch from the first, one from the
// second, and so on. Order within a batch is preserved; order between sources
// is not a guarantee, only the current behaviour.
//
// done says when the merged stream ends — see [WhenAll] and [WhenAny]. Merging
// no streams yields nothing, whatever done says.
//
// Cost is O(1) in memory whatever the stream length: one [iter.Pull] per
// source, no buffering. The batch is what makes this affordable — the pull
// machinery costs ~68 ns per call, which a 1024-element batch amortizes to
// ~0.07 ns per element. Element-wise, the same operator would be unusable.
//
// Batches are passed through untouched, so an operator upstream that reuses its
// buffer keeps that contract here: retaining Items requires a copy.
func Merge[T any](done Completion, streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return Empty[T]()
	}
	return func(yield func(Batch[T]) bool) {
		next := make([]func() (Batch[T], bool), len(streams))
		stops := make([]func(), len(streams))
		// One deferred call for the whole set rather than one per source: the
		// sources must be released together however this function returns —
		// exhaustion, early stop, or a panic crossing the yield.
		defer func() {
			for _, stop := range stops {
				stop()
			}
		}()
		for i, s := range streams {
			next[i], stops[i] = iter.Pull(iter.Seq[Batch[T]](s))
		}

		live := len(streams)
		for live > 0 {
			for i, n := range next {
				if n == nil {
					continue
				}
				b, ok := n()
				if !ok {
					next[i] = nil
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

// ErrUnsorted reports that a [MergeJoinBy] input went backwards.
//
// The operator walks both inputs once and never looks back, so an out-of-order
// key silently produces a wrong result — rows that should have matched are
// emitted as unmatched, and no error surfaces anywhere. Rather than document
// that as a caveat, MergeJoinBy checks the order it depends on and panics with
// this value. The check costs one comparison per element against the several
// the merge already performs; it is not worth making optional.
//
// Recover on it only to improve the diagnostic. It signals unsorted input — a
// wiring mistake — not a runtime condition to handle.
var ErrUnsorted = errors.New("sluice: MergeJoinBy input is not sorted by its key")

// EitherOrBoth carries one row of a [MergeJoinBy] result: a left row, a right
// row, or a matched pair.
//
// Values are held directly rather than behind pointers. A pointer into an
// operator's reused buffer dangles as soon as the next batch arrives, which is
// exactly the mistake the Batch contract warns about; carrying values keeps
// each row valid on its own terms.
type EitherOrBoth[L, R any] struct {
	Left     L
	Right    R
	HasLeft  bool
	HasRight bool
}

// Both reports whether the row matched on both sides.
func (e EitherOrBoth[L, R]) Both() bool { return e.HasLeft && e.HasRight }

// MergeJoinBy merges two streams sorted on a common key, in O(1) memory over
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
// requires. That is the one place memory grows: a key repeated n times on the
// left and m times on the right buffers those runs to pair them, costing
// O(n+m). Keys unique on at least one side — the usual case for a join on an
// identifier — cost O(1).
//
// Output batches reuse an internal buffer, like [Filter] and [Convert]:
// retaining Items beyond the call that receives it requires a copy.
//
// Rows accumulate until a batch is full, so the inputs run ahead of what the
// consumer has seen: an early stop takes effect at the next batch boundary, not
// at the next row. That is the cost of emitting full batches rather than
// one-row ones, and it is what makes the operator worth its name.
//
// MergeJoinBy panics with [ErrUnsorted] if either input goes backwards, and
// with a plain message if cmp, keyL or keyR is nil.
func MergeJoinBy[L, R any, K any](
	left Stream[L],
	right Stream[R],
	keyL func(L) K,
	keyR func(R) K,
	cmp func(K, K) int,
) Stream[EitherOrBoth[L, R]] {
	if keyL == nil || keyR == nil {
		panic("sluice: MergeJoinBy requires non-nil key functions")
	}
	if cmp == nil {
		panic("sluice: MergeJoinBy requires a non-nil cmp function")
	}

	return func(yield func(Batch[EitherOrBoth[L, R]]) bool) {
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
				lc.next()
				if !out.push(EitherOrBoth[L, R]{Left: lv, HasLeft: true}) {
					return
				}

			case !lok: // left exhausted: the rest of right is unmatched
				rc.next()
				if !out.push(EitherOrBoth[L, R]{Right: rv, HasRight: true}) {
					return
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
					// Equal keys: emit the cross product of the two runs. This
					// is the only path that holds more than one element.
					lrun := lc.takeRun(lk)
					rrun := rc.takeRun(rk)
					for _, l := range lrun {
						for _, r := range rrun {
							if !out.push(EitherOrBoth[L, R]{
								Left: l, Right: r, HasLeft: true, HasRight: true,
							}) {
								return
							}
						}
					}
				}
			}
		}
	}
}

// cursor reads one input of a merge join element by element, checking as it
// goes that the keys never go backwards.
//
// The current element and its key are cached: the merge loop peeks the same
// element several times before consuming it — once to compare, once more to
// gather a run — and extracting the key each time was measured at 44% of the
// operator's runtime. The cache is invalidated by [cursor.next], the only thing
// that moves the cursor.
type cursor[T, K any] struct {
	pull  func() (Batch[T], bool)
	stop  func()
	batch []T
	pos   int
	done  bool
	key   func(T) K
	cmp   func(K, K) int

	cur     T
	curKey  K
	cached  bool
	lastKey K
	hasLast bool
	run     []T // scratch for takeRun, reused between calls
}

func newCursor[T any, K any](s Stream[T], key func(T) K, cmp func(K, K) int) *cursor[T, K] {
	n, stop := iter.Pull(iter.Seq[Batch[T]](s))
	return &cursor[T, K]{pull: n, stop: stop, key: key, cmp: cmp}
}

// peek returns the current element and its key without consuming it. Repeated
// calls return the cached value rather than re-extracting the key.
//
// Upstream operators may emit empty batches — [Filter] does, to keep the
// cadence — so the advance loops rather than testing once.
func (c *cursor[T, K]) peek() (elem T, key K, ok bool) {
	if c.cached {
		return c.cur, c.curKey, true
	}
	for c.pos >= len(c.batch) {
		if c.done {
			var (
				zeroT T
				zeroK K
			)
			return zeroT, zeroK, false
		}
		b, ok := c.pull()
		if !ok {
			c.done = true
			continue
		}
		c.batch, c.pos = b.Items, 0
	}

	v := c.batch[c.pos]
	k := c.key(v)

	// Check monotonicity as each element is first seen, so every element is
	// checked exactly once whichever path goes on to consume it.
	if c.hasLast && c.cmp(c.lastKey, k) > 0 {
		panic(ErrUnsorted)
	}
	c.lastKey, c.hasLast = k, true

	c.cur, c.curKey, c.cached = v, k, true
	return v, k, true
}

// next consumes the current element, invalidating the cache.
func (c *cursor[T, K]) next() {
	c.pos++
	c.cached = false
}

// takeRun consumes every consecutive element sharing the current key and
// returns them. The returned slice is reused by the next call.
// It is only called with an element available — the equal-keys branch has just
// peeked one — so the run is never empty.
func (c *cursor[T, K]) takeRun(k K) []T {
	c.run = c.run[:0]
	for {
		v, vk, ok := c.peek()
		if !ok || c.cmp(vk, k) != 0 {
			return c.run
		}
		c.run = append(c.run, v)
		c.next()
	}
}

// emitter batches rows on their way out, so a merge join produces full batches
// rather than one-element ones.
type emitter[T any] struct {
	yield func(Batch[T]) bool
	buf   []T
}

func newEmitter[T any](yield func(Batch[T]) bool) *emitter[T] {
	return &emitter[T]{yield: yield, buf: make([]T, 0, DefaultBatchSize)}
}

// push adds a row, flushing when the buffer fills. It reports false once the
// consumer has stopped.
func (e *emitter[T]) push(v T) bool {
	e.buf = append(e.buf, v)
	if len(e.buf) < DefaultBatchSize {
		return true
	}
	ok := e.yield(Batch[T]{Items: e.buf})
	e.buf = e.buf[:0]
	return ok
}

// flush emits what is left, if anything, and reports whether the consumer took
// it. Today's callers return immediately either way, but emitter is the shared
// way for an N-to-1 operator to batch its output: the next one to have work
// after the flush would silently lose the refusal if the result were dropped
// here. Returning it costs nothing and keeps that from being a new bug.
func (e *emitter[T]) flush() bool {
	if len(e.buf) == 0 {
		return true
	}
	ok := e.yield(Batch[T]{Items: e.buf})
	e.buf = e.buf[:0]
	return ok
}

// ZipLongest pairs two streams positionally: first with first, second with
// second, and so on until the longer one runs out.
//
// The output carries [EitherOrBoth] rows, like [MergeJoinBy]: Both() while
// both streams still have elements, then HasLeft or HasRight alone for the tail
// of whichever is longer. Nothing is dropped.
//
// This is the primitive rather than a Zip that stops at the shorter stream,
// because stopping at the shorter one hides bugs: two streams that were meant
// to be the same length quietly produce a truncated result. Rust found it
// necessary to add zip_eq, which panics on unequal lengths — a sign the
// stop-at-shortest default is considered dangerous. A caller who wants it asks:
//
//	pairs := Filter(ZipLongest(a, b), EitherOrBoth[A, B].Both)
//
// Memory is O(1): one [iter.Pull] per stream, no queue. In a push model a zip
// needs an unbounded buffer per source, because a single slow producer forces
// the others to keep their values somewhere; pulling makes that impossible by
// construction.
//
// Output batches reuse an internal buffer: retaining Items beyond the call that
// receives it requires a copy. Rows accumulate until a batch is full, so an
// early stop takes effect at the next batch boundary rather than the next row.
func ZipLongest[L, R any](left Stream[L], right Stream[R]) Stream[EitherOrBoth[L, R]] {
	return func(yield func(Batch[EitherOrBoth[L, R]]) bool) {
		lc := newWalker[L](left)
		defer lc.stop()
		rc := newWalker[R](right)
		defer rc.stop()

		out := newEmitter(yield)

		for {
			lv, lok := lc.next()
			rv, rok := rc.next()

			switch {
			case !lok && !rok:
				_ = out.flush() // nothing follows: both streams are exhausted
				return
			case lok && rok:
				if !out.push(EitherOrBoth[L, R]{
					Left: lv, Right: rv, HasLeft: true, HasRight: true,
				}) {
					return
				}
			case lok:
				if !out.push(EitherOrBoth[L, R]{Left: lv, HasLeft: true}) {
					return
				}
			default:
				if !out.push(EitherOrBoth[L, R]{Right: rv, HasRight: true}) {
					return
				}
			}
		}
	}
}

// walker reads one input element by element, without the key extraction and
// lookahead a merge join needs. [cursor] does more and costs more; positional
// pairing needs neither.
type walker[T any] struct {
	pull  func() (Batch[T], bool)
	stop  func()
	batch []T
	pos   int
	done  bool
}

func newWalker[T any](s Stream[T]) *walker[T] {
	n, stop := iter.Pull(iter.Seq[Batch[T]](s))
	return &walker[T]{pull: n, stop: stop}
}

// next returns the next element and consumes it. Upstream operators may emit
// empty batches — [Filter] does — so the advance loops rather than testing once.
func (w *walker[T]) next() (elem T, ok bool) {
	for w.pos >= len(w.batch) {
		if w.done {
			var zero T
			return zero, false
		}
		b, ok := w.pull()
		if !ok {
			w.done = true
			continue
		}
		w.batch, w.pos = b.Items, 0
	}
	v := w.batch[w.pos]
	w.pos++
	return v, true
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

// Coalesce regroups batches until they hold size elements.
//
// Place it after a selective operator — [Filter], a join — to keep sparse
// batches from propagating across several stages. A size of zero or less means
// [DefaultBatchSize].
//
// Coalesce accumulates into an internal buffer, so it costs O(size) memory, and
// the batch it produces is valid only for the duration of the call. The buffer
// is allocated once and refilled, so two batches retained without copying alias
// each other.
//
// An early stop discards the elements accumulated since the last emitted batch
// — up to size-1 of them. They are not flushed, because the consumer asked to
// stop.
func Coalesce[T any](s Stream[T], size int) Stream[T] {
	if size <= 0 {
		size = DefaultBatchSize
	}
	return func(yield func(Batch[T]) bool) {
		buf := make([]T, 0, size)
		stopped := false
		s(func(b Batch[T]) bool {
			for _, v := range b.Items {
				buf = append(buf, v)
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
// Like a Stream, a Split that is never consumed runs nothing: the source is not
// touched, and there is nothing to release.
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
	next, stop := iter.Pull(iter.Seq[Batch[T]](s))

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
