// Package pushdown is experimental: a consumer tells its source something
// better than "stop" — how many rows it still needs, and how far ahead it
// may skip.
//
// The mechanism is tested and measured on its own, but no supported path
// uses it yet, so its shape may change before v1.
//
// The mechanism has a name — sideways information passing — and a modern
// reference: DataFusion's dynamic filters, where a TopK operator hands its
// current threshold to the upstream scan and the scan tightens as execution
// proceeds, skipping rows and then whole files. The measured gains there reach
// ×22.
//
// What makes it affordable here is the same thing that makes everything else
// here affordable: the demand is read **once per batch**, so the hot path pays
// one atomic load per thousand elements rather than a lock per row.
//
// # Why this is not in the core
//
// The core is `Stream` and `Batch` and the operators that need nothing else.
// A `Take` that published its remaining count would make the core know about
// pushdown, and a core that knows its extensions can no longer evolve. So the
// demand-aware operators live here and **compose** with the core ones: a
// source consults a [Demand], an operator publishes to it, and neither of them
// is a different kind of `Stream` for it.
//
// # The demand only tightens
//
// A limit may fall, a lower bound may rise, a column set may shrink — never
// the reverse. This is not a restriction, it is what makes the mechanism
// sound: a source that has already skipped a thousand rows on the strength of
// a bound cannot un-skip them if the bound relaxes, so a relaxation is a
// programming error rather than a case to handle. Attempting one is ignored
// and reported by [Demand.Violations], where a test can see it, rather than
// silently producing a result that is missing rows.
//
// # Status: experimental
//
// No source in this repository consumes a [Demand] yet: the operators here
// publish, and the postgres and gateway sources do not yet read. The package
// exists anyway because the design is the part worth recording — what a
// demand is, why it only tightens, what one atomic load per batch buys — and
// docs/design/architecture.md ("Enriched stop signal — pushdown") records it against this code rather than against
// a sketch. Treat the API as one that may still move.
package pushdown

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// Unlimited is the limit of a demand that has not been given one.
const Unlimited int64 = -1

// Demand is what a consumer knows and its source does not: how much is still
// wanted, and what can be skipped to get there.
//
// The zero value is usable and means "everything": no limit, no lower bound,
// every column. It is safe for concurrent use — a source reads it while
// operators tighten it — and the read side is lock-free in the case that
// matters, which is the one where nothing changed.
type Demand struct {
	// gen changes whenever anything else does. A reader compares it against
	// what it last saw and takes the lock only when they differ, so a batch
	// that arrives during a quiet stretch costs one atomic load.
	gen atomic.Uint64

	mu         sync.Mutex
	limit      int64
	hasLimit   bool
	lowerBound []byte
	columns    []string
	hasColumns bool
	violations atomic.Int64
}

// Snapshot is a consistent view of a demand, taken when its generation
// changed.
type Snapshot struct {
	// Limit is how many elements the consumer still wants, or [Unlimited].
	Limit int64

	// LowerBound is the key below which nothing is wanted any more; nil means
	// no bound. It is the consumer's own encoding — this package compares
	// bytes and does not interpret them, because the ordering that matters is
	// the source's.
	LowerBound []byte

	// Columns is the set of columns still needed, or nil for all of them.
	//
	// Nil and empty are different answers, and both occur: nil means the
	// consumer never narrowed, empty — non-nil — means it narrowed to nothing,
	// a count-only consumer that needs no payload column at all. Test for nil
	// to ask "was there a narrowing", never for length.
	Columns []string
}

// generation reports the demand's current version.
//
// This is the whole read side of the fast path: a [Reader] keeps the value it
// last acted on, compares, and calls [Demand.Snapshot] only when they differ.
// One atomic load per batch, no lock, no allocation.
func (d *Demand) generation() uint64 { return d.gen.Load() }

// Snapshot takes a consistent view. It locks, so it belongs on the path taken
// when the demand's generation has changed — the check a [Reader] makes — not
// on every batch.
func (d *Demand) Snapshot() Snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Unlimited unless a limit was ever set, which is what makes the zero
	// Demand mean "everything": its limit field is zero, and reporting that
	// zero as the limit would read "wants everything" as "wants nothing".
	s := Snapshot{Limit: Unlimited}
	if d.hasLimit {
		s.Limit = d.limit
	}
	if d.lowerBound != nil {
		// Copied out: the caller holds this while the demand keeps tightening,
		// and a shared slice would change underneath a source mid-scan.
		s.LowerBound = bytes.Clone(d.lowerBound)
	}
	if d.hasColumns {
		// make rather than a bare append: a column set narrowed to nothing —
		// NeedColumns with no survivors — must come out empty and non-nil,
		// because nil is the "never narrowed, decode everything" answer. An
		// append onto nil of zero elements returns nil, which would silently
		// widen a demand at exactly the moment it tightened the most.
		s.Columns = make([]string, len(d.columns))
		copy(s.Columns, d.columns)
	}
	return s
}

// Violations reports how many attempts were made to loosen the demand.
//
// It is not an error channel — a loosening is ignored, so nothing is wrong
// with the data — it is a counter a test asserts on. A pipeline that publishes
// a limit going back up has a bug in its operators, and this is where it
// becomes visible instead of quietly producing a short result.
func (d *Demand) Violations() int64 { return d.violations.Load() }

// LimitTo lowers the remaining limit to n. A value at or above the current
// limit is a loosening and is ignored.
//
// [Unlimited] asks for no limit at all: it is the loosest value there is, so
// it changes nothing — a no-op on a demand without a limit, a counted
// loosening on one that has a limit. Any other negative n means nothing more
// is wanted and is treated as zero.
func (d *Demand) LimitTo(n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n == Unlimited {
		if d.hasLimit {
			d.violations.Add(1)
		}
		return
	}
	if n < 0 {
		n = 0
	}
	if d.hasLimit && n >= d.limit {
		if n > d.limit {
			d.violations.Add(1)
		}
		return
	}
	d.limit, d.hasLimit = n, true
	d.gen.Add(1)
}

// AdvanceTo raises the lower bound to key. A key at or below the current bound
// is a loosening and is ignored.
//
// The comparison is bytewise, which means the caller encodes its keys so that
// bytewise order is the order it means — big-endian for integers, as the
// binary protocol already encodes them.
func (d *Demand) AdvanceTo(key []byte) {
	// An empty key is a non-advance whatever the current bound: the empty
	// key is the bytewise minimum, so there is nothing below it to give up
	// on. One rule for both states — treating it as a violation against an
	// existing bound would flood the counter once per empty-keyed row, and
	// bumping the generation with no bound would send every reader through
	// its slow path per batch, for nothing. A key function that returns nil
	// for some rows must not defeat the fast path this package is for.
	if len(key) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lowerBound != nil {
		switch bytes.Compare(key, d.lowerBound) {
		case 0:
			return
		case -1:
			d.violations.Add(1)
			return
		}
	}
	// Reused rather than cloned: Snapshot copies the bound out, so nothing
	// aliases this slice, and the steady state — one advance per batch —
	// allocates only until the buffer reaches the key size.
	d.lowerBound = append(d.lowerBound[:0], key...)
	d.gen.Add(1)
}

// NeedColumns narrows the set of columns the consumer needs.
//
// Narrowing only: a column already dropped cannot be asked for again, since
// the source may have stopped decoding it batches ago. The first call sets the
// set; later calls intersect it.
func (d *Demand) NeedColumns(names ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.hasColumns {
		d.columns = append([]string(nil), names...)
		d.hasColumns = true
		d.gen.Add(1)
		return
	}

	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	kept := d.columns[:0]
	for _, c := range d.columns {
		if want[c] {
			kept = append(kept, c)
			delete(want, c)
		}
	}
	if len(kept) == len(d.columns) {
		// Nothing was dropped, so any name left in want means the call asked
		// for strictly more than remains — a widening attempt, which is the
		// loosening Violations exists to count. An ordinary intersection —
		// two independent narrowers each stating their own need — both drops
		// and misses, and is not a violation: each caller is narrowing, and
		// the doc invites exactly that shape.
		if len(want) > 0 {
			d.violations.Add(1)
		}
		return
	}
	d.columns = kept
	d.gen.Add(1)
}

// Reader is the source's side of a demand: it holds the last generation acted
// on, so consulting the demand costs one atomic load until something changes.
//
// A Reader is used by one goroutine — the one driving the source — and is not
// itself safe to share.
//
// The zero Reader reads no demand and reports the unlimited snapshot, the
// same as NewReader(nil): [Reader.Current] answers the nil-demand case with a
// fresh unlimited Snapshot rather than with its cached one, which is what
// keeps a zero value — whose cache would otherwise say Limit 0, "wants
// nothing" — from stopping a source before it starts. That branch is the
// no-demand path, so the hot path over a real demand is untouched by it.
type Reader struct {
	demand *Demand
	gen    uint64
	snap   Snapshot
	primed bool

	// pending records a refresh taken by [Reader.Exhausted] that
	// [Reader.Current] has not yet reported: the change stays news until the
	// caller who re-plans has seen it. Without it, a source that checks
	// Exhausted before Current would consume the changed flag in the check and
	// keep its stale plan while believing nothing moved.
	pending bool
}

// NewReader returns a reader over d. A nil demand yields a reader that always
// reports the unlimited snapshot, so a source written against pushdown works
// unchanged when nobody is pushing anything down.
func NewReader(d *Demand) *Reader {
	return &Reader{demand: d, snap: Snapshot{Limit: Unlimited}}
}

// refresh brings the cached view up to the demand's current generation,
// reporting whether it moved. It is the one place the gen/snap/primed triple
// is updated: Current and Exhausted differ only in what they do with the
// pending flag, and a second copy of this dance is how the two would drift.
func (r *Reader) refresh() bool {
	g := r.demand.generation()
	if r.primed && g == r.gen {
		return false
	}
	r.gen, r.snap, r.primed = g, r.demand.Snapshot(), true
	return true
}

// Current returns the demand as it stands, and whether it changed since the
// last call.
//
//	r := pushdown.NewReader(d)
//	for each batch {
//	    if snap, changed := r.Current(); changed {
//	        // re-plan: tighten the predicate, skip ahead
//	    }
//	}
//
// The changed flag is what a source acts on: re-planning per batch would undo
// the point of the mechanism, which is that the common batch costs an atomic
// load and nothing else.
func (r *Reader) Current() (snap Snapshot, changed bool) {
	if r.demand == nil {
		return Snapshot{Limit: Unlimited}, false
	}
	changed = r.refresh() || r.pending
	r.pending = false
	return r.snap, changed
}

// Exhausted reports whether the consumer has said it wants nothing more, which
// a source checks to stop early rather than to skip.
//
// It refreshes the reader's view but does not consume the changed flag: a
// source that checks Exhausted first and [Reader.Current] second still sees
// the change in Current, which is where re-planning happens.
func (r *Reader) Exhausted() bool {
	if r.demand == nil {
		return false
	}
	if r.refresh() {
		r.pending = true
	}
	return r.snap.Limit == 0
}
