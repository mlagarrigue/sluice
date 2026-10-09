package join

import (
	"time"

	"github.com/mlagarrigue/sluice"
)

// Interval joins two time-ordered streams on a key and a temporal
// predicate, in memory bounded by the caller — the stream-stream join for
// infinite inputs that §9.3 specifies.
//
//	attributed := Interval(clicks, impressions,
//	    func(c Click) string { return c.UserID },
//	    func(i Impression) string { return i.UserID },
//	    func(c Click) time.Time { return c.At },
//	    func(i Impression) time.Time { return i.At },
//	    -15*time.Minute, 0, // impression at most 15 minutes before the click
//	    func(c Click, i Impression) Attribution { ... },
//	    BuildLimit{MaxEntries: 100_000, OnOverflow: sluice.Fail})
//
// A left row l and a right row r match when their keys are equal and
//
//	tsR(r) - tsL(l) is within [lower, upper]
//
// so lower and upper say where the right side may sit relative to the left:
// a negative lower reaches into the right's past, a positive upper into its
// future, and lower <= upper is required. Every matching pair is emitted
// exactly once, inner-join only: a window that would need to know when to
// give up finding a partner is a watermark, and without one the semantics of
// an outer emission is undefined — the restriction is taken from Flink and
// stated rather than lied about. The window bounds are computed with
// [time.Time.Add], which wraps silently near the extremes a time.Time can
// represent — timestamps within |lower| or |upper| of those extremes will
// match wrongly, so keep sentinel times (zero, math.MaxInt64 epochs) out of
// the inputs.
//
// # Both inputs must be ordered by event time
//
// What replaces the watermark is ordering: each input must be non-decreasing
// on its extracted timestamp, checked as elements are first seen, panicking
// with [sluice.ErrUnsorted] when one goes backwards — the same discipline, check and
// sentinel as [Merge], because the failure is the same silent wrong
// answer. Ordering is what makes state finite: a row can be evicted the
// moment the other side's clock has passed the last instant that could still
// match it, and no lateness needs to be guessed.
//
// # State is bounded by the caller, not by hope
//
// The operator retains each row until the other side's time passes its match
// window: O(throughput × interval) rows in the steady state. A row whose
// window the other side has already passed when it arrives is joined and not
// retained at all, so it never counts against the limit. That product is
// not knowable here, so per S1 the bound is a required parameter: limit
// counts the retained rows of both sides together, [sluice.Fail] panics with
// [sluice.ErrOverflow] when it is reached, and [sluice.DropNewest] stops retaining new rows
// — an arriving row still probes and joins against what is buffered, but is
// not kept for rows yet to come, so the result is missing pairs, never wrong
// ones. Under a dropping policy the first drop reports a Critical diagnostic
// to limit.Diagnostics when one is set, once rather than per row (§4.5).
//
// # Cost
//
// This is a hash-family operator, not a merge-family one: every element pays
// a map probe and most pay a map insert, exactly the term that dominates
// [StreamTable]. The temporal machinery itself (timestamps, eviction, the
// frontier) costs a fraction on top of the hashing, a wider window adds the
// cost of the extra pairs it emits, and a window that excludes zero costs no
// more per probe than one that does: rows still waiting for their window to
// open are kept out of the probes' way (BenchmarkIntervalJoinNarrow and
// BenchmarkIntervalJoinWide in internal/bench, BenchmarkIntervalJoinOffset
// here; the figures live in docs/design/architecture.md, "Infinite streams: interval join"). Sorted inputs joined
// on equality alone belong in [Merge]; this operator is bought for the
// temporal predicate.
//
// # Output
//
// Pairs are emitted in discovery order — when the later row of each pair is
// processed, ties resolved left first — which is deterministic for given
// inputs but is not globally time-ordered. Output batches reuse an internal
// buffer, like every join here: retaining Items beyond the call that receives
// it requires a copy. Rows accumulate until a batch fills, so an early stop
// takes effect at the next batch boundary.
//
// Interval panics if a key, timestamp or merge function is nil, if
// lower > upper, or if limit is not usable.
func Interval[L, R any, K comparable, Out any](
	left sluice.Stream[L],
	right sluice.Stream[R],
	keyL func(L) K,
	keyR func(R) K,
	tsL func(L) time.Time,
	tsR func(R) time.Time,
	lower, upper time.Duration,
	merge func(L, R) Out,
	limit BuildLimit,
) sluice.Stream[Out] {
	if keyL == nil || keyR == nil {
		panic("sluice: Interval requires non-nil key functions")
	}
	if tsL == nil || tsR == nil {
		panic("sluice: Interval requires non-nil timestamp functions")
	}
	if merge == nil {
		panic("sluice: Interval requires a non-nil merge function")
	}
	if lower > upper {
		panic("sluice: Interval requires lower <= upper")
	}
	if !limit.valid() {
		panic("sluice: Interval requires a BuildLimit with a positive MaxEntries " +
			"and an OnOverflow of Fail or DropNewest")
	}
	if limit.MaxEntries > maxIntervalEntries {
		// Half of what the hash join accepts, because the buffers here count
		// dead rows as well as live ones: eviction only moves a frontier, and
		// compact lets the dead prefix grow as large as the live suffix before
		// rebuilding. len(vals) can therefore reach ~2×MaxEntries, and the
		// int32 chain indices must still address it — see intervalBuffer.put.
		panic("sluice: Interval requires MaxEntries <= 2^30-1; " +
			"the buffer holds up to twice the live bound before compaction")
	}

	return func(yield func(sluice.Batch[Out]) bool) {
		// The cursors' key is the timestamp and their cmp is time.Compare, so
		// the peek cache and the sluice.ErrUnsorted monotonicity check are inherited
		// rather than rebuilt.
		lc := newCursor[L](left, tsL, time.Time.Compare)
		defer lc.stop()
		rc := newCursor[R](right, tsR, time.Time.Compare)
		defer rc.stop()

		lbuf := newIntervalBuffer[K, L]()
		rbuf := newIntervalBuffer[K, R]()
		out := newEmitter(yield)
		dropped := false // a diagnostic is reported once, not per dropped row

		// retain reports whether an incoming row may be buffered under the
		// limit, applying the policy when it may not.
		retain := func() bool {
			if lbuf.live()+rbuf.live() < limit.MaxEntries {
				return true
			}
			if limit.OnOverflow == sluice.Fail {
				panic(sluice.ErrOverflow)
			}
			if !dropped {
				dropped = true
				reportBuildOverflow(limit, lbuf.live()+rbuf.live())
			}
			return false
		}

		for {
			lv, lts, lok := lc.peek()
			rv, rts, rok := rc.peek()

			switch {
			case !lok && !rok:
				_ = out.flush()
				return

			case lok && (!rok || lts.Compare(rts) <= 0):
				// Left first on ties: with the tie going this way, a pair
				// sharing one instant is discovered when its right row probes.
				lc.next()
				k := keyL(lv)
				// Rights older than lts+lower can match no current or future
				// left — left time is non-decreasing — so they go first.
				rbuf.evictBefore(lts.Add(lower))
				for _, i := range rbuf.probe(k, lts.Add(lower), lts.Add(upper)) {
					if !out.push(merge(lv, rbuf.vals[i])) {
						return
					}
				}
				// A row is retained only while the other side can still
				// produce a partner for it: rights to come are at or after
				// rts, so a left whose window closes before rts is dead at
				// birth and must not count against the limit.
				if rok && !rts.After(lts.Add(upper)) && retain() {
					lbuf.put(k, lts, lv)
				}

			default:
				rc.next()
				k := keyR(rv)
				lbuf.evictBefore(rts.Add(-upper))
				for _, i := range lbuf.probe(k, rts.Add(-upper), rts.Add(-lower)) {
					if !out.push(merge(lbuf.vals[i], rv)) {
						return
					}
				}
				// Same rule from this side: lefts to come are at or after
				// lts, and match this right only up to rts-lower.
				if lok && !lts.Add(lower).After(rts) && retain() {
					rbuf.put(k, rts, rv)
				}
			}
		}
	}
}

// maxIntervalEntries caps an Interval's MaxEntries at half of
// maxBuildEntries. The hash join's table never holds more than MaxEntries
// rows, so its int32 indices are safe up to maxBuildEntries; here the arrays
// also carry a dead prefix that compact tolerates up to the size of the live
// suffix, so the index space must leave room for 2×MaxEntries.
const maxIntervalEntries = maxBuildEntries / 2

// intervalBuffer retains one side's rows in arrival order — which the
// ordering contract makes time order — chained by key for probing, evicted
// from the front as the other side's clock advances.
//
// Eviction only moves a frontier: entries below firstLive are dead but still
// occupy their slots and their chains, and probes stop at them. compact
// rebuilds once the dead outnumber the live, so the arrays stay O(live)
// amortized and a probe's chain walk stays short — the same lazy-delete
// economy as a tombstoned hash table, chosen over unlinking dead chain nodes
// eagerly, which would walk every chain at every eviction.
type intervalBuffer[K comparable, V any] struct {
	head map[K]int32
	vals []V
	ts   []time.Time
	keys []K
	prev []int32

	// firstLive is the eviction frontier: everything below it is dead.
	firstLive int

	// linked is the visibility frontier: entries in [firstLive, linked) are
	// chained under their key, entries from linked on are retained but not
	// yet chained, because no probe so far has reached their timestamp. A
	// window that excludes zero keeps rows waiting before their window opens;
	// chaining them at once would put them at the head of every chain, and
	// each probe would walk over all of them — O(window distance) per probe.
	// Chained lazily, a probe only walks rows inside its window.
	linked int

	// scratch returns probe's match indices, newest-first off the chain and
	// reversed into arrival order; reused across probes.
	scratch []int32
}

// newIntervalBuffer leaves head nil: a nil map reads as empty, and chain
// claims it on the first row a probe links. A join opens two buffers, and a
// side that stays empty — or whose rows all die before a window reaches them —
// should not pay for a pre-sized map it never writes.
func newIntervalBuffer[K comparable, V any]() *intervalBuffer[K, V] {
	return &intervalBuffer[K, V]{}
}

func (b *intervalBuffer[K, V]) live() int { return len(b.vals) - b.firstLive }

func (b *intervalBuffer[K, V]) put(k K, at time.Time, v V) {
	// Appended only: chaining waits for link, when a probe's window reaches at.
	b.vals = append(b.vals, v)
	b.ts = append(b.ts, at)
	b.keys = append(b.keys, k)
	b.prev = append(b.prev, -1)
}

// link chains every retained entry whose timestamp is at or below hi. The
// probes' upper bounds are non-decreasing — each is the probing side's clock
// plus a constant — and the arrays are in time order, so this only advances a
// frontier: amortized O(1) per retained row.
func (b *intervalBuffer[K, V]) link(hi time.Time) {
	for b.linked < len(b.ts) && b.ts[b.linked].Compare(hi) <= 0 {
		b.chain(b.linked)
		b.linked++
	}
}

// chain pushes entry i onto its key's chain.
func (b *intervalBuffer[K, V]) chain(i int) {
	k := b.keys[i]
	previous, seen := b.head[k]
	if !seen {
		previous = -1
	}
	b.prev[i] = previous
	if b.head == nil {
		b.head = make(map[K]int32, initialBuildTable)
	}
	// len(vals) counts dead rows as well as live ones, so the hash join's
	// argument is not enough on its own: evictBefore compacts whenever the
	// dead prefix reaches the live suffix, so dead < live here, and the join
	// retains a row only while live() < MaxEntries ≤ maxIntervalEntries.
	// Hence i < 2×MaxEntries ≤ maxBuildEntries and the cast fits.
	b.head[k] = int32(i) //nolint:gosec // G115: < 2×maxIntervalEntries, see above
}

// probe returns the indices of the live entries under k whose timestamp lies
// in [lo, hi], in arrival order. The slice is reused by the next call. hi must
// not decrease from one call to the next; see link.
func (b *intervalBuffer[K, V]) probe(k K, lo, hi time.Time) []int32 {
	b.link(hi)
	b.scratch = b.scratch[:0]
	i, seen := b.head[k]
	if !seen {
		return b.scratch
	}
	// The chain runs newest to oldest and — one key being a subsequence of
	// the whole — its timestamps are non-increasing, and link chained nothing
	// above hi: collect while at or above lo, stop at the first entry below
	// lo or below the eviction frontier, since everything deeper is older.
	for i >= 0 && int(i) >= b.firstLive {
		if b.ts[i].Compare(lo) < 0 {
			break
		}
		b.scratch = append(b.scratch, i)
		i = b.prev[i]
	}
	// Reversed into arrival order, so the output is deterministic the way
	// mapTable.Get's is.
	for l, r := 0, len(b.scratch)-1; l < r; l, r = l+1, r-1 {
		b.scratch[l], b.scratch[r] = b.scratch[r], b.scratch[l]
	}
	return b.scratch
}

// evictBefore retires every entry strictly older than deadline. Arrival order
// is time order, so this only advances the frontier.
func (b *intervalBuffer[K, V]) evictBefore(deadline time.Time) {
	for b.firstLive < len(b.ts) && b.ts[b.firstLive].Compare(deadline) < 0 {
		b.firstLive++
	}
	// Rows can die before any probe reached them; they never get chained.
	b.linked = max(b.linked, b.firstLive)
	b.compact()
}

// compact rebuilds the arrays and chains from the live suffix once the dead
// prefix outweighs it. Amortized O(1) per retained row: a row is copied at
// most once per halving.
func (b *intervalBuffer[K, V]) compact() {
	if b.firstLive == 0 || 2*b.firstLive < len(b.vals) {
		return
	}
	live := len(b.vals) - b.firstLive
	copy(b.vals, b.vals[b.firstLive:])
	copy(b.ts, b.ts[b.firstLive:])
	copy(b.keys, b.keys[b.firstLive:])
	// The dead tail of vals would otherwise retain its values past the
	// truncation — same reasoning as ring.pop. ts is cleared too: a
	// time.Time carries a *time.Location, so the tail would pin loaded
	// zones the same way.
	clear(b.vals[live:])
	clear(b.ts[live:])
	clear(b.keys[live:])
	b.vals = b.vals[:live]
	b.ts = b.ts[:live]
	b.keys = b.keys[:live]
	b.prev = b.prev[:live]

	// Chains are rebuilt fresh over the linked entries only; clear keeps the
	// map's storage and drops the keys whose every entry was dead.
	clear(b.head)
	b.linked -= b.firstLive
	b.firstLive = 0
	for i := range b.linked {
		b.chain(i)
	}
}
