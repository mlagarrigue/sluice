package join

import (
	"maps"
	"slices"

	"github.com/mlagarrigue/sluice"
)

// BuildLimit bounds the table a hash join materialises.
//
// It is a required positional argument of [StreamTable] rather than an
// option, because the alternative is a join that looks correct and runs until
// the process dies. §9.1 takes the position from engines that learned it the
// hard way: Spark refuses a stream-stream outer join without a watermark at
// planning time, and Kafka deprecated its implicit 24-hour grace period
// (KIP-633). Making unbounded state inexpressible beats detecting it at
// runtime.
//
// The zero value is not usable, and deliberately so: a limit of zero entries
// under the zero policy would silently drop every row, which is the failure
// this type exists to prevent. [StreamTable] rejects it.
type BuildLimit struct {
	// MaxEntries is the largest number of build-side elements the table holds.
	// It must be positive.
	MaxEntries int

	// OnOverflow says what happens when MaxEntries is reached.
	//
	// [sluice.Fail] panics with [sluice.ErrOverflow]. [sluice.DropNewest] keeps the table as it
	// stands and joins against it, which is a choice a caller can legitimately
	// make and must make knowingly: the result is then missing rows, never
	// wrong ones.
	//
	// [sluice.DropOldest] is rejected rather than accepted. A hash table has no oldest
	// entry — a map has no order, and imposing one would mean a second index
	// whose only purpose is eviction. Accepting the policy and quietly
	// behaving as sluice.DropNewest would offer a choice that does nothing, which is
	// worse than refusing it.
	OnOverflow sluice.Overflow

	// Report, when set, is called once when the table reaches MaxEntries,
	// with the limit and the number of entries it was asked to hold.
	//
	// sluice.Overflow must be loud: a dropping policy without a report is a table
	// silently truncated, and a short result that looks like a legitimate
	// one. sluice.Fail is loud on its own; under sluice.DropNewest this callback is the only
	// way to learn the table was truncated, so a caller choosing to drop
	// should set it — typically to raise a Critical diagnostic.
	Report func(limit, held int)
}

// valid reports whether the limit is usable, checked at construction so that a
// misconfigured join fails where it is wired rather than when the data arrives.
// It knows nothing of any table's capacity: that ceiling belongs to the
// default table, and a caller-supplied one is not bound by it.
func (l BuildLimit) valid() bool {
	return l.MaxEntries > 0 && (l.OnOverflow == sluice.Fail || l.OnOverflow == sluice.DropNewest)
}

// maxBuildEntries is the largest table the default [BuildTable] addresses.
//
// Its chain indices are int32, which halves the memory the chain costs against
// int on a 64-bit platform. Two billion rows in one join is far past where a
// single-process engine stops being the right tool — §9.1 already says the
// limit is a budget shared across the plan — so the ceiling is stated and
// rejected at construction rather than left to overflow silently.
//
// A caller who genuinely needs more supplies their own [BuildTable] through
// [StreamTableWith], which is what the seam is for.
const maxBuildEntries = 1<<31 - 1

// BuildTable holds the materialised side of a hash join.
//
// It is an interface so that the in-memory map below is a default rather than a
// decision baked into the operator. §9.1 is explicit about the counter-example:
// Kafka Streams ended up mandating RocksDB in its foreign-key join, in
// contradiction with its own agnosticism goal. A caller who needs spill to disk
// — radix partitioning is the reference implementation — supplies it here
// instead of forking the join.
//
// Implementations are used from a single goroutine and need no synchronisation
// of their own.
type BuildTable[K comparable, B any] interface {
	// Put adds a value under a key. Several values may share a key; Get
	// returns them all.
	Put(k K, v B)

	// Get returns the values stored under a key, or nil. The slice must not be
	// retained by the caller past the next Put.
	Get(k K) []B

	// Len reports how many values are stored, which is what [BuildLimit]
	// bounds — values, not distinct keys, since a skewed key is exactly what
	// makes a table grow unexpectedly.
	Len() int
}

// mapTable is the default [BuildTable]: every value in one flat slice, with a
// map from key to the index of that key's most recent value and a parallel
// slice chaining each value to the previous one under the same key.
//
// The obvious shape — map[K][]B — allocates a slice per distinct key, which on
// a build side with many keys is the dominant cost: measured against the chain
// below, it took several times the time and two orders of magnitude more
// allocations (figures in docs/design/architecture.md, "Hash join — the build side is a table"; this table's build cost
// is BenchmarkJoinHashBuildOnly in internal/bench). That gap pays for the
// indirection, and it is the reason this is not the two-line version.
//
// Values under one key come back in insertion order, which the chain reverses,
// so Get walks it and then reverses into a reused buffer. That buffer is why
// [BuildTable.Get] forbids retaining the result past the next Put.
type mapTable[K comparable, B any] struct {
	// head maps a key to the index in vals of its most recently added value,
	// or -1 for none.
	head map[K]int32

	// vals holds every value, in arrival order across all keys.
	vals []B

	// prev[i] is the index of the previous value under the same key as
	// vals[i], or -1 if it is the first.
	prev []int32

	// scratch is returned by Get, refilled on each call.
	scratch []B

	// limit is the caller's MaxEntries, the size the map is allowed to reach.
	// Put uses it to bound its own pre-sizing so that growth stops where the
	// join stops filling.
	limit int

	// reserved is the size head was last built for. Growth is driven from it
	// rather than from the map's own capacity, which Go does not expose.
	reserved int
}

func newMapTable[K comparable, B any](hint int) *mapTable[K, B] {
	// Sized from the limit but capped, for the reason every bound here is
	// claimed lazily: a limit set high against an unlikely worst case must not
	// become a large allocation before a single row arrives.
	//
	// The cap is a floor on the map's size, not a ceiling on it: [mapTable.Put]
	// rebuilds the map at a larger size as the build side turns out to be
	// large, so a limit of 65536 costs 64 entries until the rows justify more.
	// Leaving the runtime to grow it instead cost 28% of the join's runtime in
	// (*table).split alone — ten rehashes, each copying everything already
	// inserted — because a map grown by insertion never learns how many rows
	// are still coming.
	reserved := min(hint, initialBuildTable)
	return &mapTable[K, B]{
		head:     make(map[K]int32, reserved),
		limit:    hint,
		reserved: reserved,
	}
}

// initialBuildTable caps the map pre-sizing on a hash join's build side.
const initialBuildTable = 64

// Put implements [BuildTable]. It appends the value and chains it to the
// previous one under the same key, growing the map toward the limit rather
// than letting the runtime grow it one insertion at a time.
func (t *mapTable[K, B]) Put(k K, v B) {
	// Safe: StreamTable caps MaxEntries at maxBuildEntries before building
	// this table, and the join stops filling at that bound, so len(t.vals) cannot reach int32's range.
	idx := int32(len(t.vals)) //nolint:gosec // G115: bounded by maxBuildEntries
	previous, seen := t.head[k]
	if !seen {
		previous = -1
		t.reserve()
	}
	t.vals = append(t.vals, v)
	t.prev = append(t.prev, previous)
	t.head[k] = idx
}

// reserve rebuilds head at double its capacity when it is about to fill,
// doubling from the small size newMapTable claimed rather than growing one
// insertion at a time.
//
// Go's map grows on its own, so this is not about correctness — it is about who
// chooses the size. A map grown by insertion rehashes whenever it fills, and
// each rehash copies every entry already there; the runtime cannot do better,
// because it never learns how many keys are still coming. Here that number is
// known: it is bounded by the caller's MaxEntries. Rebuilding on the doubling
// boundary spends the same total copying but pays it in log(n) steps sized from
// the limit rather than from what happens to have arrived, and it stops as soon
// as the reservation reaches the limit.
//
// Called only when a key is new, since only a new key grows the map.
func (t *mapTable[K, B]) reserve() {
	size := len(t.head)
	if size < t.reserved || t.reserved >= t.limit {
		return
	}
	next := min(2*t.reserved, t.limit)
	if next <= t.reserved {
		return
	}
	grown := make(map[K]int32, next)
	maps.Copy(grown, t.head)
	t.head, t.reserved = grown, next
}

// Get implements [BuildTable]. The returned slice is reused by the next call,
// which is why the interface forbids retaining it past the next Put.
func (t *mapTable[K, B]) Get(k K) []B {
	i, seen := t.head[k]
	if !seen {
		return nil
	}
	// A key with one value behind it is the common case — a join on an
	// identifier is one-to-one — and it needs neither the walk nor the reversal:
	// the chain ends immediately and a single element is already in order. The
	// general path below stays for keys that repeat.
	if t.prev[i] < 0 {
		t.scratch = append(t.scratch[:0], t.vals[i])
		return t.scratch
	}
	// The chain runs newest to oldest, so collect and reverse: callers see
	// build-side arrival order, which is what makes the output deterministic.
	t.scratch = t.scratch[:0]
	for i >= 0 {
		t.scratch = append(t.scratch, t.vals[i])
		i = t.prev[i]
	}
	slices.Reverse(t.scratch)
	return t.scratch
}

// Len implements [BuildTable], counting values rather than distinct keys: a
// skewed key is exactly what makes a table grow unexpectedly.
func (t *mapTable[K, B]) Len() int { return len(t.vals) }

// StreamTable joins a streamed side against a materialised one.
//
//	enriched := StreamTable(orders, customers,
//	    func(o Order) int { return o.CustomerID },
//	    func(c Customer) int { return c.ID },
//	    func(o Order, c Customer) Enriched { return Enriched{o, c} },
//	    BuildLimit{MaxEntries: 10_000, OnOverflow: sluice.Fail})
//
// # Which side is materialised
//
// build is read to completion first and held in memory; probe is then streamed
// against it and alone drives the output. Naming both sides in the signature
// removes the ambiguity every join API has about which one is kept — this is
// Kafka Streams' stream/table duality, where the table is a side input and the
// stream is what produces results.
//
// The consequence is a hard requirement: **build must be finite**. probe may be
// infinite, and typically is. Passing an infinite build side does not fail
// cleanly, it fills the table until the limit stops it — which is what the limit
// is for, but a caller who swapped the arguments will see an empty or truncated
// result rather than an error saying so.
//
// # The limit is not optional
//
// A join that materialises one side is the operator most likely to exhaust
// memory, so [BuildLimit] is a required argument with no usable zero value.
// What happens at the limit is stated too, since dropping and failing are both
// defensible and neither is a safe default.
//
// The bound counts **values, not distinct keys**. A single key with a million
// rows behind it is exactly the shape that makes a table grow unexpectedly, and
// a bound on keys would not see it.
//
// # Semantics
//
// This is an inner join: a probe element with no match produces nothing, and a
// probe element matching n build elements produces n results, in build-side
// arrival order. Output batches reuse an internal buffer, like every join
// here: retaining Items beyond the call that receives them requires a copy.
// Results accumulate until a batch fills, so an early stop takes effect at
// the next batch boundary.
//
// Output batches are cut at [sluice.DefaultBatchSize], whatever the probe side's
// batching. Under a skewed key one probe batch can produce len(batch) × the
// matches behind that key; handing that over as one batch would leave
// downstream reusing a buffer of that size forever, which is the O(build)
// bound broken on the output side.
//
// Left and outer variants are not provided — they are a filter away over an
// [EitherOrBoth] shape, which is what [Merge] already gives for sorted
// inputs, and adding a second spelling here would mean two joins differing in
// ways callers get wrong.
//
// # Cost
//
// O(build) memory, bounded by the limit. One map lookup per probe element, and
// the build side is walked once. A large share of the per-element cost is the
// map itself — hashing on insert and on lookup. That share is what a hash join
// is, and it is the reason the cost is what it is rather than something a
// tighter loop would fix (BenchmarkJoinHash in internal/bench).
//
// **Where both inputs are sorted on the key, use [Merge] instead.** §9.2
// says the sorted-merge strategy does the same work in O(1); the gap is larger
// than "O(1) versus O(build)" conveys — several times the speed and orders of
// magnitude less memory on the same data (BenchmarkJoinMerge against
// BenchmarkJoinHash; the figures live in docs/design/architecture.md, "Strategies").
// Materialising a side is what you do when you cannot sort, not a default.
//
// StreamTable panics if keyA, keyB or merge is nil, or if limit is not
// usable — which includes a MaxEntries past 2^31-1, the default table's index
// range.
func StreamTable[A, B any, K comparable, R any](
	probe sluice.Stream[A],
	build sluice.Stream[B],
	keyA func(A) K,
	keyB func(B) K,
	merge func(A, B) R,
	limit BuildLimit,
) sluice.Stream[R] {
	checkJoinArgs(keyA, keyB, merge, limit)
	if limit.MaxEntries > maxBuildEntries {
		panic("sluice: StreamTable requires MaxEntries <= 2^31-1, the default table's " +
			"int32 index range; supply a BuildTable through StreamTableWith for more")
	}
	return joinStreamTable(probe, build, keyA, keyB, merge, limit,
		newMapTable[K, B](limit.MaxEntries))
}

// StreamTableWith is [StreamTable] over a caller-supplied table.
//
// It exists so that spill-to-disk, or any other storage policy, is something a
// caller adds rather than something this package chooses for them. table must
// be empty; it is filled from build and not reset afterwards, so a table reused
// across joins carries its previous contents.
//
// StreamTableWith panics under the same conditions as [StreamTable],
// except the default table's MaxEntries ceiling, which a supplied table is
// not bound by — and additionally if table is nil.
func StreamTableWith[A, B any, K comparable, R any](
	probe sluice.Stream[A],
	build sluice.Stream[B],
	keyA func(A) K,
	keyB func(B) K,
	merge func(A, B) R,
	limit BuildLimit,
	table BuildTable[K, B],
) sluice.Stream[R] {
	checkJoinArgs(keyA, keyB, merge, limit)
	if table == nil {
		panic("sluice: StreamTableWith requires a non-nil table")
	}
	return joinStreamTable(probe, build, keyA, keyB, merge, limit, table)
}

// checkJoinArgs rejects a misconfigured join where it is wired rather than when
// the data arrives. Shared by both constructors so there is one set of rules to
// keep true, not two that can drift.
func checkJoinArgs[A, B any, K comparable, R any](
	keyA func(A) K,
	keyB func(B) K,
	merge func(A, B) R,
	limit BuildLimit,
) {
	if keyA == nil || keyB == nil {
		panic("sluice: a hash join requires non-nil key functions")
	}
	if merge == nil {
		panic("sluice: a hash join requires a non-nil merge function")
	}
	if !limit.valid() {
		panic("sluice: a hash join requires a BuildLimit with a positive MaxEntries " +
			"and an OnOverflow of Fail or DropNewest")
	}
}

// joinStreamTable is the shared body. The build side is materialised lazily —
// on the first pull, not at construction — so that a join that is never
// consumed reads nothing, which is the contract every other operator here
// keeps.
func joinStreamTable[A, B any, K comparable, R any](
	probe sluice.Stream[A],
	build sluice.Stream[B],
	keyA func(A) K,
	keyB func(B) K,
	merge func(A, B) R,
	limit BuildLimit,
	table BuildTable[K, B],
) sluice.Stream[R] {
	return func(yield func(sluice.Batch[R]) bool) {
		fillTable(build, keyB, limit, table)

		// Output goes through the shared emitter, as in every other join:
		// accumulating a probe batch's whole cross product into one slice
		// would emit up to len(batch)×MaxEntries elements under a skewed key
		// — unbounded relative to sluice.DefaultBatchSize, against the O(build)
		// claim above, and a buffer a downstream operator then reuses at that
		// inflated size forever.
		out := newEmitter(yield)
		probe(func(b sluice.Batch[A]) bool {
			for _, a := range b.Items {
				for _, bv := range table.Get(keyA(a)) {
					if !out.push(merge(a, bv)) {
						return false
					}
				}
			}
			return true
		})
		_ = out.flush()
	}
}

// fillTable materialises the build side, applying the overflow policy.
//
// The policy is known valid here — [BuildLimit.valid] admits only sluice.Fail and
// sluice.DropNewest — so the switch has no third case to guard against.
func fillTable[K comparable, B any](
	build sluice.Stream[B],
	keyB func(B) K,
	limit BuildLimit,
	table BuildTable[K, B],
) {
	build(func(b sluice.Batch[B]) bool {
		for _, v := range b.Items {
			if table.Len() < limit.MaxEntries {
				table.Put(keyB(v), v)
				continue
			}
			if limit.OnOverflow == sluice.Fail {
				panic(sluice.ErrOverflow)
			}
			// sluice.DropNewest: the table keeps what it has, and this row and every
			// one after it are not joinable. Reported once rather than per row
			// — a truncated table overflows for the whole rest of the stream,
			// and a diagnostic per dropped row is the pathological batch §4.5
			// exists to prevent.
			reportBuildOverflow(limit, table.Len())
			return false
		}
		return true
	})
}

// reportBuildOverflow calls the limit's Report once. It is a no-op when the
// caller supplied none, which is the case where sluice.Fail carries the signal.
func reportBuildOverflow(limit BuildLimit, held int) {
	if limit.Report == nil {
		return
	}
	limit.Report(limit.MaxEntries, held)
}
