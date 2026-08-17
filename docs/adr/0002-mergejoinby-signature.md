# ADR 0002 — `MergeJoinBy` carries values, keys are extracted, order is always checked

- **Status:** accepted
- **Date:** 2026-08-17
- **Scope:** `MergeJoinBy`, `EitherOrBoth`, core API, pre-1.0

## Context

§7.2 specifies `MergeJoinBy` as the central primitive of the N→1 family: six
join semantics reduced to filters over one output, in O(1) memory over
potentially infinite streams. The design is right. The signature it proposes is
not, on three counts, and the section says so itself about the third.

```go
type EitherOrBoth[L, R any] struct {
    Left  *L   // nil if absent
    Right *R   // nil if absent
}

func MergeJoinBy[L, R any](left Stream[L], right Stream[R], cmp func(L, R) int) Stream[EitherOrBoth[L, R]]
```

## Decision

### 1. Values, not pointers

`EitherOrBoth` holds `L` and `R` directly, with `HasLeft` / `HasRight` flags.

A pointer into an operator's reused buffer dangles the moment the next batch
arrives — the exact hazard the `Batch` doc calls "the one core rule a caller can
break silently". Demonstrated before deciding: three rounds of rows retained by
pointer all read back as the last round's values. Presence-by-nil is a compact
encoding that hands every caller a loaded gun.

The cost is copying the values into the output row. Measured at 2.3 ns/element
of the 3.4 ns floor — real, and the reason the operator misses the per-stage
budget. Pointers would not have avoided it: the rows still have to be
materialized somewhere, and somewhere safe means copying.

### 2. Two key functions plus a key comparator

```go
func MergeJoinBy[L, R any, K any](
    left Stream[L], right Stream[R],
    keyL func(L) K, keyR func(R) K,
    cmp func(K, K) int,
) Stream[EitherOrBoth[L, R]]
```

`cmp func(L, R) int` cannot compare two elements of the *same* side, which the
algorithm needs twice: to verify the input is sorted, and to gather a run of
equal keys for the cross product. The proposed signature makes both impossible.

Extracting a key also lets the two sides be unrelated types joined on a shared
field — `order.customerID` against `customer.id` — which is what a join is for.
Five parameters is more than four; three of them are the join condition, stated
once.

### 3. The sort check is always on, not a debug mode

§7.2 asks for "a debug mode [to] check key monotonicity" and calls the silent
failure "unacceptable as it stands". We agree with the diagnosis and reject the
remedy: a check that only runs in debug builds is absent exactly when it matters.

`MergeJoinBy` panics with `ErrUnsorted` when an input goes backwards. Measured
cost: one comparison per element against the several the merge already performs,
lost in the noise. A debug flag would add an API surface, a build mode and a
class of "works in test, wrong in production" bugs, to save nothing measurable.

Panic rather than error: an `iter.Seq` has nowhere to put an error, and unsorted
input is a wiring mistake, not a runtime condition.

## Consequences

- The operator misses the 1.5 ns per-stage budget, at 11.6 ns/element against a
  3.4 ns floor for the same work hand-written. Result 8 in
  `BENCHMARK-STEP-0.md` records what was measured, which hypotheses were tested,
  and which two turned out wrong. The budget was set for stage operators over a
  batch; an element-at-a-time merge is a different shape of work, and the
  ceiling does not transfer.
- Memory is O(1) except on keys duplicated on **both** sides, where the two runs
  are buffered to be paired: O(n+m) for that key. Unique keys on either side —
  a join on an identifier — stay O(1).
- An early stop takes effect at the next output batch boundary, not the next
  row, because rows accumulate until a batch is full. That is the price of
  emitting full batches, and it is documented on the operator.

## What would change our mind

The 3.4 → 11.6 ns gap has since been located, and it is **not** the cost of
joining (Result 9). `ZipLongest` runs the same cursors, the same emitter and the
same 32-byte output rows at 2.95 ns; a join on inputs with disjoint key ranges,
where the equal-key and cross-product paths never execute, still costs 10.7 ns.

What separates them is `cursor` against `walker`: the join peeks both sides on
every turn even when only one advances, and each peek carries a key cache and a
monotonicity check. Reworking the cursor so a turn touches only the side that
moves is the change worth measuring — a rework, not a tweak, and not attempted
here. The three earlier hypotheses (key memoization, kept; indirect `cmp` call
and manual inlining, both refuted) are recorded in Result 8 so they are not
retried.
