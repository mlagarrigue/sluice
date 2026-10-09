# ADR 0004 — The merge join carries values, extracts keys, and always checks order

- **Created:** 2026-08-17
- **Revised:** 2026-10-07 (renumbered; `MergeJoinBy` became `join.Merge`)
- **Theme:** join

## Problem

The merge join is the central N→1 primitive: six join semantics reduced to
filters over one output, in O(1) memory over infinite sorted streams. The
signature first sketched for it — `Left *L` / `Right *R` presence-by-nil
and a single `cmp func(L, R) int` — fails three times: a pointer into an
operator's reused buffer dangles when the next batch arrives, a cross-side
comparator cannot compare two elements of the same side, and the sort
check it needs was proposed as a debug mode.

## Decision

```go
func Merge[L, R any, K any](left Stream[L], right Stream[R],
    keyL func(L) K, keyR func(R) K, cmp func(K, K) int) Stream[EitherOrBoth[L, R]]
```

1. **Values, not pointers.** `EitherOrBoth` holds `L` and `R` directly with
   `HasLeft` / `HasRight` flags. Rows retained by pointer all read back as
   the last round's values; this was demonstrated before deciding.
2. **Two key functions plus a key comparator.** The algorithm compares
   elements of the same side twice — to verify sorting and to gather a run
   of equal keys for the cross product — and two unrelated types join on a
   shared field.
3. **The sort check is always on.** Out-of-order input panics with
   `sluice.ErrUnsorted`. A check that only runs in debug builds is absent
   exactly when it matters, and it costs one comparison per element against
   the several the merge already performs.

## Rationale

Copying values into the output row is the cost, and pointers would not have
avoided it: the rows must be materialised somewhere, and somewhere safe
means copying. Memory is O(1) except on keys duplicated on both sides,
where the right run is buffered for that key. An early stop takes effect at
the next output batch boundary, the price of emitting full batches.

The operator misses the 1.5 ns per-stage budget — the budget was set for
stage operators over a batch, and an element-at-a-time merge is a different
shape of work. The gap is not the join logic: `ZipLongest` on the same
cursors runs at a third of the cost, and the difference is that the join
peeks both sides on every turn. Reworking the cursor so a turn touches only
the side that moves is the change worth measuring; three other hypotheses
(key memoisation, kept; indirect `cmp` call and manual inlining, refuted)
are recorded so they are not retried.
