# ADR 0016 — A hash join's build side is bounded, and the bound is required

- **Created:** 2026-10-07 (the decision dates from the operator; this writes it down)
- **Revised:** —
- **Theme:** join

## Problem

A hash join materialises one side — the build side — in memory, then
streams the other against it. An unbounded table is a join that looks
correct and runs until the process dies, on the day the reference data has
grown; and Kafka Streams' experience says a dropping policy without a
report is a table silently truncated, producing a short result that looks
legitimate.

## Decision

`join.StreamTable` and `join.Interval` take a `join.BuildLimit` as a
**required positional parameter**: `MaxEntries` (counting values, not
distinct keys, since one skewed key is exactly what a key-based bound would
not see), `OnOverflow` (`Fail` panics with `sluice.ErrOverflow`;
`DropNewest` joins against the truncated table; `DropOldest` is refused,
because a hash table has no oldest entry), and `Report func(limit, held
int)`, called once on overflow so the caller can raise its `Critical`
diagnostic. The zero value is refused: a limit of zero under the zero
policy would drop every row. Storage is an interface (`BuildTable`), with
`StreamTableWith` as the seam for spill or any other policy. The default
table is a flat value slice with an `int32` index chain, not
`map[K][]B`: measured at 2.6× the time and 110× the allocations against the
obvious shape, which caps `MaxEntries` at the chain's addressable range and
refuses more at construction.

## Rationale

Making unbounded state inexpressible beats detecting it at run time: Spark
refuses a stream-stream outer join without a watermark at planning time,
Kafka deprecated its implicit grace period. The report is a callback rather
than a diagnostics collector so that `join` imports nothing of
`diagnostics`, which keeps the dependency rule checkable by the compiler
and leaves the diagnostic's code and origin to the caller who knows the
domain. Never hardwiring the storage policy is the lesson of Kafka Streams
mandating RocksDB against its own agnosticism goal.

Reversal condition: none foreseen for the bound itself. The storage seam is
where a measured need for spill would land.
