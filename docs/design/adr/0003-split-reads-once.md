# ADR 0003 — `Split` reads the source once, with one batch of slack per branch

- **Created:** 2026-08-17
- **Revised:** 2026-10-07 (renumbered; content unchanged in substance)
- **Theme:** core

## Problem

`Split` fans one stream out to N branches. Returning N closures that each
call the source looks right on a replayable source and is silently wrong on
a single-pass one — a cursor, a network read — where the second branch
receives nothing at all. The architecture's impossibility theorem governs
the fix: with a non-replayable source you cannot have independent branches,
a single read, and bounded memory at once.

## Decision

The source is traversed exactly once through `iter.Pull`, and each branch
gets a slot holding **at most one batch**. The branch asked for a batch
drives the shared traversal and deposits the result into every live
destination; a branch whose slot is still full blocks the pull.

What is sacrificed is **branch independence**: concurrent branches must be
advanced in alternation. Three consequences, each deliberate:

- Branches consumed one after the other **panic** rather than yield a
  silent prefix: `ErrSplitStalled` when a sibling holds an undrained batch,
  `ErrSplitDrained` when a sibling already ran the source dry. Lateness is
  tracked per branch, so a branch `route` never chose stays silent.
- A branch nobody consumes is not a destination: batches routed to it are
  dropped, so one branch of a partition can be read alone.
- A branch is single-pass, like the stream it comes from.

## Rationale

Restricting the contract to replayable sources would remove the operator's
reason to exist. Unbounded buffering — the Web Streams model — violates S1.
An explicit `Run()` after wiring would preserve independence at the cost of
the common case: the return would stop being a rangeable `[]Stream[T]`.

Measured: partition costs 0.27 ns/element, broadcast over two branches
0.94 ns, no allocation. Every branch belongs to one goroutine; consuming
two concurrently is a data race that `-race` catches — an entry counter
strict enough to detect it also rejected the supported `iter.Pull` wiring,
so detection stays with the race detector.

Reversal condition: a multi-goroutine execution model, where per-branch
goroutines and a channel of depth one make branches independent again.
That is a different architecture, not a tweak to this one.
