# ADR 0001 — `Split` reads the source once, with one batch of slack per branch

- **Status:** accepted
- **Date:** 2026-08-17
- **Scope:** `Split`, core API, pre-1.0 (nothing tagged, no compatibility cost)

## Context

`Split` fans one stream out to N branches. The first implementation returned N
closures, each of which called the source for its own account. On a replayable
source — `Of` over a slice — two branches produced identical results and the
design looked sound.

It was not. The source ran once per consumed branch, and on a single-pass
source — a cursor, a network read, anything consumed as it is produced — the
second branch received nothing at all. No error, no panic: a silent prefix of
the data, on the one use case `Split` exists to serve. The doc comment claimed
the opposite ("branches share a single run of the source"), which is how the gap
survived review.

§6.2 of the architecture states the constraint that governs this: with a
non-replayable source you cannot have independent branches, a single read, and
bounded memory at once. One of the three must go.

## Decision

Traverse the source exactly once through `iter.Pull`, and give each branch a
slot holding **at most one batch**.

The branch asked for a batch drives the shared traversal and deposits the result
into every live destination. A branch whose slot is still full blocks the pull:
the batch stays held rather than overwriting undrained data.

We sacrifice **branch independence** — §6.2's second row, the choice Akka makes
for `Broadcast`. Concurrent branches must be advanced in alternation.

Three consequences follow, each a deliberate choice:

- **Branches consumed one after the other panic with `ErrSplitStalled`.** An
  `iter.Seq` has nowhere to return an error, and yielding a silent prefix is the
  failure mode we are removing. Failing loudly is the point.
- **A branch nobody consumes is not a destination.** Batches routed to it are
  dropped. Without this, a partition read one branch at a time would stall on a
  consumer that never arrives. The cost: attachment depends on consumption
  order, so a branch must be consumed to receive anything.
- **A branch is single-pass**, like the stream it comes from. Consuming one
  twice yields nothing the second time.

## Alternatives rejected

**Restrict the contract to replayable sources.** Zero code change: document that
`Split` re-runs its source. Rejected — it removes the operator's reason to
exist, since the framework targets web, database and ETL sources, all
single-pass, and §6 of the architecture falls with it.

**Unbounded buffering.** Full branch independence, the Web Streams model. MDN
describes the consequence plainly: unread data queues with no limit and no
backpressure. Rejected by guarantee S1 — one slow branch grows memory without
bound.

**Explicit attachment before start (a `Run()` the caller invokes after wiring
branches).** Correct, and it would preserve independence. Rejected on API cost:
the return stops being a rangeable `[]Stream[T]`, and the common case stops
fitting in a few lines. The complexity belongs inside the framework, not in
every call site.

## Cost

Measured, not projected — see Result 6 in `BENCHMARK-STEP-0.md`:

| Mode | Cost | vs a plain batched traversal |
|---|---|---|
| Partition, one branch | 0.271 ns/element | below it (half the batches are dropped) |
| Broadcast, two branches | 0.938 ns/element | ×2.9, i.e. one visit per branch |

Both are well inside the 1.5 ns per-stage budget, and `Split` allocates nothing
itself.

- `iter.Pull` costs 68.6 ns per call (measured, `BENCHMARK-STEP-0.md`). Amortized
  over a 1024-element batch: ~0.07 ns per element, against a budget of 1.5 ns
  per stage per element. Not a concern.
- `route` is called **exactly once** per batch. It is the caller's function and
  may be stateful — round-robin is — so calling it again while re-attempting a
  deposit would misroute the data. The destinations are computed when the batch
  is pulled and kept until it is placed.
- Memory: one batch per branch, plus one held batch. O(n) in branch count, O(1)
  in stream length.
- Callers wiring two live branches must use `iter.Pull` rather than two nested
  `range` loops. This is documented on `Split` and shown in
  `ExampleSplit_broadcast`.

## What would change our mind

A multi-goroutine execution model. The bounded slot exists because a single
goroutine cannot suspend one branch while another advances; with per-branch
goroutines and a channel of depth 1, branches become independent again and the
sacrifice moves from independence back to scheduling cost. That is a different
architecture, not a tweak to this one.

## Verification

`TestSplitBroadcastSinglePass` asserts a single traversal on a source that fails
the test if traversed twice. `TestSplitStalls` pins the panic.
`TestSplitPartitionSingleBranch`, `TestSplitEarlyStopKeepsSiblingUsable` and
`TestSplitAbandonedBranchReleasesSource` cover the attachment and release rules.
`Split` is at 100% statement coverage.
