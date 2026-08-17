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

- **Branches consumed one after the other panic.** An `iter.Seq` has nowhere to
  return an error, and yielding a silent prefix is the failure mode we are
  removing. Failing loudly is the point. Two distinct panics cover the two
  shapes this takes, because the first version only caught one of them:
  - `ErrSplitStalled` — a branch is drained while a sibling sits mid-consumption
    holding an undrained batch. The pipeline cannot advance.
  - `ErrSplitDrained` — a branch is consumed *after* a sibling ran the source
    dry. Nothing stalls; there is simply nothing left, so the branch would yield
    an empty stream indistinguishable from a legitimate one.

  Lateness is tracked **per branch**, not per source: the check fires only on a
  branch that was named a destination at least once and then found nothing. A
  branch `route` never chose is empty for the same reason every branch over an
  empty source is — nothing was addressed to it — and stays silent. Getting this
  grain wrong turns partition, where a branch matching nothing is routine, into
  a crash on correct code.
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
- **Every branch belongs to one goroutine.** The shared state above is held in
  plain variables, so consuming two branches concurrently is a data race. Unlike
  the two ordering mistakes, this one is documented rather than detected: the
  supported way to drive two branches is `iter.Pull`, which runs each branch body
  on its own coroutine and parks it inside `yield` between calls. A branch parked
  that way is indistinguishable — by entry counter or by goroutine identity —
  from one running concurrently elsewhere, so any check strict enough to catch
  the mistake also rejects `ExampleSplit_broadcast`. Measured, not assumed: an
  entry counter was implemented and it failed `TestSplitPartition` and the
  example. `-race` catches the real thing.

## What would change our mind

A multi-goroutine execution model. The bounded slot exists because a single
goroutine cannot suspend one branch while another advances; with per-branch
goroutines and a channel of depth 1, branches become independent again and the
sacrifice moves from independence back to scheduling cost. That is a different
architecture, not a tweak to this one.

## Verification

`TestSplitBroadcastSinglePass` asserts a single traversal on a source that fails
the test if traversed twice. `TestSplitStalls` pins the mid-consumption panic and
`TestSplitLateBranchPanics` the drained-source one.

A panic that fires when it should not is worse than no panic at all, so
`ErrSplitDrained` is pinned from both sides: `TestSplitLateBranchFalsePositives`,
`TestSplitSingleBranchOfPartitionIsSilent` and
`TestSplitUnmatchedPartitionBranchIsSilent` assert silence on every supported
consumption — empty source, branch consumed twice, early stop then sibling, one
branch of a partition read alone, and a branch `route` never chose.

`TestSplitUnmatchedBranchFirstStillReportsLoss` is the counterweight: the same
wiring, consumed in the other order, *does* lose data — the batches addressed to
the sibling are written off as the unmatched branch drives the source — and is
still reported. The pair pins the distinction the check rests on.

`TestSplitPartitionSingleBranch`, `TestSplitEarlyStopKeepsSiblingUsable` and
`TestSplitAbandonedBranchReleasesSource` cover the attachment and release rules.
`TestSplitSharesBatchWithoutCopying` pins the no-copy contract between branches,
and `TestSplitAlternationIsRaceFree` keeps the supported alternation clean under
`-race`.

`Split` is at 100% statement coverage.
