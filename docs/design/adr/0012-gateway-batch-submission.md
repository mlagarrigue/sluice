# ADR 0012 — The gateway takes a transport batch in one submission

- **Created:** 2026-08-22
- **Revised:** 2026-10-07 (renumbered)
- **Theme:** gateway

## Problem

Two batching mechanisms — the gateway gathering concurrent callers by
waiting, the stream transport handing over what arrived together — composed
only in test wiring: a goroutine per element around `Do`, exploding the
transport's batch so the gateway's timer could re-coalesce what was already
coalesced.

## Decision

`DoBatch(ctx, ins) []Outcome` submits a batch in one motion and waits for
all of it. **Positional, total, never dropping**: the result has exactly
`len(ins)` elements and outcome *i* answers input *i*; when the context
ends or the pipeline dies mid-wait, answered elements keep their answers
and the rest carry the cause. **The batch joins the shared window; it does
not bypass it**: its calls share `Within` with every other caller and spill
past `Size` into the next pipeline batch — no flush-now flag. **No
goroutines**: submission is a loop over the existing channel.

Three rules of the gateway itself are recorded here with it, since this is
the decision that fixed its contract: the element *is* the call, carrying
the channel its answer returns through, so **a call is never dropped** —
reject by replying with a rejection; **nothing checks per batch that every
call was answered**, because returning from a yield means accepted
downstream, not handled; and **the value is I/O amortisation only**,
measured ×6.9 against a bounded backend and ×65 slower against an
unbounded one.

## Rationale

A batch that must not wait for company belongs on its own pipeline, not on
a gateway asked to stop gathering. A second channel carrying whole batches
would buy one hand-off instead of N sends, against a backend round trip
three orders of magnitude larger; it can be built later without changing
the API if a measurement ever shows per-call sends dominating. `Outcome`
exists because failure semantics had to be specified per element.
