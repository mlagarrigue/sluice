# ADR 0012 — The gateway takes a transport batch in one submission

- **Created:** 2026-08-22
- **Revised:** 2026-10-07 (renumbered), 2026-10-24 (`Config.Eager`), 2026-10-28 (gain re-measured)
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
measured at ×11.9 the throughput of one query per call against a bounded
backend, and a pure cost with nothing behind it.

**A gateway may also not wait.** `Config.Eager` sends a batch as soon as
the run loop holds a call, with every call already queued behind it;
`Within` is then zero. Batches form while the pipeline works: at low
traffic a call leaves alone and waits for nothing, and as the backend slows,
more calls arrive per batch served, so the batches grow by themselves up to
`Size`, with no threshold and no switch. The default remains the `Within`
window.

## Rationale

A batch that must not wait for company belongs on its own pipeline, not on
a gateway asked to stop gathering. A second channel carrying whole batches
would buy one hand-off instead of N sends, against a backend round trip
three orders of magnitude larger; it can be built later without changing
the API if a measurement ever shows per-call sends dominating. `Outcome`
exists because failure semantics had to be specified per element.

`Eager` was measured on the study's server (local PostgreSQL, unit reads,
generator and server on one machine: workshop figures, never published):
at 2 000 req/s the p50 goes from 2.27 ms to 0.82 ms; the p99 is better at
every rate; near saturation it holds at least what the 2 ms window holds,
its batches reaching 50 keys. What it costs: at medium load, five times the
SQL queries (1 800 against 350 a second at 20 000 req/s), since a backend
that is not saturated receives small batches. That is why it is an option
and not the default: a shared database may prefer fewer queries to a lower
latency. A threshold on concurrent calls (PostgreSQL's `commit_siblings`)
and a `Within` tuned to the observed rate were set aside: each adds a
setting and a switch that oscillates, where the natural grouping adjusts
by itself. With an asynchronous pipeline (`parallel.Unordered`), batches
form only once every worker is busy.
