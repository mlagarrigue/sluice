# ADR 0006 — Sentinel panics and `Try`: named errors an iterator has nowhere to return

- **Created:** 2026-10-07
- **Revised:** 2026-10-28 (every panic that crosses an operator's goroutine keeps its stack, in `sluice.PanicError`)
- **Theme:** core

## Problem

A `Stream` is an `iter.Seq`, and an iterator has no channel for an error
raised mid-traversal. Some conditions are data-dependent and must stop the
pipeline: a bounded operator reaching its limit under `Fail`, an ordered
input seen out of order, a `Split` branch consumed in the wrong order. A
panic is the only signal available. In a batch job a crash is the right
outcome; in a server it is process death for every in-flight request, and
every serious consumer ends up writing a recovery boundary — each guessing
alone which panics to convert.

Once operators moved into their own packages, the root's `Try` could no
longer hold a list of every sentinel without importing the packages that
declare them, which would reverse the dependency rule.

## Decision

Every sentinel is declared through one internal constructor,
`internal/batch.NewSentinel`, whose value is a `*batch.Sentinel`. `Try`
recovers a panic whose error chain contains a `*Sentinel` (through
`errors.As`, so a wrapped sentinel such as `parallel.ErrState` with its
cause is found too) and returns the panic value as raised; any other panic
is re-raised untouched. The root declares `ErrOverflow`, `ErrUnsorted`,
`ErrSplitStalled`, `ErrSplitDrained`; `parallel` declares `ErrState`. A
test asserts that every exported `Err*` of the root is a sentinel, so a new
one cannot be declared any other way by mistake.

A panic raised on an operator's goroutine — a worker of `Ordered`,
`OrderedWithState` or `Unordered`, the producer of `Async` or
`CoalesceWithin` — is raised again on the consumer's, the one goroutine the
caller sees, in a `*sluice.PanicError`: the value raised in `Value`, the
stack it was raised on in `Stack`, and `Unwrap` returning `Value` when it is
an error. A panic relayed twice (an `Async` over an `Ordered`) stays one
envelope, with the deepest stack. `Try` looks inside the envelope and
returns the sentinel as the goroutine raised it.

## Rationale

**Sentinels only** is the whole design. A `Try` that recovered every panic
would convert user bugs into error returns and hide them: a nil pointer
dereference in a `Map` function must keep crashing the process, loudly,
with its stack. The marker type is what lets each operator package declare
its own sentinels without the core knowing any of them — a registry filled
at `init` time would also work but makes the set depend on link order, and
a list in the root would import its own extensions.

The same promise holds across an operator's goroutine. Raised again as it
was, a worker's panic crashed the process from the consumer's stack, and the
faulty line was nowhere in the output. Every value is wrapped, not only a
runtime error: a `panic("boom")` in a `Map` function is a fault like a nil
dereference, and its stack is as useful. The value stays reachable —
`errors.Is` and `errors.As` through `Unwrap` for an error, the `Value` field
otherwise — at the cost of a `recover() == "boom"` that no longer matches in
the caller. The type is public and in the root because `Try`, which must
recognise it, lives there; what would revise it: a consumer that needs the
stack of every hop.

Panic rather than an error field on `Batch`: an error in the batch would
have to be checked by every operator (the ADR 0005 argument again), and a
sentinel is a wiring or data fault, not a value.
