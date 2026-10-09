# ADR 0006 — Sentinel panics and `Try`: named errors an iterator has nowhere to return

- **Created:** 2026-10-07
- **Revised:** —
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

## Rationale

**Sentinels only** is the whole design. A `Try` that recovered every panic
would convert user bugs into error returns and hide them: a nil pointer
dereference in a `Map` function must keep crashing the process, loudly,
with its stack. The marker type is what lets each operator package declare
its own sentinels without the core knowing any of them — a registry filled
at `init` time would also work but makes the set depend on link order, and
a list in the root would import its own extensions.

Panic rather than an error field on `Batch`: an error in the batch would
have to be checked by every operator (the ADR 0005 argument again), and a
sentinel is a wiring or data fault, not a value.
