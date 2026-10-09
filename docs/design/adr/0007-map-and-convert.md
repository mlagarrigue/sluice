# ADR 0007 — `Map` and `Convert` stay two functions, on measurement

- **Created:** 2026-10-07
- **Revised:** —
- **Theme:** core

## Problem

`Map[T]` transforms elements in place (`T → T`, no allocation) and
`Convert[A, B]` writes into its own buffer (`A → B`). Two names for "apply
a function to every element" is the kind of thing explained to every
newcomer, and a single `Map[A, B]` would read better.

## Decision

The two functions stay. `Map` writes in place and shares the caller's
slice through `Of`; `Convert` allocates one output buffer, reuses it across
batches, and never writes to its input. Both documented, with the figures
below on `Convert`.

## Rationale

Go has no compile-time test for `A == B` inside a generic function, so a
single `Map[A, B]` must either always allocate — which measured **+8% per
element** on the request-path benchmark, where batches hold one element —
or decide at construction with a runtime type assertion on `f`. That
assertion is a dynamic lookup when the target type is built from type
parameters, and it measured **+60% per construction** on the same
benchmark, which rebuilds its pipeline per request. A per-batch assertion
was worse still (+72%).

Both variants were measured with fifteen paired rounds and a sign test
before deciding. Two names cost nothing at run time; the merge cost on one
of two hot paths whichever way it was built.

Reversal condition: a language change that lets a generic function
specialise on type identity at compile time.
