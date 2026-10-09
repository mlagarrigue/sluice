# ADR 0005 — `Batch` carries no context; context travels in the element type

- **Created:** 2026-08-17
- **Revised:** 2026-10-07 (renumbered)
- **Theme:** core

## Problem

Should a batch carry a `context.Context` — a cancellation signal and a
tenant, available to every stage — or should a batch be only a group of
values? The question blocks every core type above `Batch`, because
`Source` and `Diagnostic` cannot be designed without knowing whether a
batch carries ambient state.

## Decision

**`Batch` holds `Items` and nothing else.** Context travels in the element
type, in a wrapper the caller defines outside the core:

```go
type Envelope[T any] struct {
    Ctx   context.Context
    Value T
}
s := sluice.Filter(envelopes, func(e Envelope[Order]) bool { return e.Ctx.Err() == nil })
```

Cancellation stays a parameter of the source constructor, checked once per
batch with a hoisted `done` channel, never per element.

## Rationale

Cost was measured and is not the reason: a `context.Context` is an
interface, so the batch would grow from 24 to 40 bytes, and a traversal of
a million elements is unchanged (343 against 346 ns), with or without
per-batch cancellation checks.

The deciding argument is structural. No operator in the core reads a
batch's context, and half of them **rebuild** the batch because they
change its contents. A `Ctx` field would have to be copied at each of those
sites and at every one written later; the omission compiles, runs, and
silently drops the context from that stage onward — the failure mode this
project turns into a panic everywhere else. What a caller pays instead is
16 bytes per element when they do need per-element context: a real trade,
made where the caller knows they need it.

The context-merging question (there is no `context.Merge` in Go) is
answered by the element: a merge pairs elements without combining their
contexts, still O(1).

Rejected: a second batch type `CtxBatch[T]` (every operator written twice),
and an unexported field with accessors (the propagation obligation remains
and the accessors hide it).

Reversal condition: a core operator that must read the context to be
correct. Cancellation does not qualify; if pushdown's `Demand` ever needs
ambient per-batch state that cannot travel in the element, that is the case
to reopen this on.
