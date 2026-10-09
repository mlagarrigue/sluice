# ADR 0001 — The package map: a core that retains nothing, operators by what they retain, the periphery by external system

- **Created:** 2026-10-07
- **Revised:** —
- **Theme:** core

## Problem

Seventy-three files sat in one root package: the two core types beside
joins, windows, parallel operators, and every exported symbol read as equal
to every other. A newcomer could not tell from the import path which
operators were cheap and which held state, and the periphery (`postgres`,
`quic`, `httpstream`, `webstream`) was named by implementation rather than
by what it connected to. The core also imported `diagnostics` from one
operator, which broke its own rule that it knows none of its extensions.

## Decision

Operators are grouped by **what they retain between two batches**:

| Package | Retains |
|---|---|
| `sluice` (root) | nothing beyond the batch in flight |
| `join` | one side of the other stream |
| `window` | time |
| `parallel` | goroutines |
| `distinct` | keys |

`Coalesce` stays in the root as the one documented exception: it holds at
most one batch, and it is the core's own tool for restoring full batches.
The periphery is grouped like the standard library, by external system:
`database/postgres`, `net/quic`, `net/httpstream`, with `web/stream` beside
`web`. Cross-cutting extensions (`diagnostics`, `gateway`, `probe`,
`pushdown`) stay as root-level packages. Shared machinery (`internal/batch`,
`internal/pgwire`, `internal/streamtest`) is internal. The root imports
nothing of the module beyond `internal/`, and a test enforces it.

## Rationale

The import path now says the cost before the documentation does: `join.`
in a call site means "this holds a side in memory". Grouping the periphery
by system follows what a Go reader already knows from `net/http` and
`database/sql`, and leaves room for `database/mysql` or `net/grpc` without
renaming anything. Grouping it by direction (provider/consumer, client/server)
was rejected: a PostgreSQL connector both reads and writes, and `quic`
holds both roles.

The split was measured before it shipped: twelve paired rounds against the
pre-split tree showed no benchmark slower, so the cross-package calls to
`internal/batch` cost nothing observable.

Reversal condition: an operator whose retention does not fit one of the
four families. The criterion would then need a fifth package, not a return
to one flat namespace.
