# ADR 0001 — The package map: a core that retains nothing, operators by what they retain, the periphery by external system

- **Created:** 2026-10-07
- **Revised:** 2026-10-23 (what the root retains, the internal packages, operators and stages, two names that collide, where Partitions lives), 2026-10-28 (written in the present tense)
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

Three root operators retain more than the batch in flight, each by a
bound that does not grow with the stream: `Coalesce` holds at most one
batch, and it is the core's own tool for restoring full batches; `Split`
holds at most one pending batch per branch; `Merge` holds one `iter.Pull`
coroutine per source and buffers no batch.

Two words name what these packages export. An **operator** is a function
from `Stream[A]` to `Stream[B]` and composes over a whole stream. A
**stage** is a function from one batch to the next, called once per batch:
`parallel.Ordered` takes one, `httpstream.Handler` is one, and `web/stream`
writes its stages as functions over the batch's `[]Exchange`.

The package is part of a name, as in the standard library (`strings.Split`
and `bytes.Split`). `sluice.Merge` interleaves streams, Rx's sense;
`join.Merge` is a merge join over sorted inputs, the databases' sense. Both
keep the name their reader already knows. `gateway.Partitions` groups a
batch by key like a `Split` would, but it stays in `gateway`: it keeps a key
index and its buffers from one batch to the next, which the root does not,
and its reason — the security principal in the batching key — is the
gateway's.

The periphery is grouped like the standard library, by external system:
`database/postgres`, `net/quic` (with `net/quic/udp`), `net/httpstream`,
and `web` with `web/stream` beside it. Cross-cutting extensions
(`diagnostics`, `gateway`, `probe`, `pushdown`) stay as root-level
packages. Shared machinery is internal: `internal/batch`, `internal/dgram`,
`internal/handoff`, `internal/ipkey`, `internal/pgwire`, `internal/streamtest`,
`internal/varint` for the module, and an `internal/` under the one package
that needs it (`database/postgres/internal/entity`, `web/internal/reqbody`,
`net/httpstream/internal/interopserver`). The root imports nothing of the
module beyond `internal/batch`, and a test enforces it.

## Rationale

The import path says the cost before the documentation does: `join.`
in a call site means "this holds a side in memory". Grouping the periphery
by system follows what a Go reader already knows from `net/http` and
`database/sql`, and leaves room for `database/mysql` or `net/grpc` without
renaming anything. Grouping it by direction (provider/consumer, client/server)
was rejected: a PostgreSQL connector both reads and writes, and `quic`
holds both roles.

The split was measured before it shipped: twelve paired rounds against the
pre-split tree showed no benchmark slower, so the cross-package calls to
`internal/batch` cost nothing observable.

Reversal condition: an operator whose retention fits none of the packages
above. The criterion would then need one more package, not a return to one
flat namespace.
