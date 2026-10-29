# Decisions

Each decision holds in three sections — Problem, Decision, Rationale — and
is revised in place; its history is its `git log`. They are grouped here by
theme, the question each one settles beside its title.

## The core (`core`)

- [ADR 0001 — The package map: a core that retains nothing, operators by what they retain, the periphery by external system](0001-package-map.md)
  — how the packages are laid out: by what each keeps between two batches.
- [ADR 0003 — `Split` reads the source once, with one batch of slack per branch](0003-split-reads-once.md)
  — how one stream fans out to N branches without reading its source twice.
- [ADR 0005 — `Batch` carries no context; context travels in the element type](0005-batch-carries-no-context.md)
  — whether a batch carries a `context.Context`.
- [ADR 0006 — Sentinel panics and `Try`: named errors an iterator has nowhere to return](0006-sentinel-panics-and-try.md)
  — how an iterator reports an error it has nowhere to return.
- [ADR 0007 — `Map` and `Convert` stay two functions, on measurement](0007-map-and-convert.md)
  — why transforming in place and converting stay two functions.

## Joins (`join`)

- [ADR 0004 — The merge join carries values, extracts keys, and always checks order](0004-merge-join-signature.md)
  — what the merge join's signature carries, and why order is always
  checked.
- [ADR 0016 — A hash join's build side is bounded, and the bound is required](0016-hash-join-bound.md)
  — why a hash join's build side must be given a bound.

## PostgreSQL (`postgres`)

- [ADR 0022 — The PostgreSQL connector's shape: one connection, binary, the codec apart](0022-postgres-connector-shape.md)
  — the connector's shape, and why each part of it holds.
- [ADR 0023 — Logical replication is a `Source` of changes, acknowledged by the consumer](0023-logical-replication.md)
  — how logical replication becomes a source the consumer acknowledges.
- [ADR 0024 — `LISTEN/NOTIFY` is a `Source` on a connection of its own](0024-listen-notify.md)
  — how `LISTEN/NOTIFY` becomes a source.

## HTTP (`httpstream`)

- [ADR 0008 — A multiplexed connection is a reader, a responder and a writer](0008-multiplexed-connection.md)
  — how a multiplexed connection splits reading, answering and writing.
- [ADR 0009 — One request contract for three protocols](0009-request-contract.md)
  — how three HTTP versions present one request.
- [ADR 0013 — QPACK's static table ships, checked](0013-qpack-static-table.md)
  — how HTTP/3's static header table is shipped and checked.
- [ADR 0017 — Shutdown drains, it does not cut](0017-graceful-shutdown.md)
  — how a server stops without cutting what it is answering.
- [ADR 0018 — Zero is the safe default, the bound is mandatory](0018-zero-values-and-mandatory-bounds.md)
  — what a zero or a negative setting means.
- [ADR 0025 — What the server and the gateway report: a `slog.Logger`, nothing else](0025-observability.md)
  — what is logged, at which level, and what never leaves the log.

## QUIC (`quic`)

- [ADR 0014 — TLS stays in front of the transport](0014-tls-stays-in-front.md)
  — why the transport does not terminate TLS itself.
- [ADR 0015 — The QUIC server: transport machinery, a real listener, a moving client](0015-quic-server.md)
  — what a QUIC server needs beyond a handshake.
- [ADR 0019 — 0-RTT: accepted once, delivered early, acted on when allowed](0019-quic-early-data.md)
  — when requests sent in 0-RTT are accepted and acted on.
- [ADR 0021 — `net/quic` stays in the module](0021-net-quic-stays.md)
  — why `net/quic` stays in this module.

## The web path (`web`)

- [ADR 0010 — Routing is a table matched on the raw target](0010-routing-table.md)
  — how routing matches a request's target.
- [ADR 0011 — The middle arrows ship beside the core, as `web/stream`](0011-web-stream-beside-core.md)
  — where the middle stages of the web path live.

## The gateway (`gateway`)

- [ADR 0012 — The gateway takes a transport batch in one submission](0012-gateway-batch-submission.md)
  — how the gateway takes what arrived together in one call.

## Tooling (`ci`)

- [ADR 0002 — The dependency graph is a decision, never a default](0002-dependency-graph.md)
  — why every production dependency is a recorded decision.

## Documentation (`docs`)

- [ADR 0020 — What v1 freezes](0020-compatibility.md)
  — what version 1 commits to.
