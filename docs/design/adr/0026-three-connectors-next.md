# ADR 0026 — The three connectors after PostgreSQL: CSV and JSON Lines, Redis, NATS JetStream

- **Created:** 2026-11-12
- **Revised:** 2026-11-12 (each protocol fact read in its published text and cited; Redis written, the acknowledged shape confirmed)
- **Theme:** core

## Problem

The PostgreSQL connector was the library's only one. It had shown the
relational source, the bulk sink (`COPY`), change capture acknowledged by
the consumer ([ADR 0023](0023-logical-replication.md)) and notification
([ADR 0024](0024-listen-notify.md)). The architecture names four domains —
web, database, ETL, messaging — and messaging had no code; ETL had no
source that did not need an external system. Each connector is a protocol
written again, a cost accepted because the common drivers do not batch,
but a cost all the same: with no order written down, the next one would
have been whichever was asked for last.

## Decision

**Three connectors, in this order.**

1. **CSV and JSON Lines**, a source and a sink over an `io.Reader` and an
   `io.Writer`: a batch is N lines, and the reading and writing are the
   standard library's (`encoding/csv`, `encoding/json`).
2. **Redis (and Valkey, which speaks the same protocol)**: RESP over one
   connection, a batch sent in one go (pipelining); reading a Redis stream
   (`XREADGROUP`) as a source the consumer acknowledges (`XACK`).
3. **NATS JetStream**: a pull consumer that asks for one batch at a time,
   each message acknowledged by the consumer; publishing by batch.

**What the protocols say**, read in their published texts. Valkey speaks
RESP2 and RESP3 and pipelines commands sent in one write
([Valkey, protocol](https://valkey.io/topics/protocol/)).
`XREADGROUP … COUNT n` returns at most n entries per stream, each pending
in the group's list until the consumer's `XACK`
([Redis, XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/)).
A JetStream fetch names a batch size and a timeout, and acknowledgement is
explicit ([NATS, consumers](https://docs.nats.io/nats-concepts/jetstream/consumers));
a batch published atomically, stored whole or not at all, needs a NATS
server of version 2.12 or later
([nats-io ADR-50](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md)).

**Where they live**, by [ADR 0001](0001-package-map.md) (the periphery by
external system, like the standard library): `encoding/csvstream` and
`encoding/jsonl`, `database/redis`, `messaging/nats`. The `stream` suffix
is added only when the plain name is that of a standard library package
the same caller imports — the rule that already gave `net/httpstream`.

**What they take from [ADR 0022](0022-postgres-connector-shape.md):** the
transport is the caller's (a dial recipe, the caller's `tls.Config`); one
connection and no pool — grouping calls is `gateway`'s job; batches asked
for ahead where the protocol allows it. **From ADR 0023:** a source that
cannot replay is acknowledged by its consumer, once it has finished a
batch, never by the connector as it reads.

**Redis, written (`database/redis`).** RESP2 only, which every server
accepts without negotiation; `Conn.Pipeline` sends a batch of commands in
one write. The consumer group took ADR 0023's shape unchanged —
`Conn.ReadGroup`, `Group.Entries` and `Group.Ack` stand where `Replicate`,
`Replication.Changes` and `Replication.Ack` stand — with one difference the
protocol makes: an XACK names entries, where PostgreSQL's acknowledgement is
a position, so `Group.Ack` takes IDs and the acknowledgements ride the next
read's round trip. The shape generalises: an acknowledged source outside
PostgreSQL needed no new idea.

**Kafka is not among the three.** A Kafka consumer reads the batches its
producers compressed, and three of the protocol's codecs (Snappy, LZ4,
Zstd) are not in the standard library: a record batch's attributes name
gzip, snappy, lz4 and zstd
([Kafka, message format](https://kafka.apache.org/42/implementation/message-format/)). It comes in once a choice is made
between a dependency ([ADR 0002](0002-dependency-graph.md)) and a consumer
that refuses batches compressed with anything but gzip.

## Rationale

The order follows what each one proves, for what it costs. CSV and JSON
Lines prove nothing new about the batch, but they take days, close the ETL
pipeline with no external system (file → transformations → `COPY`) and
serve the ETL scenario of the comparative study. Redis proves two shapes
for a small protocol: enrichment by batch (a batch of elements, one round
trip, an enriched batch — `gateway`'s idea applied to a cache) and an
acknowledged source outside PostgreSQL, which tells whether ADR 0023's
shape generalises. NATS JetStream carries the messaging domain on the
standard library alone: one batch per request, one acknowledgement per
message.

Set aside, with their reason: MySQL and MongoDB bring no new shape for a
large protocol; Parquet and Arrow need codecs and a large format; an OTLP
export must first say whether it reopens [ADR 0025](0025-observability.md)
(the library's own telemetry) or is a sink like any other; the S3 API and
Elasticsearch's `_bulk` prove real shapes (a transport unit the service
imposes, an answer per element) that no use and no measurement asks for
before the first three.

Reversal condition: a real use that needs another connector before these,
or a decision on Kafka's dependency.
