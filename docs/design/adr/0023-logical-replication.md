# ADR 0023 — Logical replication is a `Source` of changes, acknowledged by the consumer

- **Created:** 2026-10-23
- **Revised:** —
- **Theme:** postgres

## Problem

The connector read (queries) and wrote (COPY) but could not follow: change
data capture — every insert, update and delete of a table, in commit order —
is what PostgreSQL offers through logical replication, and it was missing.
`Params["replication"]` was refused, because a replication connection speaks
only the simple query protocol and answers `START_REPLICATION` with a stream
nothing read.

## Decision

**An entry point of its own.** `Replicate(ctx, dial, cfg, ReplicationConfig)`
opens the connection with `replication=database`, runs
`START_REPLICATION SLOT … LOGICAL` on an existing slot with `pgoutput`
(`proto_version '1'`, `binary 'true'`, the named publications), and returns
a `*Replication`. `Params["replication"]` stays refused: this API is the one
way in. The slot and the publication are created by the caller, in SQL
(`pg_create_logical_replication_slot`, `CREATE PUBLICATION`): they are durable
objects of the server, whose life is the deployment's, not a connection's.

**The element is a decoded change** (`Change`): `Begin`, `Commit`, `Insert`,
`Update`, `Delete`, `Truncate`, with its position in the log (`LSN`), the
table it applies to as last described (`*Relation`: schema, name, columns,
types) and its tuples in binary, which `pgtype` decodes. `Relation` messages
update the relation table and are not delivered; `Type` and `Origin` are read
and ignored; any other message (logical decoding messages, the
streamed-transaction messages of protocol 2 and later) is refused with
`ErrProtocol`, since it was not asked for. The relation table is bounded.

**A batch is what the server has already delivered**, capped by
`ReplicationConfig.BatchChanges` (zero: `sluice.DefaultBatchSize`): a batch
closes when it is full or when the next message is not already received, so
a received change is never held back waiting for the next one. A batch's
bytes live until the next batch (rule 1).

**The consumer acknowledges.** `Replication.Ack(lsn)` declares everything
before `lsn` processed — typically a `Commit`'s `EndLSN`. The position
confirmed to the server (the standby status update) is the highest
acknowledged one, sent at most every `StatusInterval` (zero: ten seconds,
the server's `wal_receiver_status_interval`), whenever a keepalive asks for
a reply, and when the stream stops. The server keeps the log the consumer has
not acknowledged: retention is bounded by the consumer, and delivery is at
least once (after a restart, what follows the last acknowledgement comes
again).

**Backpressure is TCP's.** The stream is pulled: while the consumer works on
a batch nothing is read and the server's sender waits. A batch held longer
than `wal_sender_timeout` (a minute by default) leaves the server's
keepalives unanswered, and the server closes the connection.

**Stopping.** When the consumer stops pulling, the stream reports the
acknowledged position, sends `CopyDone`, reads to `ReadyForQuery` and
returns; `Replication.Close` closes the connection. A cancelled context
closes the connection without an exchange: the slot resumes after the last
position the server was told.

## Rationale

Logical replication is an infinite stream whose pace the consumer sets:
exactly a `Source`. Delivering `Relation` messages would make every consumer
keep the relation table; keeping it in the source and pointing each change at
its description keeps the element self-contained. Binary values are the rest
of the connector's format ([ADR 0022](0022-postgres-connector-shape.md)) and
reuse `pgtype`. Acknowledging from the consumer rather than on delivery is
what makes a restart safe: a position confirmed before processing would lose
the changes in flight at a crash. Creating the slot inside `Replicate` was
rejected: a forgotten slot holds the log without end, and creating one
deserves to be a visible step of the caller's.

What is lost: in-progress transactions (protocol 2 and later, `streaming`)
are not delivered before they commit, so the server holds a large
transaction until its end; `LISTEN/NOTIFY` is another source, still to come.

Reversal condition: a consumer that needs in-progress transactions, or an
implicit acknowledgement.
