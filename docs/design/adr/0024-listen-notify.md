# ADR 0024 — `LISTEN/NOTIFY` is a `Source` on a connection of its own

- **Created:** 2026-10-23
- **Revised:** 2026-10-27
- **Theme:** postgres

## Problem

`NOTIFY channel, 'payload'` broadcasts a short message to every session that
ran `LISTEN channel`; the server sends it to them as a
`NotificationResponse` when the sending transaction commits, at any point of
the protocol. The connector tolerated the message and discarded it: no API
delivered it.

## Decision

**A connection of its own.** `Listen(ctx, dial, cfg, ListenConfig)` opens a
connection, runs `LISTEN` for every channel named, in one query, and returns
a `*Listener` whose `Notifications(ctx)` is a `Source[Notification]`
(`Channel`, `Payload`, the sending session's `PID`). The connection does
nothing else: it stays outside any transaction, so it receives as early as
the server sends.

**Nothing is lost while subscribing.** A notification can arrive before the
`ReadyForQuery` that closes the `LISTEN` query; it is kept and delivered
first, in batches of `BatchNotifications`, where `Exec` would discard it.
The protocol does not bound how many arrive: what is kept is bounded at
4 MiB, a bound of the package and not the caller's batch size; past it the
server is holding its answer back to flood, and the connection is closed.

**A batch is what has already been received**, capped by
`ListenConfig.BatchNotifications` (zero: `sluice.DefaultBatchSize`), as for
replication ([ADR 0023](0023-logical-replication.md)).

**Backpressure is the server's, and shared.** While the consumer works on a
batch nothing is read; the server keeps pending notifications in a queue
common to the whole instance (8 GB in a standard installation). When it
fills, the transactions that send `NOTIFY` fail at commit: a listener that
falls behind weighs on every sender. The guide says so.

**Stopping.** When the consumer stops pulling, the stream returns;
`Listener.Close` closes the connection, which ends the subscriptions. A
cancelled context closes the connection.

## Rationale

An ordinary connection would mix notifications with queries, and a session
that stays inside a transaction keeps the server from cleaning its
notification queue (the `NOTIFY` documentation); a connection that only
listens never does. A record of its own rather than a revision of 0023: the
same shape of asynchronous source, but with no slot, no acknowledgement and
no resumption — a notification sent while nobody listens is gone, which is
what `NOTIFY` means.

What is lost: no resumption and no at-least-once delivery; a consumer that
must see every event reads a table, and uses notifications as the signal to
look.

Reversal condition: a need to listen on a connection that also serves
queries.
