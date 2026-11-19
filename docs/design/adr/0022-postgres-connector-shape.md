# ADR 0022 — The PostgreSQL connector's shape: one connection and a fixed pool, binary, the codec apart

- **Created:** 2026-10-23
- **Revised:** 2026-11-19 (the pool looks at an idle connection before lending it, and closes one left in a transaction block); 4 earlier, from 2026-10-28, in its `git log`
- **Theme:** postgres

## Problem

`database/postgres` took its shapes one at a time — no pool, TLS supplied by
the caller, two protocol versions, binary format only, batches asked for
ahead, `FetchAuto`, COPY as successive transactions — and none of them was
written down as a decision: they lived in scattered godoc. Its surface
mixed two things. `go doc -short` listed 190 lines, about 120 of them codec
(`Append*`, `Decode*`, the `OID*` constants, `Numeric`, `Interval`,
`Null`…); 124 of its symbols had no use outside the package, and
`cmd/sluicegen` emitted about twenty. A reader looking for how to open a
connection and read a batch read a hundred encoding functions first.

A program that served independent range reads on several connections — the
comparison study's reference program, under `STUDY_DB_POOL` — held them in a
channel written by hand, which never replaced a broken connection: every
request that drew it answered 503 until the process restarted.

## Decision

**One connection, and a fixed pool beside it.** A `Conn` is one
connection. Grouping calls stays `gateway`'s job, which does with one
connection what a pool does with several for keyed reads. `Pool` holds at
most N connections for what remains — independent queries that grouping
merges only by running them one after the other on a single server
process: it opens them when first needed, lends one per call (for `Query`,
as long as its stream is read), closes a broken one when it comes back, so
that the next call opens a new one, and looks at an idle one before lending
it: one the server closed while it waited is replaced in the pool, not paid
for by the call that finds it. A pool carries no transaction: a connection
that comes back inside a transaction block, open or aborted — the server's
word in its last ReadyForQuery — is closed, which rolls the block back,
rather than lent to a caller whose writes would land in it. No idle
timeout, no lifetime, no setting.

**The transport is the caller's.** `Connect` takes a recipe (`DialTCP`,
`DialTLS`, `DialTLSDirect` or the caller's own); the `tls.Config` is the
caller's. `NewConn` serves a transport already authenticated.

**Two protocol versions**, 3.0 and 3.2, bounded by `Config.MinProtocol` and
`Config.MaxProtocol`.

**Binary only**, for parameters and results.

**Batches asked for ahead**: the next one is requested before the current
one reaches the consumer, as many as cover one round trip, between two and
64. `FetchAuto` reads in one round trip a statement the cache knows fits in
one batch.

**The statement cache is sized once**, when the connection is built:
`Config.StatementCacheSize`, which `Connect` and `NewConn` both take (zero:
512, negative: none). An open connection's cache is not resized.

**COPY is at-least-once**: one transaction every `CopyConfig.RowsPerTx`
rows, and the idempotence is the caller's.

**The codec moves to `database/postgres/pgtype`.** It takes the OIDs, the
scalar, array and nullable `Append*` and `Decode*` functions, the value
types (`Numeric`, `NumericSign`, `Interval`, `Bits`, `TimeOfDay`, `TimeTZ`,
`Null`, `InfinityTime`, `NegInfinityTime`, `JSONBVersion`, `ParseNumeric`)
and the `ErrCodec` error. `pgtype` does not import `postgres`; `postgres`
imports it. What needs a connection or the COPY stream stays in `postgres`:
the `Scan*` functions (they read `*RowBlock`), the framing of a COPY row
(`AppendTupleHeader`, `BeginField`, `EndField`, `AppendField`,
`AppendFieldNull`), and `ErrNullValue`, which wraps `pgtype.ErrCodec`. The
move is a break (`feat(postgres)!`), in the one grouped breaking release;
`sluicegen` emits `pgtype.`.

**Five names are not in the API**: `ParseError` and `ParseRowDescription`
(the message layer is internal: a caller receives an `*Error` through
`errors.As`, never a message body), and `ReadBufferSize`,
`MaxServerParameters`, `MaxServerParameterBytes` (internal bounds no
setting exposes; making them public would mean a `Config` field with zero
as its default).

## Rationale

The import path says the cost before the documentation does (ADR 0001):
`pgtype.DecodeInt8` reads as a pure conversion, `postgres.Conn` as a
connection. The codec is useful without a connection — building a COPY
payload, testing an encoding, writing a generator — and pgx made the same
cut (`pgtype`), which gives a reader a landmark; the price is an import
alias for a project that uses both. `codec` was rejected: `codec.Numeric`
does not say a value is held. Keeping the codec in `postgres` behind a godoc
index was rejected: an index does not shorten what `go doc` shows, and the
split costs one update of `sluicegen` today and could not be made after v1.
The `Scan*` functions stay: moving them would make `pgtype` import
`postgres`, and the cycle is forbidden.

The cache bounds statements prepared *on the server*, so shrinking it on an
open connection would have to close statements the server holds, mid-session
and between two queries the caller believes independent; growing it would be
the one case that costs nothing, and it is not worth a method of its own. `NewConn`
takes the same `Config` and reads what still applies past the startup —
the cache, the message bound, the logger — so a transport someone else
authenticated differs from one `Connect` opens by its defaults, never by
what it can be set to.

The reversal condition this decision first carried — a need for a pool
that `gateway` does not cover — was met: N range scans grouped into one
query run one after the other on one server process, where N connections
run them side by side; and the channel every program wrote in the pool's
place kept a broken connection. The pool stays minimal because its
settings — idle timeout, lifetime — are a deployment's, and a pool that
needs them is a module of its own.

The look before a lend is not a setting. Without it, after a restart of
PostgreSQL up to N calls fail, one per idle connection, and every caller
writes its own retry. It is one `recv` with `MSG_PEEK|MSG_DONTWAIT`: a sound
lend goes from 62 to 560 ns (×9.1, the median of fifteen paired rounds,
`BenchmarkPoolLendIdle`), beside a round trip counted in tens of
microseconds. A read with a deadline a moment ahead cost half a
millisecond, the timer's wake-up, and was set aside. Shown against
PostgreSQL 17 with `pg_terminate_backend`; on a build without that
system call, the call that finds the connection ended fails, as before.

What is lost: one import for the whole connector, one more break in the
breaking release, resizing an open connection's cache, and "no pool" as a
rule a reader keeps in one line.

Reversal condition: a caller who cannot write their code without importing
both packages for an operation that needs one, a need for pool settings —
idle timeout, lifetime — which would go into a module of its own.
