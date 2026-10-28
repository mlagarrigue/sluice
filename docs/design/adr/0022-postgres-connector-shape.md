# ADR 0022 — The PostgreSQL connector's shape: one connection, binary, the codec apart

- **Created:** 2026-10-23
- **Revised:** 2026-10-28 (written in the present tense)
- **Theme:** postgres

## Problem

`database/postgres` took its shapes one at a time — no pool, TLS supplied by
the caller, two protocol versions, binary format only, batches asked for
ahead, `FetchAuto`, COPY as successive transactions — and none of them is
written down as a decision: they live in scattered godoc. Its surface mixes
two things. `go doc -short` lists 190 lines, about 120 of them codec
(`Append*`, `Decode*`, the `OID*` constants, `Numeric`, `Interval`,
`Null`…); 124 of its symbols have no use outside the package, and
`cmd/sluicegen` emits about twenty. A reader looking for how to open a
connection and read a batch reads a hundred encoding functions first.

## Decision

**One connection, no pool.** A `Conn` is one connection; holding several is
the caller's business, and `gateway` does with one connection what a pool
does with several, by grouping the calls.

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

**COPY is at-least-once**: one transaction every `CopyConfig.RowsPerTx`
rows, and the idempotence is the caller's.

**The codec moves to `database/postgres/pgtype`.** It takes the OIDs, the
scalar, array and nullable `Append*` and `Decode*` functions, the value
types (`Numeric`, `NumericSign`, `Interval`, `Bits`, `TimeOfDay`, `TimeTZ`,
`Null`, `InfinityTime`, `NegInfinityTime`, `JSONBVersion`, `ParseNumeric`)
and the `ErrCodec` error. `pgtype` does not import `postgres`; `postgres`
imports it. What needs a connection or the COPY stream stays in `postgres`:
the `Scan*` functions (they read `*Rows`), the framing of a COPY row
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

What is lost: one import for the whole connector, and one more break in the
breaking release.

Reversal condition: a caller who cannot write their code without importing
both packages for an operation that needs one, or a need for a pool that
`gateway` does not cover.
