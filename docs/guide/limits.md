# Limits

This page says what Sluice.go **does not do**, module by module, and why.
Three reasons come up, and the difference matters:

- **On purpose**: a design choice, with its reason, that will not change
  without a new argument.
- **Not yet**: a known gap, waiting for work, sometimes on the roadmap.
- **Experimental**: code that exists but leaves that status only under
  precise conditions, stated here.

## The project as a whole

- **Nothing has run in production.** Everything is tested and measured;
  nobody has yet run it under real load for weeks. A first real use is
  being built on top of it, and its frictions will land here.
- **The API will change before version 1.** In v0, a minor version
  (0.1 → 0.2) may rename or remove a function. Changes are announced in the
  release notes; the compatibility policy of a v1 is still to be published.
- **One machine.** Everything fits in one process. Spreading a pipeline
  over several machines, or resuming on another machine after a crash, is
  neither provided nor decided: the question is whether that is the job of
  a library or of the orchestrator around it (Kubernetes, message queues).
  It will be settled by a written decision before any code.
- **No resumption of a long job.** A pipeline that runs for eight hours and
  whose process dies starts over. The architecture names what is missing —
  a "phase runner", a unit of resumption that outlives the process, with
  its state recorded in the application's schema — and explains why it is
  not built: its state must live with you, not in the library. It is the
  first item on the ETL side of the roadmap.
- **Dependencies are decided case by case.** Today, the standard library
  plus `golang.org/x/crypto`, and nothing enters without a written decision
  that says why and what it brings along. The criterion is the least
  useless code, not a count: a maintained compressor (zstd, brotli) beats a
  home-made one, and it will enter when the measurement says it pays. What
  is excluded is the dependency that arrives without a decision.

## The core

- **No `Sort`, `GroupBy` or `Materialize`** — on purpose. Each needs to see
  every value; you write `Collect` then the standard function, so that
  "everything is loaded" can be read in the code.
- **No `Buffer`** — on purpose. A buffer between two stages decouples
  nothing in a synchronous pull; `parallel.Async` is the operator that
  decouples, and it costs a goroutine, which is said.
- **A batch is valid only for the duration of the call.** That is not a
  limit, it is the rule that makes everything fast; it is repeated here
  because it is trap number one (see [Getting started](getting-started.md),
  rule 1).
- **Deduplication is local.** `distinct.By(n)` remembers only the last `n`
  keys — on purpose, because a global set over an infinite stream does not
  fit in memory.

## PostgreSQL

- **PostgreSQL only.** No other system has a connector. An inventory of the
  most useful connectors (MySQL, SQLite, Redis, Kafka, S3, CSV and Parquet
  files…) is the first item on the roadmap; it will pick the first three
  by demand, by what the batch model brings to each, and by whether they
  can be written without a dependency.
- **No connection pool** — on purpose. How many connections to open is
  your application's decision; batching does with one connection what a
  pool does with many.
- **Authentication**: SCRAM-SHA-256 (with channel binding over TLS),
  cleartext password on explicit request, and `trust`. **`md5` is refused**
  — on purpose: the hash stored server-side is as good as the password, and
  PostgreSQL has deprecated it. The error message says what to change on
  the server.
- **SASLprep is not applied to the password.** That Unicode normalisation
  needs tables the standard library does not have. For a password in
  printable ASCII there is no difference; beyond that, a password the
  server normalised fails cleanly rather than authenticating with a
  weakened version.
- **Types**: present, the common ones (`bool`, `bytea`, `date`,
  `float4/8`, `inet`/`cidr`, `int2/4/8`, `interval`, `json`, `jsonb`,
  `numeric`, `text`, `timestamp`, `timestamptz`, `uuid`, `time`, `timetz`,
  `macaddr`, `bit`). Thirteen of them — `bool`, `int2/4/8`, `float4/8`,
  `text`, `bytea`, `uuid`, `timestamptz`, `timestamp`, `date`, `numeric` —
  also have a one-dimensional array, the same array with NULL elements, and
  a column scanner; the table in the [PostgreSQL guide](postgres.md#the-codecs-in-one-table)
  says which has what. **Missing** — not yet: `money` (its scale depends on
  a server setting, a trap more than a type), `tsvector`, ranges, composite
  types, multi-dimensional arrays, and arrays and scanners of the other
  types (`json`, `jsonb`, `inet`, `cidr`, `interval`, `time`, `timetz`,
  `macaddr`, `bit`) — on demand, as no use has asked for them yet. Each
  addition needs vectors taken from a real server.
- **TLS is yours.** The connector dials nothing and negotiates no TLS: you
  hand it an already open connection, encrypted or not. On purpose: which
  host, which certificate, which timeout are deployment decisions.
- **No automatic resumption of an interrupted COPY.** Delivery is
  at-least-once and idempotence is yours; a resumption watermark should
  live in your schema, not in the library.
- **`pushdown` pushes the limit and the key bound, not the column set**:
  the SQL is the caller's and its select list is not rewritten.

## Web and gateway

- **No router, no adapter, no middleware** — on purpose. `http.ServeMux`
  routes since Go 1.22; the handler stays eight lines you can read; an
  adapter would hide the status code and the encoding.
- **One token verifier provided**: HMAC (a symmetric signature with a
  shared secret). Public-key tokens (RSA, ECDSA, EdDSA) are not provided —
  not yet; the `Verifier` interface is the extension point.
- **`ClaimAuthorizer` trusts the token.** It builds the permission from
  what the token says about itself; that is correct only if the issuer is
  your service. For an external identity provider, implement `Authorizer`
  against whatever knows the policy.
- **The gateway only amortises round trips.** Without a database or a
  remote service behind it, it costs more than it saves (measured: ×65
  slower). And under WSL2, any `Within` under a millisecond is worth
  1.12 ms.
- **`gateway.Partitions` is experimental**: the mechanism (grouping a batch
  by key) works and is tested, but no measurement yet says when it pays.
- **No HTTP compression** (`Content-Encoding: gzip`…) — not yet, on either
  path. gzip and deflate will come from the standard library, zstd and
  brotli from a maintained module, the day the measurement says what each
  brings.

## Experimental: the exit conditions

These packages leave the label only by meeting these conditions, or move
to a separate module before a v1.

- **`net/quic`**: being green in the public *QUIC Interop Runner*; an
  external cryptographic review. Two conditions left this list on
  2026-10-10: key update (RFC 9001 §6) — both sides rotate, and a long
  connection no longer dies of its packet count — and the first
  third-party peer: quic-go, on each side of the handshake, in continuous
  integration (the `interop/` module, job `interop-quic`). The runner
  itself was first run against quic-go on 2026-10-10 (`interop/cmd/endpoint`,
  the `Interop runner` workflow). As a server, sluice passes handshake,
  transfer, longrtt, multiplexing, retry, http3, blackhole, keyupdate,
  amplificationlimit, transferloss, transfercorruption, ipv6, rebind-port,
  rebind-addr, handshakeloss and handshakecorruption, and fails
  connectionmigration (the server closes with INTERNAL_ERROR once the
  client moves to the preferred address). As a client it passes handshake,
  transfer, longrtt, multiplexing, retry, blackhole, transferloss,
  transfercorruption, ipv6, handshakeloss and handshakecorruption, and
  fails amplificationlimit, rebind-port and rebind-addr (the connection
  dies of its idle timeout after the NAT rebinds). The two handshake cases
  under 30 % loss or corruption passed on both sides once a probe timeout
  resent the two oldest unacknowledged packets instead of copying the
  first again (RFC 9002 §6.2.4). The cases it refuses, as the runner's
  exit status 127 asks, are the features documented as not there — 0-RTT,
  resumption, ECN, QUIC v2 — plus chacha20 (Go's TLS 1.3 offers no way to
  restrict the cipher suites), a forced key update and HTTP/3 on the client
  side (no HTTP/3 client in the module). Green means every one of these
  passed against at least two peers.
- **`net/quic/udp`**: follows `quic`; packet information on Darwin and
  Windows the day a deployment there needs it.
- **`web/stream`**: a real consumer of its stages. Its transport,
  `net/httpstream`, left this list on 2026-10-10: curl, h2spec and quic-go's
  client in continuous integration, fuzzed parsers, the per-batch cost of a
  streamed response measured.

## Measurements: what is missing

- **No end-to-end measurement with a real remote client**: the network
  figures are on loopback, and the ETL bench simulates its I/O. A
  "to the browser" bench is to be built.
- **No comparison with other languages**: it is framed and specified, but
  nothing is measured; its results will be published with their method,
  not before.
- **The figures are a laptop's under WSL2**, not a dedicated rig: read
  ratios, never absolutes ([Benchmarks](../benchmarks.md)).

## What comes next

In the order the roadmap takes them: the connector inventory and
prioritisation, the phase runner for long jobs, the end-to-end bench, the
comparative study, streaming compression, and the exit from experimental
of what meets its conditions. None of it is started; each needs a written
decision first.
