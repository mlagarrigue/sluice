# Limits

This page says what Sluice.go **does not do**, module by module, and why.
Three reasons come up, and the difference matters:

- **On purpose**: a design choice, with its reason, that will not change
  without a new argument.
- **Not yet**: a known gap, waiting for work, sometimes on the roadmap.
- **Experimental**: code that exists but leaves that status only under
  precise conditions, stated here.

**In short.** Nothing has run in production yet, and the API changes before
version 1 ([the project](#the-project-as-a-whole)). One process, and no
sort or buffer stage ([the core](#the-core)). PostgreSQL alone, without a
pool, read in binary ([PostgreSQL](#postgresql)). No router, no
compression, no CONNECT tunnel ([web and gateway](#web-and-gateway)). QUIC
is experimental, under [written conditions](#experimental-the-exit-conditions).
The figures are a laptop's ([measurements](#measurements-what-is-missing)).

## The project as a whole

- **Nothing has run in production.** Everything is tested and measured;
  nobody has yet run it under real load for weeks. A first real use is
  being built on top of it, and its frictions will land here.
- **The API will change before version 1.** In v0, a minor version
  (0.1 → 0.2) may rename or remove a function. Changes are announced in the
  release notes. What v1 will freeze — the stable packages, the sentinel
  errors, the diagnostic codes — and what it leaves free, the experimental
  packages, is in [ADR 0020](../design/adr/0020-compatibility.md), which
  `scripts/check-api.sh` applies.
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
  plus `golang.org/x/crypto` and `golang.org/x/text`, and nothing enters
  without a written decision
  that says why and what it brings along. The criterion is the least
  useless code, not a count: a maintained compressor (zstd, brotli) beats a
  home-made one, and it will enter when the measurement says it pays. What
  is excluded is the dependency that arrives without a decision.
- **Platforms: Linux first.** The whole suite runs on Linux amd64; the
  network transport (`net/quic`, `net/quic/udp`, `net/httpstream`) also
  runs on Windows, macOS and Linux arm64; the rest of the code is only
  compiled and vetted there. Supported means: CI runs it on every push,
  and a failure on that platform is a defect to fix. Elsewhere (the BSDs)
  the code builds with no guarantee, and QUIC datagrams stay at 1200 bytes
  for want of a "Don't Fragment" option. The table:
  [Platforms, in detail](../reference/protocol-positions.md#platforms-in-detail).

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

### What it connects to

- **PostgreSQL only.** No other system has a connector. An inventory of the
  most useful connectors (MySQL, SQLite, Redis, Kafka, S3, CSV and Parquet
  files…) is the first item on the roadmap; it will pick the first three
  by demand, by what the batch model brings to each, and by whether they
  can be written without a dependency.
- **No connection pool** — on purpose. How many connections to open is
  your application's decision; batching does with one connection what a
  pool does with many.

### Authentication, types and certificates

- **Authentication**: SCRAM-SHA-256 (with channel binding over TLS, required
  or disabled by `Config.ChannelBinding`), cleartext password on explicit
  request, and `trust`. **`md5` is refused** — on purpose: the hash stored
  server-side is as good as the password, and PostgreSQL has deprecated it;
  the error message says what to change on the server. SCRAM-SHA-1 is not
  implemented (no PostgreSQL server offers it). The PBKDF2 iteration count
  and the salt length are bounded, adjustable through
  `Config.MinSCRAMIterations`, `MaxSCRAMIterations` and `MinSCRAMSaltLen`.
  Where this departs from RFC 5802 and RFC 4013, and why:
  [Authentication, in detail](../reference/protocol-positions.md#authentication-in-detail).
- **Types**: the [PostgreSQL reference's table](../reference/postgres.md#the-codecs-in-one-table)
  says which has what; [the types not covered](postgres.md#the-types-not-covered)
  names every other built-in type, by family. Arrays of any shape, ranges,
  multiranges and composites go through the
  [generic containers](postgres.md#containers-arrays-ranges-composites).
  `hstore`, `citext`, `ltree` and `vector` go through their resolved OID
  ([`LookupType`](postgres.md#types-an-extension-creates)). **Missing**:
  PostGIS's types, left as EWKB bytes (a geometry library decodes them),
  pgvector's `halfvec` and `sparsevec`, and other extensions' types. Such
  a column arrives as raw binary bytes through `Rows.Value`, or cast to
  `text` in the query; `aclitem` and `gtsvector`, which have no binary form,
  must be cast.
- **Certificates are yours.** The connector opens the connection with
  the recipe you give it: `DialTCP`, `DialTLS` (which performs the
  SSLRequest peculiar to PostgreSQL), `DialTLSDirect` (direct TLS, since
  PostgreSQL 17), or your own (a UNIX socket, a proxy). Which certificates to trust stays your decision,
  in the `tls.Config` you pass: see the
  [PostgreSQL guide](postgres.md#encrypting-the-connection).
- **An RSA-PSS signature whose salt is not the hash length is not
  verified** by `crypto/x509`, Go's standard library; libpq verifies it.
  It is OpenSSL's default when signing with PSS. A server certificate
  placed itself in `RootCAs` (self-signed, trusted as it stands) passes,
  and SCRAM-SHA-256-PLUS binding works: its signature is not checked. But
  if an authority signed the server's certificate or an intermediate that
  way, the TLS handshake fails before any authentication: re-sign with
  `-sigopt rsa_pss_saltlen:digest`.

### What it reads and writes

- **Notifications on a connection of their own, no notices.** `Listen`
  opens a connection that only listens and streams its notifications
  ([ADR 0024](../design/adr/0024-listen-notify.md)); a `LISTEN` run through
  `Exec` on an ordinary connection still has its notifications discarded.
  The server's notices (`RAISE NOTICE`, warnings) are discarded too.
- **No COPY TO STDOUT.** Only loading (`COPY ... FROM STDIN`) has an API;
  a `COPY ... TO STDOUT` fails with an error that says so. To read in bulk,
  `Query` on a `SELECT` already delivers batches.
- **Everything is read in binary, with no per-column text fallback.** A
  type with no binary send function (some extension types) fails the whole
  query; the workaround is in SQL: `column::text`.
- **Logical replication only, through `Replicate`.** `Params["replication"]`
  is refused before anything is sent, because `Query` and `Exec` cannot drive
  a replication connection; `Replicate` opens one and streams a slot's
  changes ([ADR 0023](../design/adr/0023-logical-replication.md)). Physical
  replication is not spoken, transactions arrive only once committed (no
  `streaming`), and the slot and publication are the caller's to create.
- **NegotiateProtocolVersion is read as a whole version word.** The
  PostgreSQL documentation (§54.7) calls its first field the newest minor
  version the server supports; the server writes the whole word, major in
  the high half, and this connector reads it that way. A server that sent
  a bare minor would be refused as a malformed message, and none exists;
  `TestStartupFollowsANegotiatedDowngrade` pins the reading.
- **No automatic resumption of an interrupted COPY.** Delivery is
  at-least-once and idempotence is yours; a resumption watermark should
  live in your schema, not in the library.

### Bounds, and what is pushed down

- **A message from the server is bounded at 64 MiB** — a row with all its
  values, a COPY chunk. Over it, the message is refused unread and the
  connection is broken (`ErrMessageTooLarge`, then `ErrConnBroken`).
  `Config.MaxMessage` raises the bound, up to the 1 GiB PostgreSQL
  itself allows per field; the handshake keeps its own 64 KiB bound.
- **An outgoing message is bounded at 1 GiB less one byte** — PostgreSQL's
  allocation limit, which nothing moves: in practice, a query parameter
  that large. It is refused before a byte leaves (`ErrOutgoingTooLarge`),
  and the connection serves the next query. A larger value is split:
  PostgreSQL stores no field over 1 GiB anyway.
- **A batch is bounded at 64 MiB of values** as well as at `BatchRows` rows.
  A batch whose values reach it is delivered with the row that crossed it,
  and the next row opens the next batch: nothing fails, the consumer sees
  more and smaller batches, and `Config.Logger` gets one warning per result.
  `QueryConfig.BatchBytes` sets it, up to 1 GiB; past it, or below zero, the
  query is refused. A single row wider than the bound travels alone, so
  memory per batch is the bound plus one row.
- **`pushdown` pushes the limit and the key bound, not a column set**:
  the SQL is the caller's, and its select list already says which columns
  travel. A column demand would come back with a source that can skip
  decoding a column on its own.

## Web and gateway

### Serving and authenticating

- **On the `net/http` path, no router and no adapter** — on purpose.
  `http.ServeMux` routes since Go 1.22; the handler stays short enough to read
  at a glance; an adapter would hide the status code and the encoding. Two
  wrappers exist, and neither picks the status or the encoding of an
  ordinary response: `web.Recover` turns a panic into an observed 500, and
  `web.TryHandler` a sentinel panic into the answer the caller writes. The
  `web/stream` path has its own router (`stream.NewRouter`), because
  `http.ServeMux` does not see a batch.
- **`net/httpstream` streams request bodies by default.** A body that did
  not arrive whole with its headers reaches the handler through
  `Request.Stream` as it comes, on the three protocols — on HTTP/3 when
  the QUIC windows cover a request's head, which those of
  `Config.QUICConfig` do; on smaller windows bodies arrive whole.
  `Config.BodyDelivery: BodyWhole` asks for the other contract, every body
  complete before its handler runs, bounded by `Config.MaxBodyBytes`
  (1 MiB). What is accepted is the same either way
  ([ADR 0009](../design/adr/0009-request-contract.md), article 4). A body a
  handler leaves unread costs the HTTP/1.1 connection, a `RST_STREAM` with
  `NO_ERROR` after the response on HTTP/2, a `STOP_SENDING` with
  `H3_NO_ERROR` on HTTP/3.
- **A streaming connection is bounded by its windows, not `MaxBodyBytes`**:
  `MaxConcurrentStreams` times `InitialWindowSize` on HTTP/2, times
  `InitialMaxStreamDataBidiRemote` on HTTP/3 — the figure to multiply by
  the connections a server accepts.
- **One token verifier provided**: HMAC (a symmetric signature with a
  shared secret). Public-key tokens (RSA, ECDSA, EdDSA) are not provided —
  not yet; the `Verifier` interface is the extension point.
- **`ClaimAuthorizer` trusts the token.** It builds the permission from
  what the token says about itself; that is correct only if the issuer is
  your service. For an external identity provider, implement `Authorizer`
  against whatever knows the policy.

### The gateway

- **The gateway only amortises round trips.** Without a database or a
  remote service behind it, it costs more than it saves
  ([measured](../benchmarks.md#the-gateway-against-a-bounded-backend): one
  call every 3.7 µs at best, for nothing; 2026-10-22, one laptop). And under WSL2, a `Within` of
  500 µs is worth 1.11 ms.
- **`gateway.Partitions` is experimental**: the mechanism (grouping a batch
  by key) works and is tested, and its own cost is measured
  (`BenchmarkPartition*`, [benchmarks](../benchmarks.md)), but no measurement
  yet says when the round trips it saves pay for it.

### What the HTTP layer leaves out

- **No HTTP compression** (`Content-Encoding: gzip`…) — not yet, on either
  path. gzip and deflate will come from the standard library, zstd and
  brotli from a maintained module, once the measurement says what each
  brings.
- **Request trailers arrive apart from the headers** — identically over
  HTTP/1.1, 2 and 3: parsed, held to the same bounds as the headers
  (`MaxHeaders`, `MaxHeaderBytes`), refused when malformed, then handed to
  the handler through `Request.Trailers()` once the body has ended —
  before the handler is called for a whole body, once it has been read to
  its end for a streamed one — never merged into `Request.Headers`.
  A trailer is a field that arrived after the body (a checksum, a
  signature); RFC 9110 §6.5 keeps it apart because the request was
  already framed and routed by then, so nothing in the package reads it.
- **Preconditions, ranges and validators are the handler's.** Neither the
  transport nor `web/stream` reads the `If-*` fields (RFC 9110 §13.1),
  `Range` or `Content-Range` (§14); `RenderOpen` sets neither `ETag` nor
  `Last-Modified`; an `Expect` other than `100-continue` reaches the
  handler, with no 417 (§10.1.1, a MAY). The fields arrive untouched in
  `Request.Headers`, for the handler, which alone holds the representation.
  So a conditional GET gets a 200 instead of a 304, and a PUT carrying
  `Content-Range` that a handler ignores replaces the whole resource, where
  §14.5 says 400. In plain words: the transport delivers the letter, reading
  it is yours; with a state, set `ETag` or `Last-Modified` and answer the
  `If-*` fields; without one, refuse with 400 a PUT carrying `Content-Range`.
- **No CONNECT tunnel.** A CONNECT request is answered 501 by the transport
  on HTTP/1.1, 2 and 3 (RFC 9110 §9.3.6, §15.6.2) and never reaches the
  handler: this is an origin server, not a proxy, and the request has no
  shape a handler could serve — its target is a host and port, not a path.
  On HTTP/2 and HTTP/3 a CONNECT that carries `:scheme` or `:path`, or
  omits `:authority`, is malformed (RFC 9113 §8.5, RFC 9114 §4.4) and its
  stream is reset as such. On HTTP/1.1 the connection closes with the 501
  and nothing that follows the CONNECT is read (RFC 9931 §8): a client that
  already sent the tunnel's bytes does not see them read as a request. In
  plain words: a client that asks this server
  to open a connection to somewhere else is told, with a status, that the
  server does not do that; it is not told its request was wrong.
- **Misdirected requests are the handler's to answer.** An `https` request
  on a connection whose certificate does not cover its host must be refused
  (RFC 9110 §7.4), with a 421 when a client reused a connection for another
  name. The transport does not check it: it never sees the certificate
  (the caller sets up TLS, [ADR 0014](../design/adr/0014-tls-stays-in-front.md)),
  and clear text may come from a trusted gateway that ended TLS upstream,
  the case §7.4 sets apart. A handler can: `Request.TLS` holds the
  handshake's `ServerName` (nil in clear text), and a 421 goes out as it
  is. In plain words: if you serve several names on one address, compare
  `Host` with what your certificate covers and answer 421 to the rest.
- **The never-indexed bit is not carried.** An HTTP/2 or HTTP/3 client may
  mark a field as never to be indexed (RFC 7541 §6.2.3, RFC 9204 §4.5.4);
  `Header` keeps the name and the value only, and the bit is lost at
  decoding. The response encoders set it by name alone (`Set-Cookie`,
  `Authorization`, `Proxy-Authorization`) and never index anything: a
  response is safe to send, but a program that relays a field it received
  under another name cannot preserve the bit, which RFC 9204 §7.1.3 asks of
  an intermediary; the bit is to be carried when a relay is built on this
  origin server. In plain words: if you copy the headers you received toward another HTTP/2
  or HTTP/3 server, a secret the client had marked "do not compress" may
  leave compressed.
- **Unknown HTTP/3 elements are skipped, not counted.** A frame, a setting
  or a stream of an unknown type is ignored as RFC 9114 §9 requires, with
  no counter and no cap on their number (§10.5 asks to track their use).
  Measured (`TestH3UnknownControlFramesCostNothingHere`): a control stream fed GREASE frames, empty or a kilobyte each, leaves
  nothing in the carry buffer and allocates nothing per frame — they are
  skipped by counter, never buffered. A peer that sends them without end
  pays its own bandwidth, and the transport's flow control bounds it: credit
  is given back only as this side reads. What does cost something — field
  sections, request streams, cancellations — has a bound of its own
  (`MaxHeaderBytes`, the per-stream cap, the cancellation allowance); to
  revisit if a frame is measured to cost the processor disproportionately.
- **A QUIC server belongs behind ingress filtering.** A client that spoofs
  its source address can have a QUIC server send packets to an address of
  its choosing (request forgery, RFC 9000 §21.5). The server bounds what it
  sends before an address is validated, but the RFC asks that no QUIC
  server be deployed in a network without ingress filtering (BCP 38:
  dropping a packet whose source address cannot come from where it enters)
  that also holds poorly secured UDP services. That is the network's to
  provide, not the library's. In plain words: have your network drop
  spoofed source addresses before they reach the server.

## Experimental: the exit conditions

The packages marked experimental leave the label only by meeting written
conditions, checkable one by one; they stay in the module
([ADR 0021](../design/adr/0021-net-quic-stays.md)). The conditions, and
where each package stands:
[Exit conditions](../reference/exit-conditions.md).

## Measurements: what is missing

- **No end-to-end measurement with a real remote client is published**:
  the network figures are on loopback, and the ETL bench simulates its I/O.
  The client exists — `comparison/loadgen`, in a process of its own,
  HTTP/1.1, HTTP/2 and HTTP/3 over TLS, every answer checked — but it has
  run on loopback only, and none of its figures is published.
- **No comparison with other languages**: it is framed and specified, but
  nothing is measured; its results will be published with their method,
  not before.
- **The figures are a laptop's under WSL2**, not a dedicated rig: read
  ratios, never absolutes ([Benchmarks](../benchmarks.md)).
- **The interop runner's two measurements, goodput and crosstraffic, are
  not run**: the weekly pass plays the cases, whose verdict gates the
  job, on a shared CI runner, whose throughput would be what those two
  measured rather than the server's. They wait for the dedicated machine
  the end-to-end measurement above waits for.

## What comes next

In the order the roadmap takes them: the connector inventory and
prioritisation, the phase runner for long jobs, the end-to-end bench, the
comparative study, streaming compression, and the exit from experimental
of what meets its conditions. The end-to-end bench and the comparative
study have their apparatus in `comparison/` (the study's server, one flag
per axis, and its load generator); what they lack is the campaign on the
machines the protocol names, and its results. `pushdown`,
`net/httpstream` and `web/stream` are out of the label; for the rest, each item needs a
written decision first.

## In detail

The longest limits are argued in full — how each was measured, and where the
library departs from a specification — in
[Protocol positions](../reference/protocol-positions.md): the platforms,
authentication, the interop runner, the datagram size and TLS.
