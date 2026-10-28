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
  [Platforms, in detail](#platforms-in-detail).

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
- **Authentication**: SCRAM-SHA-256 (with channel binding over TLS, required
  or disabled by `Config.ChannelBinding`), cleartext password on explicit
  request, and `trust`. **`md5` is refused** — on purpose: the hash stored
  server-side is as good as the password, and PostgreSQL has deprecated it;
  the error message says what to change on the server. SCRAM-SHA-1 is not
  implemented (no PostgreSQL server offers it). The PBKDF2 iteration count
  and the salt length are bounded, adjustable through
  `Config.MinSCRAMIterations`, `MaxSCRAMIterations` and `MinSCRAMSaltLen`.
  Where this departs from RFC 5802 and RFC 4013, and why:
  [Authentication, in detail](#authentication-in-detail).
- **Types**: the common ones, each with its Go type, and for thirteen of
  them a one-dimensional array (NULL elements included) and a column
  scanner — the [PostgreSQL guide's table](postgres.md#the-codecs-in-one-table)
  says which has what. **Missing** — not yet: `money` (its scale depends
  on a server setting, a trap more than a type), `tsvector`, ranges,
  composite types, multi-dimensional arrays, and arrays and scanners of
  the types the table leaves without — on demand, as no use has asked for
  them yet. Each addition needs vectors taken from a real server.
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

- **On the `net/http` path, no router and no adapter** — on purpose.
  `http.ServeMux` routes since Go 1.22; the handler stays short enough to read
  at a glance; an adapter would hide the status code and the encoding. Two
  wrappers exist, and neither picks the status or the encoding of an
  ordinary response: `web.Recover` turns a panic into an observed 500, and
  `web.TryHandler` a sentinel panic into the answer the caller writes. The experimental
  `web/stream` path has its own router (`stream.NewRouter`), because
  `http.ServeMux` does not see a batch.
- **`net/httpstream` streams request bodies by default.** A body that did
  not arrive whole with its headers reaches the handler through
  `Request.Stream` as it comes, on the three protocols — on HTTP/3 when
  the QUIC windows cover a request's head, which those of
  `Config.TransportParameters` do; on smaller windows bodies arrive whole.
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
- **The gateway only amortises round trips.** Without a database or a
  remote service behind it, it costs more than it saves
  ([measured](../benchmarks.md#the-gateway-against-a-bounded-backend): one
  call every 3.7 µs at best, for nothing). And under WSL2, a `Within` of
  500 µs is worth 1.11 ms.
- **`gateway.Partitions` is experimental**: the mechanism (grouping a batch
  by key) works and is tested, and its own cost is measured
  (`BenchmarkPartition*`, [benchmarks](../benchmarks.md)), but no measurement
  yet says when the round trips it saves pay for it.
- **No HTTP compression** (`Content-Encoding: gzip`…) — not yet, on either
  path. gzip and deflate will come from the standard library, zstd and
  brotli from a maintained module, once the measurement says what each
  brings.
- **Request trailers arrive apart from the headers** — identically over
  HTTP/1.1, 2 and 3: parsed, held to the same bounds as the headers
  (`MaxHeaders`, `MaxHeaderBytes`), refused when malformed, then handed to
  the handler in `Request.Trailers` — or, for a streamed body, which ends
  after the handler was given the request, through
  `Request.StreamTrailers` once the body has been read to its end — never
  merged into `Request.Headers`.
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
  Measured before it was written (`TestH3UnknownControlFramesCostNothingHere`):
  a control stream fed GREASE frames, empty or a kilobyte each, leaves
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

These packages leave the label only by meeting these conditions; they stay
in the module ([ADR 0021](../design/adr/0021-net-quic-stays.md)). One
bullet per package, each condition in it checkable:

- **`web/stream`**: a first real consumer, an application outside this
  repository, serves its requests through these stages, and what it
  needed changed has been changed. The figures against third-party
  clients (HTTP/1.1, HTTP/2, HTTP/3) are already in
  [the benchmarks](../benchmarks.md). Checked by: the consumer named in
  this bullet with the sluice version it runs, and each change it asked
  for named by its commit.
- **`net/quic/udp`**: it leaves with `net/quic`, not before. Its options
  are the socket half of the transport and are judged by the same passes;
  the `test-udp` job already runs it on Windows, macOS and Linux ARM.
  Checked by: `net/quic`'s conditions met, and `test-udp` green on the
  commit that lifts the label.
- **`net/quic`**: three conditions, each checked on its own.
  - *Interop*: being green in the *QUIC Interop Runner* — its weekly
    workflow green with `interop/expected-results.json` unchanged, every
    case against quic-go and ngtcp2 in each direction passing or failing
    for its written cause. Not yet: no complete pass has been green with
    the register unchanged ([in detail](#the-interop-runner-in-detail)).
  - *Review*: an external review of the handshake and key handling,
    linked from `SECURITY.md`. Not done.
  - *Measurement*: throughput and latency against a real third-party
    client, over a network between two machines, in
    [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client)
    with date and machines. Missing: see "Measurements: what is missing".

What `net/quic` does today, and within which bounds — limits it keeps
whichever way the conditions go:

- **`net/quic` throughput**: a 64 KiB response over HTTP/3 stays below
  the same response over HTTP/2 with every core available, and catches up
  on two; the figures, their date and the machine that measured them are
  in [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client),
  the one place that keeps them current. On Linux and Windows, the
  datagrams one `Stream.Write` produces leave in one system call (UDP
  segmentation offload); on macOS, which offers no public interface for
  it, or on a system that refuses the option, each datagram is still its
  own write. Sizing the QUIC windows does not buy the gap back (scaled 1
  to 16 times, on either side, they move nothing): the gap is the Go
  scheduler, as a request crosses goroutines and each hand-off wakes one.
- **`net/quic` datagram size**: datagrams start at 1200 bytes, the
  smallest maximum RFC 9000 §14.1 guarantees, and rise by a search for the
  largest datagram the path lets through (DPLPMTUD, §14.3, RFC 8899), up to
  `TransportParameters.MaxDatagramSize` (zero: 1452; 8952 for a 9000-byte
  jumbo path); the size reached reads in `Stats.DatagramSize`. A path that
  shrinks under a connection is detected and the size falls back to 1200
  (`Stats.MTUBlackHoles`). ICMP "packet too big" messages are ignored, so a
  path that cannot carry 1200 bytes is never detected: the connection ends
  at its idle timeout. How the search probes, and the RFC positions:
  [Datagram size, in detail](#datagram-size-in-detail).
- **`net/quic`, a Dial to a closed port on Windows**: on Linux and macOS,
  the ICMP refusal that answers each flight fails the Dial after a few of
  them. On Windows, `net` and `udp.Configure` turn `SIO_UDP_CONNRESET` off
  on every UDP socket, so the refusal never surfaces: the Dial waits out
  the whole handshake deadline (`DefaultHandshakeTimeout`, 10 s, or the
  context's). A `PacketConn` that keeps the option on gets the early
  failure.
- **`net/quic` reassembly**: the bytes of a stream that arrive out of
  order are kept until the gap before them fills, up to the flow-control
  limit this end granted — and in at most `max(held / 1024, 256)` disjoint
  fragments, 16,384 at the ceiling, `held` being the bytes parked. Past
  that the connection closes with INTERNAL_ERROR, the reason naming the
  bound. A conformant sender stays well under it: on a deep-buffered path
  a 1.4 MB flight left 257 holes, each between whole packets, where the
  bound by then allows 1,367. The bound is there for the peer that sends
  one byte per frame, who would otherwise choose this end's bookkeeping
  cost. In plain words: a stream may be delivered in any order, but not
  as confetti.
- **`net/quic` closing period and idle floor**: a closed connection stays
  closing or draining for three probe timeouts (PTO, the delay after which
  a sender resends what was not acknowledged), as RFC 9000 §10.2 asks, and
  ten seconds at most; the idle timeout's §10.1 floor of three PTOs stops
  at ten seconds too. The peer chooses most of a PTO — its
  `max_ack_delay`, legal up to 2^14 ms — and uncapped, legal parameters
  gave 3 min 25 s during which a listener keeps the connection's
  `MaxConns` and `MaxConnsPerIP` slots. Ten seconds covers three PTOs of a
  2.5 s round trip; a late packet is then answered with a Stateless Reset.
  In plain words: a closed connection keeps its place ten seconds at most,
  an idle one its idle timeout or ten seconds, whichever is longer.
- **`net/quic` versions**: version 1 only. QUIC version 2 (RFC 9369) and
  compatible version negotiation (RFC 9368) are not implemented: a server
  that does not speak version 1 ends the dial, and a peer's
  `version_information` parameter is skipped like any unknown parameter.
  Version 2 differs from 1 in constants only and exists against
  middleboxes that ossify on version 1; none of this package's peers has
  shown one, and when one does, that is when to add it. In plain words:
  there is one dialect, and it is the one everybody speaks today.
- **`net/quic` spin bit**: the latency spin bit (RFC 9000 §17.4), an
  optional signal from which an observer on the path reads the round
  trip, is not implemented: the bit is random per connection and per
  connection identifier, as the section asks of an endpoint that does not
  spin, and the peer's is ignored. An operator who asks to measure paths
  this way is the signal to add it. In plain words: the packets carry no
  signal put there for a watcher to time the round trip.
- **`net/quic` TLS**: the handshake is `crypto/tls`'s, from the Go release
  `go.mod` names as the floor, run through `tls.QUICClient` and
  `tls.QUICServer`; what it refuses leaves as CRYPTO_ERROR. Three of its
  positions depart from RFC 9846, none visible to a conformant peer, and
  the compatibility mode is neither asked for nor policed; they are
  inherited as they are, listed in [TLS, in detail](#tls-in-detail).
  Rewriting the TLS stack is out of the question (ADR 0002); a peer refused
  over one of them is the signal to revisit. In plain words: the lock on
  the door is the one Go ships, quirks included, and it is kept rather
  than copied.
- **`net/quic` secrets**: a secret or key is forgotten by becoming
  unreachable, never by being overwritten. RFC 9846 §6 has endpoints
  "forget" the keys of a failed connection and §7 would have each secret
  erased once used; here the sealers and openers of a closed connection
  are released to the garbage collector, a rotated key is replaced, and
  no copy is cleared — Go gives no guarantee that clearing a slice erases
  its bytes, and `crypto/tls` and `crypto/cipher` hold their own copies
  that this package cannot reach. What that costs is a window the length of the
  process, open only to whoever can read the process's memory, who has
  everything else as well. To revisit once Go or `crypto/tls` offers
  guaranteed erasure. In plain words: keys are dropped, not shredded,
  because the shredder does not exist.
- **`net/quic` IPv6 flow label**: on Linux the kernel labels each flow
  (`IPV6_AUTOFLOWLABEL`, set by `net/quic/udp`), by a hash of the
  addresses and ports: the label changes when the peer's address does and
  stays when only the connection identifier rotates, where RFC 9000 §9.7
  would have the identifier counted in. Setting a label per datagram would
  cost a control message on every send; and on a constant four-tuple the
  packets are already linked for any observer, label or not. macOS and
  Windows offer no control over the label at all. To revisit on a
  measurement where the label alone tells two connections apart. In
  plain words: the tag on the envelope is the postal service's, and the
  address links the envelopes anyway.

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
machines the protocol names, and its results. `pushdown` and
`net/httpstream` are out of the label; for the rest, each item needs a
written decision first.

## Details

The entries above say the limit; what follows is the full record of the
longest ones — how each was measured or argued, and where it departs from
a specification.

### Platforms, in detail

What CI runs on every push, for Go 1.26 (the floor `go.mod` names) and the
latest stable release:

| System | Architecture | What runs | Supported |
|---|---|---|---|
| Linux | amd64 | the whole suite under `-race`, PostgreSQL 14 to 18, QUIC and HTTP interop | yes |
| Linux | arm64 | `net/quic/udp`, `net/quic`, `net/httpstream` under `-race` | yes for these packages; the rest is pure Go, not run |
| Windows | amd64 | the same packages; all the code is vetted for Windows from the Linux runner (`GOOS=windows go vet`) | likewise |
| macOS | arm64 | the same packages; all the code is vetted for macOS from the Linux runner (`GOOS=darwin go vet`) | likewise |
| BSD, others | — | nothing | no: builds, without "Don't Fragment" (QUIC stays at 1200 bytes) |

To contribute on Windows, use WSL: the scripts are `sh`
([CONTRIBUTING](../../CONTRIBUTING.md)). In plain words: in production,
Linux is the safe platform; Windows and macOS run the tested network code,
and the rest of the code is Go that compiles there.

### Authentication, in detail

SCRAM-SHA-256 (with channel binding over TLS, required
or disabled by `Config.ChannelBinding`),
cleartext password on explicit request, and `trust`. SCRAM-SHA-1, which
RFC 5802 §4 requires of every SCRAM client, is not implemented: no
PostgreSQL server offers it. The SCRAM username field is sent empty, as
libpq sends it: PostgreSQL takes the role from the startup packet and
ignores it, where RFC 5802 §5.1 wants a name. A password that SASLprep
(RFC 4013) would refuse — not UTF-8, a prohibited character, mixed
bidirectional text — is sent as its raw bytes, the way `pg_saslprep`
derived the role's key on the server: refusing it here would lock out a
password the server accepts.

**`md5` is refused**
— on purpose: the hash stored server-side is as good as the password, and
PostgreSQL has deprecated it. The error message says what to change on
the server.

The PBKDF2 iteration count the server asks for is accepted
between 4096 (RFC 7677's floor, PostgreSQL's default) and 1,048,576, and
the salt at 16 bytes or more; a server outside that range is refused with
the field to set in the message — `Config.MinSCRAMIterations`,
`MaxSCRAMIterations`, `MinSCRAMSaltLen` — for a role stored at
`scram_iterations = 1000` (a fast test fixture) or at two million (a
hardened server). Zero means the default; a negative value is refused
before anything is sent.

### The interop runner, in detail

The `Interop runner` workflow (the runner at revision `ab039a4`) crosses
every implementation in its list every Sunday, on each side its role
allows — 31 peer-directions — with the image the `Interop image` workflow
builds from each release tag from v0.3.0 on
(`ghcr.io/mlagarrigue/sluice-interop`; the three tags before it carry
images pushed by hand, and `latest` never moves back to them). The
table of its latest complete pass is on the
[`interop-results` branch](https://github.com/mlagarrigue/sluice/tree/interop-results);
a failed case, or a direction that wrote no results, is run once more.
`interop/expected-results.json` records
every case of every implementation the workflow crosses, with the cause
of every failure, and a pass that departs from it fails the job. Green
means every case passed against at least two peers.

What the record holds against quic-go and ngtcp2. As a server, sluice
passes handshake, transfer, longrtt, chacha20, multiplexing, retry,
resumption, zerortt, http3, blackhole, keyupdate, amplificationlimit,
transferloss, transfercorruption, ipv6, rebind-port and rebind-addr
against both, plus ecn and connectionmigration against ngtcp2;
handshakeloss and handshakecorruption pass against quic-go and, against
ngtcp2, in most passes: under 30 % loss a handshake sometimes outlives
`ListenerConfig.HandshakeTimeout` (10 s), and under corruption a file
sometimes never arrives; both are recorded as alternating, with their
cause.

As a client it passes handshake, transfer,
longrtt, multiplexing, retry, resumption, zerortt, blackhole,
amplificationlimit, handshakeloss, transferloss, handshakecorruption,
transfercorruption, ipv6, rebind-port and rebind-addr against both, plus
ecn against ngtcp2. It refuses, with the exit status 127 the runner asks
for, what is documented as not there: QUIC v2, and on the client side
chacha20 (Go's TLS 1.3 offers no way to restrict the cipher suites it
offers), a forced key update and HTTP/3.

Failures whose cause lies outside sluice: quic-go's image refuses ecn
itself. quic-go's client never moves to a server's preferred address, so
connectionmigration sees one path. msquic's server sometimes answers
first under the client's own connection ID, then changes it, and RFC
9000 §7.2 has the client discard what follows (four client-side cases).
And against ngtcp2's server, connectionmigration fails on the client
side: the runner wants a PATH_CHALLENGE in the server's first packet on
the new path, and ngtcp2 probes only after a non-probing packet, which a
client that validates first, as §9.6.2 requires, never sends there.

### Datagram size, in detail

Datagrams start at 1200 bytes, the
smallest maximum RFC 9000 §14.1 guarantees on any path QUIC runs on, and
rise by a search (DPLPMTUD, §14.3, RFC 8899 — probing for the largest
datagram the path lets through): a probe — one PING+PADDING
datagram — of 1452 bytes first (an Ethernet MTU less the IPv6 and UDP
headers), then, while nothing is lost, the ceiling itself; after the
first size lost, each probe halves the range between the last size
confirmed and the smallest lost, down to 20 bytes. A size is lost after
three unanswered probes. A tunnel with an MTU of 1420 settles within
20 bytes of its 1372 bytes of payload, the IPv6 minimum of 1280 at 1212
or more.

The ceiling is `TransportParameters.MaxDatagramSize` (zero:
1452; 8952 for a 9000-byte jumbo path), bounded by the
`MaxUDPPayloadSize` the peer announces; the size reached reads in
`Stats.DatagramSize`. The search starts over at 1200 after each
migration. It needs a socket that refuses to fragment (the "Don't
Fragment" option, which the package sets on a `*net.UDPConn` on Linux,
macOS and Windows): on a platform without the option, datagrams stay at
1200 bytes, since a probe the network may fragment proves nothing.

A path that shrinks under a connection (a "black hole",
RFC 8899 §4.3) shows as three probe timeouts in a row, or three
full-size packets lost while only smaller ones are acknowledged: the size
falls back to 1200, what was lost is resent cut to that size, the search
runs again, and `Stats.MTUBlackHoles` counts the fall back. A total
outage of three probe timeouts counts too, and costs one probe once the
path is back. A completed search is not run again on a path that might
have grown. ICMP "packet too big" messages are ignored (§14.2.1 does not
require them, and `net.PacketConn` does not deliver them).

A path that cannot carry 1200 bytes is therefore never detected: sending on it goes
on at probe-timeout pace, doubling each time, until the idle timeout
ends the connection; §14.2's "cease sending" has no trigger here, and
the backoff is the bound. A lost probe is not a congestion event
(§14.4).

### TLS, in detail

The handshake is `crypto/tls`'s, from the Go release
`go.mod` names as the floor, run through `tls.QUICClient` and
`tls.QUICServer`; what it refuses leaves as CRYPTO_ERROR, as the package
documentation says.

The ClientHello's X25519 key share repeats the key inside the hybrid
X25519MLKEM768 share, the group RFC 10024 defines. RFC 9846 §4.3.8 asks
for key shares generated independently, and RFC 9954 §3.2, which RFC 10024
builds on, relaxes that for the same algorithm reused within one
ClientHello: the repetition is the shape both texts allow, not a departure.

Three of its positions depart from RFC 9846 and are
inherited as they are, none of them visible to a conformant peer: a
`pre_shared_key` extension out of last place
or a version never offered end the handshake with decode_error or
protocol_version rather than illegal_parameter (§4.3.11, §4.3.1); an
extension known but forbidden in the message carrying it is ignored
rather than refused (§4.3, §4.4.1); a `pre_shared_key` without
`psk_key_exchange_modes` continues without the PSK rather than aborting
(§4.3.9).

The TLS 1.3 compatibility mode is neither asked for by this
client nor policed by this server: `crypto/tls` sends no
`legacy_session_id` in QUIC and echoes one it receives without the
change_cipher_spec RFC 9846 Appendix E.4 would want — RFC 9001 §8.4
removes that record from QUIC — and the ClientHello field is not
exposed to the server, so the PROTOCOL_VIOLATION §8.4 asks of a server
waits on `crypto/tls`.

A server issues one ticket per connection:
`crypto/tls` refuses a second `SendSessionTicket` on a QUIC connection. A
client presents a ticket once — RFC 9846 Appendix C.4 asks it not to reuse
one, and the cache `Dial` wraps drops it at its first resumption. Of six
connections opened at once from one ticket, the first resumes and the other
five run a full handshake — one round trip more, the certificate checked
again, and no 0-RTT — until each resumed connection has handed back a ticket
of its own; the same appendix recommends that a server offer as many
tickets as a client might open connections.

A `DialEarly` client that
offers 0-RTT does not survive a HelloRetryRequest — the answer of a server
that wants a key share the ClientHello did not carry: `crypto/tls` computes
the PSK binders of its second ClientHello before it drops `early_data` from
it, and the server closes with decrypt_error. The connection is lost, not
only its 0-RTT; its error wraps `ErrEarlyDataRejected` and names the
HelloRetryRequest, and `Dial`, which offers no 0-RTT, resumes through a
HelloRetryRequest unharmed.

Rewriting the TLS stack is out of the question (ADR 0002:
the standard library owns the sensitive ground); these follow the Go
release, and a peer refused over one of them is the signal to revisit.
In plain words: the lock on the door is the one Go ships, quirks
included, and it is kept rather than copied.
