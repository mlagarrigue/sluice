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
  or disabled by `Config.ChannelBinding`),
  cleartext password on explicit request, and `trust`. SCRAM-SHA-1, which
  RFC 5802 §4 requires of every SCRAM client, is not implemented: no
  PostgreSQL server offers it. The SCRAM username field is sent empty, as
  libpq sends it: PostgreSQL takes the role from the startup packet and
  ignores it, where RFC 5802 §5.1 wants a name. A password that SASLprep
  (RFC 4013) would refuse — not UTF-8, a prohibited character, mixed
  bidirectional text — is sent as its raw bytes, the way `pg_saslprep`
  derived the role's key on the server: refusing it here would lock out a
  password the server accepts. **`md5` is refused**
  — on purpose: the hash stored server-side is as good as the password, and
  PostgreSQL has deprecated it. The error message says what to change on
  the server. The PBKDF2 iteration count the server asks for is accepted
  between 4096 (RFC 7677's floor, PostgreSQL's default) and 1,048,576, and
  the salt at 16 bytes or more; a server outside that range is refused with
  the field to set in the message — `Config.MinSCRAMIterations`,
  `MaxSCRAMIterations`, `MinSCRAMSaltLen` — for a role stored at
  `scram_iterations = 1000` (a fast test fixture) or at two million (a
  hardened server). Zero means the default; a negative value is refused
  before anything is sent.
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
- **`net/httpstream` streams request bodies only when asked.** By default a
  body is delivered whole, under `Config.MaxBodyBytes` (1 MiB): an upload
  larger than memory is refused, and a slow body holds its connection's
  buffer until it is complete or `ReadTimeout` passes.
  `Config.MaxStreamedBodyBytes` turns streaming on for the three protocols
  ([ADR 0009](../design/adr/0009-request-contract.md), article 4) — on
  HTTP/3 when the QUIC windows cover a request's head, which those of
  `Config.TransportParameters` do. A body a handler leaves unread costs the
  HTTP/1.1 connection, a reset of its stream (`RST_STREAM` with `NO_ERROR`)
  after the response on HTTP/2, and a `STOP_SENDING` with `H3_NO_ERROR`
  after it on HTTP/3.
- **One token verifier provided**: HMAC (a symmetric signature with a
  shared secret). Public-key tokens (RSA, ECDSA, EdDSA) are not provided —
  not yet; the `Verifier` interface is the extension point.
- **`ClaimAuthorizer` trusts the token.** It builds the permission from
  what the token says about itself; that is correct only if the issuer is
  your service. For an external identity provider, implement `Authorizer`
  against whatever knows the policy.
- **The gateway only amortises round trips.** Without a database or a
  remote service behind it, it costs more than it saves (measured: one call
  every 3.7 µs at best, for nothing). And under WSL2, a `Within` of 500 µs
  is worth 1.11 ms.
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
  the handler in `Request.Trailers`, never merged into `Request.Headers`.
  A trailer is a field that arrived after the body (a checksum, a
  signature); RFC 9110 §6.5 keeps it apart because the request was
  already framed and routed by then, so nothing in the package reads it.
- **Preconditions, ranges and validators are the handler's.** Neither the
  transport nor `web/stream` reads `If-Match`, `If-None-Match`,
  `If-Modified-Since`, `If-Unmodified-Since` or `If-Range` (RFC 9110
  §13.1), nor `Range` and `Content-Range` (§14); `RenderOpen` sets neither
  `ETag` nor `Last-Modified` (§8.8, §15.3.1); and the transport answers no
  417: an `Expect` member other than `100-continue` reaches the handler
  like any other field (§10.1.1, a MAY). The fields arrive untouched in
  `Request.Headers`; evaluating them takes the representation — its tag,
  its date, its length — which only the handler holds. Two concrete
  effects: a conditional GET gets a full 200 instead of a 304, and a PUT
  carrying `Content-Range` that a handler ignores replaces the whole
  resource, where §14.5 says to answer 400. In plain words: the transport
  delivers the letter, reading what it asks is yours; if your resource has
  a state, set `ETag` or `Last-Modified` and answer the `If-*` fields; if
  not, refuse with 400 any PUT that carries `Content-Range`.
- **No CONNECT tunnel.** A CONNECT request is answered 501 by the transport
  on HTTP/1.1, 2 and 3 (RFC 9110 §9.3.6, §15.6.2) and never reaches the
  handler: this is an origin server, not a proxy, and the request has no
  shape a handler could serve — its target is a host and port, not a path.
  On HTTP/2 and HTTP/3 a CONNECT that carries `:scheme` or `:path`, or
  omits `:authority`, is malformed (RFC 9113 §8.5, RFC 9114 §4.4) and its
  stream is reset as such. In plain words: a client that asks this server
  to open a connection to somewhere else is told, with a status, that the
  server does not do that; it is not told its request was wrong.
- **The never-indexed bit is not carried.** An HTTP/2 or HTTP/3 client may
  mark a field as never to be indexed (RFC 7541 §6.2.3, RFC 9204 §4.5.4);
  `Header` keeps the name and the value only, and the bit is lost at
  decoding. The response encoders set it by name alone (`Set-Cookie`,
  `Authorization`, `Proxy-Authorization`) and never index anything: a
  response is safe to send, but a program that relays a field it received
  under another name cannot preserve the bit, which RFC 9204 §7.1.3 asks of
  an intermediary. This server is an origin, not a relay; the field that
  would carry the bit is the thing to add when a relay is built on it. In
  plain words: if you copy the headers you received toward another HTTP/2
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
  (`MaxHeaderBytes`, the per-stream cap, the cancellation allowance). To
  revisit if a measurement shows a disproportionate processor cost per
  frame.

## Experimental: the exit conditions

These packages leave the label only by meeting these conditions; they stay
in the module ([ADR 0021](../design/adr/0021-net-quic-stays.md)). One
condition per package, each one checkable:

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
- **`net/quic`, the measurement**: throughput and latency measured against
  a real third-party client, beside the interop. Interop proves agreement,
  not cost. Checked by: a table in
  [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client)
  measured over a network between two machines, not loopback, with
  `comparison/loadgen` against HTTP/3 and HTTP/2 on the same host, its
  date and machines given (today it is missing: see "Measurements: what
  is missing" below).
- **`net/quic`, the interop**: checked by the weekly `Interop runner`
  workflow green with `interop/expected-results.json` unchanged — every
  case against quic-go and ngtcp2, in each direction, passing or failing
  for the cause written there — and by an external review of the
  handshake and key handling, its report linked from `SECURITY.md`. The
  runner's last complete pass, of 22 October 2026, is green; the review
  has not been done.

- **`net/quic`**: being green in the *QUIC Interop Runner* against
  quic-go and ngtcp2, on each side of the handshake, in our own continuous
  integration; an external cryptographic review. Already true: key update
  (RFC 9001 §6) in both directions, and quic-go against sluice on each side
  in continuous integration (job `interop-quic`).
  The `Interop runner` workflow (the runner at revision `ab039a4`) crosses
  every implementation in its list every Sunday, on each side its role
  allows — 31 peer-directions — with the image the `Interop image` workflow
  builds from each release tag (`ghcr.io/mlagarrigue/sluice-interop`). The
  table of its latest complete pass is on the
  [`interop-results` branch](https://github.com/mlagarrigue/sluice/tree/interop-results);
  a failed case is run once more. `interop/expected-results.json` records
  every case of quic-go, ngtcp2 and msquic with the cause of every failure,
  and a pass that departs from it fails the job. Green means every case
  passed against at least two peers.
  At the last complete pass, of 22 October 2026, against quic-go and
  ngtcp2: as a server, sluice passes handshake, transfer, longrtt,
  multiplexing, retry, http3, blackhole, keyupdate, amplificationlimit,
  transferloss, transfercorruption, ipv6, rebind-port, rebind-addr,
  handshakeloss, handshakecorruption and chacha20, plus ecn and
  connectionmigration against ngtcp2. As a client it passes handshake,
  transfer, longrtt, multiplexing, retry, blackhole, transferloss,
  transfercorruption, ipv6, handshakeloss, handshakecorruption,
  amplificationlimit, rebind-port and rebind-addr, plus ecn against ngtcp2.
  It refuses, with the exit status 127 the runner asks for, what is
  documented as not there: QUIC v2, and on the client side chacha20 (Go's
  TLS 1.3 offers no way to restrict the cipher suites it offers), a forced
  key update and HTTP/3. Session resumption and 0-RTT are served and
  fetched (`ListenerConfig.SessionTickets`, `ListenerConfig.EarlyData`,
  `DialEarly`) but have not run in a pass yet.
  Four failures have their cause outside sluice. quic-go refuses ecn
  itself. quic-go's client never moves to a server's preferred address, so
  connectionmigration sees one path. msquic's server sometimes answers
  first under the client's own connection ID, then changes it, and RFC
  9000 §7.2 has the client discard what follows (four client-side cases).
  And against ngtcp2's server, connectionmigration fails on the client
  side: the runner wants a PATH_CHALLENGE in the server's first packet on
  the new path, and ngtcp2 probes only after a non-probing packet, which a
  client that validates first, as §9.6.2 requires, never sends there.
- **`net/quic` throughput**: a 64 KiB response over HTTP/3 stays below
  the same response over HTTP/2 with every core available, and catches up
  on two; the figures, their date and the machine that measured them are
  in [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client),
  the one place that keeps them current. On Linux and Windows, the
  datagrams one `Stream.Write` produces leave in one system call (UDP
  segmentation offload); on macOS, which offers no public interface for
  it, or on a system that refuses the option, each datagram is still its
  own write. The QUIC windows are not the cause —
  scaled from 1 to 16 times, on either side, they move nothing — so sizing
  them will not buy the gap back. The gap is the Go scheduler: a request
  crosses several goroutines, and each hand-off wakes one, often on another
  core.
- **`net/quic` datagram size**: datagrams start at 1200 bytes, the
  smallest maximum RFC 9000 §14.1 guarantees on any path QUIC runs on, and
  rise by a search (DPLPMTUD, §14.3, RFC 8899 — probing for the largest
  datagram the path lets through): a probe — one PING+PADDING
  datagram — of 1452 bytes first (an Ethernet MTU less the IPv6 and UDP
  headers), then, while nothing is lost, the ceiling itself; after the
  first size lost, each probe halves the range between the last size
  confirmed and the smallest lost, down to 20 bytes. A size is lost after
  three unanswered probes. A tunnel with an MTU of 1420 settles within
  20 bytes of its 1372 bytes of payload, the IPv6 minimum of 1280 at 1212
  or more. The ceiling is `TransportParameters.MaxDatagramSize` (zero:
  1452; 8952 for a 9000-byte jumbo path), bounded by the
  `MaxUDPPayloadSize` the peer announces; the size reached reads in
  `Stats.DatagramSize`. The search starts over at 1200 after each
  migration. A path that shrinks under a connection (a "black hole",
  RFC 8899 §4.3) shows as three probe timeouts in a row, or three
  full-size packets lost while only smaller ones are acknowledged: the size
  falls back to 1200, what was lost is resent cut to that size, the search
  runs again, and `Stats.MTUBlackHoles` counts the fall back. A total
  outage of three probe timeouts counts too, and costs one probe once the
  path is back. A completed search is not run again on a path that might
  have grown. ICMP "packet too big" messages are ignored (§14.2.1 does not
  require them, and `net.PacketConn` does not deliver them). A path that
  cannot carry 1200 bytes is therefore never detected: sending on it goes
  on at probe-timeout pace, doubling each time, until the idle timeout
  ends the connection; §14.2's "cease sending" has no trigger here, and
  the backoff is the bound. A lost probe is not a congestion event
  (§14.4).
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
- **`net/quic` closing period**: after a close, a connection stays in the
  closing or draining state for three probe timeouts (PTO, the delay
  after which a sender resends for want of an acknowledgement), as RFC
  9000 §10.2 asks, and ten seconds at most. The peer chooses most of a
  PTO — its `max_ack_delay`, legal up to 2^14 ms, and a smoothed RTT its
  delayed acknowledgements inflate: uncapped, legal parameters gave 3 min
  25 s, during which a listener's connection keeps its `MaxConns` and
  `MaxConnsPerIP` slots. Ten seconds covers three PTOs of a path with a
  2.5 s round trip. Past it, a late packet is answered with a Stateless
  Reset instead of the close repeated: both tell the peer the connection
  is gone. In plain words: a closed connection does not keep its place
  for more than ten seconds, whatever the peer says.
- **`net/quic` versions**: version 1 only. QUIC version 2 (RFC 9369) and
  compatible version negotiation (RFC 9368) are not implemented: a server
  that does not speak version 1 ends the dial, and a peer's
  `version_information` parameter is skipped like any unknown parameter.
  Version 2 differs from 1 in constants only and exists against
  middleboxes that ossify on version 1; none of this package's peers has
  shown one, and when one does, that is when to add it. In plain words:
  there is one dialect, and it is the one everybody speaks today.
- **`net/quic` TLS**: the handshake is `crypto/tls`'s, from the Go release
  `go.mod` names as the floor, run through `tls.QUICClient` and
  `tls.QUICServer`; what it refuses leaves as CRYPTO_ERROR, as the package
  documentation says. Four of its positions depart from RFC 8446 and are
  inherited as they are: the ClientHello's X25519 key share repeats the
  key inside the hybrid X25519MLKEM768 share (§4.2.8, the one departure a
  conformant peer can see); a `pre_shared_key` extension out of last place
  or a version never offered end the handshake with decode_error or
  protocol_version rather than illegal_parameter (§4.2.11, §4.2.1); an
  extension known but forbidden in the message carrying it is ignored
  rather than refused (§4.2, §4.3.1); a `pre_shared_key` without
  `psk_key_exchange_modes` continues without the PSK rather than aborting
  (§4.2.9). The TLS 1.3 compatibility mode is neither asked for by this
  client nor policed by this server: `crypto/tls` sends no
  `legacy_session_id` in QUIC and echoes one it receives without the
  change_cipher_spec RFC 8446 Appendix D.4 would want — RFC 9001 §8.4
  removes that record from QUIC — and the ClientHello field is not
  exposed to the server, so the PROTOCOL_VIOLATION §8.4 asks of a server
  waits on `crypto/tls`. Rewriting the TLS stack is out of the question (ADR 0002:
  the standard library owns the sensitive ground); these follow the Go
  release, and a peer refused over one of them is the signal to revisit.
  In plain words: the lock on the door is the one Go ships, quirks
  included, and it is kept rather than copied.
- **`net/quic` secrets**: a secret or key is forgotten by becoming
  unreachable, never by being overwritten. RFC 8446 §6 has endpoints
  "forget" the keys of a failed connection and §7 would have each secret
  erased once used; here the sealers and openers of a closed connection
  are released to the garbage collector, a rotated key is replaced, and
  no copy is cleared — Go gives no guarantee that clearing a slice erases
  its bytes (the collector, stacks and registers keep copies), and
  `crypto/tls` and `crypto/cipher` hold their own copies that this
  package cannot reach. What that costs is a window the length of the
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

## What comes next

In the order the roadmap takes them: the connector inventory and
prioritisation, the phase runner for long jobs, the end-to-end bench, the
comparative study, streaming compression, and the exit from experimental
of what meets its conditions. The end-to-end bench and the comparative
study have their apparatus in `comparison/` (the study's server, one flag
per axis, and its load generator); what they lack is the campaign on the
machines the protocol names, and its results. The exit from experimental
has begun: `pushdown` and `net/httpstream` are out of it. The rest
is not started, and each item needs a written decision first.
