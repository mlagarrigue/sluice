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
- **Authentication**: SCRAM-SHA-256 (with channel binding over TLS),
  cleartext password on explicit request, and `trust`. **`md5` is refused**
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
- **No LISTEN/NOTIFY, no notices.** A `LISTEN` goes through `Exec`, but
  the notifications that arrive afterwards are discarded, for want of an
  API to receive them; the server's notices (`RAISE NOTICE`, warnings) are
  too.
- **No COPY TO STDOUT.** Only loading (`COPY ... FROM STDIN`) has an API;
  a `COPY ... TO STDOUT` fails with an error that says so. To read in bulk,
  `Query` on a `SELECT` already delivers batches.
- **Everything is read in binary, with no per-column text fallback.** A
  type with no binary send function (some extension types) fails the whole
  query; the workaround is in SQL: `column::text`.
- **No replication connection.** `Params["replication"]` is refused before
  anything is sent: a server in replication mode speaks only the simple
  query protocol, and what it sends next (`CopyBoth`) nothing here reads.
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

## Experimental: the exit conditions

These packages leave the label only by meeting these conditions, or move
to a separate module before a v1.

- **`net/quic`**: being green in the *QUIC Interop Runner* against
  quic-go and ngtcp2, on each side of the handshake, in our own continuous
  integration; an external cryptographic review. Two conditions left this list on
  2026-10-10: key update (RFC 9001 §6) — both sides rotate, and a long
  connection no longer dies of its packet count — and the first
  third-party peer: quic-go, on each side of the handshake, in continuous
  integration (the `interop/` module, job `interop-quic`). The runner
  itself was first run against quic-go on 2026-10-10 (`interop/cmd/endpoint`,
  the `Interop runner` workflow). Since 2026-10-11 the endpoint's image,
  `ghcr.io/mlagarrigue/sluice-interop`, is built from every release tag
  (the `Interop image` workflow). Each run of the `Interop runner`
  workflow, not this paragraph, is the record: its log holds the table of
  cases, its artifact the captures and each side's logs. As a server,
  sluice passes handshake,
  transfer, longrtt, multiplexing, retry, http3, blackhole, keyupdate,
  amplificationlimit, transferloss, transfercorruption, ipv6, rebind-port,
  rebind-addr, handshakeloss and handshakecorruption, and fails
  connectionmigration for a reason outside it: quic-go's client never
  migrates to a server's preferred address, so the runner sees a single
  path while the transfer itself completes. As a client it passes handshake,
  transfer, longrtt, multiplexing, retry, blackhole, transferloss,
  transfercorruption, ipv6, handshakeloss, handshakecorruption,
  amplificationlimit, rebind-port and rebind-addr. Since 2026-10-19 the
  server also passes chacha20 against both peers — the client offers
  ChaCha20-Poly1305 alone (`0x1303`) and the server picks it, read in the
  capture — and ecn passes on each side against ngtcp2: every packet
  sluice sends leaves marked ECT(0), and each side reports its counts in
  its ACK frames. quic-go refuses that case itself: its image exits 127 in
  both roles. The two
  handshake cases under 30 % loss or corruption passed on both sides once
  a probe timeout resent the two oldest unacknowledged packets instead of
  copying the first again; amplificationlimit passed on the client side
  once a probe with a single payload to resend sent it twice, so that a
  server allowed three times what it received is unblocked after seven
  doubling intervals rather than sixty-three (RFC 9002 §6.2.4); the two
  rebind cases passed on the client side once `Dial` issued connection
  identifiers of its own — a server may not probe a client's new address
  under an identifier it already uses towards the old one (RFC 9000
  §9.5), so quic-go, holding no spare, kept sending to the address the
  NAT had abandoned until the idle timer fired; the connectionmigration
  transfer completed once the server's long-header gate accepted every
  identifier it had issued, not only its own — quic-go names the
  preferred_address identifier (sequence 1, RFC 9000 §5.1.1) from the
  Handshake packets that carry its Finished, and a server dropping them
  resent its own Handshake until its deadline closed the connection with
  INTERNAL_ERROR (`interop/preferred_test.go`). The second peer, ngtcp2,
  was crossed the same day: every case above passes on both sides, and
  connectionmigration splits. ngtcp2's client does move to a preferred
  address, but only to one of the family its connection already uses
  (RFC 9000 §9.6.1), and it had connected over IPv6 to a server announcing
  IPv4 alone — the endpoint now announces one address per family from a
  dual-stack socket (`ListenerConfig.PreferredIPv4` and `PreferredIPv6`).
  The runner then wants the first server packet on the new path to carry
  a PATH_CHALLENGE, which
  is §9.6.2's own demand ("the server MUST probe on the path toward the
  client from its preferred address"): the server now puts that probe in
  the packet that answers the client's challenge, under an identifier
  never used on the old path (§9.5). The reverse direction stays red for
  a reason outside sluice: the client validates the preferred address
  before moving, as §9.6.2 requires, so the first packet it sends there
  carries nothing but a PATH_CHALLENGE. ngtcp2's server answers such a
  packet with a PATH_RESPONSE alone
  ([`conn_recv_path_challenge`](https://github.com/ngtcp2/ngtcp2/blob/48228939787d4b5bea94c7795b8c7fe0781d918e/lib/ngtcp2_conn.c#L6145)
  only queues the response) and probes in turn only once a non-probing
  packet arrives
  ([`conn_recv_non_probing_pkt_on_new_path`](https://github.com/ngtcp2/ngtcp2/blob/48228939787d4b5bea94c7795b8c7fe0781d918e/lib/ngtcp2_conn.c#L8893),
  called [from this one place](https://github.com/ngtcp2/ngtcp2/blob/48228939787d4b5bea94c7795b8c7fe0781d918e/lib/ngtcp2_conn.c#L9835-L9840));
  the runner, though, wants a PATH_CHALLENGE in the server's first packet
  on the new path
  ([`testcases_quic.py`](https://github.com/quic-interop/quic-interop-runner/blob/ab039a4a4b43ae2e21bb5704c9fad1f8fe6257d6/testcases_quic.py#L1023-L1029)).
  No client that validates first can satisfy it (ngtcp2's own client
  probes the same way, `conn_write_path_challenge`), and the public matrix
  shows it: in the
  [run of 15 October 2026](https://interop.seemann.io/logs/quic/2026-10-15T19:05/result.json),
  ngtcp2's server fails CM against each of the 14 clients that attempt it
  (Chrome does not). ngtcp2's code read at revision `4822893`, the
  runner's at revision `ab039a4`. The cases
  it refuses, as the runner's
  exit status 127 asks, are the features documented as not there — 0-RTT,
  resumption, QUIC v2 — plus chacha20 on the client side (Go's TLS 1.3
  offers no way to restrict the cipher suites it offers), a forced key update and HTTP/3 on the client
  side (no HTTP/3 client in the module). Green means every one of these
  passed against at least two peers.
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
  rise by a search (DPLPMTUD, §14.3, RFC 8899): a probe — one PING+PADDING
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
  require them, and `net.PacketConn` does not deliver them). A lost probe
  is not a congestion event (§14.4).
- **`net/quic/udp`**: follows `quic`.
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
