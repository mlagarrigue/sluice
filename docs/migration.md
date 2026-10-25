# Migrating from v0.2.2

In v0, a minor version may break what the previous one offered; `main`
groups the breaking changes for the next one. Each entry below names the
commit that made the change, says what changed, and what to write instead.
The packages come in the order a reader meets them; within a package, the
changes of behavior that keep the signatures — a value now refused — come
last.

Since v0.2.2, a negative size, count, bound or timeout is refused across
the library: the core and the operators panic with a message that names
the argument, and a configuration returns an error that names the field.
Zero keeps its documented meaning — usually "the default" — so a program
that never passed a negative value has nothing to change there.

## The core: `sluice`

- **`FlatMap` appends into the output batch** (`632eeb9`). The function is
  handed the slice to fill: `func(a A) []B` becomes `func(dst []B, a A) []B`
  — append to `dst` and return it:
  `func(dst []Line, o Order) []Line { return append(dst, o.Lines...) }`.
- **`Overflow` and `Completion` have no zero value** (`2a9a2ab`).
  `DropOldest` and `WhenAll` are no longer zero, and a zero `Overflow` or
  `Completion` is invalid. Name the policy wherever a field or a variable
  was left at zero.
- **A `Split` route outside the branches panics** (`d4a8182`). An index
  outside `[0, n)` was ignored, silently dropping the batch; it now panics,
  naming the index.
- **A negative size or count panics** (`372329f`): `Of`, `Take`,
  `Coalesce`, `window.CoalesceWithin` and the other operators that take one.

## Joins: `join`

- **`Interval` takes an `IntervalSpec`** (`948ecc3`). The positional key,
  time and merge functions move into the fields of `join.IntervalSpec`
  (`KeyL`, `KeyR`, `TimeL`, `TimeR`, `Lower`, `Upper`, `Merge`).
- **The storage seam is gone** (`948ecc3`): `BuildTable` and
  `StreamTableWith` no longer exist; use `StreamTable`, whose table is in
  memory.

## Fan-out: `parallel`

- **`WithState` is `OrderedWithState`** (`336817d`), and `ErrState` names
  it in its message.
- **A negative depth or worker count panics** (`cd7dce7`); zero or one
  means no concurrency, as before.
- **A runtime error raised on a worker or a producer comes back wrapped**
  (`94858d0`): `Ordered`, `OrderedWithState`, `Unordered`, `Async`, and
  `window.CoalesceWithin`, raise it again in an error that carries the
  stack it was raised on. `errors.As` with a `runtime.Error` target still
  finds it; a direct assertion `r.(runtime.Error)` on the recovered value
  no longer does. Any other value comes back as raised.

## Time windows: `window`

- **`Tumbling` takes a late-element policy** (`feat(window)!: a late element
  meets a policy, not a panic`): `Tumbling(s, ts, size, maxPerWindow,
  policy, late)`. Pass `window.LateFail` to keep what the operator did
  before — a panic with `sluice.ErrUnsorted` — or `window.LateDrop` to step
  over an element whose window had already closed. There is no zero value,
  like `sluice.Overflow`: say which.

  The ordering requirement is also narrower than it was. Only an element
  older than the **open window's start** is late now; reordering inside the
  open window is accepted and changes nothing, since a `Pane` holds its
  elements in arrival order and never claimed they were sorted. A pipeline
  that relied on `ErrUnsorted` firing on any decrease — two elements of the
  same window swapped — no longer sees it. Nothing else changes: the panes,
  their bounds and their contents are what they were for an input that was
  already non-decreasing.

## Deduplication: `distinct`

- **A negative window panics** (`3cf7f66`).

## Demand: `pushdown`

- **`Demand` carries no column set** (`60c9544`): the `Columns` field and
  `NeedColumns` are gone; nothing read them.

## The web path: `web` and `web/stream`

- **`web.Shutdown` drains any server** (`a42f4d6`). It takes a
  `web.Server` — anything with `Shutdown(context.Context) error`, which
  `*http.Server` already has, so existing calls compile — and drains it
  before the closers. `web.Servers` drains several at once; `web.Start`
  runs a `Serve(ctx)` and returns its handle.
- **The Bearer challenge names its realm** (`46efad5`):
  `web.BearerChallenge(realm, d)` and, in `web/stream`,
  `Authenticate(exchanges, v, realm)`; `web.ValidRealm` checks a realm.
- **`web/stream` stages take the batch first** (`d30311a`):
  `Authenticate(exchanges, v)` and `Authorize(exchanges, a)` instead of the
  verifier or authorizer first; `DecodeJSON` takes an `int64` limit; the
  `Exchange` field `Route` is `RouteIndex`.
- **A zero shutdown grace is five seconds** (`66a4787`,
  `web.DefaultShutdownGrace`), not "no grace".
- **A negative `MaxTokenBytes` is refused** (`dae2743`).
- **`HMACVerifier` refuses what RFC 7519 and RFC 8725 refuse**
  (`fix(web)!: verify tokens as RFC 7519 and 8725 ask`). A token that
  carries an `aud` claim is refused when `HMACConfig.Audience` is empty:
  set `Audience` to the value your issuer puts in `aud`. A token is refused
  at the second its `exp` names, not one second later; a header or payload
  that is not UTF-8 is refused; header names are compared exactly, so
  `"ALG"` is no longer read as `alg`. A `typ` of `application/jwt` is now
  accepted, like `JWT`.

## PostgreSQL: `database/postgres`

- **`Connect` replaces `Startup`** (`0ddc63b`).
  `postgres.Startup(rw, postgres.StartupConfig{…})` becomes
  `postgres.Connect(ctx, postgres.DialTLS(addr, nil), postgres.Config{…})`
  (or `DialTCP` on a loopback): `Connect` takes a recipe that opens a
  connection, not an open one, because cancelling a query opens a second.
  `StartupConfig` is `Config`, and its `CancelDial` is gone — the recipe
  serves cancellation.
- **Parameters are `[]postgres.Param`** (`aa67ba7`). `Query`, `Exec` and
  their kin take `[]postgres.Param{{OID: …, Value: …}}` instead of
  `[][]byte` beside `QueryConfig.ParamOIDs`; the statement cache is sized
  by `Config.StatementCacheSize`.
- **`QueryConfig.AllRows` is `QueryConfig.Fetch`** (`dd787ed`):
  `AllRows: true` becomes `Fetch: postgres.FetchAll`; the default,
  `FetchAuto`, reads a known small result in one round trip;
  `FetchBatches` always asks for `BatchRows` rows at a time.
- **The codecs live in `pgtype`** (`1608e0c`):
  `database/postgres/pgtype` holds `Numeric`, `Null`, `ErrCodec` and the
  encoders and decoders; `postgres.ScanNumeric` returns `[]pgtype.Numeric`.
- **Five internals are unexported** (`30e0381`, ADR 0022):
  `MaxServerParameters`, `MaxServerParameterBytes`, `ReadBufferSize`,
  `ParseError` and `ParseRowDescription`. Nothing outside the package
  needed them; an error arrives as a `*postgres.Error` from the calls that
  run statements.
- **Channel binding is chosen, not just required** (`f831ae9`):
  `Config.RequireChannelBinding: true` becomes
  `Config.ChannelBinding: postgres.ChannelBindingRequire`;
  `ChannelBindingPrefer` is the default, `ChannelBindingDisable` turns it
  off.
- **The protocol version is named** (`27236c3`, `cf4e082`). `Config.MaxProtocol`
  takes `postgres.Protocol30` or `Protocol32` (version 3.2, whose
  cancellation keys are longer); a server that speaks none of them is
  refused with `ErrProtocolVersion`. `CancelKey.Secret` is a `string`, no
  longer a `uint32`.
- **A COPY chunk is `CopyConfig.ChunkBytes`** (`32d8386`), formerly
  `MaxMessage`; `NewRows` takes the byte bound and `Rows.SetMaxBytes` is
  gone. A batch that reaches its byte bound now ends there and the next
  one carries on, where it used to fail.
- **The remembered server parameters share one budget** (`e920182`).
- **An out-of-range batch bound or a negative statement cache bound is
  refused** (`a3b3f23`, `655ba3b`).

## HTTP: `net/httpstream`

- **`Handler` takes a context** (`4f1e365`):
  `func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response]`.
  The context carries the server's values and is cancelled only by the
  forced stop. `Server` serves with a normal stop (`Shutdown`) and a forced
  one (`Close`); `ErrShutdownForced` says a grace passed with requests in
  flight; `Config.Logger` receives the warnings.
- **`H2Config` is merged into `Config`** (`e8847f8`): its fields
  (`MaxFrameSize`, `MaxFramesPerBatch`, `MaxContinuationFrames`,
  `MaxHeaderBlockBytes`…) are fields of `Config`.
- **`ServeH2` and `ServeH3` are `ServeH2Conn` and `ServeH3Conn`**
  (`6131d90`): they serve one connection the caller accepted.
- **`WriteBatch` takes the `Config`** (`9a24bf1`):
  `WriteBatch(dst, cfg, requests, responses)`, so a kept connection
  announces its keep-alive.
- **An out-of-range `Config` value is refused** (`f8478ea`): `Serve`,
  `Server`, `ServeH2Conn` and `ServeH3Conn` return an error naming the
  field, and `Config.TransportParameters` returns
  `(quic.TransportParameters, error)`.
- **`ServeH2Conn` drains on cancellation** (`735873e`) instead of cutting
  the connection: see [ADR 0017](design/adr/0017-graceful-shutdown.md).
- **Request bodies stream by default**
  (`feat(httpstream)!: stream request bodies by default`): the zero `Config` now
  hands a handler a body through `Request.Stream` as it arrives, with
  `Request.Body` nil, unless the body was already whole in the read that
  brought the request's headers. A handler written against the old
  contract reads `Request.Body` and finds nothing; `Request.ReadAll(dst,
  limit)` returns either form whole and is the one-line change. A handler
  that never reads the body no longer makes the server assemble it: the
  body is stopped after the response, and a client waiting for `100
  Continue` is never invited.

  To keep the old contract, set `Config.BodyDelivery: httpstream.BodyWhole`.
  `Config.MaxStreamedBodyBytes` is now the bound of a streamed body and
  nothing else — it no longer turns streaming on — and its zero is
  `MaxBodyBytes`, so what a server accepts is unchanged either way. A
  configuration that set it to a positive value keeps that value and that
  meaning; one that left it at zero to mean "whole" says `BodyWhole`
  instead. See [ADR 0009](design/adr/0009-request-contract.md), article 4.

## QUIC: `net/quic`

- **One `Dial`, with a context** (`bc762a9`): `Dial(ctx, pc, addr, cfg,
  params)` replaces both `Dial` without a context and `DialContext`.
  `Stream.WriteClose` is `WriteFinal`; `Conn.SetManualCredit` and
  `Conn.SetClosingPeriod` are gone — a stream's credit is returned through
  `OnStreamFrames`.
- **`Accept` takes the first `Datagram`** (`16d8523`):
  `Accept(pc, quic.Datagram{Peer: …, Payload: …, ECN: …}, cfg, params)`
  instead of the peer and the bytes apart.
- **`OpenStream` and `AcceptStream` take a context** (`01548d2`,
  `2738054`); `OpenStream` waits for stream credit instead of failing.
- **`Conn.Stream(id)` returns an error** (`0f440d6`), `ErrStreamLimit` for
  an identifier past the limit.
- **Why a connection ended is an error value** (`b913611`, `7613a95`):
  `Conn.PeerClosed` is gone; `Err` returns `ErrIdleTimeout`,
  `ErrStatelessReset`, `ErrVersionNegotiation` or a `*quic.ConnectionError`
  (`Code`, `Application`, `Reason`). `SetKeepAlive` returns an error and
  `KeepAlive` reads the period back.
- **The listener's preferred address is one per family** (`6b43d2c`):
  `ListenerConfig.PreferredIPs []net.IP` becomes `PreferredIPv4` and
  `PreferredIPv6` (`netip.Addr`); the listener's tuning (`ConnQueueLen`,
  `RetryTokenTTL`, `MaxSpentRetryTokens`) and its keys
  (`StatelessResetKey`, `TokenKey`) are fields.
- **`MaxConnsPerIP` counts an address by its prefix** (`a4178a7`): /32 for
  IPv4 and /64 for IPv6 unless `IPv4PrefixBits` or `IPv6PrefixBits` say
  otherwise; clients that shared a /64 now share a count.
- **BBR version 1 is retired** (`f209b83`): `CongestionBBRv1` is gone;
  choose `CongestionBBR`, `CongestionCubic` or the default.
- **The varints are internal** (`3df8222`): `quic.Varint`,
  `quic.AppendVarint` and `quic.MaxVarint` are gone.
- **A negative listener bound is refused** (`72c4625`).
