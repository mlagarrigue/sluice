# Something is wrong: errors, causes, remedies

This page starts from what you see — an error, a status code, a panic — and
says when it happens, why, and what to do. Every error the library exports
is here, one table per package, in the order a newcomer meets them.

Three habits first:

- **Match with `errors.Is`, never on the message.** The messages explain;
  the sentinels (`ErrOverflow`, `ErrConnBroken`…) are what stays stable.
- **A panic from the core is an error in disguise.** An `iter.Seq` has no
  error to return, so the core's bounded operators panic with a sentinel;
  `sluice.Try` turns it back into an error, and re-raises any other panic.
- **A broken connection is closed, not retried.** When an error says the
  protocol position is lost (PostgreSQL, HTTP), the only safe move is to
  close that connection and open another.

> **In plain terms: a sentinel.** A sentinel is an exported error value,
> always the same one (`ErrOverflow`), that code compares against rather
> than reading its message. `errors.Is(err, sluice.ErrOverflow)` finds it
> even wrapped inside another error that adds context; comparing the text
> breaks the day the message changes a word.

To find which stage of a pipeline makes the others wait, rather than which
one failed, see the `probe` package.

## The core: `sluice`, `parallel`

| Error | When | What to do |
|---|---|---|
| `sluice.ErrOverflow` | A bounded operator reached its bound under the `Fail` policy. Panicked; `Try` returns it. | Raise the bound, or choose a policy that drops (`DropOldest`, `DropNewest`) knowingly. |
| `sluice.ErrSplitStalled` | A `Split` branch cannot advance because a sibling holds a batch it has not drained. A wiring mistake. | Read the branches together (one goroutine each, or in lock-step); see the operators guide, "Split". |
| `sluice.ErrSplitDrained` | A `Split` branch was read after a sibling had drained the source, which is read once. | Attach every branch before reading any. |
| `sluice.ErrUnsorted` | An operator that needs ordered input saw its key go backwards (`join.Merge`, the time windows). | Sort the input, or use an operator that does not need order (`join.StreamTable`). |
| `parallel.ErrState` | The state factory of `OrderedWithState` failed. Panicked, like `ErrOverflow`. | Fix what the factory needs (a connection, a file); `Try` gives you its error. |

## PostgreSQL: `database/postgres`, `pgtype`

| Error | When | What to do |
|---|---|---|
| `postgres.ErrAuth` | The authentication exchange was refused by this client (a server signature that does not verify, a nonce or an iteration count outside what a client should accept). Distinct from a wrong password, which the server reports as an `*Error`. | Check the server's identity and TLS; the message names the step. |
| `postgres.ErrAuthUnsupported` | The server asked for a method this package does not implement (md5, for one). The error names it. | Use SCRAM-SHA-256 for the role, or cleartext over TLS with `Config.AllowCleartextPassword`. |
| `postgres.ErrProtocolVersion` | The server settled on a protocol version below `Config.MinProtocol`. | Lower `MinProtocol`, or upgrade the server. |
| `postgres.ErrProtocol` | A backend message could not be parsed. The connection is finished. | Close and reconnect; if it repeats, report it with the server version. |
| `postgres.ErrMessageTooLarge` | A message exceeded `Config.MaxMessage`. The connection is finished. | Raise `Config.MaxMessage` if the value is legitimate (a large `bytea`, a long text). |
| `postgres.ErrMalformedMessage` | A message length field cannot be right. The connection is finished. | Close and reconnect; check what sits between you and the server. |
| `postgres.ErrConnBroken` | The connection's position in the protocol is no longer known: a malformed message, a failed write, a read stopped mid-message — or the server closed it after saying why ("connection closed by the server"; `errors.As` finds its `*postgres.Error`: 57P01 terminated, 57P05 idle session, 25P03 idle in transaction). | Close it and open another; nothing more can be sent on it. If the server closed it on a timeout, raise that timeout or keep the session busy. |
| `postgres.ErrConnBusy` | A query started while another is still being consumed on the same connection — typically from inside a consumer's own callback. | Finish (or stop) the first result before starting the next, or use a second connection. |
| `postgres.ErrRowRetained` | A `Row` was read after its query: the batch it came in was valid only during the call. Panicked. | Copy what you keep (`slices.Clone`, a string conversion) inside the callback. |
| `postgres.ErrTooManyRows` | A result was larger than the batch bound you set. | Raise the bound, or read in batches instead of all at once. |
| `postgres.ErrStalePlan` | A prepared statement was refused because the schema changed under it (SQLSTATE 0A000). | Run the statement again: the cache drops the stale plan. Expect it after a migration. |
| `postgres.ErrPipelineAborted` | The parts of a pipeline the server never ran, after an earlier part failed. | Look at the first error of the pipeline; the aborted parts did nothing. |
| `postgres.ErrTxOpen` | `Begin` on a connection that already has a transaction. | Commit or roll back the first; use savepoints for nesting. |
| `postgres.ErrTxDone` | An operation on a transaction already committed or rolled back. | Keep the work inside the transaction's lifetime. |
| `postgres.ErrTxCommitRollback` | `Commit` on a transaction in an aborted state: the server rolled back, and nothing was persisted. | Find the statement that failed earlier in the transaction; the commit did not happen. |
| `postgres.ErrCopyRefused` | The server answered a `COPY` with something other than "go ahead" — almost always a missing table or a column mismatch, wrapped here. | Read the wrapped server error; check the table and the column list. |
| `postgres.ErrNullValue` | A `Scan*` met a NULL in a column scanned as not-null. Wraps `pgtype.ErrCodec`. | Scan into `pgtype.Null[T]`, or `COALESCE` in the query. |
| `pgtype.ErrCodec` | A value's bytes do not match the type they were read as. The stream and its description disagree. | Check the column type against the decoder you used (`DecodeInt8` on an `int4` column, for one). |

## Web and gateway: `web`, `gateway`

| Error | When | What to do |
|---|---|---|
| `web.ErrCredentialsMissing` | The request carries no bearer token a verifier could use: no `Authorization` field, two of them, another scheme, or a malformed value. Answered 401. | Send `Authorization: Bearer <token>`, once. |
| `web.ErrTokenMalformed` | The token is not a well-formed JWT. Answered 401. | Obtain a new token. |
| `web.ErrTokenSignature` | The signature does not verify — deliberately indistinguishable from a wrong key. Answered 401. | Check the issuer's key and the verifier's; obtain a new token. |
| `web.ErrTokenAlgorithm` | The token's algorithm is not the one the verifier was built for (`none` included). Answered 401. | Issue tokens with the verifier's algorithm. |
| `web.ErrTokenExpired` | The token is past its `exp`, or not yet valid by its `nbf`. Answered 401 with a challenge. | Renew the token and ask again; check the clocks. |
| `web.ErrTokenClaims` | Required claims are missing or wrong: audience, issuer, expiry. Answered 401. | Issue tokens with the claims the verifier requires. |
| `web.ErrForbidden` | The principal is known and has no grant for what it asked. Answered 403. | Grant it, or ask for something it is allowed; retrying with the same credentials changes nothing. |
| `gateway.ErrClosed` | A call was submitted to a gateway that is closed or closing. | Answer as a service shutting down answers (503), and stop submitting. |
| `gateway.ErrNoReply` | The pipeline consumed a call without answering — it was filtered out, or stopped mid-batch. A wiring mistake. | Never filter a call out of a gateway's pipeline; answer every one, refusals included. |
| `gateway.ErrPipelinePanicked` | The pipeline panicked. Every caller of the batch gets this same error, without the panic value (callers may be different tenants). The value and the stack are in the log of `gateway.Config.Logger` (`slog.Default()` by default); the gateway does not restart, build a new one. | Find the panic in your logs; fix the stage that raised it. |

## HTTP: `net/httpstream`

| Error | When | What to do |
|---|---|---|
| `httpstream.ErrMalformed` | A request this package will not parse. The server answers with a status and closes. | Fix the client; a request whose framing is in doubt cannot be resynchronised. |
| `httpstream.ErrTooLarge` | A request past one of `Config`'s bounds. The server answers it (413, or 414 for the request line, 431 for headers) before any handler runs. | Raise the matching `Config` field if the size is legitimate; for large uploads, see `Config.MaxStreamedBodyBytes`. |
| `httpstream.ErrHeaderInjection` | A response header carries CR, LF or another control byte; it is not written. | Remove the control bytes from the value your handler sets. |
| `httpstream.ErrHPACK` | An HTTP/2 header block this package will not decode. The connection ends. | Fix the client's HTTP/2 encoder; one rule set per sentinel: HPACK here. |
| `httpstream.ErrH2Protocol` | An HTTP/2 frame sequence this package will not accept. The connection ends. | Fix the client's HTTP/2 framing. |
| `httpstream.ErrH3Protocol` | An HTTP/3 frame or request this package will not accept. | Fix the client's HTTP/3 framing. |
| `httpstream.ErrShutdownForced` | `Config.ShutdownGrace` passed before the requests in flight were answered; returned by `Serve`, `ServeH2Conn`, `ServeH3Conn`. | Raise `ShutdownGrace`, or make handlers honour their context. |

## QUIC: `net/quic` (experimental)

| Error | When | What to do |
|---|---|---|
| `quic.ErrQUIC` | Marks every error about QUIC itself: bytes it will not parse, a connection or stream used in a state that does not allow it. | `errors.Is(err, quic.ErrQUIC)` tells them from a context's or the socket's own errors. |
| `quic.ErrTruncated` | A buffer ends inside what it was describing. | In a datagram, the packet is malformed; on a stream, wait for more bytes. |
| `quic.ErrIdleTimeout` | Neither end sent anything for the negotiated idle period. The peer is presumed gone. | Reconnect; keep the connection alive with traffic if it must stay idle longer. |
| `quic.ErrStatelessReset` | The peer answered with a stateless reset: it no longer holds this connection, as after a restart. | Reconnect. |
| `quic.ErrVersionNegotiation` | The server's version list does not include version 1, the only one this package speaks. | Use a server that speaks QUIC version 1. |
| `quic.ErrEarlyDataRejected` | The server refused 0-RTT; nothing written on the early stream reached it. | Send again on a stream opened after the handshake. |
| `quic.ErrStreamLimit` | A stream identifier past the limit of its kind (the peer's `MAX_STREAMS`, or this end's). | Wait for streams to close, or raise the limit this end grants. |
| `quic.ErrMigrationDisabled` | `Conn.Rebind` while the server announced `disable_active_migration`. | Keep the address the handshake used, or reconnect from the new one. |
| `quic.ErrNoFreshConnectionID` | `Conn.Rebind` with no connection identifier left that was never used from another address. | Wait for the server to issue more, or reconnect. |
| `quic.ErrZeroParameters` | A zero `TransportParameters`: no limit of any kind announced. | Start from `quic.DefaultParameters()` and change what you need. |
