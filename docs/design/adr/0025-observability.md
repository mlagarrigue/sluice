# ADR 0025 — What the server and the gateway report: a `slog.Logger`, nothing else

- **Created:** 2026-10-23
- **Revised:** —
- **Theme:** httpstream

## Problem

Three failures had no observer. A panic in the handler became a 500, one in
a streamed body's producer a reset stream, with neither value nor stack
anywhere. The error that ended an HTTP/2 or HTTP/3 connection `Serve` or
`Server` had accepted — a peer flooding RST_STREAM, an HPACK block that does
not decode — was discarded (`_ =`). And a panic in a `gateway` pipeline
killed the gateway for good, every caller getting `ErrPipelinePanicked` with
no cause. The operator saw 500s and 503s and nothing of why.

## Decision

1. The one channel is a `*slog.Logger` in the `Config`
   (`httpstream.Config.Logger`, `gateway.Config.Logger`), nil meaning
   `slog.Default()`. No callback (`OnPanic`, `OnConnError`), no counter, no
   dependency.

2. At the error level: a panic in the caller's code — the handler, a
   producer, a `Trailers` function, a gateway's pipeline — with its value
   and the stack taken at the `recover`; and a handler that answers a
   different number of requests than it was given. The value goes to the
   log, never into the response nor into the error handed to callers: the
   log is the operator's, the response and the error go to clients that are
   not the same tenant.

3. At the warn level: the error that ended a connection `Serve` or `Server`
   accepted, except the ordinary ends (the peer's close, a closed socket, a
   deadline, a reset, and the forced stop, reported already), with the
   protocol and the peer's address. `ServeH2Conn` and `ServeH3Conn` return
   that error to their caller and log nothing.

4. Not logged: a response field the server refuses, or a peer's size limit —
   the handler meets the first as a 500 in its own tests, the peer caused
   the second — and any end of an HTTP/1.1 connection, whose errors are
   already responses (400, 408, 431 and the others).

5. A gateway whose pipeline panicked does not restart: the pipeline's state
   is whatever the panic left, and a new pipeline needs the caller's code to
   build it. The service builds a new gateway.

## Rationale

`slog` is in the standard library, and every operator already knows how to
route it to whatever they run; a callback per event is API surface to
freeze for v1 and a calling order to document, for a need the level and the
attributes cover. Splitting the value from the error follows `net/http`,
which logs a handler's panic with its stack and closes the connection
without telling the client anything of it. Sorting connection ends aims at
the line someone can act on: a peer that breaks the protocol is a signal
(abuse, a broken client), a network reset is not. One line per connection
at worst, as `net/http` logs its TLS handshake errors. To be revisited if an
operator asks for counters: they would sit beside the log, not replace it.
