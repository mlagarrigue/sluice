# ADR 0017 — Shutdown drains, it does not cut

- **Created:** 2026-10-15
- **Revised:** 2026-10-15 (HTTP/2 and HTTP/3 drain; `web.Shutdown` for every `Serve(ctx)`)
- **Theme:** httpstream

## Problem

Cancelling the context of `Serve(ctx)` closed the listener and every
connection in the same gesture. A response being written was cut without
`Connection: close`; an HTTP/2 connection got no GOAWAY; an HTTP/3
connection was closed `H3_NO_ERROR` without waiting for its requests;
`web.Shutdown` only knew how to stop a `*http.Server`. And nothing wrote
down in which order a process that serves streams stops: if the gateway
(which batches calls toward the back end) closes before the handlers, the
handlers in flight see their calls refused; if the connection pools close
before the gateway, its last batches fail.

## Decision

**A shutdown is a bounded drain, front to back.**

1. **Order.** A layer stops after the layer that calls it: the server
   first (nothing new admitted, the responses in flight finish), then the
   gateway (its pending batches go out, nothing new accepted), then the
   connections to the back end (PostgreSQL pools, clients). In that order
   no call is refused at a layer whose caller is still running.

2. **Grace.** `Config.ShutdownGrace` (zero means 5 s) bounds, for one
   `Serve`, the time between the cancellation and the return. It covers
   the handler and the response's writes together; `WriteTimeout` bounds
   one write. When it passes, the connections still open are closed under
   their handler and `Serve` returns without waiting for the goroutines
   still inside one: a handler that has not returned after the grace is
   the handler's bug, and waiting for it would turn the bug into a hang.

3. **What closes at once.** The listener, and every connection with no
   request in flight: between two keep-alive requests, or accepted and
   silent. Nothing is owed to it; waiting for it means returning one
   `IdleTimeout` late.

4. **What finishes.** A connection on which a request has begun to arrive,
   is being handled or is being answered. The response goes out whole; no
   new request is read after it.

5. **What each protocol tells the client.**
   - HTTP/1.1: the last response carries `Connection: close` (RFC 9112
     §9.6: the server "SHOULD send a "close" connection option in its
     final response" and "MUST NOT process any further requests"). A
     shutdown that begins once the header is out can no longer be
     announced: the client finds the close at its next request, the case
     §9.6 describes for a connection the server closed.
   - HTTP/2: GOAWAY `NO_ERROR` with the last stream identifier at 2^31−1
     and a PING, then, at the PING's acknowledgement — the round trip
     RFC 9113 §6.8 asks to allow for the streams already on the wire — a
     second GOAWAY with the last stream actually taken; a stream opened
     past it is refused `REFUSED_STREAM`, the open streams finish, the
     connection closes. A peer that never acknowledges the PING is closed
     by the grace. A connection with no stream open closes at once, like
     an idle HTTP/1.1 one.
   - HTTP/3: a GOAWAY frame on the control stream naming the first request
     stream the connection rejects — the one after the highest it admitted,
     decided under the same lock as the admission so no stream is both
     admitted and named (RFC 9114 §5.2: "requests with the indicated
     identifier or greater are rejected") — a request from there on reset
     `H3_REQUEST_REJECTED`, the admitted requests answered, then
     `H3_NO_ERROR` (or §8.1's grease) once the last answer is
     acknowledged, which `quic.Conn.ShutdownWithError` waits for: an
     application close sent at once can overtake the response it follows.
     Past the grace the connection is closed under its handler. What this
     slice leaves: `ServeH3` still waits for a handler that has not
     returned after the grace, where `Serve` does not (§2).
   - `web.Shutdown(ctx, srv web.Server, closers...)` stops every
     `Serve(ctx)` in this order, not only a `*http.Server`: `web.Server` is
     `Shutdown(context.Context) error`, which `*http.Server` already has;
     `web.Start(ctx, serve)` runs a `Serve(ctx)` on its own goroutine and
     returns the handle whose `Shutdown` cancels that context and waits for
     the return; `web.Servers(...)` drains several at once, the front being
     one layer whatever the number of listeners. The handle lives in `web`,
     not in `httpstream`, so that `web` does not import the transport and so
     that a server keeps one signal.

## Rationale

The handler's contract says a request handed over is whole; the symmetric
promise is that a response produced goes out whole. Cutting the socket
during a write loses the client a response the server already computed
and, for a POST, leaves it unable to tell whether to replay. That is why
the drain is the rule and the cut the bounded exception.

The bound is a duration per `Serve` rather than a separate `Shutdown(ctx)`:
a `Serve(ctx)` has one stop signal, its context, and a second entry point
would make the caller coordinate the two. `web.Shutdown` composes that
signal across several servers; it does not add one.

Closing idle connections at once rather than leaving them to the grace is
what `net/http` does (`Shutdown` closes `StateIdle`), and it is what
returns at the cancellation rather than at the next wake of a silent
client. The unavoidable race — a request byte arriving while the close
runs — is the one every HTTP/1.1 server has and that clients handle by
replaying on a fresh connection.

What this decision does not do: the staged close against the TCP reset
(RFC 9112 §9.6, last paragraphs: half-close, then read until the client
closes). A client that pipelined a request behind the one the shutdown
closed on may see a RST erase the last response. It is a backlog entry,
to be measured against a real client before it is built. Revision
expected if a conformant client loses responses at shutdown, or when the
HTTP/2 and HTTP/3 slices show the grace has to be per protocol.
