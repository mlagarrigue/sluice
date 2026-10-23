# ADR 0009 — One request contract for three protocols

- **Created:** 2026-08-22
- **Revised:** 2026-10-15 (what a bounded body costs; `*` for OPTIONS; the authority rule), 2026-10-23 (the response says its payload one way; the body streamed when asked for)
- **Theme:** httpstream

## Problem

HTTP/1.1 sends a `Host` field with header case preserved; HTTP/2 and
HTTP/3 send an `:authority` pseudo-header with lowercase enforced on the
wire. Early on the three protocols disagreed inside this package too, and a
handler had to know which one it was under — exactly what taking a batch of
protocol-neutral requests was meant to remove.

## Decision

The contract is stated in full on `Handler`, and each protocol's assembly
enforces it:

1. **Authority present, under one name.** Delivered as `host` on all three;
   a request carrying both `Host` and `:authority` in disagreement is
   refused, not resolved. The value is an authority by one grammar on all
   three (RFC 9110 §7.2: a host, an optional port, no userinfo, never
   empty); `Host: u@h` is refused on HTTP/1.1 as `:authority: u@h` is on
   HTTP/2 and HTTP/3.
2. **Target raw and origin-form.** Begins with `/`, query attached, nothing
   percent-decoded. The one exception is `*` on an `OPTIONS` request (RFC
   9110 §7.1), accepted on all three; other forms are refused by the parser.
3. **Header names keep the case the protocol sent.** The handler matches
   case-insensitively (`Request.Get` does) rather than the package rewriting
   what the client sent.
4. **The body is complete and bounded, or streamed when asked for.** An
   element is a whole request under `MaxBodyBytes`; no partial reads, no
   framing left to check. Streaming is the next step, decided and landing
   protocol by protocol (HTTP/1.1, then HTTP/2, then HTTP/3); until a
   protocol has it, the sentence before holds for it:
   - **Additive, through a bound.** `Config.MaxStreamedBodyBytes`: zero
     keeps the whole-body contract; positive turns streaming on and bounds
     a streamed body. Past it the request is refused — 413 before any body
     is read when `Content-Length` says so, and no 100 Continue; an
     `ErrTooLarge` from the stream midway for a body of unannounced length.
   - **What stays batched.** A body already whole in the read that brought
     its headers, under `MaxBodyBytes`, arrives in `Body` as today, so
     small requests still batch. Any other body arrives through
     `Request.Stream`, a `sluice.Source[[]byte]`, with `Body` nil;
     `Request.ReadAll(dst, limit)` returns either form whole.
   - **Consumed within the call.** The stream is valid while the handler
     runs. A body left unread when it returns closes the connection after
     the response on HTTP/1.1, and resets the stream with `NO_ERROR` after
     the response on HTTP/2 and HTTP/3 (RFC 9113 §8.1, RFC 9114 §4.1).
   - **Batches and backpressure.** On HTTP/1.1 a streamed request ends its
     batch, and nothing past what the handler consumed is read; chunked
     coding is undone as it arrives. On HTTP/2 and HTTP/3 a batch may hold
     several streamed bodies; a stream's window is returned as its body is
     consumed, the connection's as data arrives, under the existing
     aggregate bound.
   - **Progress, not duration.** Each read of a streamed body that waits
     for bytes has `ReadTimeout`; there is no total deadline.
   - **The default switches** (a break) once the three protocols stream, a
     real consumer has used it, and a paired A/B of small requests shows
     noise.
5. **The cookie arrives whole.** HTTP/2 and HTTP/3 let a client split its
   `Cookie` into one field per pair (RFC 9113 §8.2.3, RFC 9114 §4.2.1);
   the handler sees them joined again by `"; "` as one field, as HTTP/1.1
   delivers it, and `MaxHeaders` counts that one field, not the crumbs.
6. **The response says its payload one way.** `Body` for a payload in
   hand, `Stream` for one produced as it goes, never both; `ContentLength`
   describes a `Stream` and nothing else. The forbidden combinations are
   not made inexpressible: they are refused with `ErrMalformed` at the one
   point the three protocols' writers go through, on every write and
   whatever the data.

## Rationale

Normalising everything to one canonical form (lowercase names, decoded
target) was rejected because each normalisation rewrites the client's
bytes, and every rewrite is a place where two hops can disagree — the
request-smuggling family. The contract promises what a handler needs and
otherwise leaves the bytes alone. `web/stream`'s stages are written against
this contract alone, which is what lets one handler serve all three
protocols; anything the contract does not promise is explicitly not
portable, and a handler relying on it anyway has a documented bug rather
than a surprise.

Article 4 has a cost, and it is accepted rather than streamed away: a
connection's read buffer grows toward one request at its ceiling —
request line, headers and body bounds together, about 1 MiB at the
defaults — and holds it for `ReadTimeout` at most before an unfinished
request is refused. The server's bound is that sum times `MaxConns`; a
server without `MaxConns` has none. Streaming the body instead would hand
the handler a request whose framing is still open, which is exactly what
the contract exists to remove, and every `web/stream` stage would have to
learn a partial element. It is revisited if a workload needs request
bodies that do not fit in memory (uploads); until then the bound is a
configuration, not a design.

That workload is the library's own philosophy — process on arrival, bounded
memory, backpressure — so article 4 now carries its streaming form. Keeping
`Body` for a body whole in the first read keeps the batch, which is the unit
of transport; a `Source` on the request rather than a second `Handler` type
keeps one contract, and a handler that ignores the stream keeps working
while the bound is zero. Returning a stream's window on consumption is what
makes backpressure a property of the protocol rather than of the server's
memory; returning the connection's on arrival is what keeps one unread body
from stalling the others. What is lost: with streaming on, a handler meets
two forms (`ReadAll` joins them), and an unread body costs an HTTP/1.1
connection. That last choice is revisited if a consumer finds the closed
connection too dear, by draining up to a bound instead.

Article 6 keeps exported fields over a type the compiler checks. Every
forbidden combination — `Body` beside `Stream`, a `ContentLength` with no
`Stream`, beside a `Body` or negative, content on a 205 — fails on every
write regardless of the data, so a handler's first test sees it. Payload
fields hidden behind constructors would also need accessors for every piece
of code that reads a response (tests, a compressing middleware, a
renderer): twice the surface for a check that already exists. Constructors
added beside the fields would be a second spelling of the same thing. It is
the shape `net/http.Request` has (`Body`, `ContentLength`, `GetBody`),
which a Go reader already knows. It is revisited if a forbidden combination
passes a handler's tests and is first seen in production.
