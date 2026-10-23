# ADR 0009 — One request contract for three protocols

- **Created:** 2026-08-22
- **Revised:** 2026-10-15 (what a bounded body costs; `*` for OPTIONS; the authority rule), 2026-10-23 (the response says its payload one way)
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
4. **The body is complete and bounded.** An element is a whole request
   under `MaxBodyBytes`; no partial reads, no framing left to check.
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
