# ADR 0009 — One request contract for three protocols

- **Created:** 2026-08-22
- **Revised:** 2026-10-15 (what a bounded body costs)
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
   refused, not resolved.
2. **Target raw and origin-form.** Begins with `/`, query attached, nothing
   percent-decoded. Other forms are refused by the parser.
3. **Header names keep the case the protocol sent.** The handler matches
   case-insensitively (`Request.Get` does) rather than the package rewriting
   what the client sent.
4. **The body is complete and bounded.** An element is a whole request
   under `MaxBodyBytes`; no partial reads, no framing left to check.
5. **The cookie arrives whole.** HTTP/2 and HTTP/3 let a client split its
   `Cookie` into one field per pair (RFC 9113 §8.2.3, RFC 9114 §4.2.1);
   the handler sees them joined again by `"; "` as one field, as HTTP/1.1
   delivers it, and `MaxHeaders` counts that one field, not the crumbs.

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
