# ADR 0010 — Routing is a table matched on the raw target

- **Created:** 2026-08-22
- **Revised:** 2026-10-04 (a fully covered route is refused), 2026-10-07 (renumbered), 2026-10-09 (a refusal panics), 2026-10-28 (dot segments and a leading `//` are not normalised)
- **Theme:** web

## Problem

The experimental path needed a routing operator. `net/http.ServeMux` cannot
be it — its API needs an `*http.Request` — and hand-switching on target
bytes is not an arrow of the architecture's table. How much router to
build, on a path whose requests arrive with an undecoded origin-form
target?

## Decision

**A table, compiled once.** `NewRouter` takes the whole route list and
returns something immutable; first match wins in table order, which is the
caller's statement of priority, readable in one place. Exact segments plus
single-segment wildcards (`{name}` matches exactly one non-empty segment);
no multi-segment wildcard, no suffix match. **Nothing is percent-decoded**:
literal segments match byte for byte (`/%68ealth` does not match
`/health`) and wildcard values are handed over as sent; the query is split
off before matching and never matched on.

**Nor are dot segments removed**: `.` and `..` are segments like any other, matched byte for byte
by a literal and handed over as sent by a wildcard, and a target that
begins with `//` is an absolute path whose first segment is empty. RFC 3986
§6.2.2.3 advises removing dot segments before comparing; this router does
not, for the reason it does not decode: two hops that normalise
differently route differently, and the path the handler sees must be the
one the client sent. What follows is the handler's: a wildcard value that
becomes a file name or an upstream target must refuse `.` and `..`, and a
target copied into a relative reference must guard against the leading
`//`, which would read there as an authority.

A method mismatch on an existing path is 405 with `Allow`; an unmatched path
is 404; both are refusals recorded on the exchange. `NewRouter` refuses a
row an earlier row fully covers (`/orders/{id}` before `/orders/new`),
naming both patterns, instead of compiling a route that silently never
answers. Every refusal is a panic rather than a returned error: the table is
written in the program, so a bad row is a programming error, and an error a
caller may ignore (`rt, _ :=`) is a route that quietly never answers — the
constructor rule in `CONTRIBUTING.md` and `ServeMux`'s own behaviour on a
bad pattern.

## Rationale

Each richer pattern reintroduces the ambiguity that needs an arbiter —
ServeMux's specificity rules — which a small table does not have. Decoding
before routing is how two layers route one request differently; `%2F`, a
slash hidden in a segment, is the classic. Routing costs one pass over the
table with no allocation until a wildcard matches, and the router's whole
behaviour is legible from the route list.

Reversal condition: a real API outgrowing single-segment wildcards. The
arbiter question then reopens, and it should arrive with a use case in
hand.
