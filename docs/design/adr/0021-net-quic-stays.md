# ADR 0021 — `net/quic` stays in the module

- **Created:** 2026-10-23
- **Revised:** —
- **Theme:** quic

## Problem

`net/quic` is experimental, and the limits guide promised it would leave the
label through its exit conditions "or move to a separate module before a
v1". Three facts weighed for the move: `golang.org/x/crypto` is in the
module's graph for it alone; `net/httpstream`, stable, exposes its types
(`ServeH3Conn` takes a `*quic.Conn`, `Server.ServeQUIC` a `*quic.Listener`,
`Config.TransportParameters` returns `quic.TransportParameters`), so the
promise of `httpstream` rested on a type with none; and `Varint`,
`AppendVarint`, `MaxVarint` were public for the sole use of `httpstream`'s
HTTP/3.

## Decision

**One module.** `net/quic` and `net/quic/udp` stay in the root module,
experimental, outside the v1 promise as ADR 0020 defines it. They join it
through their exit conditions, not by moving out.

**The leak is closed symbol by symbol.** The three HTTP/3 entry points of
`httpstream` carry an "Experimental:" paragraph in their documentation and
are listed in `scripts/api-experimental.txt`; `scripts/check-api.sh` treats
them as the experimental package whose types they take — a break announced
with `!` passes, even after v1 — and a test
(`TestExperimentalSymbolsMatchTheirList`) holds the list and the
documentation to each other, both ways. The rest of `httpstream` — HTTP/1.1,
HTTP/2, `Request`, `Response`, `Config` — keeps the promise.

**The varints leave the surface.** They move to `internal/varint`, shared by
`net/quic` and `httpstream`'s HTTP/3. `Preface` (`httpstream`) stays: a
caller that demultiplexes clear-text HTTP/2 itself needs it.
`Config.QPACKStatic` stays, by ADR 0013.

## Rationale

A separate module would have taken `x/crypto` out of the root module's
graph, at the price of a second set of tags, a two-module release
configuration, `replace` directives in development, and moving
`httpstream`'s HTTP/3 — which shares its handler, writers and bounds with
HTTP/1.1 and HTTP/2 — into that module. ADR 0020 already gives what the
separate module was for: `net/quic` can change after v1 without touching
the promise. The cost of running it decides. `x/crypto` stays a decided
dependency (ADR 0002), used by one package.

What this costs: `httpstream` imports `net/quic` for its HTTP/3, so a
program that serves HTTP/1.1 alone still compiles `net/quic` and
`x/crypto`; and three symbols of a stable package are outside the promise,
which their documentation says.

Revisit before v1 only (after, a module path no longer changes): a user who
needs a root module without `x/crypto`, or a `net/quic` that has to version
at another pace than the rest.
