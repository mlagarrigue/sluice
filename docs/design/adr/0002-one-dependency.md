# ADR 0002 — The dependency graph is a decision, never a default

- **Created:** 2026-10-07 (the rule predates the record; this writes it down)
- **Revised:** 2026-10-10 (the interop module, nested on `main`), 2026-10-16 (`golang.org/x/text`, for SASLprep in `database/postgres`)
- **Theme:** ci

## Problem

A library that promises to be understood end to end cannot carry a
dependency graph its users did not choose: every module in the graph is
code to maintain, an attack surface, and a version to follow. Yet the
standard library has gaps a protocol can mandate. Two today: the
ChaCha20-Poly1305 cipher that `crypto/tls` negotiates in TLS 1.3 and does
not export, which the QUIC transport needs; and Unicode NFKC
normalisation, which SASLprep (RFC 4013) applies to the password before
PostgreSQL's SCRAM exchange. Without it, a non-ASCII password the server
normalised does not authenticate.

## Decision

**Every production dependency is a recorded decision, and the graph is
kept as small as the job allows.** Today it holds the standard library
and two modules:

- `golang.org/x/crypto` (and the `golang.org/x/sys` it brings), for
  ChaCha20-Poly1305;
- `golang.org/x/text`, of which one package, `unicode/norm`, is imported
  by one package, `database/postgres`, for SASLprep's NFKC.

CI enforces the current list two ways on every push: `go.mod` may require
nothing outside it, and no non-test package may import anything outside
it; it also checks that `x/text` has no importer but `database/postgres`.
A new entry changes the list and this record, with
its reason, its transitive graph and what it replaces; it never enters by
a `go get` on the side.

The criterion is the least useless code, not a count. Writing a cipher, a
compressor or a Unicode table ourselves is not "zero dependencies", it is
a worse dependency: zstd and brotli, for instance, are expected to enter
through a maintained module when the measurement shows they pay, rather
than be reimplemented or excluded. The line falls where the data
changes: SASLprep's tables (RFC 3454) are frozen by the RFC at Unicode 3.2
and generated from its text in this repository; NFKC follows every Unicode
version and comes from the module.

**Tests that compare against other libraries live in a separate module**,
which depends on this one and not the reverse. A test-only import in the
root module would still put the library in `go.mod` and in every consumer's
module graph; a nested module is excluded from the root's `./...`, so
nothing it requires reaches a user's binary or `go.mod`, and CI's `deps`
job keeps it that way. Two such modules exist: `benchmarks/`, on the
`benchmarks` branch, where `pgx`, `gin` or `echo` are compared — and
`TestPGContendersAgree` asserts that the connector reads the same rows as
`pgx` — and `interop/`, on `main`, where `net/quic` is confronted with
`quic-go` on every push (since 2026-10-10). The interop module is on `main`
because a conformance test that does not run on the code being changed
proves nothing about it; the comparison module is on its own branch because
a measurement is taken on purpose, not on every push.

## Rationale

`x/crypto` and `x/text` are maintained by the Go team, pinned by `go.sum`,
watched by Dependabot and `govulncheck`. Neither the cipher nor the
normalisation is something this project writes: writing ciphers is how
CVEs are made, and a decomposition table copied by hand is wrong at the
next Unicode version. `x/text` is a large module, but only `unicode/norm`
and the `transform` package it builds on reach a binary, and only through
`database/postgres`: a program that does not import that package compiles
none of it. The same reasoning will
decide the next entry: who maintains it, what it pulls in, and whether the
alternative is code of our own that nobody wants to own.

Reversal condition for the list as it stands: a measured need, or a
standard library that exports what a module supplies. For the
rule itself: none — a dependency that enters without a record is the thing
the rule exists to prevent.
