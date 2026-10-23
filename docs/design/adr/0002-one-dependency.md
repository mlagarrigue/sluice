# ADR 0002 — The dependency graph is a decision, never a default

- **Created:** 2026-10-07
- **Revised:** 2026-10-10 (the interop module, nested on `main`), 2026-10-16 (`golang.org/x/text`, for SASLprep in `database/postgres`), 2026-10-19 (the comparison module, nested on `main` and run on every push; the study's modules `comparison/sluice` and `comparison/loadgen`, and what watches the nested modules for advisories, and what `scripts/check.sh` and the `lint` job check in them)
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

**Tests and tools that confront the library with others live in separate
modules**, which depend on this one and never the reverse. A test-only
import in the root module would still put the library in `go.mod` and in
every consumer's module graph; a nested module (a `go.mod` in a
subdirectory) is excluded from the root's `./...`, so nothing it requires
reaches a user's binary or `go.mod`, and CI's `deps` job keeps it that way.
Four such modules exist, all on `main`:

- `benchmarks/`, where `pgx`, `gin` or `echo` are compared — and
  `TestPGContendersAgree` asserts that the connector reads the same rows as
  `pgx`;
- `interop/`, where `net/quic` is confronted with `quic-go` on every push;
- `comparison/sluice` and `comparison/loadgen`, the server and the load
  generator of the comparison study.

`benchmarks/` and `comparison/sluice` replace the root with the working
tree. The root's `go test ./...` does not see them; `scripts/check.sh` runs
`go vet` and golangci-lint (the root's linters, with its configuration) in
each of them, without their tests, which want PostgreSQL, a `quic-go` peer
or a load campaign.

The comparison module lives on `main` for the reason interop does: on a
branch of its own it falls behind, and a measurement of code that is no
longer the code proves nothing about it. The measurements are still taken on
purpose, not on every push; what runs on every push is `go vet` and one pass
of every benchmark in the `benchmarks-module` job, `go vet` and `go mod tidy
-diff` of the study's two modules in the same job, golangci-lint in all four
in the `lint` job, and `TestPGContendersAgree` against PostgreSQL 17 and 18
in the `postgres` job, so an API change that breaks a module, a benchmark
that hangs, or a connector that reads other rows than `pgx` goes red the day
it lands.

**The nested modules never ship, but they are watched.** They run on the
project's machines and pin `quic-go`, `pgx`, `gin` and the `x/` modules
they bring. `govulncheck` (the Go team's tool, which reports a
vulnerability only when the code reaches it) runs in each of them daily and
on every push, as it does for the root. Dependabot opens a security update
in any of them; its version updates target only the root and `interop/`: in
`benchmarks/` and `comparison/`, a version bump would move what the study
measures, and the compared versions change only by decision.

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
