# Security Policy

## Supported versions

**Today, in v0:** the latest tagged minor is supported, and a fix ships as a
patch of it. A v0 minor may break the API; the
[migration guide](docs/migration.md) says what to write instead.

**From v1.0.0:**

- the latest minor receives every fix, as a patch;
- the previous minor receives security fixes for three months after its
  successor ships, as patches too; after that, the answer to a security
  problem is to move to the latest minor, which broke nothing
  ([ADR 0020](docs/design/adr/0020-compatibility.md));
- the Go versions covered are the ones CI builds: the floor in `go.mod` and
  the latest stable release. A fix does not raise the floor; raising it is
  a commit of its own, announced in the CHANGELOG;
- the experimental packages (`net/quic` and `net/quic/udp` today) are fixed
  in the latest minor only, and a fix there may change their API, announced
  with a `!` like any break of those packages.

A version published by mistake, or made dangerous by a defect, is withdrawn
with the `retract` directive in `go.mod`.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report it privately through
[GitHub Security Advisories](https://github.com/mlagarrigue/sluice/security/advisories/new).
That channel is private until an advisory is published, so a fix can be prepared
before the problem is known.

Please include what the issue is, how to reproduce it, and what an attacker
could achieve with it. A failing test or a short program is worth more than a
description.

You should get an acknowledgement within a week. If a report is accepted, you
will be credited in the advisory unless you ask otherwise.

## What counts as a vulnerability here

Sluice.go is a library, not a service, so the threat model is what a caller
can be made to suffer by data it does not control. The architecture states
fourteen security guarantees as testable properties, each naming the test
that checks it
([Security — verifiable properties](docs/design/architecture.md#security--verifiable-properties)). A
reproducible breach of any of them is a vulnerability, in particular:

- **unbounded allocation** driven by input, where the specification promises a
  bound — memory exhaustion from a stream the caller did not size
- **a panic crossing a public boundary**, which turns a data problem into a
  process crash — excepting the documented sentinel panics (`ErrUnsorted`,
  `ErrOverflow` under `Fail`, the two `Split` sentinels and
  `parallel.ErrState`), which are the stated
  contract and are what `Try` exists to catch at the boundary
- **goroutine leaks**, or an `iter.Pull` whose `stop` is never reached
- **internal detail leaking to a client** through an error path
- **silently wrong results** — a join or filter that drops or duplicates data
  without raising anything

That last one matters as much as the others. A wrong answer nobody notices is
worse than a crash.

## Scope

Out of scope: the devcontainer, CI workflows, and anything under
`internal/bench` or `internal/dgram` (an in-memory packet pair imported only by
tests). These are development tooling and never reach a user's binary.

## Dependencies

Every production dependency is a recorded decision (ADR 0002). Today there
are two, both maintained by the Go team. `golang.org/x/crypto` is used for
the ChaCha20-Poly1305 packet protection the QUIC transport needs
(`crypto/tls` negotiates the suite but does not export the cipher); it
brings `golang.org/x/sys/cpu` with it for CPU feature detection.
`golang.org/x/text/unicode/norm` normalises a PostgreSQL password (SASLprep,
RFC 4013), imported by `database/postgres` alone. All are pinned in
`go.sum`, scanned by `govulncheck` in CI and tracked by Dependabot. CI fails
if a module outside the recorded list appears in the graph, so the supply
chain to audit is that list and the standard library.
