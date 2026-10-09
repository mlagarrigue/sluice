# Security Policy

## Supported versions

Sluice.go is in v0: the latest tagged minor is supported, and a fix ships
as a patch of it. Once a v1 is tagged, this section will state a support
window.

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
fourteen security guarantees as testable properties (reach it from
[docs/README.md](docs/README.md), "Security — verifiable properties"). A
reproducible breach of any of them is a vulnerability, in particular:

- **unbounded allocation** driven by input, where the specification promises a
  bound — memory exhaustion from a stream the caller did not size
- **a panic crossing a public boundary**, which turns a data problem into a
  process crash — excepting the documented sentinel panics (`ErrUnsorted`,
  `ErrOverflow` under `Fail`, and the `Split` sentinels), which are the stated
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
is one, `golang.org/x/crypto`, maintained by the Go team and used for the
ChaCha20-Poly1305 packet protection the QUIC transport needs (`crypto/tls`
negotiates the suite but does not export the cipher). It brings
`golang.org/x/sys/cpu` with it for CPU feature detection. Both are pinned in
`go.sum`, scanned by `govulncheck` in CI and tracked by Dependabot. CI fails
if a module outside the recorded list appears in the graph, so the supply
chain to audit is that list and the standard library.
