# Security Policy

## Supported versions

sluice is in early design and has no released version. Only `main` is
supported. Once a `v1` is tagged, this section will state a support window.

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

sluice is a library, not a service, so the threat model is what a caller can be
made to suffer by data it does not control. The specification states thirteen
security guarantees as testable properties
([docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), §11). A reproducible breach of any
of them is a vulnerability, in particular:

- **unbounded allocation** driven by input, where the specification promises a
  bound — memory exhaustion from a stream the caller did not size
- **a panic crossing a public boundary**, which turns a data problem into a
  process crash
- **goroutine leaks**, or an `iter.Pull` whose `stop` is never reached
- **internal detail leaking to a client** through an error path
- **silently wrong results** — a join or filter that drops or duplicates data
  without raising anything

That last one matters as much as the others. A wrong answer nobody notices is
worse than a crash.

## Scope

Out of scope: the devcontainer, CI workflows, and anything under
`internal/bench`. These are development tooling and never reach a user's binary.

## Dependencies

sluice has no production dependencies, by design and enforced in CI. There is no
transitive supply chain to audit — the standard library is the whole of it.
