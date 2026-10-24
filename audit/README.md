# audit — what this machine, this image and this binary were set to

This module reads settings and says what each one costs. It **changes
nothing**, ever: there is no apply, no fix, no write. Every recommendation
carries five things — the value found, the value proposed, the goal it
serves, the reason in plain words, and a link to the documentation that
reason comes from. A rule without that link does not start: a test refuses
it, because a reason nobody can check against a published document is an
opinion with a command-line flag.

It is a module of its own, on the standard library alone. It audits builds
and hosts, so it must not depend on what it audits, and it never widens
the dependency graph the root module records in its decision log.

```
go run -C audit ./cmd/audit -axes
go run -C audit ./cmd/audit -binary ./interop-endpoint
go run -C audit ./cmd/audit -module . -goals security -cve yes -sources tools
go run -C audit ./cmd/audit -dockerfile ../interop/Dockerfile
go run -C audit ./cmd/audit -binary ./x -sbom -output json
```

## Axes, not modes

Every parameter is an axis, and axes are independent and multi-valued: a
run names the components it cares about, the goals it pursues, the profile
it reports under, whether advisories are judged, the sources it may read
and the output it wants. **Any combination is valid**, and an axis left
out means all of its values. "Security only, no advisories, on this
binary" is a selection like any other — not a mode, not a branch in the
program.

A rule declares the axis values it needs and runs only when the selection
covers them. So a selection that does not allow the `tools` source calls
no external tool, and one that asks for `performance` alone reports no
security finding. An axis named with no value, and a value the axes do not
know, are both refused: those are the two ways to ask for nothing by
accident, and a report that found nothing because it ran nothing is the
worst one there is.

Two questions have no answer in anything readable — what the machine does,
and whether it is reachable from the internet. Each has a default, so a
run in CI answers nothing and still gets a report; `-answer role=build`
sets one.

## What it reads today

| family | source | what it reads |
|---|---|---|
| `gobuild/*` | the binary | what the build fixed: `-trimpath`, `CGO_ENABLED`, `GOAMD64`, the profile, the toolchain, the revision stamped |
| `deps/*` | the module, and `govulncheck` | the requirements, their sums, and every advisory with its verdict |
| `docker/*` | the Dockerfile's text | stages, the bases and their digests, the runtime image, `USER`, secrets in layers, `.dockerignore` |
| `-sbom` | the binary | the bill of materials: the main module, the toolchain, every dependency linked |

**The build is read in the binary, not in the files that built it.** A
Dockerfile says what it meant to do; `debug/buildinfo` says what was done.
The gap between the two is exactly what an audit exists to find.

**Advisories are read by calling `govulncheck`, never by reimplementing
it.** The database, the call-graph analysis and the advisory format are the
Go team's and move daily; a second implementation here would be a second
thing to be wrong, and wrong quietly. Each advisory gets one of three
verdicts and no other — **applies** when a call path to the vulnerable
function was proved, **undetermined** when the package is linked but no
path was, **does not apply** when the module is only required — and each
verdict says what decided it. A machine without the tool produces a
finding that says so, because a report silent about vulnerabilities reads
as a report that found none.

## What it does not do

It proposes; a person decides. A finding is not a failure: the interop
image runs as root on a simulator's base image on purpose, and the rules
that say so are right to say it — the answer is the deployer's.
