# interop — sluice against peers that share none of its code

This directory is a **second Go module**. It depends on the framework;
nothing in the framework depends on it. That is what lets a project with one
production dependency (`golang.org/x/crypto`) be confronted with `quic-go`
without acquiring it: a nested module is excluded from the root's `./...`,
so `quic-go` never enters the graph the CI `deps` job checks.

```
cd interop
go test ./...
```

What runs here, and in the CI job `interop-quic` on every push:

| Test | Peer | What it proves |
|---|---|---|
| `TestSluiceClientAgainstQuicGoServer` | quic-go listens, `quic.Dial` connects | handshake, a 256 KiB echo across flow-control windows, `Close` seen by the peer |
| `TestQuicGoClientAgainstSluiceServer` | `quic.NewListener` listens, quic-go dials | the mirror, with the peer's application close surfaced by `PeerClosed` |
| `TestQuicGoHTTP3ClientAgainstServeH3` | `httpstream.ServeH3` serves, `quic-go/http3` requests | a plain response, a streamed one, several on one connection, two streamed at once |

The first of these found a bug on its first day: one echo in twenty came
back a frame short, because `quic.Stream.Read` reported EOF between the
read loop marking the stream finished and the last delta reaching the read
buffer (`net/quic/readend_internal_test.go`). Nothing in the root module's
own tests had the timing to see it.

Two benchmarks, not run in CI, measure the same servers from the same
clients — `net/http` pinned to HTTP/2 by ALPN, `quic-go/http3` — on a
plain response, the three-line streamed one and a 64 KiB streamed one.
They are the measurement `docs/guide/limits.md` names as the exit
condition of `httpstream`'s experimental label; `docs/benchmarks.md`
publishes their figures.

```
cd interop
go test -run xxx -bench 'H2|H3' -benchtime 3s -count 3
```

The root module's `TestNestedModulesReplaceThisOne` keeps this module pointing
at the working tree by a `replace` directive rather than at a published tag,
so what is tested is the code being changed.

## What is pinned

| | Version |
|---|---|
| `github.com/quic-go/quic-go` | v0.63.0 |

Dependabot bumps it like the root's dependencies; a bump that breaks the
handshake is a finding about one side or the other, not noise.

## The public QUIC Interop Runner

`cmd/endpoint` is sluice under the contract of the public [QUIC Interop
Runner](https://github.com/quic-interop/quic-interop-runner), which crosses
many implementations two by two inside a simulated network: `ROLE`,
`TESTCASE` and `REQUESTS` in the environment, `/www` served on port 443,
`/downloads` written, exit status 127 for a case it does not implement.
`Dockerfile` and `run_endpoint.sh` make the image the runner starts, from
the repository root; `scripts/interop-runner.sh` builds it, clones the
runner and crosses sluice with one peer in both directions, printing one
row per case. It needs Docker, so it runs in the `Interop runner` workflow
(by hand) rather than in the devcontainer. The cases passed and refused are
recorded in `docs/guide/limits.md`.
