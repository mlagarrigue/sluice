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

The root module's `TestNestedModulesReplaceThisOne` keeps this module pointing
at the working tree by a `replace` directive rather than at a published tag,
so what is tested is the code being changed.

## What is pinned

| | Version |
|---|---|
| `github.com/quic-go/quic-go` | v0.63.0 |

Dependabot bumps it like the root's dependencies; a bump that breaks the
handshake is a finding about one side or the other, not noise.

The public *QUIC Interop Runner* — many implementations, crossed two by
two — is the next step and is not here yet (`docs/guide/limits.md`).
