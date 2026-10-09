# Contributing to Sluice.go

The project is at v0.1: the API is not stable and will change before v1. The
most useful contributions today are **arguments about the design**,
measurements, and adversarial tests — not only patches.

## Before you write code

Read the [architecture](docs/design/architecture.md) and the
[decisions](docs/design/adr/). They cite their sources, including the ones
that argue against the choices made here. If you disagree with a decision,
that is where the reasoning lives and where a counter-argument belongs: open
a "design argument" issue before a substantial change. A pull request that
contradicts a recorded decision without addressing its reasoning is hard to
review, however good the code.

## The rules that are not negotiable

They define what the project *is*. A change that breaks one must argue
against the design, not work around it.

1. **Every dependency is a recorded decision.** Today the graph holds the
   standard library plus `golang.org/x/crypto`, for the ChaCha20-Poly1305
   cipher that `crypto/tls` negotiates in TLS 1.3 without exporting it. CI
   fails on any module outside the current list; a new one enters through
   an ADR that states its reason and its transitive graph, never through a
   `go get` on the side. The criterion is the least useless code, not a
   count: a maintained compressor beats a home-made one. Tests that compare
   against other libraries (`pgx`, `gin`…) live in a separate module on the
   `benchmarks` branch, so that nothing reaches a user's `go.mod`.
2. **The batch is the unit of transport.** Not the element. A function that
   handles N elements handles 1 without effort; the converse is false.
3. **No unbounded allocation.** Every operator that retains state takes its
   bound as a required parameter, with no default: `Overflow`,
   `join.BuildLimit`, `window.Tumbling`'s `n`, `parallel.Async`'s `depth`,
   `CopyConfig.RowsPerTx`. A policy nobody chose is a policy nobody owns.
4. **No silent failure.** A wrong answer with no signal is worse than a
   crash. NULL is never the empty value. In a `gateway` pipeline a call is
   never dropped: it receives a rejection as its reply.
5. **The core stays small.** Before adding anything to it, apply the
   dependency test: *if this type were removed, would the rest stop
   compiling?* If not, it belongs outside. The root package imports nothing
   of the module beyond `internal/`, and a test checks it.
6. **A batch is valid for the call that receives it.** An operator that
   retains a batch beyond that copies it first: upstream operators reuse
   their output buffers. Test the property with `Convert |> X`, never with
   `Of |> X`: `Of` hands out slices of the caller's own array, which are
   stable, and that is precisely the source that hides the defect.

## Where things go

| What you add | Where |
|---|---|
| an operator that retains nothing between two batches | the root package |
| an operator that retains one side, time, goroutines, keys | `join/`, `window/`, `parallel/`, `distinct/` |
| a connector to an external system | `database/<system>`, `net/<protocol>` |
| shared machinery with no value to a caller | `internal/` |
| a cross-cutting extension (diagnostics, instrumentation) | a root-level package, beside `diagnostics` |

A package is experimental as long as its documentation says so in its first
sentence. Experimental means: no compatibility promise, and not yet
confronted with other implementations. It does not live in a separate
directory.

## Checking

Before every commit, all of this, and none of it may print anything:

```bash
gofumpt -l .
go vet ./...
go test -race -shuffle=on ./...
golangci-lint run ./...
go generate ./...        # generated files are committed; drift fails a test
```

`scripts/check.sh` runs the first four in that order and stops at the first
failure, then `scripts/check-hygiene.sh`, which refuses personal tool
configuration and attributions in tracked files and commits. CI runs the
same scripts, plus `scripts/check-api.sh`, which compares the exported API
with the last tag.

The PostgreSQL integration tests run whenever `SLUICE_PG` is set and skip
otherwise. In the devcontainer the `sluice-postgres` server is there and the
variables are set: `go test ./database/postgres/` runs them. Outside it,
`eval "$(internal/bench/scripts/pgdev.sh start)"` starts a throwaway cluster
with a role that is **not a superuser** — row-level security does not apply
to one, so a suite run as `postgres` passes without proving anything. A
failed start is loud: a suite that skips its tests reads exactly like one
that passes.

Verify commits one by one, not only the tip:

```bash
git worktree add -q --detach /tmp/verify HEAD
for sha in $(git rev-list --reverse main..HEAD); do
  git -C /tmp/verify checkout -q "$sha"
  (cd /tmp/verify && scripts/check.sh) >/dev/null 2>&1 \
    && echo "OK   $sha" || echo "FAIL $sha"
done
git worktree remove --force /tmp/verify
```

After any scripted edit (`sed`, `python`), run the suite again: a
substitution "from this marker to the end of the file" takes everything after
it along.

CI additionally runs `go vet` for Windows and macOS (`net/quic/udp` has
per-OS files a Linux build never compiles), runs the integration tests against
PostgreSQL 17 with the same `init.sql` as the devcontainer, and runs every
fuzz target for 30 seconds nightly (`.github/scripts/fuzz.sh`, which also
runs locally); a failing input is uploaded as an artifact, and committing it
under the package's `testdata/fuzz/` makes it a regression test. Test order
is shuffled: a failure that depends on order prints its seed, and
`-shuffle=<seed>` reproduces it.

## Measuring

Performance is a stated goal, so it is measured, never asserted. The
[measurements](docs/benchmarks.md) give the method, the ceiling — a native
loop at 0.310 ns per element — and the budget of a core operator: **about
1.5 ns per stage per element**.

If your change touches a hot path, include a benchmark. If it makes
something slower, say so and say why it is worth it.

**Never compare two single runs.** On a laptop, and especially under WSL2
where Linux neither sees nor controls the clock, two runs of the *same
binary* differ by up to 14%. What works is pairing: both trees are built as
frozen binaries, alternated round by round on one pinned core; the ratio is
taken *within* each round so drift cancels out; the rounds are judged by a
sign test.

```bash
internal/bench/scripts/compare.sh 'BenchmarkYourThing$' 12 HEAD
```

Three habits go with it:

- **An untouched benchmark as a control.** A verdict on one benchmark is only
  believable next to a flat control: a real run once reported "faster −4.9%"
  on a path nothing had touched, from binary layout alone.
- **Read a small verdict sceptically.** The sign test controls the error
  rate of *one* comparison; a suite of twenty produces a spurious verdict
  about two times in three. The CI gate therefore also requires a 3% effect
  (`MINEFFECT`). Set it to 0 for an investigation, never for a gate.
- **Measure before believing your own mechanism**, including the one that
  was right last time. And check the inliner before assuming a helper is
  free: `go build -gcflags=-m`, look for "can inline". A helper that cannot
  be is a call, and on a per-element loop the call is the cost.

**A benchmark lives in `internal/bench`**: `compare.sh` builds only that
package, so a benchmark anywhere else cannot be paired against a git ref and
therefore cannot be gated. **Adding an operator means adding it to the gated
set of `internal/bench/scripts/gate.sh`**: an operator missing from it is one
whose regressions nobody watches. The script fails when a gated name matches
no benchmark, or produced no line: a comparison that never happened reads
exactly like one that found nothing wrong. An operator that fails its own
gate does not ship.

A pull request is compared to its merge base by `gate.sh`, which fails on any
benchmark the sign test declares slower past 3%. Absolute figures do not
transfer between sessions; only ratios within a session carry.

## Style and conventions

- **Everything in the repository is in English**: code, comments, tests,
  documentation, commit messages. Discussion may happen in any language; the
  repository does not.
- **A constructor panics on a programming error** (nil argument, impossible
  configuration) **and returns an `error` on an environment error** (socket,
  server, file).
- **The godoc of every exported symbol starts with its name**, says what it
  retains and what it costs where that matters, and links to the
  architecture's named anchors, never to section numbers.
- **When a document and the code disagree, the code wins**, and the document
  is corrected in the same commit. A stale document is worse than a missing
  one: it reads as authoritative.
- Formatting: `gofumpt`. Linting: `golangci-lint` (its configuration is in
  the repository). Go **1.26 or newer** is required.
- Windows: use WSL; the scripts are `sh`.

## Commits and review

[Conventional Commits](https://www.conventionalcommits.org):

```
<type>(<scope>): <subject>
```

- **Types**: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`,
  `ci`, `chore`. A `!` after the scope marks an API break.
- **Scopes**, a closed list checked by a test: `core`, `join`, `window`,
  `parallel`, `distinct`, `postgres`, `quic`, `httpstream`, `web`,
  `gateway`, `diagnostics`, `bench`, `docs`, `ci`, `devcontainer`. The scope
  is omitted when the change crosses all of them.
- **Subject**: one line, imperative, no capital after the colon unless the
  word is a name (`README`, `PostgreSQL`), no trailing period, 50 characters
  aimed for, 72 at most. A test checks the whole history. A body only when the why is
  not in the diff. Never a `Co-Authored-By` trailer.
- **One commit per reason to exist**, but never a commit that fails CI:
  deleting a dead function the linter found belongs *with* the lint fix,
  otherwise the commit alone leaves the linter red and breaks bisection.
- **Never on the default branch.** A work branch is named after the work,
  not the tool.

In review, comments follow
[Conventional Comments](https://conventionalcomments.org): `suggestion:`,
`issue:`, `question:`, `nitpick:`, with `(blocking)` where it applies.

## Releases and versions

- Versions follow SemVer, carried by `vX.Y.Z` tags with the `v`. In v0 the
  API may change at every minor; v1.0.0 commits.
- [release-please](https://github.com/googleapis/release-please) reads the
  conventional commits, opens the release pull request and writes the
  CHANGELOG. The CHANGELOG is never edited by hand. In v0 a `feat!` bumps the
  minor, not the major.
- [gorelease](https://pkg.go.dev/golang.org/x/exp/cmd/gorelease) compares
  the exported API to the last tag on every pull request; a detected break
  without a `!` in one of the request's commits blocks.
- A published tag never moves: the module proxy has frozen it. A version
  published by mistake is withdrawn with the `retract` directive in `go.mod`.
- Deprecating: `// Deprecated: use X instead.` on the symbol, kept at least
  one minor before removal.

## What is genuinely useful right now

- **Arguments against a decision**, with the reasoning or a benchmark.
- **Measurements of what is not yet measured**: the open list is at the end
  of the [measurements](docs/benchmarks.md).
- **Running the connector against a real PostgreSQL**, with types and
  versions the suite does not cover: everything else is tested against a
  scripted server, which cannot disagree with the implementation about what
  the protocol means, since the same reading of the specification wrote
  both.
- **Comparing two libraries, not two commits**: `versus.sh` pairs two
  benchmarks inside one binary, which is what a cross-library claim needs.
  Null-test the harness against itself before quoting a verdict from it: a
  harness that cannot say "these are the same" will say anything.
- **Adversarial tests** on the core invariants: immediate release, early-stop
  propagation, absence of unbounded buffers.

## Security

Report a vulnerability privately: see [SECURITY](SECURITY.md).

## License

Contributions are accepted under the [Apache 2.0](LICENSE) license.
