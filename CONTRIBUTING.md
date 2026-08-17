# Contributing to sluice

The project is in early design. The core API is not stable, and large parts of
the framework are still specification rather than code — so the most useful
contributions right now are **arguments about the design**, not just patches.

## Before you write code

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). It is long, and it cites its
sources — including the ones that argue against the choices made here. If you
disagree with a decision, that document is where the reasoning lives, and where
a counter-argument belongs.

Open an issue before a substantial change. A pull request that contradicts a
documented decision without addressing the reasoning behind it is hard to
review, however good the code is.

## The rules that are not negotiable

These are what the project *is*. A change that breaks one of them needs to argue
against the design, not work around it.

1. **Zero production dependencies.** The standard library only. Test and tooling
   dependencies are fine as long as they cannot reach a user's binary.
2. **The batch is the unit of transport.** Not the element. See
   [docs/BENCHMARK-STEP-0.md](docs/BENCHMARK-STEP-0.md) for why.
3. **No unbounded allocation.** Every blocking operator takes its bound as a
   required parameter, so unbounded state is not expressible.
4. **No silent failure.** A wrong answer with no signal is worse than a crash.
5. **The core stays small.** Before adding to it, apply the dependency test: *if
   this type were removed, would the rest stop compiling?* If not, it belongs
   outside the core.

## Performance claims

Performance is a stated goal, so it is measured, never asserted.

`docs/BENCHMARK-STEP-0.md` establishes the ceiling — a native loop at
0.310 ns/element — and every other figure is expressed against it. A core
operator has a budget of **~1.5 ns per stage per element**.

If your change touches a hot path, include a benchmark. If it makes something
slower, say so and say why it is worth it.

## Practical checks

```bash
go vet ./...
go test -race ./...
gofumpt -l .            # must print nothing
go test -run=XXX -bench=. ./internal/bench/
```

## Style

- Everything in the repository is in English: code, comments, tests,
  documentation and commit messages.
- Commit messages: imperative mood, and explain *why*. The diff already shows
  what changed.
- Do not add `Co-Authored-By` trailers.

## What is genuinely useful right now

- **Arguments against a design decision**, with the reasoning or a benchmark.
- **Benchmarks of what is not yet measured** — the open list is at the end of
  `docs/BENCHMARK-STEP-0.md`.
- **Adversarial tests** on the core invariants: prompt finalization, early stop
  propagation, absence of unbounded buffers.

## License

Contributions are accepted under the [Apache 2.0](LICENSE) license.
