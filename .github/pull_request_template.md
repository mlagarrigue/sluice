## What and why

<!-- The diff shows what changed. Explain why it should change. -->

## Design decisions touched

<!-- Which sections of docs/ARCHITECTURE.md does this affect, if any?
     A change that contradicts a documented decision needs to argue against the
     reasoning, not work around it. -->

## Checks

- [ ] `go vet ./...` passes
- [ ] `go test -race ./...` passes
- [ ] `gofumpt -l .` prints nothing
- [ ] Comments, tests and commit messages are in English
- [ ] No `Co-Authored-By` trailer

## Performance

<!-- If this touches a hot path, include a benchmark. If it makes something
     slower, say so and say why it is worth it. Core operator budget:
     ~1.5 ns per stage per element (docs/BENCHMARK-STEP-0.md). -->

- [ ] Not a hot path, or benchmark included below
