## What and why

<!-- The diff shows what changed. Explain why it should change. -->

## Decisions touched

<!-- Which architecture section or decision record does this affect, if any?
     Both are reached from docs/README.md. A change that contradicts a
     documented decision argues against its reasoning, in the record, rather
     than working around it. -->

## Checks

- [ ] `go vet ./...` passes
- [ ] `go test -race ./...` passes
- [ ] `gofumpt -l .` and `golangci-lint run ./...` print nothing
- [ ] Commits follow `<type>(<scope>): <subject>`, one line, in English
- [ ] An API break carries `!` and says so in the description

## Performance

<!-- If this touches a hot path, include a paired benchmark (the scripts in
     internal/bench/scripts; the method is under Measurements in
     docs/README.md). If it makes something slower, say so and say why it is
     worth it. -->

- [ ] Not a hot path, or the paired comparison is below
