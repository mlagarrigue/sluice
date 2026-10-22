#!/bin/sh
# Everything CI checks on a pull request, runnable before pushing:
#
#   scripts/check.sh
#
# Stops at the first failure and prints that step's output only; a passing
# step prints its name. The tools are the pinned versions CI uses; the
# devcontainer installs them, `go install` does elsewhere (see
# CONTRIBUTING.md).
set -eu
cd "$(dirname "$0")/.."

log=$(mktemp)
trap 'rm -f "$log"' EXIT

# step <name> <command...>: run quietly, show the output on failure.
step() {
  name=$1
  shift
  printf '## %s' "$name"
  if "$@" >"$log" 2>&1; then
    echo
  else
    echo " FAILED"
    cat "$log" >&2
    exit 1
  fi
}

format() {
  unformatted=$(gofumpt -l .)
  if [ -n "$unformatted" ]; then
    echo "not gofumpt-formatted:"
    echo "$unformatted"
    return 1
  fi
}

# nested <dir>: vet and lint a module beside the root one, which the root's
# ./... skips. Their tests want PostgreSQL, a quic-go peer or a load campaign
# and stay in CI; compiling, vet and lint are what a push would otherwise
# learn from CI first. The one exception is interop's result guard, which
# reads a recorded pass and needs no peer: an edit of
# interop/expected-results.json is checked here rather than in CI.
nested() {
  (cd "$1" && GOWORK=off go vet ./... && GOWORK=off golangci-lint run ./...) || return 1
  if [ "$1" = interop ]; then
    (cd "$1" && GOWORK=off go test -run '^TestExpectedResultsGuard$' .)
  fi
}

step vet go vet ./...
step format format
step lint golangci-lint run ./...
step test go test -race -shuffle=on ./...
for m in interop benchmarks comparison/sluice comparison/loadgen; do
  step "nested $m" nested "$m"
done
step hygiene scripts/check-hygiene.sh
echo "ok"
