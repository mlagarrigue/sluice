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

step vet go vet ./...
step format format
step lint golangci-lint run ./...
step test go test -race -shuffle=on ./...
step hygiene scripts/check-hygiene.sh
echo "ok"
