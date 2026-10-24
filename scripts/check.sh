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
# ./... skips, and run the tests that need nothing of their own. Only
# benchmarks/ stays in CI, its tests being a campaign; the others need
# nothing — the comparison server's skip without SLUICE_PG, the generator's
# calibrate against the clock, and interop/ links quic-go rather than
# reaching for a peer, so its nineteen tests run here in under a second with
# nothing skipped. A check that does not run them is a gate that goes green
# while CI is red: that is how main stayed red for four commits on
# 2026-10-24, and how a change of the root module broke interop/ undetected
# the same evening (the request body streaming by default, b2b76e2).
nested() {
  (cd "$1" && GOWORK=off go vet ./... && GOWORK=off golangci-lint run ./...) || return 1
  case $1 in
    interop) (cd "$1" && GOWORK=off go test ./...) ;;
    # SLUICE_PG emptied on purpose: the runner has none, so its integration
    # tests skip there and what this reproduces is the runner's own result.
    # On a machine that has the variable they want the study's tables loaded
    # (go run ./internal/bench/cmd/pgfixtures), which is a campaign's
    # preparation and not a gate's.
    comparison/sluice) (cd "$1" && SLUICE_PG= GOWORK=off go test -race ./...) ;;
    # The two tests skipped are the calibration the study demands of its own
    # machines; the list is the one in .github/workflows/ci.yml, and
    # TestCheckRunsTheNestedTestsCIRuns holds the two equal. Each name is
    # anchored on its own rather than grouped in parentheses: a parser that
    # splits a command on parentheses then reads the end anchor as the start
    # of a command whose name comes from a variable, which is a shape a shell
    # wrapper may well refuse to run.
    comparison/loadgen) (cd "$1" && GOWORK=off go test -skip '^TestCalibration$|^TestTrivialServesEveryScenario$' ./...) ;;
    # The audit tool reads build records and hosts: its tests build their own
    # binaries in a temporary directory and want nothing else.
    audit) (cd "$1" && GOWORK=off go test ./...) ;;
  esac
}

step vet go vet ./...
step format format
step lint golangci-lint run ./...
step test go test -race -shuffle=on ./...
for m in interop benchmarks comparison/sluice comparison/loadgen audit; do
  step "nested $m" nested "$m"
done
step hygiene scripts/check-hygiene.sh
echo "ok"
