#!/bin/sh
# Everything CI checks on a pull request, runnable before pushing:
#
#   scripts/check.sh
#
# Stops at the first failure. The tools are the pinned versions CI uses;
# the devcontainer installs them, `go install` does elsewhere (see
# CONTRIBUTING.md).
set -eu
cd "$(dirname "$0")/.."

echo "## vet"
go vet ./...
echo "## format"
unformatted=$(gofumpt -l .)
if [ -n "$unformatted" ]; then
  echo "not gofumpt-formatted:" >&2
  echo "$unformatted" >&2
  exit 1
fi
echo "## lint"
golangci-lint run ./...
echo "## test"
go test -race -shuffle=on ./...
echo "## hygiene"
scripts/check-hygiene.sh
echo "ok"
