#!/bin/sh
# Compares the exported API with the last tag and refuses an incompatible
# change that no commit in the range announced with a `!`.
#
#   scripts/check-api.sh [base-ref]
#
# base-ref is where the commit range starts (default: the last tag). Without
# any tag there is nothing to compare against and the script says so.
# gorelease is pinned for the same reason gofumpt is: its verdicts change
# between releases.
#
# A gorelease run that renders no verdict fails the script: a report that
# ends without the `# summary` header, or a non-zero exit (uncommitted
# changes in the tree, `go run` unable to fetch the tool), is not "no
# incompatible change". Observed on 2026-10-09:
#   clean tree               → report ends with `# summary`, exit 0
#   uncommitted file         → "gorelease: repo ... has uncommitted changes", exit 1
#   GOPROXY=off, bad version → "go: ...: module lookup disabled by GOPROXY=off", exit 1
#   incompatible + `!` commit → exit 0
set -eu
cd "$(dirname "$0")/.."

tag=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || true)
if [ -z "$tag" ]; then
  echo "check-api: no tag yet, nothing to compare against"
  exit 0
fi
base="${1:-$tag}"

status=0
report=$(go run golang.org/x/exp/cmd/gorelease@v0.0.0-20261007192929-f45ad48fbe92 -base="$tag" 2>&1) || status=$?
echo "$report"
if [ "$status" -ne 0 ]; then
  echo "check-api: gorelease exited $status without a verdict" >&2
  exit 1
fi
if ! echo "$report" | grep -q '^# summary'; then
  echo "check-api: gorelease rendered no '# summary' header, cannot conclude" >&2
  exit 1
fi
if ! echo "$report" | grep -q '^## incompatible changes'; then
  exit 0
fi
if git log --format=%s "$base..HEAD" | grep -qE '^[a-z]+(\([a-z]+\))?!: '; then
  echo "check-api: incompatible changes, announced with a '!' in the range"
  exit 0
fi
echo "check-api: incompatible changes since $tag and no commit in $base..HEAD carries a '!'" >&2
exit 1
