#!/bin/sh
# Compares the exported API with the last tag and refuses an incompatible
# change that no commit since that tag announced with a `!`.
#
#   scripts/check-api.sh
#
# The `!` is looked for in the whole range since the tag, not only in the
# commits of the pull request under review: gorelease reports every
# incompatible change since the tag, and the commit that announced one may
# already be on main — the release pull request, which carries a single
# chore commit, saw its predecessors' breaks and found no `!` of its own
# (2026-10-11). The bump release-please derives reads the same range.
# Without any tag there is nothing to compare against and the script says so.
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
if git log --format=%s "$tag..HEAD" | grep -qE '^[a-z]+(\([a-z]+\))?!: '; then
  echo "check-api: incompatible changes, announced with a '!' since $tag"
  exit 0
fi
echo "check-api: incompatible changes since $tag and no commit in $tag..HEAD carries a '!'" >&2
exit 1
