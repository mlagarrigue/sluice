#!/bin/sh
# What belongs to a contributor's own setup never enters the repository:
# tool configuration, working notes, generated attributions. Checked over
# the tracked files and the commit history, by shape rather than by name,
# so this script does not have to list what it refuses.
#
#   scripts/check-hygiene.sh
set -eu
cd "$(dirname "$0")/.."

status=0

# Attributions and generation banners, in files and in commit messages.
pattern='co-authored-by:|generated (with|by) \[|🤖'
if git ls-files -z -- . ':!scripts/check-hygiene.sh' ':!repo_test.go' \
    | xargs -0 grep -IinE "$pattern" -- 2>/dev/null; then
  echo "check-hygiene: the lines above carry an attribution" >&2
  status=1
fi
# release-please's own commits, "chore(main): release X.Y.Z", name the bot
# that opened the pull request, and GitHub writes dependabot's squash
# messages with a Co-authored-by line per author the branch carried; those
# two are skipped here, as in repo_test.go, and every other commit is read.
if git log --format='%H%x09%an%x09%s' \
    | awk -F'\t' '$2 != "dependabot[bot]" && $3 !~ /^chore\(main\): release / { print $1 }' \
    | git log --no-walk --stdin --format='%h %b' \
    | grep -iE "$pattern"; then
  echo "check-hygiene: the commits above carry an attribution" >&2
  status=1
fi

# Identifiers of the maintainers' private notes (RP-<date>-<topic>) name
# nothing a reader of this repository can open: the commit and the test
# carry the story, the comment states the rule.
if git ls-files -z -- . ':!scripts/check-hygiene.sh' \
    | xargs -0 grep -InE 'RP-20[0-9]{6}-[a-z]' -- 2>/dev/null; then
  echo "check-hygiene: the lines above cite private notes no reader can open" >&2
  status=1
fi

# Hidden directories: only the project's own three are tracked. Anything
# else at the root that starts with a dot is somebody's tool configuration.
for d in $(git ls-files | grep -E '^\.[^/]+/' | cut -d/ -f1 | sort -u); do
  case "$d" in
    .github|.devcontainer|.vscode) ;;
    *) echo "check-hygiene: $d/ is tracked" >&2; status=1 ;;
  esac
done

# Root-level upper-case Markdown files are the project's own documents; an
# instruction file for a tool is spelled the same way and is not one of them.
for f in $(git ls-files | grep -E '^[A-Z][A-Z_.]*\.md$'); do
  case "$f" in
    README.md|CONTRIBUTING.md|SECURITY.md|CODE_OF_CONDUCT.md|CHANGELOG.md|LICENSE.md) ;;
    *) echo "check-hygiene: $f is tracked" >&2; status=1 ;;
  esac
done

# Hidden files at the root: the same rule, with the project's own list.
for f in $(git ls-files | grep -E '^\.[^/]+$'); do
  case "$f" in
    .gitignore|.dockerignore|.golangci.yml|.release-please-manifest.json|.gitattributes|.editorconfig) ;;
    *) echo "check-hygiene: $f is tracked" >&2; status=1 ;;
  esac
done

# unsafe is forbidden in production code except under a named audit (S4 in
# the architecture's rules). The audited files are listed here, and a new
# import of unsafe outside a test fails until it is named in S4 and here.
# gosec does not run under GOOS=windows, where the only exception lives, so
# this is the check that sees it.
audited_unsafe=' net/quic/udp/configure_windows.go '
for f in $(git grep -lE '^import .unsafe.|^[[:space:]]+.unsafe.' -- '*.go' ':!*_test.go'); do
  case "$audited_unsafe" in
    *" $f "*) ;;
    *) echo "check-hygiene: $f imports unsafe outside the audited list (S4)" >&2; status=1 ;;
  esac
done

# Every shell script is tracked executable. Checked on the index, not the
# disk: a checkout on another machine gets the mode git recorded. The path
# benchmarks' gate, committed 100644, failed its CI job with "Permission
# denied" from the day it was written; run on pull requests alone, nobody
# saw it until the job ran on a push.
for f in $(git ls-files -s | awk '$4 ~ /[.]sh$/ && $1 != "100755" { print $4 }'); do
  echo "check-hygiene: $f is tracked without the executable bit" >&2; status=1
done

exit $status
