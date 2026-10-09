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
# that opened the pull request; that one shape is skipped here, as in
# repo_test.go, and every other commit is read.
if git log --format='%h %b' --invert-grep --grep='^chore(main): release ' \
    | grep -iE "$pattern"; then
  echo "check-hygiene: the commits above carry an attribution" >&2
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
    .gitignore|.golangci.yml|.release-please-manifest.json|.gitattributes|.editorconfig) ;;
    *) echo "check-hygiene: $f is tracked" >&2; status=1 ;;
  esac
done

exit $status
