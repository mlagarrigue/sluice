#!/bin/sh
# Refuses an incompatible change to the exported API that the commit making
# it does not announce with a `!` in its subject — and, once a v1 tag exists,
# any incompatible change to a stable package (ADR 0020; see v1_era below).
#
#   scripts/check-api.sh                 every commit not pushed yet (@{upstream}..HEAD)
#   scripts/check-api.sh BASE [HEAD]     every commit in BASE..HEAD (HEAD by default)
#   scripts/check-api.sh --index SUBJECT the staged tree against HEAD, for a
#                                        commit about to be made with SUBJECT
#
# Each commit is compared with its parent, not the range with its base: a
# `!` anywhere in the range used to excuse a break anywhere else in it, and
# the release pull request, a single chore commit, failed on the breaks of
# the commits before it (2026-10-11). Now each commit answers for itself,
# and a commit that changes no Go file outside the tests and not go.mod is
# not compared at all. Merge commits are skipped: the commits they bring
# are in the range on their own. CI passes the pull request's base..head or
# the push's before..after; the commit hook passes --index.
#
# The comparison is apidiff's, the engine gorelease uses, run on the export
# data of each tree: gorelease only compares against a published version,
# never against an arbitrary commit. Internal packages are left out, as
# gorelease leaves them. apidiff is pinned for the same reason gofumpt is:
# its verdicts change between releases.
#
# A run that cannot load a tree (a commit that does not build, `go run`
# unable to fetch the tool) fails the script: no export data is not "no
# incompatible change".
set -euf
cd "$(dirname "$0")/.."

apidiff=golang.org/x/exp/cmd/apidiff@v0.0.0-20261007192929-f45ad48fbe92
bang='^[a-z]+(\([^)]*\))?!: '
module=$(go list -m)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# export DIR OUT writes the export data of the module checked out in DIR.
export_data() {
  if ! (cd "$1" && go run "$apidiff" -m -w "$2" "$module") >"$tmp/log" 2>&1; then
    cat "$tmp/log" >&2
    echo "check-api: cannot load the module in $3" >&2
    exit 1
  fi
}

# commit_data SHA prints the path of SHA's export data, computing it once.
commit_data() {
  out="$tmp/$1.exp"
  if [ ! -f "$out" ]; then
    rm -rf "$tmp/tree" && mkdir "$tmp/tree"
    git archive "$1" | tar -x -C "$tmp/tree"
    export_data "$tmp/tree" "$out" "$(git rev-parse --short "$1")"
  fi
  echo "$out"
}

# incompatible OLD NEW prints apidiff's incompatible changes, if any.
incompatible() {
  if ! go run "$apidiff" -m -incompatible "$1" "$2" 2>"$tmp/log"; then
    cat "$tmp/log" >&2
    echo "check-api: apidiff rendered no verdict" >&2
    exit 1
  fi
}

gochange='*.go :!*_test.go go.mod'

# From v1.0.0 the promise of ADR 0020 holds: a break of a stable package —
# not experimental (the first sentence of its documentation says so, the
# label repo_test.go holds the README and the guides to), not internal, not
# under example/ — would need a new major version, which in Go is a new
# module path; it is refused outright, `!` or not. An experimental package
# keeps v0's rule: a break is announced with `!`. The era is the latest tag
# reachable from the commit; CHECK_API_MAJOR=1 forces v1, so that the rule
# can be exercised before a v1 tag exists.
v1_era() {
  [ "${CHECK_API_MAJOR-}" = 1 ] && return 0
  git describe --tags --abbrev=0 --match 'v[1-9]*' "$1" >/dev/null 2>&1
}

unstable=
# stable_breaks prints the lines of apidiff's report (`- ./<package>.<symbol>:
# ...`) that name a stable package.
stable_breaks() {
  if [ -z "$unstable" ]; then
    unstable=$(go list -f '{{.ImportPath}}|{{.Doc}}' ./... | grep -i '|.*experimental' | cut -d'|' -f1 | sed "s|^$module||; s|^/||")
  fi
  echo "$1" | while IFS= read -r line; do
    pkg=${line#- ./}
    pkg=${pkg%%.*}
    case "$pkg" in example | example/*) continue ;; esac
    echo "$unstable" | grep -qx "$pkg" && continue
    echo "$line"
  done
}

# judge <changes> <announced> <era-ref> <what>: the verdict on one commit's
# incompatible changes, 1 for a refusal.
judge() {
  [ -n "$1" ] || return 0
  if v1_era "$3"; then
    stable=$(stable_breaks "$1")
    if [ -n "$stable" ]; then
      echo "$stable" >&2
      echo "check-api: $4 breaks a stable package after v1 (ADR 0020): that is a new major version, a new module path, not a commit" >&2
      return 1
    fi
  fi
  [ "$2" = 1 ] && return 0
  echo "$1" >&2
  echo "check-api: $4 breaks the exported API and carries no '!'" >&2
  return 1
}

if [ "${1-}" = --index ]; then
  subject=${2-}
  announced=0
  if echo "$subject" | grep -qE "$bang"; then
    announced=1
  fi
  if [ "$announced" = 1 ] && ! v1_era HEAD; then
    exit 0
  fi
  # The pathspecs are meant to split; set -f keeps the shell from globbing them.
  # shellcheck disable=SC2086
  if git diff --cached --quiet HEAD -- $gochange; then
    exit 0
  fi
  old=$(commit_data HEAD)
  mkdir "$tmp/index"
  git checkout-index -a --prefix="$tmp/index/"
  export_data "$tmp/index" "$tmp/index.exp" "the index"
  changes=$(incompatible "$old" "$tmp/index.exp")
  judge "$changes" "$announced" HEAD "the staged change" || exit 1
  exit 0
fi

base=${1-}
head=${2:-HEAD}
if [ -z "$base" ]; then
  base='@{upstream}'
fi
for r in "$base" "$head"; do
  if ! git rev-parse --quiet --verify "$r^{commit}" >/dev/null; then
    echo "check-api: $r is not a commit of this clone (a force push, a shallow fetch?)" >&2
    exit 1
  fi
done

status=0
checked=0
for c in $(git rev-list --reverse --no-merges "$base..$head"); do
  # shellcheck disable=SC2086
  if git diff --quiet "$c^" "$c" -- $gochange; then
    continue
  fi
  subject=$(git log -1 --format=%s "$c")
  announced=0
  if echo "$subject" | grep -qE "$bang"; then
    announced=1
  fi
  if [ "$announced" = 1 ] && ! v1_era "$c"; then
    continue
  fi
  checked=$((checked + 1))
  old=$(commit_data "$c^")
  new=$(commit_data "$c")
  changes=$(incompatible "$old" "$new")
  judge "$changes" "$announced" "$c" "$(git rev-parse --short "$c") \"$subject\"" || status=1
done
if [ "$status" -eq 0 ]; then
  echo "check-api: $checked commit(s) in $base..$head compared with their parent, no unannounced break"
fi
exit "$status"
