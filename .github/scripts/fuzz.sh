#!/usr/bin/env bash
# Runs every fuzz target in the given packages (default ./...) for FUZZTIME
# each (default 30s).
#
#   .github/scripts/fuzz.sh [--list] [packages...]
#
# `go test -fuzz` accepts one target per invocation and one package per
# invocation, so the targets are discovered with `go test -list` and run one
# by one. Every target runs even after one fails — a nightly run that stops at
# the first finding hides the others until the next night — and the script
# exits non-zero if any did.
#
# A failing input is written by the toolchain under the package's
# testdata/fuzz/. Those new files are copied to $FUZZ_OUT (default
# fuzz-failures/) with their paths kept, so CI can upload exactly the
# reproducers and not the committed seed corpus beside them.
#
# --list prints "package target" pairs and runs nothing.
set -euo pipefail

list_only=false
if [ "${1:-}" = "--list" ]; then
  list_only=true
  shift
fi
[ "$#" -eq 0 ] && set -- ./...

fuzztime="${FUZZTIME:-30s}"
out="${FUZZ_OUT:-fuzz-failures}"

# `go test -list` prints a package's matching names, then its "ok <pkg>" line;
# packages without tests print "? <pkg> [no test files]" and are dropped.
# A compile error makes it exit non-zero, which fails the run loudly instead
# of fuzzing nothing.
targets=$(go test -list '^Fuzz' "$@" | awk '
  /^ok[ \t]/ { for (i = 0; i < n; i++) print $2, names[i]; n = 0; next }
  /^Fuzz/    { names[n++] = $1 }
')

if [ -z "$targets" ]; then
  echo "fuzz.sh: no fuzz targets found in $*" >&2
  exit 1
fi

if $list_only; then
  printf '%s\n' "$targets"
  exit 0
fi

failed=()
while read -r pkg target; do
  echo "::group::$pkg $target"
  if ! go test -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzztime" "$pkg"; then
    failed+=("$pkg $target")
  fi
  echo "::endgroup::"
done <<< "$targets"

if [ "${#failed[@]}" -eq 0 ]; then
  echo "fuzz.sh: $(wc -l <<< "$targets") targets, no failure"
  exit 0
fi

# Untracked files under any testdata/fuzz are what this run found.
mkdir -p "$out"
git ls-files --others --exclude-standard -- ':(glob)**/testdata/fuzz/**' |
  while read -r f; do cp --parents "$f" "$out/"; done

echo "fuzz.sh: ${#failed[@]} target(s) failed:" >&2
printf '  %s\n' "${failed[@]}" >&2
exit 1
