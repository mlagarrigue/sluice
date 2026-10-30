#!/usr/bin/env bash
# Fuzzing: the one way go test -fuzz runs in this repository, locally, in CI
# (the Fuzz workflow) and in reviews. A test checks that nothing else starts
# it (TestHeavyTestsRunThroughTheirScripts).
#
#   fuzz.sh [--list] [--wait MIN] [packages...]
#
# Runs every fuzz target in the given packages (default ./...) for FUZZTIME
# each (default: fuzztime in fuzz.yaml, beside this script).
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
# What the script holds, so that no caller has to (alone.sh, shared with
# mutation testing):
#   - one configuration, fuzz.yaml;
#   - running alone: at the start, no other build or test and a calm
#     one-minute load (--wait MIN waits up to MIN minutes, otherwise the
#     script refuses); between two targets it waits while other work runs;
#   - one heavy run at a time, fuzzing and mutation alike (a lock);
#   - as many workers (-parallel) as the memory left holds at fuzz.worker-kib
#     each, never more than fuzz.parallel;
#   - an address-space cap on every process, sluice.memory-cap-kib plus
#     sluice.memory-cap-per-worker-kib for each worker.
#
# --list prints "package target" pairs and runs nothing.
#
# MODULE names the module the packages belong to, as a directory (default
# the root): the study's module comparison/sluice is a module of its own,
# outside the root's ./..., and carries a target of its own.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
heavy_name=fuzz
# shellcheck source=alone.sh
. "$here/alone.sh"
cfg=$here/fuzz.yaml
[ -f "$cfg" ] || heavy_die "$cfg is missing"

mod="${MODULE:-.}"
list_only=false wait_min=0
while [ $# -gt 0 ]; do
  case "$1" in
    --list) list_only=true ;;
    --wait) wait_min=${2:?--wait MIN}; shift ;;
    -*) heavy_die "unknown option $1" ;;
    *) break ;;
  esac
  shift
done
[ "$#" -eq 0 ] && set -- ./...

fuzztime="${FUZZTIME:-$(yaml_value "$cfg" fuzztime)}"
out="${FUZZ_OUT:-fuzz-failures}"
max_workers=$(yaml_value "$cfg" parallel)
worker_kib=$(yaml_value "$cfg" worker-kib)
cap_kib=$(yaml_value "$cfg" memory-cap-kib)
cap_worker_kib=$(yaml_value "$cfg" memory-cap-per-worker-kib)
margin_mib=$(yaml_value "$cfg" memory-margin-mib)
for v in "$max_workers" "$worker_kib" "$cap_kib" "$cap_worker_kib" "$margin_mib"; do
  [[ $v =~ ^[0-9]+$ ]] || heavy_die "fuzz.yaml: parallel, worker-kib and the memory keys must be integers"
done

# `go test -list` prints a package's matching names, then its "ok <pkg>" line;
# packages without tests print "? <pkg> [no test files]" and are dropped.
# A compile error makes it exit non-zero, which fails the run loudly instead
# of fuzzing nothing.
targets=$(go -C "$mod" test -list '^Fuzz' "$@" | awk '
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

heavy_lock
heavy_start "$wait_min"
workers=$(heavy_workers "$worker_kib" "$max_workers" "$margin_mib")
cap=$((cap_kib + workers * cap_worker_kib))
heavy_say "$(wc -l <<<"$targets") targets, $fuzztime each, -parallel $workers, cap $cap KiB a process"

# One target; its output goes to the terminal and to $2 for the verdict.
# 9>&-: the lock stays with this script, not with a worker left behind.
fuzz_one() { # $1 = package, $2 = target, $3 = output file
  (ulimit -v "$cap" && exec go -C "$mod" test -run '^$' -fuzz "^${2}\$" -fuzztime "$fuzztime" \
    -parallel "$workers" "$1" 9>&-) 2>&1 | tee "$3"
  return "${PIPESTATUS[0]}"
}
# The fuzzing engine sometimes ends a run with "context deadline exceeded"
# and no failing input: a worker was still busy when -fuzztime ran out
# (seen in this repository at 2 s and at 60 s a target, on a calm machine as
# under load). Such a failure has nothing to reproduce; it is run once more,
# and only a second failure, or any failure that wrote an input, counts.
engine_timeout() { # $1 = output file
  grep -q 'context deadline exceeded' "$1" && ! grep -q 'Failing input written to' "$1"
}

failed=()
log=$(mktemp)
trap 'rm -f "$log"' EXIT
while read -r pkg target; do
  heavy_stay
  echo "::group::$pkg $target"
  if ! fuzz_one "$pkg" "$target" "$log"; then
    if engine_timeout "$log"; then
      echo "fuzz.sh: $target ended on the engine's deadline without an input; run once more"
      fuzz_one "$pkg" "$target" "$log" || failed+=("$pkg $target")
    else
      failed+=("$pkg $target")
    fi
  fi
  echo "::endgroup::"
done <<<"$targets"

if [ "${#failed[@]}" -eq 0 ]; then
  echo "fuzz.sh: $(wc -l <<<"$targets") targets, no failure"
  exit 0
fi

# Untracked files under any testdata/fuzz are what this run found.
mkdir -p "$out"
git ls-files --others --exclude-standard -- ':(glob)**/testdata/fuzz/**' |
  while read -r f; do cp --parents "$f" "$out/"; done

echo "fuzz.sh: ${#failed[@]} target(s) failed:" >&2
printf '  %s\n' "${failed[@]}" >&2
exit 1
