#!/usr/bin/env bash
# CI regression gate for the emulated-path benchmarks of net/quic: paired A/B
# of the working tree against a base ref, failing on any benchmark whose
# median transfer is more than 15% slower.
#
#   internal/bench/scripts/gate-path.sh <base-ref> [rounds]
#
# gate.sh cannot read these benchmarks, which is why this script exists
# rather than more names in its GATED set:
#
#   - they live in net/quic, and gate.sh lists and builds internal/bench alone;
#   - ab.sh pins each binary to one core under GOMAXPROCS=1, and an emulation
#     that sleeps on timers in several goroutines (the middlebox, both
#     endpoints) must not be — pinned, the path is the scheduler;
#   - they measure the congestion controller, not the CPU: a transfer takes as
#     long as the path lets it, so the machine's load barely moves the figure
#     and the spread is 1-4%. gate.sh's 3% effect by sign test is a CPU bound;
#     the rule docs/benchmarks.md states for these rows is 15% on the median.
#
# So: build net/quic's test binary from both trees, run the gated benchmarks
# as interleaved rounds (before, after, before, after...) so drift cancels out
# of the comparison as it does in ab.sh, one transfer per round
# (-test.benchtime=1x: the figure is one transfer's wall clock, and the
# median of eight transfers is the statistic), and fail when the 'after'
# median exceeds the 'before' median by more than PATHEFFECT.
#
# The round count lives here: CI calls the script without one.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BASE="${1:?usage: gate-path.sh <base-ref> [rounds]}"
ROUNDS="${2:-8}"
PKG="./net/quic"

# The gated set, every name anchored. A path benchmark enters it when its
# median holds within 5% of itself on an identical tree (this script, HEAD
# against the working tree, eight rounds); one that does not stays out, with
# the measurement that says so here. Measured 2026-10-14 on an identical
# tree: the eight gated names within 2.1% of themselves (Bottleneck 0.979,
# the rest 0.991 to 1.013); BenchmarkTransferReorder at 0.934, out — a
# spurious loss is declared or not by a few microseconds of scheduling, and
# its figure moves 7% with nothing changed.
GATED='BenchmarkTransferClean$|BenchmarkTransferLoss1pct$|BenchmarkTransferBurstLoss$|BenchmarkTransferBottleneck$|BenchmarkTransferDeepBuffer$|BenchmarkTransferTwoFlows$|BenchmarkTransferRTT40$|BenchmarkTransferRTT80$'

# 15%: the bound docs/benchmarks.md sets on these rows, above their measured
# spread (±4% on the lossy profile) and below what a change to the controller
# itself costs: slow-start growth halved puts Clean at +43% (measured
# 2026-10-14, the injection this gate was proven red on). What it does not
# see: an initial window halved costs Clean +8%, inside the bound — a change
# of that size is for the local protocol, docs/benchmarks.md's command.
PATHEFFECT="${PATHEFFECT:-0.15}"
PCT="$(awk -v e="$PATHEFFECT" 'BEGIN { printf "%d%%", e * 100 }')"

# Every gated name must name a benchmark that exists — see gate.sh for why a
# gate that matches nothing must fail rather than pass.
have="$(go test -list '.*' "$ROOT/$PKG" 2>/dev/null)"
missing=""
while IFS= read -r name; do
  grep -qx "$name" <<<"$have" || missing="$missing $name"
done < <(tr '|' '\n' <<<"$GATED" | sed 's/\$$//')
if [ -n "$missing" ]; then
  echo "path gate: these gated names match no benchmark:$missing"
  echo "a gate that matches nothing reports success, so this is a failure rather than a warning"
  exit 1
fi

BINDIR="$(mktemp -d)"
WORKTREE=""
cleanup() {
  if [ -n "$WORKTREE" ]; then
    git -C "$ROOT" worktree remove --force "$WORKTREE" 2>/dev/null || true
  fi
  rm -rf "$BINDIR" "$WORKTREE"
}
trap cleanup EXIT

echo "building 'after' from the working tree..."
go test -c -o "$BINDIR/quic.after" "$ROOT/$PKG"

WORKTREE="$(mktemp -d)"
echo "building 'before' from $BASE..."
git -C "$ROOT" worktree add -q --detach "$WORKTREE" "$BASE"
( cd "$WORKTREE" && go test -c -o "$BINDIR/quic.before" "$PKG" )
git -C "$ROOT" worktree remove --force "$WORKTREE"
WORKTREE=""

echo "measuring $ROUNDS interleaved rounds, one transfer each..."
rows="$(for r in $(seq 1 "$ROUNDS"); do
  for side in before after; do
    "$BINDIR/quic.$side" -test.bench="$GATED" -test.benchtime=1x -test.count=1 -test.run='^$' 2>/dev/null \
      | awk -v s="$side" '
          /^Benchmark/ {
            for (i = 2; i <= NF; i++) if ($i == "ns/op") print s, $1, $(i-1)
          }'
  done
done)"

# Medians per side, then the verdict. The name is printed without its -N
# suffix, which is GOMAXPROCS and differs between machines.
out="$(awk -v EFFECT="$PATHEFFECT" -v ROUNDS="$ROUNDS" '
  {
    name = $2; sub(/-[0-9]+$/, "", name)
    n[$1, name]++
    v[$1, name, n[$1, name]] = $3
    seen[name] = 1
  }
  function median(side, name,    k, i, j, t, a) {
    k = n[side, name]
    for (i = 1; i <= k; i++) a[i] = v[side, name, i]
    for (i = 2; i <= k; i++) { t = a[i]; for (j = i - 1; j >= 1 && a[j] > t; j--) a[j+1] = a[j]; a[j+1] = t }
    if (k % 2) return a[(k + 1) / 2]
    return (a[k / 2] + a[k / 2 + 1]) / 2
  }
  END {
    for (name in seen) {
      if (n["before", name] != ROUNDS || n["after", name] != ROUNDS) {
        printf "%s: measured %d before and %d after rounds of %d — MISSING\n", name, n["before", name], n["after", name], ROUNDS
        continue
      }
      b = median("before", name); a = median("after", name)
      ratio = a / b
      verdict = "ok"
      if (ratio > 1 + EFFECT) verdict = "SLOWER"
      else if (ratio < 1 - EFFECT) verdict = "faster"
      printf "%s: median before %.1f ms, after %.1f ms, ratio %.3f — %s\n", name, b / 1e6, a / 1e6, ratio, verdict
    }
  }' <<<"$rows" | sort)"
echo "$out"

# Every gated name must have produced a line, and none may be short of rounds:
# a benchmark that was not measured is a failure of the gate, not a finding.
silent=""
while IFS= read -r name; do
  grep -qE "^$name: " <<<"$out" || silent="$silent $name"
done < <(tr '|' '\n' <<<"$GATED" | sed 's/\$$//')
if [ -n "$silent" ]; then
  echo "path gate: these gated benchmarks produced no line:$silent"
  echo "they ran on neither side or their output was not extracted — the gate did not measure them"
  exit 1
fi
if grep -q "MISSING" <<<"$out"; then
  echo "path gate: a benchmark is short of rounds on one side — the gate did not measure it"
  exit 1
fi
if grep -q "SLOWER" <<<"$out"; then
  echo "path gate: at least one transfer median is more than $PCT slower than $BASE"
  exit 1
fi
echo "path gate: no transfer median more than $PCT slower than $BASE"
