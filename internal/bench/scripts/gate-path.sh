#!/usr/bin/env bash
# CI regression gate for the emulated-path benchmarks of net/quic: paired A/B
# of the working tree against a base ref, failing on any benchmark whose
# median transfer is more than 6% slower (10% for the few a controller
# spreads wider, listed below).
#
#   SLUICE_QUIC_CC=<reno|cubic|bbr> internal/bench/scripts/gate-path.sh <base-ref> [rounds]
#
# Both sides run the controller SLUICE_QUIC_CC names (netem_bench_test.go
# reads it), Reno when it is unset: CI runs the script once per controller,
# so a regression in one that is not the default is read too.
#
# gate.sh cannot read these benchmarks, which is why this script exists
# rather than more names in its GATED set:
#
#   - they live in net/quic, and gate.sh lists and builds internal/bench alone;
#   - gate.sh measures pinned to one core under GOMAXPROCS=1, and an
#     emulation that sleeps on timers in several goroutines (the middlebox,
#     both endpoints) must not be — pinned, the path is the scheduler;
#   - they measure the congestion controller, not the CPU: a transfer takes as
#     long as the path lets it, so the machine's load barely moves the figure
#     and the spread is 1-3%. gate.sh's 3% effect by sign test is a CPU
#     bound; these rows are judged on the median, with a wider bound.
#
# So: build net/quic's test binary from both trees, run the gated benchmarks
# through ab.sh unpinned (CORE=''), whose interleaved rounds cancel drift and
# whose two names per side and alternating order keep the binary's path and
# the running order out of the comparison, one transfer per round
# (BENCHTIME=1x: the figure is one transfer's wall clock, and the median of
# eight transfers is the statistic), and fail when the 'after' median exceeds
# the 'before' median by more than PATHEFFECT.
#
# The round count lives here: CI calls the script without one.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BASE="${1:?usage: gate-path.sh <base-ref> [rounds]}"
ROUNDS="${2:-8}"
PKG="./net/quic"

# A misspelt controller would gate Reno under another name. The test binary
# refuses it too (benchAlgorithmNamed); refusing here fails before two builds.
CC="${SLUICE_QUIC_CC:-reno}"
case "$CC" in
  reno | cubic | bbr) ;;
  *)
    echo "path gate: SLUICE_QUIC_CC=$CC names no controller (reno, cubic, bbr)"
    exit 1
    ;;
esac
export SLUICE_QUIC_CC="$CC"

# The gated set, every name anchored. A path benchmark enters it when its
# median holds within 5% of itself on an identical tree (this script, HEAD
# against the working tree, eight rounds); one that does not stays out, with
# the measurement that says so here. Measured 2026-10-14 on an identical
# tree: the eight gated names within 2.1% of themselves (Bottleneck 0.979,
# the rest 0.991 to 1.013); BenchmarkTransferReorder at 0.934, out — a
# spurious loss is declared or not by a few microseconds of scheduling, and
# its figure moves 7% with nothing changed.
GATED='BenchmarkTransferClean$|BenchmarkTransferLoss1pct$|BenchmarkTransferBurstLoss$|BenchmarkTransferBottleneck$|BenchmarkTransferDeepBuffer$|BenchmarkTransferTwoFlows$|BenchmarkTransferRTT40$|BenchmarkTransferRTT80$'

# The same rule per controller: a name whose median does not hold within 5%
# of itself on an identical tree under that controller leaves its gate, and
# only its gate. Measured 2026-10-16, eight rounds each: Reno 0.978-1.012,
# CUBIC 0.991-1.011, BBR 0.990-1.013: every controller keeps the eight
# names, and the list of those it leaves out is empty today.
UNSTABLE=''
GATED="$(tr '|' '\n' <<<"$GATED" | grep -vxF -f <(tr '|' '\n' <<<"$UNSTABLE") | paste -sd'|')"

# 6%: four times the widest upward spread measured on an identical tree, and
# below what an initial window halved costs. Measured 2026-10-15, ten passes
# of this script on an identical tree: every gated ratio within 0.970-1.015,
# Bottleneck 0.988-1.015. The initial window halved (recovery.go, ten
# datagrams to five): RTT40 1.239, Clean 1.133, RTT80 1.120, Bottleneck
# 1.108 — four rows red; at the 15% this bound was before, RTT40 alone saw
# that injection. A runner whose identical-tree spread reaches 1.04 is the signal
# to raise it.
PATHEFFECT="${PATHEFFECT:-0.06}"

# A name that holds within 5% on a short run can still cross 6% on CI: under
# CUBIC, Bottleneck (1.076, d9cefc6) and TwoFlows (1.083, c119d13) turned the
# job red on commits that touched no Go file. Measured 2026-10-19, CUBIC, an
# identical tree, 80 interleaved rounds on four cores cut into ten passes of
# eight: Bottleneck 0.948-1.059, one transfer anywhere in 797-954 ms (the
# likely reading, not traced: where HyStart++ exits); TwoFlows 0.992-1.015,
# but bimodal — four transfers in five at 1453-1497 ms, the rest at
# 1527-1640 — so the median of eight jumps a mode when four land high. The
# other six held within 0.982-1.020. Resampled from those 160 transfers, the
# chance that one pass reads more than 6% slower is 3.9% for Bottleneck and
# 1.1% for TwoFlows; at 10% it is 0.03% and 0.01%. Sixteen rounds at 6%
# would still leave 1.2% and double the job. So these two get WIDEEFFECT
# under CUBIC. Nothing is lost on the injection above: under CUBIC the
# initial window halved moves neither (Bottleneck 0.957, TwoFlows 1.038) and
# turns the job red on the rows that keep 6% — Clean 1.140, RTT80 1.077,
# BurstLoss 1.069 — while the restored tree passes.
#
# BBR reads TwoFlows the same way, which eight rounds had missed: the job
# turned red on 47a8b93 at 1.065, a commit whose whole diff is three action
# SHAs in a workflow, and the immediate rerun read 1.009. Measured
# 2026-10-24, BBR, an identical tree, four cores, 80 interleaved rounds cut
# into ten passes of eight: the seven other names within 0.986-1.015, one
# transfer anywhere in a 1-3% band; TwoFlows within 0.984-1.007 by the pass
# but one transfer anywhere in 1449-1642 ms, the same bimodal spread CUBIC
# shows. Resampled from those 160 transfers, the chance that one pass reads
# more than 6% slower is 0.84%, the order of CUBIC's TwoFlows (1.1%); at 10%
# it is 0.01%. Nothing is lost on the injection: under BBR the initial
# window halved leaves TwoFlows at 1.018 and turns the job red four times
# over on the rows that keep 6% — Clean 1.094, RTT80 1.109, BurstLoss 1.080,
# Loss1pct 1.076 — while the restored tree passes.
case "$CC" in
  cubic) WIDE='BenchmarkTransferBottleneck$|BenchmarkTransferTwoFlows$' ;;
  bbr) WIDE='BenchmarkTransferTwoFlows$' ;;
  *) WIDE='' ;;
esac
WIDEEFFECT="${WIDEEFFECT:-0.10}"

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
go test -c -o "$BINDIR/bench.after" "$ROOT/$PKG"

WORKTREE="$(mktemp -d)"
echo "building 'before' from $BASE..."
git -C "$ROOT" worktree add -q --detach "$WORKTREE" "$BASE"
( cd "$WORKTREE" && go test -c -o "$BINDIR/bench.before" "$PKG" )
git -C "$ROOT" worktree remove --force "$WORKTREE"
WORKTREE=""

echo "measuring $ROUNDS interleaved rounds, one transfer each, controller $CC..."
rows="$(BINDIR="$BINDIR" CORE='' BENCHTIME=1x "$ROOT/internal/bench/scripts/ab.sh" "$GATED" "$ROUNDS")"

# Medians per side, then the verdict. The name is printed without its -N
# suffix, which is GOMAXPROCS and differs between machines.
out="$(awk -v EFFECT="$PATHEFFECT" -v WIDEEFFECT="$WIDEEFFECT" -v WIDE="${WIDE//\$/}" -v ROUNDS="$ROUNDS" '
  BEGIN { nw = split(WIDE, w, "|"); for (i = 1; i <= nw; i++) wide[w[i]] = 1 }
  {
    name = $3; sub(/-[0-9]+$/, "", name)
    n[$1, name]++
    v[$1, name, n[$1, name]] = $4
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
      e = (name in wide) ? WIDEEFFECT : EFFECT
      verdict = "ok"
      if (ratio > 1 + e) verdict = "SLOWER"
      else if (ratio < 1 - e) verdict = "faster"
      printf "%s: median before %.1f ms, after %.1f ms, ratio %.3f, bound %d%% — %s\n", name, b / 1e6, a / 1e6, ratio, e * 100 + 0.5, verdict
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
  echo "path gate ($CC): at least one transfer median is slower than $BASE by more than its bound"
  exit 1
fi
echo "path gate ($CC): no transfer median slower than $BASE by more than its bound"
