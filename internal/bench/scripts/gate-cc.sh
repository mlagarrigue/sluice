#!/usr/bin/env bash
# CI gate for what a congestion controller other than the default is held
# to on the emulated paths of net/quic. gate-path.sh compares a tree against
# its base and so catches a controller getting worse; it cannot catch one
# that never met its own criterion, nor one that drifts by less than 6% a
# commit. These are absolute bounds, read on one tree:
#
#   SLUICE_QUIC_CC=<cubic|bbr> internal/bench/scripts/gate-cc.sh [rounds]
#
# - every one: BenchmarkTransferTwoFlowsVsReno, the controller's flow against
#   a Reno flow on one bottleneck. The median of share, the controller's part
#   of the delivered rate, is at most MAXSHARE. 0.70 is the bound BBR was
#   held to (docs/benchmarks.md), and the one a controller sold as fair to
#   loss-based flows must keep. Measured 2026-10-14: BBR 0.46-0.61 across
#   eight rounds — the round, not the median, can touch the bound, so the
#   median is what is judged.
# - cubic: BenchmarkTransferRTT80Shallow, the long avoidance transfer CUBIC's
#   window function was designed for, run on CUBIC and on Reno in the same
#   round. The median over the rounds of Reno's time over CUBIC's is at
#   least MINGAIN. Both sides were bimodal, by where the slow start's exit
#   loss lands (CUBIC 3.7 s or 5.1 s, Reno 5.3 s or 8.6 s on 2026-10-21),
#   so the median of the rounds is judged, not the worst. Since Reno paces
#   its slow start at twice the window (newReno.pacingRate, 2026-10-28) it
#   no longer exits a round early: Reno 4.6 s, CUBIC 3.8 s, the gain 1.21 to
#   1.23 over eight rounds, where it read 1.40-1.44 against Reno's slow
#   exit. 1.15 is below that and above the 1.06-1.09 of every round with
#   CUBIC's beta at 0.5, measured the same day against the same Reno.
# - bbr: BenchmarkTransferDeepBufferECN, the deep buffer behind a marking
#   AQM. Every round, rtt_x — the final smoothed RTT over the smallest — is
#   at most MAXRTTX: the "RTT within 1.5x min_rtt" the palette plan asked of
#   a deep buffer, met by the draft through the marks (1.07-1.21 across eight
#   rounds, 2026-10-14). Reno and CUBIC read marks as losses and are not
#   held to it.
#
# One binary, built from the working tree; one transfer per benchmark and
# round (BENCHTIME 1x), unpinned for the reasons gate-path.sh gives.
#
# A median criterion that fails is read again, a whole second series, and
# judged on that one; both series are printed. A shared runner has episodes:
# on 2026-11-11 (run 43901112486) half the rounds of one job shifted, the
# RTT80Shallow median read 1.135 where seven neighbouring jobs read 1.205 to
# 1.235, on a commit that changed only the changelog. One series judged
# alone turned that episode into a red build; a controller that really
# misses its bound misses it twice. The per-round bound of bbr is not read
# again: a round over it is the failure, not noise around a median.
#
# GATE_CC_METRIC, when set, names a command run instead of the benchmark
# binary, with the controller, the benchmark and the unit as arguments, and
# nothing is built: the script's own judgement, played on series made up.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
ROUNDS="${1:-8}"
PKG="./net/quic"
MAXSHARE="${MAXSHARE:-0.70}"
MINGAIN="${MINGAIN:-1.15}"
MAXRTTX="${MAXRTTX:-1.5}"

CC="${SLUICE_QUIC_CC:-}"
case "$CC" in
  cubic | bbr) ;;
  *)
    echo "controller gate: SLUICE_QUIC_CC=$CC names no controller held to a criterion (cubic, bbr)"
    exit 1
    ;;
esac

BINDIR="$(mktemp -d)"
trap 'rm -rf "$BINDIR"' EXIT
if [ -z "${GATE_CC_METRIC:-}" ]; then
  echo "building from the working tree..."
  go test -c -o "$BINDIR/bench" "$ROOT/$PKG"
fi

# metric <controller> <benchmark> <unit>: one transfer, the figure printed
# before unit. A benchmark that does not run prints nothing, and the caller
# fails on the empty value rather than reading it as zero.
metric() {
  if [ -n "${GATE_CC_METRIC:-}" ]; then
    "$GATE_CC_METRIC" "$1" "$2" "$3"
    return
  fi
  SLUICE_QUIC_CC="$1" "$BINDIR/bench" -test.run='^$' -test.bench="^$2\$" \
    -test.benchtime=1x -test.count=1 2>/dev/null |
    awk -v u="$3" '/^Benchmark/ { for (i = 3; i <= NF; i++) if ($i == u) print $(i-1) }'
}

median() {
  tr ' ' '\n' <<<"$1" | grep . | sort -g | awk '{ a[NR] = $1 } END { print (NR % 2) ? a[(NR + 1) / 2] : (a[NR / 2] + a[NR / 2 + 1]) / 2 }'
}

# judge <name> <series-function> <comparison> <bound>: runs the series, and
# a second one when the first's median is out of bound (comparison "over":
# above it; "under": below it), and prints each. Sets fail when the last
# series read is out of bound.
judge() {
  local name="$1" series="$2" cmp="$3" bound="$4" try values m verdict
  for try in 1 2; do
    values="$("$series")"
    m="$(median "$values")"
    verdict=ok
    if [ "$cmp" = over ] && awk -v m="$m" -v b="$bound" 'BEGIN { exit !(m > b) }'; then verdict=OVER; fi
    if [ "$cmp" = under ] && awk -v m="$m" -v b="$bound" 'BEGIN { exit !(m < b) }'; then verdict=UNDER; fi
    echo "$name, series $try:$values, median $m, bound $bound — $verdict"
    [ "$verdict" = ok ] && return
    [ "$try" = 1 ] && echo "$name: read again — one series out of bound may be the runner's episode"
  done
  fail=1
}

fail=0
need() {
  if [ -z "$2" ]; then
    echo "controller gate: $1 printed no figure — the gate did not measure it" >&2
    exit 1
  fi
}

# shares prints a series of TwoFlowsVsReno shares.
shares() {
  local r s out=""
  for r in $(seq 1 "$ROUNDS"); do
    s="$(metric "$CC" BenchmarkTransferTwoFlowsVsReno share)"
    need "BenchmarkTransferTwoFlowsVsReno round $r" "$s"
    out="$out $s"
  done
  echo "$out"
}
judge "TwoFlowsVsReno ($CC): share" shares over "$MAXSHARE"

# gains prints a series of RTT80Shallow gains, Reno's time over CUBIC's,
# each round's detail on stderr.
gains() {
  local r c n gain out=""
  for r in $(seq 1 "$ROUNDS"); do
    # Alternate which side runs first, so a drift within the round does not
    # always land on the same controller.
    if [ $((r % 2)) -eq 1 ]; then
      c="$(metric cubic BenchmarkTransferRTT80Shallow ns/op)"
      n="$(metric reno BenchmarkTransferRTT80Shallow ns/op)"
    else
      n="$(metric reno BenchmarkTransferRTT80Shallow ns/op)"
      c="$(metric cubic BenchmarkTransferRTT80Shallow ns/op)"
    fi
    need "BenchmarkTransferRTT80Shallow round $r" "$c"
    need "BenchmarkTransferRTT80Shallow round $r" "$n"
    gain="$(awk -v n="$n" -v c="$c" 'BEGIN { printf "%.2f", n / c }')"
    out="$out $gain"
    awk -v r="$r" -v n="$n" -v c="$c" -v g="$gain" \
      'BEGIN { printf "RTT80Shallow round %d: Reno %.2f s, CUBIC %.2f s, gain %s\n", r, n / 1e9, c / 1e9, g }' >&2
  done
  echo "$out"
}

if [ "$CC" = cubic ]; then
  judge "RTT80Shallow ($CC): gain" gains under "$MINGAIN"
fi

if [ "$CC" = bbr ]; then
  for r in $(seq 1 "$ROUNDS"); do
    x="$(metric bbr BenchmarkTransferDeepBufferECN rtt_x)"
    need "BenchmarkTransferDeepBufferECN round $r" "$x"
    verdict=ok
    if awk -v x="$x" -v b="$MAXRTTX" 'BEGIN { exit !(x > b) }'; then verdict=OVER; fail=1; fi
    echo "DeepBufferECN round $r: rtt_x $x, bound $MAXRTTX — $verdict"
  done
fi

if [ "$fail" -ne 0 ]; then
  echo "controller gate ($CC): a criterion is not met"
  exit 1
fi
echo "controller gate ($CC): every criterion met"
