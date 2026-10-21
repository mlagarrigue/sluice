#!/usr/bin/env bash
# CI gate for what a congestion controller other than the default is held
# to on the emulated paths of net/quic. gate-path.sh compares a tree against
# its base and so catches a controller getting worse; it cannot catch one
# that never met its own criterion, nor one that drifts by less than 6% a
# commit. These are absolute bounds, read on one tree:
#
#   SLUICE_QUIC_CC=<cubic|bbr1|bbr> internal/bench/scripts/gate-cc.sh [rounds]
#
# - every one: BenchmarkTransferTwoFlowsVsReno, the controller's flow against
#   a Reno flow on one bottleneck. The median of share, the controller's part
#   of the delivered rate, is at most MAXSHARE. 0.70 is the bound BBR was
#   held to (docs/benchmarks.md), and the one a controller sold as fair to
#   loss-based flows must keep. Measured 2026-10-14: BBR 0.46-0.61 across
#   eight rounds, BBRv1 0.60-0.69 — the round, not the median, can touch
#   the bound, so the median is what is judged.
# - cubic: BenchmarkTransferRTT80Shallow, the long avoidance transfer CUBIC's
#   window function was designed for, run on CUBIC and on Reno in the same
#   round. The median over the rounds of Reno's time over CUBIC's is at
#   least MINGAIN. Both sides are bimodal, by where the slow start's exit
#   loss lands: CUBIC 3.7 s or 5.1 s, Reno 5.3 s or 8.6 s on 2026-10-21,
#   where Reno read 8.8 s every round when the bound was set (2026-10-14).
#   A round that pairs CUBIC's slow mode with Reno's fast one reads 1.03 and
#   says nothing about the controller, so the worst round, the criterion
#   until then, failed three of the sixteen CI runs of 2026-10-21; the
#   median falls under the bound only when the slow mode is the common
#   case. 1.3 is below the 1.40-1.44 of the fast modes and above
#   the 1.09-1.12 of every round with CUBIC's beta at 0.5.
# - bbr: BenchmarkTransferDeepBufferECN, the deep buffer behind a marking
#   AQM. Every round, rtt_x — the final smoothed RTT over the smallest — is
#   at most MAXRTTX: the "RTT within 1.5x min_rtt" the palette plan asked of
#   a deep buffer, met by the draft through the marks (1.07-1.21 across eight
#   rounds, 2026-10-14). Reno and CUBIC read marks as losses and are not
#   held to it.
#
# One binary, built from the working tree; one transfer per benchmark and
# round (BENCHTIME 1x), unpinned for the reasons gate-path.sh gives.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
ROUNDS="${1:-8}"
PKG="./net/quic"
MAXSHARE="${MAXSHARE:-0.70}"
MINGAIN="${MINGAIN:-1.3}"
MAXRTTX="${MAXRTTX:-1.5}"

CC="${SLUICE_QUIC_CC:-}"
case "$CC" in
  cubic | bbr1 | bbr) ;;
  *)
    echo "controller gate: SLUICE_QUIC_CC=$CC names no controller held to a criterion (cubic, bbr1, bbr)"
    exit 1
    ;;
esac

BINDIR="$(mktemp -d)"
trap 'rm -rf "$BINDIR"' EXIT
echo "building from the working tree..."
go test -c -o "$BINDIR/bench" "$ROOT/$PKG"

# metric <controller> <benchmark> <unit>: one transfer, the figure printed
# before unit. A benchmark that does not run prints nothing, and the caller
# fails on the empty value rather than reading it as zero.
metric() {
  SLUICE_QUIC_CC="$1" "$BINDIR/bench" -test.run='^$' -test.bench="^$2\$" \
    -test.benchtime=1x -test.count=1 2>/dev/null |
    awk -v u="$3" '/^Benchmark/ { for (i = 3; i <= NF; i++) if ($i == u) print $(i-1) }'
}

fail=0
need() {
  if [ -z "$2" ]; then
    echo "controller gate: $1 printed no figure — the gate did not measure it"
    exit 1
  fi
}

shares=""
for r in $(seq 1 "$ROUNDS"); do
  s="$(metric "$CC" BenchmarkTransferTwoFlowsVsReno share)"
  need "BenchmarkTransferTwoFlowsVsReno round $r" "$s"
  shares="$shares $s"
done
median="$(tr ' ' '\n' <<<"$shares" | grep . | sort -g | awk '{ a[NR] = $1 } END { print (NR % 2) ? a[(NR + 1) / 2] : (a[NR / 2] + a[NR / 2 + 1]) / 2 }')"
verdict=ok
if awk -v m="$median" -v b="$MAXSHARE" 'BEGIN { exit !(m > b) }'; then verdict=OVER; fail=1; fi
echo "TwoFlowsVsReno ($CC): share$shares, median $median, bound $MAXSHARE — $verdict"

if [ "$CC" = cubic ]; then
  gains=""
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
    gains="$gains $gain"
    awk -v r="$r" -v n="$n" -v c="$c" -v g="$gain" \
      'BEGIN { printf "RTT80Shallow round %d: Reno %.2f s, CUBIC %.2f s, gain %s\n", r, n / 1e9, c / 1e9, g }'
  done
  median="$(tr ' ' '\n' <<<"$gains" | grep . | sort -g | awk '{ a[NR] = $1 } END { print (NR % 2) ? a[(NR + 1) / 2] : (a[NR / 2] + a[NR / 2 + 1]) / 2 }')"
  verdict=ok
  if awk -v m="$median" -v b="$MINGAIN" 'BEGIN { exit !(m < b) }'; then verdict=UNDER; fail=1; fi
  echo "RTT80Shallow ($CC): gain$gains, median $median, bound $MINGAIN — $verdict"
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
