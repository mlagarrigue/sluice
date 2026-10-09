#!/usr/bin/env bash
# Paired A/B between two *benchmarks*, for comparing frameworks.
#
#   internal/bench/scripts/versus.sh BenchmarkSocketNetHTTP BenchmarkSocketSluice 15
#
# compare.sh cannot do this. It pairs two *builds* of the same benchmark — the
# working tree against a git ref — which is the right tool for "did my change
# regress this" and the wrong one for "is gin faster than echo", where both
# contenders live in one binary and differ by name rather than by commit.
#
# The reason to pair at all is the same either way. Two sequential runs of the
# same binary differ by up to 14% on this hardware (docs/benchmarks.md,
# "Measuring on a laptop"), so a table built from one run of each contender is
# reporting the machine as much as the code. Here each round runs both
# benchmarks back to back and the ratio is taken inside the round, so a clock
# ramp or a background task that slows the whole round cancels out.
#
# The verdict comes from the same sign test compare.sh uses, through the same
# paired.awk — "faster" means the second benchmark won in enough rounds that
# chance is not a credible explanation, with an effect floor so a real but
# pointless difference is reported as noise.
#
# Environment:
#   MODULE     directory to run in (default: benchmarks, where the competitors are)
#   BENCHTIME  passed to -test.benchtime (default: 2000x)
#   CORE       CPU to pin to, or "all" to leave the scheduler alone (default: all,
#              because a socket benchmark needs a client and a server at once and
#              pinning both to one core measures the contention rather than them)
#   MINEFFECT  ratio below which a verdict reads as noise (default: 0.03)
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
A="${1:?usage: versus.sh <benchmarkA> <benchmarkB> [rounds]}"
B="${2:?usage: versus.sh <benchmarkA> <benchmarkB> [rounds]}"
ROUNDS="${3:-15}"
MODULE="${MODULE:-benchmarks}"
BENCHTIME="${BENCHTIME:-2000x}"
CORE="${CORE:-all}"
export MINEFFECT="${MINEFFECT:-0.03}"

BIN="$(mktemp -d)"
trap 'rm -rf "$BIN"' EXIT

# Built once. Rebuilding between rounds would put the compiler's variability
# inside the measurement it is supposed to cancel.
echo "building $MODULE..." >&2
go -C "$ROOT/$MODULE" test -c -o "$BIN/bench.test" . >&2

run() { # run <benchmark-regex>
  if [ "$CORE" = "all" ]; then
    "$BIN/bench.test" -test.bench="^$1\$" -test.benchtime="$BENCHTIME" \
      -test.count=1 -test.run='^$' 2>/dev/null
  else
    taskset -c "$CORE" "$BIN/bench.test" -test.bench="^$1\$" -test.benchtime="$BENCHTIME" \
      -test.count=1 -test.run='^$' 2>/dev/null
  fi
}

# A short burst of work before the first measured round, so neither contender
# pays for a cold allocator that the other does not.
run "$A" >/dev/null || true
run "$B" >/dev/null || true

echo "measuring $ROUNDS paired rounds: $A vs $B" >&2
{
  for r in $(seq 1 "$ROUNDS"); do
    # Both sides in the same round, back to back. The order is fixed rather
    # than alternated: alternating would make round r's A follow round r-1's B
    # half the time and its own B the other half, which is a second source of
    # variation inside the thing being measured.
    run "$A" | awk -v r="$r" '/ns\/op/{print "before", r, "A-vs-B", $3}'
    run "$B" | awk -v r="$r" '/ns\/op/{print "after",  r, "A-vs-B", $3}'
  done
} | awk -v MINEFFECT="$MINEFFECT" -f "$ROOT/internal/bench/scripts/paired.awk" |
  sed "s/A-vs-B/$A vs $B/"
