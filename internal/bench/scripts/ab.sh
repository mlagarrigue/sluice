#!/usr/bin/env bash
# Interleaved A/B with per-round pairing.
#
# The pairing is what matters: each round runs before and after back to back on
# the same core, and the RATIO is taken within the round. Drift that moves both
# sides together — a clock ramp, a Windows task, thermal throttling — cancels
# out of the ratio instead of landing on whichever side ran first.
#
# Two things that do not move both sides together are handled here too:
#
#   - The path a binary runs from. The same bytes measured 292 ns/op under
#     /tmp/ab/bench.after and 225-232 under bench.afterx, b or
#     /tmp/ab2/bench.after (BenchmarkHPACKDecode, 2026-10-15): the length of
#     the path shifts what the process lays out at start-up, and one length
#     can land a binary on an unlucky alignment — a 25% regression that does
#     not exist, or one that hides. So each side runs from two copies under
#     two names of different lengths, both sides from names of the same length
#     in a given round, and the name changes every other round. The two
#     directories swap sides with the name, so neither directory belongs to
#     one side either. An effect that belongs to one path then shows up in
#     half the rounds only, which a sign test reads as noise rather than as a
#     verdict.
#   - The order within a round. Whichever side runs second inherits the
#     first one's cache and clock state, so the order alternates every round.
#
# The two alternations are out of phase, so four rounds cover every pairing of
# name and order.
#
#   BINDIR=<dir> internal/bench/scripts/ab.sh <bench-regex> [rounds]
#
# BINDIR holds bench.before and bench.after. CORE pins both to one core under
# GOMAXPROCS=1; CORE= (empty) runs them unpinned, for benchmarks whose figure
# is the scheduler's own work (gate-path.sh). BENCHTIME is go test's
# -test.benchtime, 300ms by default.
#
# Output, one line per benchmark per side per round: side round name value.
set -euo pipefail
SP="${BINDIR:?set BINDIR to the directory holding bench.before and bench.after}"
BENCH="${1:?bench regex}"
ROUNDS="${2:-15}"
CORE="${CORE-6}"
BENCHTIME="${BENCHTIME:-300ms}"

# Copies, not links: a symlink would leave /proc/self/exe on the original path.
# The directories x and y have names of one length, so the before and after of
# a round run from paths of one length.
RUN="$(mktemp -d)"
trap 'rm -rf "$RUN"' EXIT
mkdir "$RUN/x" "$RUN/y"
cp "$SP/bench.before" "$RUN/x/bench"
cp "$SP/bench.after" "$RUN/y/bench"
cp "$SP/bench.before" "$RUN/y/bench.alt"
cp "$SP/bench.after" "$RUN/x/bench.alt"

timeout 4 bash -c 'while :; do :; done' || true

for r in $(seq 1 "$ROUNDS"); do
  if [ $(((r - 1) / 2 % 2)) -eq 0 ]; then name=bench; else name=bench.alt; fi
  if [ $((r % 2)) -eq 1 ]; then order="before after"; else order="after before"; fi
  for side in $order; do
    if [ "$side/$name" = before/bench ] || [ "$side/$name" = after/bench.alt ]; then
      bin="$RUN/x/$name"
    else
      bin="$RUN/y/$name"
    fi
    if [ -n "$CORE" ]; then
      run=(env GOMAXPROCS=1 taskset -c "$CORE" "$bin")
    else
      run=("$bin")
    fi
    "${run[@]}" -test.bench="$BENCH" -test.benchtime="$BENCHTIME" -test.count=1 -test.run='^$' 2>/dev/null \
      | awk -v s="$side" -v r="$r" '
          # One row per benchmark: the per-element figure when the benchmark
          # reports one, otherwise the ns/op that go test prints. Only the ratio
          # within a round is ever used, so the unit need not agree across
          # benchmarks — but a benchmark that reports no ns/elem must still
          # produce a row, or the gate has nothing to judge it on.
          /^Benchmark/ {
            elem = ""; op = ""
            for (i = 2; i <= NF; i++) {
              if ($i == "ns/elem") elem = $(i-1)
              else if ($i == "ns/op") op = $(i-1)
            }
            if (elem != "") print s, r, $1, elem
            else if (op != "") print s, r, $1, op
          }'
  done
done
