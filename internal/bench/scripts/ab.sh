#!/usr/bin/env bash
# Interleaved A/B with per-round pairing.
#
# The pairing is what matters: each round runs before and after back to back on
# the same core, and the RATIO is taken within the round. Drift that moves both
# sides together — a clock ramp, a Windows task, thermal throttling — cancels
# out of the ratio instead of landing on whichever side ran first.
set -euo pipefail
SP="${BINDIR:?set BINDIR to the directory holding bench.before and bench.after}"
BENCH="${1:?bench regex}"
ROUNDS="${2:-15}"
CORE="${CORE:-6}"

timeout 4 bash -c 'while :; do :; done' || true

for r in $(seq 1 "$ROUNDS"); do
  for side in before after; do
    GOMAXPROCS=1 taskset -c "$CORE" "$SP/bench.$side" \
      -test.bench="$BENCH" -test.benchtime=300ms -test.count=1 -test.run='^$' 2>/dev/null \
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
