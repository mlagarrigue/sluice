#!/usr/bin/env bash
# CI regression gate: paired A/B of the working tree against a base ref,
# failing on any benchmark the sign test declares SLOWER.
#
# This is the committed-baseline gate done the only way this repository's own
# measurements say is valid: absolute nanosecond figures do not transfer
# across sessions or machines (docs/benchmarks.md, "Measuring on a laptop"), so
# the gate compares two binaries built from two trees, paired round by round,
# on the runner it happens to get. Drift cancels out of the ratio; the sign
# test decides. TestStageBudget in CI still guards the absolute ceiling, with
# its tolerance; this guards the *relative* regression a PR introduces.
#
#   internal/bench/scripts/gate.sh <base-ref> [rounds]
#
# The round count lives here and nowhere else: CI calls the script without
# one, so the default below is what every gated verdict is based on. Twelve
# rounds is eleven paired rounds after paired.awk drops the first (clock
# ramp), which is the smallest n the sign test rules on with margin, and at
# n = 11 a verdict needs 10 wins out of 11 (tied rounds are dropped, which
# lowers n). compare.sh and ab.sh default to fifteen for local investigation;
# that default is theirs, not the gate's.
#
# GATE_INPUT=<file> replays a saved compare.sh output through the verdict
# logic below instead of building and measuring anything. It exists so the
# gate's own failure modes can be exercised in seconds — "run the gate, do not
# read it" — and it is not a mode CI uses.
#
# What is deliberately not gated: `Parallel`, `ParallelUnordered`, `Async`,
# `CoalesceWithin` and the macro ETL. Their figures measure the scheduler and
# the core count more than the code, so a 2-vCPU runner would gate on noise
# rather than on regressions. They stay measured locally through the same
# paired protocol, by compare.sh.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BASE="${1:?usage: gate.sh <base-ref> [rounds]}"
ROUNDS="${2:-12}"
REPLAY="${GATE_INPUT:-}"

# ab.sh pins to CORE; default to the last CPU actually present rather than
# a laptop-specific number.
export CORE="${CORE:-$(($(nproc) - 1))}"

# The gated set: single-goroutine hot paths, which are the ones a small shared
# runner measures reproducibly. An operator added without a line here is an
# operator whose regressions are nobody's business.
GATED='BenchmarkFlatMap$'
GATED="$GATED|BenchmarkCoalescePassThrough$|BenchmarkCoalesceFragmented$"
GATED="$GATED|BenchmarkDistinctAllUnique$"
GATED="$GATED|BenchmarkJoinFullMatch$|BenchmarkJoinNoMatch$|BenchmarkJoinTail$|BenchmarkJoinHash$"
GATED="$GATED|BenchmarkZipLongest$|BenchmarkZipLongestUneven$"
GATED="$GATED|BenchmarkIntervalJoinNarrow$|BenchmarkIntervalJoinWide$"
GATED="$GATED|BenchmarkWindowWide$|BenchmarkWindowNarrow$"
GATED="$GATED|BenchmarkTake$|BenchmarkDrop$|BenchmarkTakeWhile$|BenchmarkDropWhile$"
GATED="$GATED|BenchmarkSplitPartition$|BenchmarkSplitBroadcast$|BenchmarkInterleave2$"
# The request path, which is the figure a web workload actually feels.
GATED="$GATED|BenchmarkWebBatchOne$|BenchmarkWebHandwritten$"
# The connector's read loop, which is upstream of every operator above: it runs
# once per protocol message on the way in, so a regression here is in front of
# every other figure this gate reports and shows up in none of them.
GATED="$GATED|BenchmarkPGReadRows$|BenchmarkPGReadDecode$"
# The `= ANY($1)` codec, which is the claim the connector is built around: a
# thousand keys out as one parameter and a thousand values back out of one array.
GATED="$GATED|BenchmarkPGDecodeInt8Array$|BenchmarkPGEncodeInt8Array$"
# The two query paths the statement cache splits into. Both are single-goroutine
# and both are steady state, so both measure reproducibly here.
# BenchmarkPGPrepareMissUnbounded is deliberately NOT gated: it lets the cache
# map grow for the whole run, so its figure moves with the run length rather
# than with the code, and a sign test on it would report the harness.
GATED="$GATED|BenchmarkPGPrepareOff$|BenchmarkPGPrepareHit$"

# Every gated name must name a benchmark that exists.
#
# This is the failure mode a regression gate has that no other test has: a
# benchmark regex matching nothing produces no rows, no SLOWER verdict, and a
# clean exit. The gate then reports success precisely because it measured
# nothing. A renamed or misspelled benchmark would silently stop being watched,
# and the first sign would be the regression it was there to catch.
have="$(if [ -n "$REPLAY" ]; then tr '|' '\n' <<<"$GATED" | sed 's/\$$//'; else go test -list '.*' "$ROOT/internal/bench" 2>/dev/null; fi)"
missing=""
while IFS= read -r name; do
  grep -qx "$name" <<<"$have" || missing="$missing $name"
done < <(tr '|' '\n' <<<"$GATED" | sed 's/\$$//')
if [ -n "$missing" ]; then
  echo "regression gate: these gated names match no benchmark:$missing"
  echo "a gate that matches nothing reports success, so this is a failure rather than a warning"
  exit 1
fi

# 3%: below this a verdict is not worth failing a pull request over, and above
# it a real regression from a code change comfortably clears. See paired.awk for
# why a floor is needed at all rather than trusting the sign test alone.
export MINEFFECT="${MINEFFECT:-0.03}"

if [ -n "$REPLAY" ]; then
  out="$(cat "$REPLAY")"
else
  out="$("$ROOT/internal/bench/scripts/compare.sh" "$GATED" "$ROUNDS" "$BASE")"
fi
echo "$out"

# Every gated name must have produced a line — a verdict or a "need >=".
#
# The third hole of the family, and the one the two checks below could not
# see: ab.sh used to keep only ns/elem rows, and four gated benchmarks report
# ns/op alone, so they reached paired.awk as nothing at all. No row, no
# verdict, no "need >=" — and the gate passed on the strength of the
# benchmarks that did resolve. A name that produced no line was never
# measured, and that is a failure of the gate rather than a finding about
# the code.
silent=""
while IFS= read -r name; do
  grep -qE "^$name(-[0-9]+)? " <<<"$out" || silent="$silent $name"
done < <(tr '|' '\n' <<<"$GATED" | sed 's/\$$//')
if [ -n "$silent" ]; then
  echo "regression gate: these gated benchmarks produced no verdict and no 'need >=' line:$silent"
  echo "they ran on neither side or their output was not extracted — the gate did not measure them"
  exit 1
fi

# "need >= N rounds" lines mean a benchmark resolved no verdict — surface it,
# do not fail on it: an unresolved benchmark is missing data, not a regression,
# and a benchmark added since the base ref has no baseline by definition.
if grep -q "need >=" <<<"$out"; then
  echo "warning: some benchmarks did not resolve; raise rounds if this persists"
fi

# But *nothing* resolving is not missing data, it is a comparison that never
# happened — and without this it reports success, because no line said SLOWER.
#
# It is the same hole as a gated name matching no benchmark, one layer out: the
# names all resolved against the working tree and the base ref had none of
# them, so both sides were asked for benchmarks and one side had nothing to
# give. Found by running the gate rather than by reading it.
if ! grep -qE "(faster|SLOWER|noise)" <<<"$out"; then
  echo "regression gate: not one benchmark produced a verdict against $BASE"
  echo "either the base ref predates the gated set, or the harness is not running — check before trusting a clean run"
  exit 1
fi
if grep -q "SLOWER" <<<"$out"; then
  echo "regression gate: at least one benchmark is SLOWER than $BASE (sign test)"
  exit 1
fi
echo "regression gate: no benchmark slower than $BASE"
