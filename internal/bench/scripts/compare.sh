#!/usr/bin/env bash
# Compare the working tree against a git ref, benchmark by benchmark.
#
#   internal/bench/scripts/compare.sh 'BenchmarkFlatMap$' 11 HEAD
#
# Why this exists rather than `go test -bench` twice: on a laptop — and
# especially under WSL2, where Linux neither sees nor controls the CPU clock —
# two sequential runs of the SAME binary differ by up to 14%. Anything smaller
# than that measured one run at a time is indistinguishable from the machine.
# See docs/benchmarks.md, "Measuring on a laptop".
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BENCH="${1:?usage: compare.sh <bench-regex> [rounds] [base-ref]}"
ROUNDS="${2:-15}"
BASE="${3:-HEAD}"
BINDIR="$(mktemp -d)"
# Declared before the trap: under `set -u` a trap naming an unset variable
# aborts on expansion, so a failed 'after' build used to leak $BINDIR. A
# worktree left behind by a failed 'before' build is unregistered too, or the
# next run's `git worktree add` trips over the stale entry.
WORKTREE=""
cleanup() {
  if [ -n "$WORKTREE" ]; then
    git -C "$ROOT" worktree remove --force "$WORKTREE" 2>/dev/null || true
  fi
  rm -rf "$BINDIR" "$WORKTREE"
}
trap cleanup EXIT

# Every function starts on a 64-byte boundary in both binaries. Without it, a
# change that only moves code — 2a9a2ab renumbered two constants and touched
# no measured path — shifted where the hot loops of other packages landed,
# and the gate on an AMD EPYC 7763 runner called seven benchmarks SLOWER by
# 3% to 32%, the same in two passes of one job; with it, one remained, at 4%.
# An injected 15% slowdown of Scan stays SLOWER. Alignment of function
# entries only: a loop inside a function can still fall across a boundary,
# and every address still moves with the code before it. FUNCALIGN sets
# another alignment for both sides: gate.sh measures a SLOWER name again
# at 128, which moves every address, and keeps only what both layouts say.
LDFLAGS="-funcalign=${FUNCALIGN:-64}"

echo "building 'after' from the working tree..."
go test -c -ldflags="$LDFLAGS" -o "$BINDIR/bench.after" "$ROOT/internal/bench"

WORKTREE="$(mktemp -d)"
echo "building 'before' from $BASE..."
git -C "$ROOT" worktree add -q --detach "$WORKTREE" "$BASE"
( cd "$WORKTREE" && go test -c -ldflags="$LDFLAGS" -o "$BINDIR/bench.before" ./internal/bench )
git -C "$ROOT" worktree remove --force "$WORKTREE"
WORKTREE=""

echo "measuring $ROUNDS paired rounds..."
BINDIR="$BINDIR" "$ROOT/internal/bench/scripts/ab.sh" "$BENCH" "$ROUNDS" \
  | awk -v MINEFFECT="${MINEFFECT:-0}" -f "$ROOT/internal/bench/scripts/paired.awk" | sort
