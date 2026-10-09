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

echo "building 'after' from the working tree..."
go test -c -o "$BINDIR/bench.after" "$ROOT/internal/bench"

WORKTREE="$(mktemp -d)"
echo "building 'before' from $BASE..."
git -C "$ROOT" worktree add -q --detach "$WORKTREE" "$BASE"
( cd "$WORKTREE" && go test -c -o "$BINDIR/bench.before" ./internal/bench )
git -C "$ROOT" worktree remove --force "$WORKTREE"
WORKTREE=""

echo "measuring $ROUNDS paired rounds..."
BINDIR="$BINDIR" "$ROOT/internal/bench/scripts/ab.sh" "$BENCH" "$ROUNDS" \
  | awk -v MINEFFECT="${MINEFFECT:-0}" -f "$ROOT/internal/bench/scripts/paired.awk" | sort
