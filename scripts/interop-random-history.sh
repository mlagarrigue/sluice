#!/bin/sh
# Judges the interop cases recorded as random over the passes that ran them:
#
#   scripts/interop-random-history.sh append <history.jsonl> <cases.jsonl> <pass> <sha>
#   scripts/interop-random-history.sh check <history.jsonl>
#
# interop-expect.sh accepts a case that interop/expected-results.json marks
# random whether it succeeded or failed: its outcome follows the losses the
# simulator draws, and a rule on one pass would go red on the draw. Over
# several passes it can be judged. check exits non-zero for every random case
# that succeeded in none of the last RANDOM_PASSES passes that ran it (six
# unless set): a client that has stopped succeeding, which no single pass can
# tell from bad luck. A pass that did not run the case does not count, and a
# case with fewer runs on record is not judged yet.
#
# Six because a pass already runs a failed case once more: the least stable
# case on record, kwik's client under handshakeloss, failed two attempts in
# three, so both attempts of a pass about four times in nine; six such
# passes in a row is under one in a hundred, while a client that has stopped
# succeeding goes red within six weekly passes.
#
# append adds one pass to the history — every case it ran, keyed by peer,
# sluice's side and name, with the first letter of the runner's final word
# (a case run once more keeps its second result) — and keeps the last
# HISTORY_PASSES (eight). The Interop runner workflow calls both on a
# complete pass and publishes the history on the interop-results branch.
set -eu
expected=${EXPECTED:-$(dirname "$0")/../interop/expected-results.json}
usage() {
  echo "usage: interop-random-history.sh append <history> <cases.jsonl> <pass> <sha> | check <history>" >&2
  exit 2
}
[ $# -ge 1 ] || usage
case $1 in
append)
  [ $# -eq 5 ] || usage
  history=$2
  touch "$history"
  jq -cs --arg pass "$4" --arg sha "$5" '
    {pass: $pass, sha: $sha,
     cases: (map({key: (if .server == "sluice" then "\(.client) server \(.name)" else "\(.server) client \(.name)" end),
                  value: .result[0:1]}) | from_entries)}
  ' "$3" >>"$history"
  tail -n "${HISTORY_PASSES:-8}" "$history" >"$history.tmp"
  mv "$history.tmp" "$history"
  ;;
check)
  [ $# -eq 2 ] || usage
  jq -rn --slurpfile want "$expected" --argjson n "${RANDOM_PASSES:-6}" '
    [inputs] as $history
    | $want[0] | to_entries[] | .key as $peer
    | .value | to_entries[] | .key as $side
    | .value | to_entries[] | select(.value | startswith("random"))
    | "\($peer) \($side) \(.key)" as $case
    | [$history[] | .cases[$case] // empty] as $runs
    | select(($runs | length) >= $n and ($runs[-$n:] | all(. != "s")))
    | "interop-random-history: \($case): no success in the last \($n) passes that ran it (\($runs[-$n:] | join("")))"
  ' "$2" >"${TMPDIR:-/tmp}/interop-random.$$"
  status=0
  if [ -s "${TMPDIR:-/tmp}/interop-random.$$" ]; then
    cat "${TMPDIR:-/tmp}/interop-random.$$" >&2
    status=1
  else
    echo "interop-random-history: every random case succeeded at least once in its last ${RANDOM_PASSES:-6} passes, or has fewer on record"
  fi
  rm -f "${TMPDIR:-/tmp}/interop-random.$$"
  exit $status
  ;;
*) usage ;;
esac
