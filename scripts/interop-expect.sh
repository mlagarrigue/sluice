#!/bin/sh
# Compares one interop runner pass against interop/expected-results.json and
# exits non-zero on any difference, in either direction:
#
#   scripts/interop-expect.sh <peer> <results as server> <results as client>
#
# The two files are the runner's results_sluice_<peer>.json and
# results_<peer>_sluice.json. A case expected to succeed that does not is a
# regression; a case that succeeds where the file says unsupported or
# failed is a change the file has to record, or the next regression of that
# case would pass unseen. A case the pass ran that the file does not list,
# and a peer the file has no entry for, fail too: an expectation nobody
# wrote cannot be checked.
#
# CASES_SERVER and CASES_CLIENT are the comma-separated case names the pass
# was *asked* to run on that side, and every one of them missing from the
# results fails: that is the coverage floor. Empty means the side was not
# run, and nothing is checked on it — how interop-runner.sh reports a
# one-sided pass. Without the floor the comparison only ever walked the
# cases the runner had written, so a pass that ran none of them answered
# "every case as expected" with a nul status. The runner at ab039a4 does
# skip a whole pair when its compliance check fails (interop.py:589-595)
# and exports its results anyway, so such a pass is not hypothetical.
#
# An expected value is the runner's own word — succeeded, unsupported or
# failed — and, after a colon, why: a failed case carries its cause. One
# word is the file's own: random, for a case whose outcome follows the
# losses the simulator draws, accepts succeeded and failed alike and
# carries its cause the same way; unsupported is still refused there.
set -eu
[ $# -eq 3 ] || {
  echo "usage: interop-expect.sh <peer> <results as server> <results as client>" >&2
  exit 2
}
expected=${EXPECTED:-$(dirname "$0")/../interop/expected-results.json}

jq -r -n --arg peer "$1" \
  --arg askedServer "${CASES_SERVER:-}" --arg askedClient "${CASES_CLIENT:-}" \
  --slurpfile want "$expected" '
  def asked: if . == "" then [] else split(",") end;
  $want[0][$peer] as $w
  | [input, input] as [$asServer, $asClient]
  | if $w == null then
      "interop-expect: no expected results for \($peer) in interop/expected-results.json"
    else
      [["server", $asServer, ($askedServer | asked)],
       ["client", $asClient, ($askedClient | asked)]][]
      | .[0] as $side
      | .[1] as $results
      | .[2] as $askedCases
      | [$results.results[0][].name] as $ran
      | ( ( $askedCases[]
            | select(. as $n | $ran | index($n) | not)
            | "interop-expect: \($peer), sluice \($side), \(.): asked for and not in the pass" ),
          ( $results.results[0][]
            | (($w[$side] // {})[.name]) as $e
            | if $e == null then
                "interop-expect: \($peer), sluice \($side), \(.name): \(.result), which the file does not list"
              elif ($e | split(":")[0]) == "random" and (.result == "succeeded" or .result == "failed") then
                empty
              elif ($e | split(":")[0]) != .result then
                "interop-expect: \($peer), sluice \($side), \(.name): \(.result), expected \($e)"
              else empty end ) )
    end
' "$2" "$3" >"${TMPDIR:-/tmp}/interop-expect.$$"
status=0
if [ -s "${TMPDIR:-/tmp}/interop-expect.$$" ]; then
  cat "${TMPDIR:-/tmp}/interop-expect.$$" >&2
  status=1
else
  echo "interop-expect: $1, every case as expected"
fi
rm -f "${TMPDIR:-/tmp}/interop-expect.$$"
exit $status
