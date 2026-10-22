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
# wrote cannot be checked. A case the file lists but the pass did not run
# (TESTS narrowed it) is not checked.
#
# An expected value is the runner's own word — succeeded, unsupported or
# failed — and, after a colon, why: a failed case carries its cause.
set -eu
[ $# -eq 3 ] || {
  echo "usage: interop-expect.sh <peer> <results as server> <results as client>" >&2
  exit 2
}
expected=${EXPECTED:-$(dirname "$0")/../interop/expected-results.json}

jq -r -n --arg peer "$1" --slurpfile want "$expected" '
  $want[0][$peer] as $w
  | [input, input] as [$asServer, $asClient]
  | if $w == null then
      "interop-expect: no expected results for \($peer) in interop/expected-results.json"
    else
      ( [["server", $asServer], ["client", $asClient]][]
        | .[0] as $side
        | .[1].results[0][]
        | (($w[$side] // {})[.name]) as $e
        | if $e == null then
            "interop-expect: \($peer), sluice \($side), \(.name): \(.result), which the file does not list"
          elif ($e | split(":")[0]) != .result then
            "interop-expect: \($peer), sluice \($side), \(.name): \(.result), expected \($e)"
          else empty end )
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
