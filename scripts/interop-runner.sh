#!/bin/sh
# Runs the public QUIC Interop Runner locally, sluice against one peer, on
# each side of the handshake, and prints the result of every case:
#
#   scripts/interop-runner.sh [peer]      # default peer: quic-go
#   scripts/interop-runner.sh --matrix [peer|all]
#
# SIDE=server or SIDE=client runs one direction only, sluice in that role;
# the default runs both. --matrix prints, as JSON, the {peer, side} pairs a
# full pass crosses: every implementation in the runner's list at
# RUNNER_REF, on each side its role allows (a server-only peer meets
# sluice as a client, a client-only one meets it as a server). It reads the
# list over HTTPS and needs only curl and jq.
#
# Needs docker (with compose), python3, tshark 4.5 or newer, jq and git.
# The runner is cloned into RUNNER_DIR (default: a directory under the
# system temporary directory) at RUNNER_REF (default: the commit pinned
# below, so that a result read today reads the same tomorrow); TESTS
# narrows the cases (comma separated, the runner's names). The image is
# built from the working tree, or pulled when IMAGE names a published one
# (ghcr.io/mlagarrigue/sluice-interop:latest, the last release),
# and the sluice entry is added to the runner's implementation list; the
# runner's own directories are left as it leaves them, so a second run
# reuses the clone and its virtual environment.
#
# The exit status says whether every case the pass ran came out as
# interop/expected-results.json records it (scripts/interop-expect.sh): a
# red case is a failure only where the file expects it green, a 127 is an
# unsupported case the runner records as such, and a case that turns green
# where the file says otherwise fails too, until the file records it. A peer
# the file has no entry for is reported and not judged: the weekly matrix
# crosses every peer, and only those whose results someone has read and
# written down can regress. The logs of every case stay in RUNNER_DIR/logs
# for reading.
set -eu
cd "$(dirname "$0")/.."

RUNNER_DIR=${RUNNER_DIR:-${TMPDIR:-/tmp}/quic-interop-runner}
# Pinned by commit, as h2spec is in interop-http.sh: the revision the
# results in docs/guide/limits.md were read at. repo_test.go refuses a
# branch here, since a branch moves under the results.
RUNNER_REF=${RUNNER_REF:-ab039a4a4b43ae2e21bb5704c9fad1f8fe6257d6}
TESTS=${TESTS:-handshake,transfer,longrtt,chacha20,multiplexing,retry,resumption,zerortt,http3,blackhole,keyupdate,ecn,amplificationlimit,handshakeloss,transferloss,handshakecorruption,transfercorruption,ipv6,v2,rebind-port,rebind-addr,connectionmigration}
image=${IMAGE:-sluice-interop:local}
SIDE=${SIDE:-both}

fail() {
  echo "interop-runner: $*" >&2
  exit 1
}

if [ "${1:-}" = "--matrix" ]; then
  want=${2:-all}
  curl -fsSL "https://raw.githubusercontent.com/quic-interop/quic-interop-runner/$RUNNER_REF/implementations_quic.json" |
    jq -c --arg want "$want" '
      [ to_entries[]
        | select(.key != "sluice" and ($want == "all" or .key == $want))
        | .key as $peer | .value.role as $role
        | (if $role != "server" then {peer: $peer, side: "server"} else empty end),
          (if $role != "client" then {peer: $peer, side: "client"} else empty end) ]
      | if length == 0 then error("no implementation named \($want) in the runner list") else . end'
  exit
fi

peer=${1:-quic-go}
case $SIDE in
server | client | both) ;;
*) fail "SIDE is $SIDE; want server, client or both" ;;
esac
for tool in docker python3 tshark jq git; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is not installed"
done

echo "## image"
if [ -n "${IMAGE:-}" ]; then
  docker pull -q "$image" >/dev/null
  echo "pulled $image"
else
  docker build -q -f interop/Dockerfile -t "$image" . >/dev/null
  echo "built $image"
fi

echo "## runner"
if [ ! -d "$RUNNER_DIR/.git" ]; then
  # Cloned without a checkout, then checked out at the ref: a commit as
  # well as a branch or tag, where --branch takes only the last two.
  git clone -q --no-checkout \
    https://github.com/quic-interop/quic-interop-runner "$RUNNER_DIR"
  git -C "$RUNNER_DIR" checkout -q "$RUNNER_REF"
fi
echo "$(git -C "$RUNNER_DIR" rev-parse --short HEAD) in $RUNNER_DIR"
if [ ! -x "$RUNNER_DIR/.venv/bin/python" ]; then
  python3 -m venv "$RUNNER_DIR/.venv"
  "$RUNNER_DIR/.venv/bin/pip" install -q -r "$RUNNER_DIR/requirements.txt"
fi
# The runner reads its implementations from one JSON file; sluice goes in
# beside the others, pointing at the image just built. Idempotent.
impls="$RUNNER_DIR/implementations_quic.json"
jq --arg image "$image" '. + {sluice: {image: $image, url: "https://github.com/mlagarrigue/sluice", role: "both"}}' \
  "$impls" >"$impls.tmp" && mv "$impls.tmp" "$impls"

# run <server> <client>: one direction, with the runner's debug output,
# which is the only place each container's own output and the compliance
# check's verdict appear (the runner keeps no log of a case it skipped).
# The runner exits non-zero when a case fails, which is a result here, not
# an error.
run() {
  echo "## server $1, client $2"
  (
    cd "$RUNNER_DIR"
    # The runner refuses a log directory that exists: one per direction.
    rm -rf "logs/$1_$2" "logs/$1_$2-again"
    .venv/bin/python run.py -d -s "$1" -c "$2" -t "$TESTS" -l "logs/$1_$2" -j "results_$1_$2.json" || true
    # A failed case is run once more and keeps both results: the runner's
    # own capture sometimes misses a handshake both ends completed
    # ("Expected exactly one version. Got []", a quarter-size pcap on one
    # side), and a weekly matrix red for that says nothing. A case red twice
    # stays red; the table shows both words, so a case that needed the
    # second try is seen, not hidden.
    [ -f "results_$1_$2.json" ] || return 0
    failed=$(jq -r '[.results[0][] | select(.result == "failed") | .name] | join(",")' "results_$1_$2.json")
    [ -n "$failed" ] || return 0
    echo "## server $1, client $2, once more: $failed"
    .venv/bin/python run.py -d -s "$1" -c "$2" -t "$failed" -l "logs/$1_$2-again" -j "results_$1_$2-again.json" || true
    [ -f "results_$1_$2-again.json" ] || return 0
    jq --slurpfile again "results_$1_$2-again.json" '
      ($again[0].results[0] | map({(.name): .result}) | add // {}) as $a
      | .results[0] |= map(if $a[.name] then {first: .result} + . + {result: $a[.name]} else . end)
    ' "results_$1_$2.json" >"results_$1_$2.tmp"
    mv "results_$1_$2.tmp" "results_$1_$2.json"
  )
}
# A direction not run stands as a pass with no cases, so the table and the
# expectation read two files either way and check nothing on that side.
asServer=$RUNNER_DIR/results_sluice_$peer.json
asClient=$RUNNER_DIR/results_${peer}_sluice.json
none=$RUNNER_DIR/results_none.json
echo '{"results": [[]]}' >"$none"
if [ "$SIDE" != client ]; then run sluice "$peer"; else asServer=$none; fi
if [ "$SIDE" != server ]; then run "$peer" sluice; else asClient=$none; fi
# The runner writes no results when it refuses its arguments — a case name
# it does not know, a peer missing from its list — and says why above.
for f in "$asServer" "$asClient"; do
  [ -f "$f" ] || fail "the runner wrote no $(basename "$f"); its output above says why"
done

# The table: one row per case, one column per direction, from the JSON the
# runner wrote (results[0] is the single server/client pair of each run).
echo "## results"
jq -r --arg peer "$peer" -n '
  ($peer) as $p
  | [input, input] as [$asServer, $asClient]
  | def word: if .first then "\(.first), then \(.result)" else .result end;
  ($asServer.results[0] | map({(.name): word}) | add // {}) as $s
  | ($asClient.results[0] | map({(.name): word}) | add // {}) as $c
  | "| case | sluice server, \($p) client | \($p) server, sluice client |",
    "|---|---|---|",
    ($s + $c | keys_unsorted[] | "| \(.) | \($s[.] // "-") | \($c[.] // "-") |")
' "$asServer" "$asClient"

echo "## expected"
if jq -e --arg peer "$peer" 'has($peer)' interop/expected-results.json >/dev/null; then
  scripts/interop-expect.sh "$peer" "$asServer" "$asClient"
else
  echo "interop-runner: no expected results for $peer: reported, not judged"
fi
