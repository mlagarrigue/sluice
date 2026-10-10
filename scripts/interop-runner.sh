#!/bin/sh
# Runs the public QUIC Interop Runner locally, sluice against one peer, on
# each side of the handshake, and prints the result of every case:
#
#   scripts/interop-runner.sh [peer]      # default peer: quic-go
#
# Needs docker (with compose), python3, tshark 4.5 or newer, jq and git.
# The runner is cloned into RUNNER_DIR (default: a directory under the
# system temporary directory) at RUNNER_REF (default: master); TESTS
# narrows the cases (comma separated, the runner's names). The image is
# built from the working tree, or pulled when IMAGE names a published one
# (ghcr.io/mlagarrigue/sluice-interop:latest, what the public matrix runs),
# and the sluice entry is added to the runner's implementation list; the
# runner's own directories are left as it leaves them, so a second run
# reuses the clone and its virtual environment.
#
# The exit status says whether the runner ran, not whether sluice is green:
# a red case is a result this script reports, a 127 is an unsupported case
# the runner records as such. The logs of every case stay in
# RUNNER_DIR/logs for reading.
set -eu
cd "$(dirname "$0")/.."

peer=${1:-quic-go}
RUNNER_DIR=${RUNNER_DIR:-${TMPDIR:-/tmp}/quic-interop-runner}
RUNNER_REF=${RUNNER_REF:-master}
TESTS=${TESTS:-handshake,transfer,longrtt,chacha20,multiplexing,retry,resumption,zerortt,http3,blackhole,keyupdate,ecn,amplificationlimit,handshakeloss,transferloss,handshakecorruption,transfercorruption,ipv6,v2,rebind-port,rebind-addr,connectionmigration}
image=${IMAGE:-sluice-interop:local}

fail() {
  echo "interop-runner: $*" >&2
  exit 1
}
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
  git clone -q --depth 1 --branch "$RUNNER_REF" \
    https://github.com/quic-interop/quic-interop-runner "$RUNNER_DIR"
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
    rm -rf "logs/$1_$2"
    .venv/bin/python run.py -d -s "$1" -c "$2" -t "$TESTS" -l "logs/$1_$2" -j "results_$1_$2.json" || true
  )
}
run sluice "$peer"
run "$peer" sluice

# The table: one row per case, one column per direction, from the JSON the
# runner wrote (results[0] is the single server/client pair of each run).
echo "## results"
jq -r --arg peer "$peer" -n '
  ($peer) as $p
  | [input, input] as [$asServer, $asClient]
  | ($asServer.results[0] | map({(.name): .result}) | add) as $s
  | ($asClient.results[0] | map({(.name): .result}) | add) as $c
  | "| case | sluice server, \($p) client | \($p) server, sluice client |",
    "|---|---|---|",
    ($asServer.results[0][] | .name | "| \(.) | \($s[.] // "-") | \($c[.] // "-") |")
' "$RUNNER_DIR/results_sluice_$peer.json" "$RUNNER_DIR/results_${peer}_sluice.json"
