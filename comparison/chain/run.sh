#!/usr/bin/env bash
# One stage of the local chain end to end (chain.sh up, web, stage, down):
# the chain validated, its answers checked, nothing published.
#
#   comparison/chain/run.sh                                  # unit, h1, 1 000 req/s, 10 s
#   SCENARIO=list RATE=300 PROTO=h2 comparison/chain/run.sh
#   RTT=20ms LOSS=1% LINK=100mbit comparison/chain/run.sh     # the network of §3.4, §8.4
#   FORCE=checkpoint comparison/chain/run.sh                  # an invalid stage, on purpose
#
# Every variable is chain.sh's. It exits 0 for a valid stage, 2 for a stage
# the apparatus invalidated, 1 for an answer that is not ok — stricter than
# a campaign's stage, which breaks by design past the capacity. Every
# container and network it made is removed on exit, error or not.
set -euo pipefail
cd "$(dirname "$0")/../.."
export OUT=${OUT:-$(mktemp -d)}
trap 'comparison/chain/chain.sh down' EXIT
comparison/chain/chain.sh up
comparison/chain/chain.sh web
code=0
comparison/chain/chain.sh stage || code=$?
if [ "$code" = 1 ]; then
  exit 1
fi
# The generator exits 1 for a stage that is not a measurement, which a
# stage under injected loss rarely is; the chain itself held when every
# answer came back ok.
if ! jq -e '.tester == 0 and (.ok == .sent or ($loss | length > 0))' --arg loss "${LOSS:-}" "$OUT/stage.jsonl" >/dev/null; then
  echo "chain: answers not all valid" >&2
  exit 1
fi
exit "$code"
