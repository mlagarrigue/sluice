#!/usr/bin/env bash
# The "not the bottleneck" control of §4: the generator against its own
# trivial server, one ramp per scenario and protocol, validation at 100 %
# on. The last stage held is the generator's ceiling for that scenario on
# this tester; a contender's capacity is interpretable only when it sits
# under half of it. Nothing printed here is a result of the study.
#
#   comparison/loadgen/selftest.sh                 # 4 scenarios × h1 h2 h3
#   SCENARIOS="list" PROTOS="h1" ./selftest.sh     # one cell
#   RATES=4000,5000 DURATION=2s ./selftest.sh      # a shorter grid
#
# RATES is a coarse ×1.25 grid by default, since the control wants an order
# of magnitude, not the study's 1.05 steps; the trivial server and the
# generator run in two processes on this machine, so both share its cores.
set -euo pipefail
cd "$(dirname "$0")"

ADDR=${ADDR:-127.0.0.1:8444}
SCENARIOS=${SCENARIOS:-static unit list mixed}
PROTOS=${PROTOS:-h1 h2 h3}
RATES=${RATES:-4000,5000,6300,8000,10000,12500,16000,20000,25000,32000,40000,50000,64000}
DURATION=${DURATION:-3s}
WARMUP=${WARMUP:-1s}
DRAIN=${DRAIN:-1s}
KEYSPACE=${KEYSPACE:-300000}
MAX_CONNS=${MAX_CONNS:-0}
OUT=${OUT:-$(mktemp -d)}

bin=$OUT/loadgen
go build -o "$bin" .

"$bin" -trivial "$ADDR" -keyspace "$KEYSPACE" > "$OUT/trivial.log" 2>&1 &
trivial=$!
trap 'kill $trivial 2>/dev/null || true' EXIT
for _ in $(seq 50); do
  grep -q '"trivial"' "$OUT/trivial.log" 2>/dev/null && break
  sleep 0.1
done
grep -q '"trivial"' "$OUT/trivial.log" || { cat "$OUT/trivial.log"; exit 1; }

printf '%-8s %-5s %10s %10s %10s %8s  %s\n' scenario proto last_held p99_ms broke_at cause out
for sc in $SCENARIOS; do
  for p in $PROTOS; do
    log=$OUT/$sc-$p.jsonl
    "$bin" -url "https://$ADDR" -proto "$p" -scenario "$sc" -rates "$RATES" \
      -duration "$DURATION" -warmup "$WARMUP" -drain "$DRAIN" -keyspace "$KEYSPACE" \
      -max-conns "$MAX_CONNS" -cpuprofile "$OUT/$sc-$p.prof" > "$log" 2> "$OUT/$sc-$p.err" || true
    held=$(jq -r 'select(.held) | .rate' "$log" | tail -1)
    p99=$(jq -r 'select(.held) | .ttlb_ms.p99' "$log" | tail -1)
    broke=$(jq -r 'select(.held | not) | .rate' "$log" | tail -1)
    cause=$(jq -r 'select(.held | not) | .cause' "$log" | tail -1)
    printf '%-8s %-5s %10s %10s %10s %8s  %s\n' "$sc" "$p" "${held:--}" "${p99:--}" "${broke:-none}" "${cause:--}" "$log"
    [ -s "$OUT/$sc-$p.err" ] && sed 's/^/  stderr: /' "$OUT/$sc-$p.err"
  done
done
echo "profiles and stage lines in $OUT (go tool pprof -top $bin $OUT/<scenario>-<proto>.prof)"
