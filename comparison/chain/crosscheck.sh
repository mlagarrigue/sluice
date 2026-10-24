#!/usr/bin/env bash
# The study's generator against two clients that share no code with it
# (§4, §2.3), on one point of the local chain: unit reads at RATE, no
# client round trip, each protocol played by the generator on a fresh
# server, then by the tool on another fresh server, from the tester's
# place. One JSON line per protocol: both p50 and p99, and the tool's gap.
# Local figures: they check the generator, nothing is published.
#
#   comparison/chain/crosscheck.sh                  # h1, h2 by oha; h2, h3 by h2load
#   PAIRS="h3:h2load" RATE=2000 comparison/chain/crosscheck.sh
#
# The tools come from comparison/crosscheck/Dockerfile (sluice-crosscheck).
# They read the same kind of keys — distinct ids drawn from the table, one
# per request — but check no body, and they time differently:
#
# - oha (h1, h2): -q imposes the rate, --latency-correction counts each
#   request from the instant it was due (the coordinated omission
#   corrected, as the generator does), to the end of the answer; a
#   warm-up run of WARMUP first, its figures dropped;
# - h2load (h2, h3): --rps per client imposes the rate, each request timed from
#   the instant it was sent, not the instant it was due, to the end of the
#   answer (--log-file); --warm-up-time drops the first WARMUP, which the
#   log already leaves out.
#
# h2load under h2 measures the tool itself: where oha and the generator
# agree, its gap is its own way of timing, which its h3 gap carries too.
# Both get the generator's connections: C = 2 R (RTT + X) of §3.5, one
# request in flight on each under h1, up to 8 streams under h2 and h3.
set -euo pipefail
cd "$(dirname "$0")/../.."

export SCENARIO=unit RATE=${RATE:-1000} DURATION=${DURATION:-20s} WARMUP=${WARMUP:-5s}
export TOOL_IMAGE=${TOOL_IMAGE:-sluice-crosscheck} ROWS=${ROWS:-100000} RTT=0ms
host=${CHAIN:-studychain}-web
conns=$(awk -v r="$RATE" 'BEGIN {c = 2 * r * 0.025; print (c == int(c)) ? c : int(c) + 1}') # X = 25 ms for unit
if (( RATE % conns != 0 )); then
  echo "crosscheck: RATE $RATE is not a multiple of the $conns connections h2load spreads it over" >&2
  exit 1
fi
# h2load runs its clients on THREADS threads, and five rather than one
# because on one its QUIC client (ngtcp2, OpenSSL's crypto in user space)
# queued: it read well over the generator at the p50 under h3, and over
# its own h2, which is the tool being the bottleneck rather than anything
# measured. On five, both come back to the order of the generator's own
# figures. What is left of the gap there is still the tool's: oha, which
# corrects the omission the same way, lands much closer on the two
# protocols both speak. A run's figures are its own output, never a
# comment here — the directory publishes no measurement of a laptop.
threads=${THREADS:-5}
if (( conns % threads != 0 )); then
  echo "crosscheck: THREADS $threads does not divide the $conns connections" >&2
  exit 1
fi
d=${DURATION%s} w=${WARMUP%s}
n=$(( RATE * (d + w) ))
# The ids each tool requests: distinct, drawn from the table, written in
# the tool's own container.
urls="seq 1 $ROWS | shuf -n $n | sed 's|^|https://$host:8443/orders/|' > /tmp/urls"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"; comparison/chain/chain.sh down' EXIT
comparison/chain/chain.sh up

for pair in ${PAIRS:-h1:oha h2:oha h2:h2load h3:h2load}; do
  proto=${pair%%:*} name=${pair#*:}
  export PROTO=$proto
  comparison/chain/chain.sh web >&2
  code=0
  OUT=$tmp comparison/chain/chain.sh stage >&2 || code=$?
  if [ "$code" = 1 ]; then
    exit 1
  fi
  gen=$(jq -c --argjson code "$code" '{p50: .ttlb_ms.p50, p99: .ttlb_ms.p99, sent, ok, held, valid: ($code == 0)}' "$tmp/stage.jsonl")

  comparison/chain/chain.sh web >&2
  case $name in
    oha)
      flags="-c $conns"
      [ "$proto" = h2 ] && flags="--http2 -c $conns -p 8"
      [ "$proto" = h3 ] && { echo "crosscheck: oha speaks no h3" >&2; exit 1; }
      oha="oha --no-tui --insecure --output-format json --latency-correction -q $RATE --urls-from-file /tmp/urls $flags"
      tool=$(comparison/chain/chain.sh tool sh -ec "$urls; $oha -z ${w}s >/dev/null; $oha -z ${d}s")
      out=$(jq -c '{name: "oha", p50: (.latencyPercentiles.p50 * 1000), p99: (.latencyPercentiles.p99 * 1000),
        rps: .summary.requestsPerSec, success: .summary.successRate, status: .statusCodeDistribution}' <<<"$tool") ;;
    h2load)
      flags="-m 8"
      [ "$proto" = h3 ] && flags="--h3 -m 8"
      [ "$proto" = h1 ] && flags="--h1 -m 1" # no pipelining
      h2load="h2load $flags -t $threads -c $conns --rps $(( RATE / conns )) -D $d --warm-up-time $w -i /tmp/urls --log-file /tmp/log"
      tool=$(comparison/chain/chain.sh tool sh -ec "$urls; $h2load >/dev/null
        awk -F '\t' '{n++; if (\$2 == 200) ok++; print \$3 / 1000} END {print \"#\", n, ok}' /tmp/log | sort -n > /tmp/ms
        grep '^#' /tmp/ms; grep -v '^#' /tmp/ms | awk '{v[NR] = \$1} END {print v[int(NR * 0.5 + 0.5)], v[int(NR * 0.99 + 0.5)]}'")
      read -r _ count okn <<<"$(head -1 <<<"$tool")"
      read -r p50 p99 <<<"$(tail -1 <<<"$tool")"
      out=$(jq -nc --argjson p50 "$p50" --argjson p99 "$p99" --argjson n "$count" --argjson ok "$okn" --argjson d "$d" \
        '{name: "h2load", p50: $p50, p99: $p99, rps: ($n / $d), success: ($ok / $n)}') ;;
  esac
  jq -nc --arg proto "$proto" --argjson rate "$RATE" --argjson conns "$conns" --argjson gen "$gen" --argjson tool "$out" \
    '{proto: $proto, rate: $rate, conns: $conns, generator: $gen, tool: $tool,
      gap_pct: {p50: (100 * ($tool.p50 / $gen.p50 - 1)), p99: (100 * ($tool.p99 / $gen.p99 - 1))}}'
done
