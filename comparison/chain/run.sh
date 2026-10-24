#!/usr/bin/env bash
# The local chain of the study (§0): its three roles — db, web, tester — as
# containers on one machine, each on physical cores of its own, the tester
# unable to reach the database. It plays one stage end to end and writes the
# stage's line, its answers checked; it validates the chain, it measures
# nothing that is published (the campaign runs the roles on three VMs).
#
#   comparison/chain/run.sh                                  # unit, h1, 1 000 req/s, 10 s
#   SCENARIO=list RATE=300 PROTO=h2 comparison/chain/run.sh
#   RTT=20ms LOSS=1% LINK=100mbit comparison/chain/run.sh     # the network of §3.4, §8.4
#
# The images are the ones comparison/README.md builds ("In a container"):
# WEB_IMAGE (sluice-study:opt by default) and TESTER_IMAGE (sluice-loadgen).
# The database is PostgreSQL tuned so that nothing fires during a stage
# (§2.2): the table loaded and frozen, checkpoints by time only, the write
# table emptied before the stage; the script reports the cache hit rate and
# whether a checkpoint or an autovacuum ran during it. Limits are the same
# for web and tester (§4): nofile, and the web container's memory. Every
# container and network it made is removed on exit, error or not.
set -euo pipefail
cd "$(dirname "$0")/../.."

CHAIN=${CHAIN:-studychain}
WEB_IMAGE=${WEB_IMAGE:-sluice-study:opt}
TESTER_IMAGE=${TESTER_IMAGE:-sluice-loadgen}
DB_IMAGE=${DB_IMAGE:-postgres:17.11@sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe}
PROTO=${PROTO:-h1}
SCENARIO=${SCENARIO:-unit}
RATE=${RATE:-1000}
DURATION=${DURATION:-10s}
WARMUP=${WARMUP:-3s}
ROWS=${ROWS:-100000}
DB_POOL=${DB_POOL:-8}
MAX_CONNECTIONS=${MAX_CONNECTIONS:-400}
NOFILE=${NOFILE:-65536}
WEB_MEMORY=${WEB_MEMORY:-2g}
OUT=${OUT:-$(mktemp -d)}
DBPASS=study # a throwaway database on a private network

# The network of §3.4, never on web: the whole client round trip on the
# tester's egress, the whole web↔db one on the database's; LINK caps the
# rate web → tester and LOSS drops in both directions, the inbound one on
# an ifb device at the tester's entrance (§8.4). Empty or 0ms injects none.
RTT=${RTT:-0ms}
DB_RTT=${DB_RTT:-1ms}
LINK=${LINK:-}
LOSS=${LOSS:-}
NETEM_LIMIT=1000000 # §0: netem's default queue of 1 000 drops at ~50 000 packets/s
ALPINE=alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f
TC_IMAGE=sluice-chain-tc:local

db=$CHAIN-db web=$CHAIN-web tester=$CHAIN-tester tnet=$CHAIN-tnet sampler=$CHAIN-sampler
back=$CHAIN-back front=$CHAIN-front

cleanup() {
  docker rm -f "$db" "$web" "$tester" "$sampler" "$tnet" >/dev/null 2>&1 || true
  docker network rm "$back" "$front" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

# tc runs from a small image of its own, in the network namespace of the
# container it shapes: the server's and the generator's images have no tc,
# and need none. Its output is read through docker logs: a short
# container's output can be lost on the way back from docker run.
if ! docker image inspect "$TC_IMAGE" >/dev/null 2>&1; then
  printf 'FROM %s\nRUN apk add --no-cache iproute2\n' "$ALPINE" | docker build -q -t "$TC_IMAGE" - >/dev/null
fi
tc_in() {
  local target=$1 name=$CHAIN-tc-$RANDOM; shift
  docker run -d --name "$name" --network "container:$target" --cap-add NET_ADMIN "$TC_IMAGE" sh -ec "$*" >/dev/null
  local code; code=$(docker wait "$name")
  docker logs "$name" 2>&1
  docker rm "$name" >/dev/null
  return "$code"
}
on() { [ -n "$1" ] && [ "$1" != 0ms ] && [ "$1" != 0 ]; }
egress_args() {
  local a="" ; on "$RTT" && a+=" delay $RTT"; on "$LOSS" && a+=" loss $LOSS"; echo "$a"
}
ingress_args() {
  local a="" ; on "$LOSS" && a+=" loss $LOSS"; on "$LINK" && a+=" rate $LINK"; echo "$a"
}
# stat prints "sent dropped" of one qdisc from `tc -s qdisc show dev <dev>`.
stat() {
  tc_in "$1" "tc -s qdisc show dev $2" | awk '/^ Sent/ {gsub(/[(,]/, " "); print $4, $7; exit}'
}

# Physical cores from lscpu, hyperthread siblings kept together, core 0
# left to the system: a third to the database, a third to web, the rest to
# the tester, which also carries netem's softirq once CMP-19 adds it.
declare -A cpus
while IFS=, read -r core cpu; do
  cpus[$core]+="${cpus[$core]:+,}$cpu"
done < <(lscpu -p=CORE,CPU | grep -v '^#')
mapfile -t cores < <(printf '%s\n' "${!cpus[@]}" | sort -n | tail -n +2)
n=${#cores[@]}
if (( n < 3 )); then
  echo "chain: $n physical cores past core 0, three roles need three" >&2
  exit 1
fi
third=$(( n / 3 ))
join() { local IFS=,; echo "$*"; }
pick() { local out=(); for c in "${cores[@]:$1:$2}"; do out+=("${cpus[$c]}"); done; join "${out[@]}"; }
DB_CPUS=$(pick 0 "$third")
WEB_CPUS=$(pick "$third" "$third")
TESTER_CPUS=$(pick $(( 2 * third )) $(( n - 2 * third )))

docker network create "$back" >/dev/null
docker network create "$front" >/dev/null

docker run -d --name "$db" --network "$back" --cpuset-cpus "$DB_CPUS" \
  -e POSTGRES_USER=study -e POSTGRES_PASSWORD="$DBPASS" -e POSTGRES_DB=study \
  "$DB_IMAGE" \
  -c max_connections="$MAX_CONNECTIONS" -c shared_buffers=256MB \
  -c checkpoint_timeout=1h -c max_wal_size=8GB >/dev/null
psql() { docker exec -i "$db" psql -q -U study -d study -v ON_ERROR_STOP=1 "$@"; }
for _ in $(seq 60); do
  docker exec "$db" pg_isready -q -U study -d study 2>/dev/null && break
  sleep 1
done
psql <<SQL
CREATE TABLE bench_orders (id bigint PRIMARY KEY, total bigint NOT NULL,
  label text NOT NULL, created timestamptz NOT NULL DEFAULT now());
INSERT INTO bench_orders (id, total, label)
  SELECT g, g * 7, 'order-' || g FROM generate_series(1, $ROWS) g;
VACUUM (FREEZE, ANALYZE) bench_orders;
SQL
if on "$DB_RTT"; then
  tc_in "$db" "tc qdisc add dev eth0 root netem delay $DB_RTT limit $NETEM_LIMIT"
fi

docker run -d --name "$web" --network "$back" --cpuset-cpus "$WEB_CPUS" \
  --memory "$WEB_MEMORY" --ulimit nofile="$NOFILE:$NOFILE" \
  -e PGHOST="$db" -e PGUSER=study -e PGPASSWORD="$DBPASS" -e PGDATABASE=study \
  -e STUDY_DB_POOL="$DB_POOL" -e STUDY_PROTOCOL="$PROTO" \
  "$WEB_IMAGE" >/dev/null
docker network connect "$front" "$web"
for _ in $(seq 60); do
  docker logs "$web" 2>&1 | grep -q '^sluice-study:' && break
  sleep 0.5
done
docker logs "$web" 2>&1 | head -1

# Before the stage (§2.2): the write table emptied, everything flushed, and
# the counters a checkpoint or an autovacuum would move read.
steady() {
  psql -At <<'SQL'
SELECT (SELECT num_requested FROM pg_stat_checkpointer) || ' ' ||
       (SELECT coalesce(sum(autovacuum_count), 0) FROM pg_stat_user_tables);
SQL
}
psql <<'SQL'
TRUNCATE comparison_writes;
VACUUM ANALYZE comparison_writes;
CHECKPOINT;
SQL
before=$(steady)

# The tester's network namespace is held by a container of its own, shaped
# before the generator starts: its first request already crosses the
# injected network. The sysctls are the namespace's (§3.6).
docker run -d --name "$tnet" --network "$front" \
  --sysctl net.ipv4.ip_local_port_range="1024 65000" --sysctl net.ipv4.tcp_tw_reuse=1 \
  "$ALPINE" sleep infinity >/dev/null
egress=$(egress_args) ingress=$(ingress_args)
if [ -n "$egress" ]; then
  tc_in "$tnet" "tc qdisc add dev eth0 root netem$egress limit $NETEM_LIMIT"
fi
if [ -n "$ingress" ]; then
  tc_in "$tnet" "ip link add ifb0 type ifb; ip link set ifb0 up
    tc qdisc add dev ifb0 root netem$ingress limit $NETEM_LIMIT
    tc qdisc add dev eth0 handle ffff: ingress
    tc filter add dev eth0 parent ffff: protocol all u32 match u32 0 0 action mirred egress redirect dev ifb0"
fi
# The largest queue netem held during the stage, sampled every second.
docker run -d --name "$sampler" --network "container:$tnet" "$TC_IMAGE" \
  sh -c 'while :; do tc -s qdisc show; sleep 1; done' >/dev/null

docker run -d --name "$tester" --network "container:$tnet" --cpuset-cpus "$TESTER_CPUS" \
  --ulimit nofile="$NOFILE:$NOFILE" \
  "$TESTER_IMAGE" -url "https://$web:8443" -proto "$PROTO" -scenario "$SCENARIO" \
  -rate "$RATE" -duration "$DURATION" -warmup "$WARMUP" -keyspace "$ROWS" -rtt "$RTT" >/dev/null
code=$(docker wait "$tester")
backlog=$(docker logs "$sampler" 2>&1 | awk '/backlog/ {sub(/p$/, "", $3); if ($3+0 > m) m = $3+0} END {print m+0}')
docker rm -f "$sampler" >/dev/null 2>&1 || true

# netem's own counters (§0, §8.4): with no loss injected, one drop
# invalidates the stage; with loss, the rate dropped must sit within 10 %
# of the rate injected — or within three standard deviations of the
# binomial draw, when that is wider: over 20 000 packets, 10 % of a 1 %
# loss is 1.4 of them, and a correct setting would fail one stage in six
# by chance. A loss not applied, or applied twice, still fails. And the
# queue stays under its limit.
netem_ok=yes
qdiscs="{}"
check_qdisc() { # name container device lossy
  local s; s=$(stat "$2" "$3")
  [ -z "$s" ] && return 0
  read -r sent dropped <<<"$s"
  qdiscs=$(jq -c --arg k "$1" --argjson s "$sent" --argjson d "$dropped" '. + {($k): {sent: $s, dropped: $d}}' <<<"$qdiscs")
  if [ "$4" = lossy ]; then
    local want=${LOSS%\%}
    if ! awk -v s="$sent" -v d="$dropped" -v w="$want" 'BEGIN {
        n = s + d; r = 100 * d / n; p = w / 100
        tol = 0.1 * w; sd = 300 * sqrt(p * (1 - p) / n); if (sd > tol) tol = sd
        exit !(r >= w - tol && r <= w + tol) }'; then
      netem_ok="no: $1 dropped $dropped of $((sent + dropped)), want ${LOSS} ± max(10 %, 3 sd)"
    fi
  elif [ "$dropped" != 0 ]; then
    netem_ok="no: $1 dropped $dropped with no loss injected"
  fi
}
lossy=clean; on "$LOSS" && lossy=lossy
[ -n "$egress" ] && check_qdisc tester_egress "$tnet" eth0 "$lossy"
[ -n "$ingress" ] && check_qdisc tester_ingress "$tnet" ifb0 "$lossy"
on "$DB_RTT" && check_qdisc db_egress "$db" eth0 clean
if (( backlog >= NETEM_LIMIT )); then
  netem_ok="no: netem's queue reached its limit ($backlog packets)"
fi
docker logs "$tester" > "$OUT/stage.jsonl" 2> "$OUT/tester.err"
after=$(steady)
hit=$(psql -At <<'SQL'
SELECT round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 3)
FROM pg_stat_database WHERE datname = 'study';
SQL
)

jq -n --arg db "$DB_CPUS" --arg web "$WEB_CPUS" --arg tester "$TESTER_CPUS" \
  --arg nofile "$NOFILE" --arg memory "$WEB_MEMORY" --arg maxconn "$MAX_CONNECTIONS" \
  --arg webimage "$WEB_IMAGE" --arg dbimage "$DB_IMAGE" --arg hit "$hit" \
  --arg steady "$([ "$before" = "$after" ] && echo yes || echo "no: $before -> $after")" \
  --arg rtt "$RTT" --arg dbrtt "$DB_RTT" --arg link "$LINK" --arg loss "$LOSS" \
  --argjson qdiscs "$qdiscs" --argjson backlog "$backlog" --arg netemok "$netem_ok" \
  '{cpus: {db: $db, web: $web, tester: $tester}, nofile: $nofile, web_memory: $memory,
    max_connections: $maxconn, web_image: $webimage, db_image: $dbimage,
    cache_hit_pct: $hit, db_steady: $steady,
    network: {rtt: $rtt, db_rtt: $dbrtt, link: $link, loss: $loss},
    netem: {qdiscs: $qdiscs, max_backlog_pkts: $backlog, ok: $netemok}}' > "$OUT/chain.json"
cat "$OUT/chain.json"
jq -c '{scenario, proto, rate, held, cause, sent, ok, bad_key, errors, timed_out,
        p50: .ttlb_ms.p50, p99: .ttlb_ms.p99, tls_version, alpn}' "$OUT/stage.jsonl"
echo "chain: stage line in $OUT/stage.jsonl, tester exit $code" >&2
if ! jq -e '.bad_key == 0 and .tester == 0 and (.ok == .sent or ($loss | length > 0))' --arg loss "$LOSS" "$OUT/stage.jsonl" >/dev/null; then
  echo "chain: answers not all valid" >&2
  exit 1
fi
if [ "$netem_ok" != yes ]; then
  echo "chain: the injected network invalidates the stage: $netem_ok" >&2
  exit 1
fi
# The generator exits 1 for a stage that is not a measurement, which a
# stage under injected loss rarely is; the chain itself held.
exit 0
