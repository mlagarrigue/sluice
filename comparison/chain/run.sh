#!/usr/bin/env bash
# The local chain of the study (§0): its three roles — db, web, tester — as
# containers on one machine, each on physical cores of its own, the tester
# unable to reach the database. It plays one stage end to end and writes the
# stage's line, its answers checked; it validates the chain, it measures
# nothing that is published (the campaign runs the roles on three VMs).
#
#   comparison/chain/run.sh                                  # unit, h1, 1 000 req/s, 10 s
#   SCENARIO=list RATE=300 PROTO=h2 comparison/chain/run.sh
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

db=$CHAIN-db web=$CHAIN-web tester=$CHAIN-tester
back=$CHAIN-back front=$CHAIN-front

cleanup() {
  docker rm -f "$db" "$web" "$tester" >/dev/null 2>&1 || true
  docker network rm "$back" "$front" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

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

docker run -d --name "$tester" --network "$front" --cpuset-cpus "$TESTER_CPUS" \
  --ulimit nofile="$NOFILE:$NOFILE" \
  --sysctl net.ipv4.ip_local_port_range="1024 65000" --sysctl net.ipv4.tcp_tw_reuse=1 \
  "$TESTER_IMAGE" -url "https://$web:8443" -proto "$PROTO" -scenario "$SCENARIO" \
  -rate "$RATE" -duration "$DURATION" -warmup "$WARMUP" -keyspace "$ROWS" >/dev/null
code=$(docker wait "$tester")
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
  '{cpus: {db: $db, web: $web, tester: $tester}, nofile: $nofile, web_memory: $memory,
    max_connections: $maxconn, web_image: $webimage, db_image: $dbimage,
    cache_hit_pct: $hit, db_steady: $steady}' > "$OUT/chain.json"
cat "$OUT/chain.json"
jq -c '{scenario, proto, rate, held, cause, sent, ok, bad_key, errors, timed_out,
        p50: .ttlb_ms.p50, p99: .ttlb_ms.p99, tls_version, alpn}' "$OUT/stage.jsonl"
echo "chain: stage line in $OUT/stage.jsonl, tester exit $code" >&2
if ! jq -e '.bad_key == 0 and .ok == .sent' "$OUT/stage.jsonl" >/dev/null; then
  echo "chain: answers not all valid" >&2
  exit 1
fi
exit "$code"
