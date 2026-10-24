#!/usr/bin/env bash
# The local chain of the study (§0): its three roles — db, web, tester — as
# containers on one machine, each on physical cores of its own, the tester
# unable to reach the database. It validates the chain and the campaign's
# orchestration; it measures nothing that is published (the campaign runs
# the roles on three VMs).
#
#   comparison/chain/chain.sh up      # networks, the database loaded, the injected network
#   comparison/chain/chain.sh web     # a fresh server: a repetition starts here (§7.1)
#   comparison/chain/chain.sh stage   # one stage, its line merged with the collector's
#   comparison/chain/chain.sh boot    # a fresh server timed: cold start, RSS at rest, time to steady state (§1.4)
#   comparison/chain/chain.sh down    # every container and network removed
#
# run.sh plays the four in a row, for one stage; the orchestrator
# (comparison/loadgen/campaign) calls them itself, one web per repetition
# and as many stages as the repetition has, the same environment for every
# verb of a campaign: what up shaped, stage reads back.
#
# The images are the ones comparison/README.md builds ("In a container"):
# WEB_IMAGE (sluice-study:opt by default) and its WEB_ARGS, whitespace-split
# (a variant of the study's server by its flags), and TESTER_IMAGE
# (sluice-loadgen). The database is PostgreSQL tuned so that nothing fires
# during a stage (§2.2): the table loaded and frozen, checkpoints by time
# only, the write table emptied before each stage; a stage reports the
# cache hit rate and whether a checkpoint or an autovacuum ran during it.
# Limits are the same for web and tester (§4): nofile, and the web
# container's memory.
#
# stage exits 0 for a measurement (held or broken, the line says which), 2
# for a stage the apparatus invalidated (§4: replay the repetition, never
# the stage), 1 for an answer that broke the response contract or a stage
# with no line at all.
set -euo pipefail
cd "$(dirname "$0")/../.."

CHAIN=${CHAIN:-studychain}
WEB_IMAGE=${WEB_IMAGE:-sluice-study:opt}
WEB_ARGS=${WEB_ARGS:-}
TESTER_IMAGE=${TESTER_IMAGE:-sluice-loadgen}
DB_IMAGE=${DB_IMAGE:-postgres:17.11@sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe}
PROTO=${PROTO:-h1}
SCENARIO=${SCENARIO:-unit}
RATE=${RATE:-1000}
DURATION=${DURATION:-10s}
WARMUP=${WARMUP:-3s}
SEED=${SEED:-1}     # the draw, the same for every contender of a round (§2.1)
CONNS=${CONNS:-0}   # the budget C; 0 is 2 R (RTT + X) at the stage's rate (§3.5), a ramp's
ROWS=${ROWS:-100000}
DB_POOL=${DB_POOL:-8}
MAX_CONNECTIONS=${MAX_CONNECTIONS:-400}
NOFILE=${NOFILE:-65536}
WEB_MEMORY=${WEB_MEMORY:-2g}
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
TC_IMAGE=sluice-chain-tools:local # tc, ss, nstat

db=$CHAIN-db web=$CHAIN-web tester=$CHAIN-tester tnet=$CHAIN-tnet sampler=$CHAIN-sampler
watch=$CHAIN-watch burn=$CHAIN-burn
back=$CHAIN-back front=$CHAIN-front

down() {
  docker rm -f "$db" "$web" "$tester" "$sampler" "$tnet" "$watch" "$burn" "$CHAIN-ready" >/dev/null 2>&1 || true
  docker network rm "$back" "$front" >/dev/null 2>&1 || true
}

# tc runs from a small image of its own, in the network namespace of the
# container it shapes: the server's and the generator's images have no tc,
# and need none. Its output is read through docker logs: a short
# container's output can be lost on the way back from docker run.
tc_in() {
  local target=$1 name=$CHAIN-tc-$RANDOM; shift
  docker run -d --name "$name" --network "container:$target" --cap-add NET_ADMIN "$TC_IMAGE" sh -ec "$*" >/dev/null
  local code; code=$(docker wait "$name")
  docker logs "$name" 2>&1
  docker rm "$name" >/dev/null
  return "$code"
}
on() { [ -n "$1" ] && [ "$1" != 0ms ] && [ "$1" != 0 ]; }
psql() { docker exec -i "$db" psql -q -U study -d study -v ON_ERROR_STOP=1 "$@"; }

# The collector (§1.2, §4). web is read from inside its own namespaces —
# its process is PID 1 there — by the tools image, so the measured image
# carries nothing: CPU ticks, RSS and its peak, bytes written, established
# connections and the listen queue, the accept queue's kernel counters,
# the interface's bytes and packets. One line of numbers, in that order.
in_web() {
  local name=$CHAIN-probe-$RANDOM
  docker run -d --name "$name" --pid "container:$web" --network "container:$web" \
    --cap-add SYS_PTRACE "$TC_IMAGE" sh -ec "$*" >/dev/null
  docker wait "$name" >/dev/null
  docker logs "$name" 2>&1
  docker rm "$name" >/dev/null
}
probe_web() {
  in_web '
    set -- $(cut -d" " -f14,15 /proc/1/stat)
    echo $(( $1 + $2 )) \
      $(awk "/^VmRSS/ {r=\$2} /^VmHWM/ {h=\$2} END {print r, h}" /proc/1/status) \
      $(if [ -r /proc/1/io ]; then awk "/^write_bytes/ {print \$2}" /proc/1/io; else echo -1; fi) \
      $(ss -Htn state established "( sport = :8443 )" | wc -l) \
      $(ss -Hltn "( sport = :8443 )" | awk "{q = \$2} END {print q + 0}") \
      $(awk "/eth/ {rb+=\$2; rp+=\$3; tb+=\$10; tp+=\$11} END {print rb, rp, tb, tp}" /proc/net/dev) \
      $(nstat -az TcpExtListenOverflows TcpExtListenDrops TcpExtSyncookiesSent | awk "NR>1 {printf \"%s \", \$2}")'
}
# The tester's cores as the host's scheduler counts them, softirq
# included (netem's work runs there, §1.2): "busy total" in ticks.
host_cpu() {
  awk -v set=",$1," 'index(set, "," substr($1, 4) ",") && $1 ~ /^cpu[0-9]/ {
    t = 0; for (i = 2; i <= NF; i++) t += $i; idle += $5 + $6; total += t
  } END {print total - idle, total}' /proc/stat
}
# The contract's metrics point, read by the generator itself from the
# tester's side of the network, in the stage's protocol (h3 included).
metrics_point() {
  local name=$CHAIN-metrics-$RANDOM
  docker run -d --name "$name" --network "container:$tnet" "$TESTER_IMAGE" \
    -metrics -url "https://$web:8443" -proto "$PROTO" >/dev/null
  docker wait "$name" >/dev/null
  docker logs "$name" 2>/dev/null || echo null
  docker rm "$name" >/dev/null
}
egress_args() {
  local a="" ; on "$RTT" && a+=" delay $RTT"; on "$LOSS" && a+=" loss $LOSS"; echo "$a"
}
ingress_args() {
  local a="" ; on "$LOSS" && a+=" loss $LOSS"; on "$LINK" && a+=" rate $LINK"; echo "$a"
}
egress=$(egress_args) ingress=$(ingress_args)
# stat prints "sent dropped" of one qdisc from `tc -s qdisc show dev <dev>`.
stat() {
  tc_in "$1" "tc -s qdisc show dev $2" | awk '/^ Sent/ {gsub(/[(,]/, " "); print $4, $7; exit}'
}
# qdiscs prints "name container device lossy" for every qdisc up shaped.
qdiscs() {
  local lossy=clean; on "$LOSS" && lossy=lossy
  [ -z "$egress" ] || echo "tester_egress $tnet eth0 $lossy"
  [ -z "$ingress" ] || echo "tester_ingress $tnet ifb0 $lossy"
  if on "$DB_RTT"; then echo "db_egress $db eth0 clean"; fi
}

# Physical cores from lscpu, hyperthread siblings kept together, core 0
# left to the system: a third to the database, a third to web, the rest to
# the tester, which also carries netem's softirq. CORES="db web tester"
# counts them otherwise: a smaller web saturates before the tester's 70 %
# does (§4), which a check of the orchestration on one machine needs.
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
read -r ndb nweb ntester <<<"${CORES:-$third $third $(( n - 2 * third ))}"
if (( ndb < 1 || nweb < 1 || ntester < 1 || ndb + nweb + ntester > n )); then
  echo "chain: CORES=\"$ndb $nweb $ntester\": one core each at least, $n in all" >&2
  exit 1
fi
join() { local IFS=,; echo "$*"; }
pick() { local out=(); for c in "${cores[@]:$1:$2}"; do out+=("${cpus[$c]}"); done; join "${out[@]}"; }
DB_CPUS=$(pick 0 "$ndb")
WEB_CPUS=$(pick "$ndb" "$nweb")
TESTER_CPUS=$(pick $(( ndb + nweb )) "$ntester")

up() {
  down
  trap down ERR
  if ! docker image inspect "$TC_IMAGE" >/dev/null 2>&1; then
    printf 'FROM %s\nRUN apk add --no-cache iproute2\n' "$ALPINE" | docker build -q -t "$TC_IMAGE" - >/dev/null
  fi
  docker network create "$back" >/dev/null
  docker network create "$front" >/dev/null

  docker run -d --name "$db" --network "$back" --cpuset-cpus "$DB_CPUS" \
    -e POSTGRES_USER=study -e POSTGRES_PASSWORD="$DBPASS" -e POSTGRES_DB=study \
    "$DB_IMAGE" \
    -c max_connections="$MAX_CONNECTIONS" -c shared_buffers=256MB \
    -c checkpoint_timeout=1h -c max_wal_size=8GB >/dev/null
  for _ in $(seq 60); do
    docker exec "$db" pg_isready -q -U study -d study 2>/dev/null && break
    sleep 1
  done
  # The table is loaded once, frozen and never written: autovacuum has
  # nothing to do on it, but the load's insert counts can reach the
  # statistics after the VACUUM and fire it a minute later, mid-stage.
  psql <<SQL
CREATE TABLE bench_orders (id bigint PRIMARY KEY, total bigint NOT NULL,
  label text NOT NULL, created timestamptz NOT NULL DEFAULT now())
  WITH (autovacuum_enabled = off);
INSERT INTO bench_orders (id, total, label)
  SELECT g, g * 7, 'order-' || g FROM generate_series(1, $ROWS) g;
VACUUM (FREEZE, ANALYZE) bench_orders;
SQL
  if on "$DB_RTT"; then
    tc_in "$db" "tc qdisc add dev eth0 root netem delay $DB_RTT limit $NETEM_LIMIT"
  fi

  # The tester's network namespace is held by a container of its own,
  # shaped before any generator starts: its first request already crosses
  # the injected network. The sysctls are the namespace's (§3.6).
  docker run -d --name "$tnet" --network "$front" \
    --sysctl net.ipv4.ip_local_port_range="1024 65000" --sysctl net.ipv4.tcp_tw_reuse=1 \
    "$ALPINE" sleep infinity >/dev/null
  if [ -n "$egress" ]; then
    tc_in "$tnet" "tc qdisc add dev eth0 root netem$egress limit $NETEM_LIMIT"
  fi
  if [ -n "$ingress" ]; then
    tc_in "$tnet" "ip link add ifb0 type ifb; ip link set ifb0 up
      tc qdisc add dev ifb0 root netem$ingress limit $NETEM_LIMIT
      tc qdisc add dev eth0 handle ffff: ingress
      tc filter add dev eth0 parent ffff: protocol all u32 match u32 0 0 action mirred egress redirect dev ifb0"
  fi
  trap - ERR
}

# launch starts a fresh server, the previous one removed, and waits for its
# first answer ok — read by the generator's -ready from the tester's side,
# whatever the contender writes in its logs. It prints "t0 ready attempts":
# the instant before docker create and the instant of that answer, in
# nanoseconds. The server starts with both networks attached.
launch() {
  local ready=$CHAIN-ready
  docker rm -f "$web" "$ready" >/dev/null 2>&1 || true
  docker run -d --name "$ready" --network "container:$tnet" "$TESTER_IMAGE" \
    -ready "${READY_WITHIN:-120s}" -url "https://$web:8443" -proto "$PROTO" \
    -scenario "$SCENARIO" -keyspace "$ROWS" >/dev/null
  until [ "$(docker inspect "$ready" --format '{{.State.Running}}' 2>/dev/null)" = true ]; do
    [ "$(docker inspect "$ready" --format '{{.State.Status}}')" = exited ] && break
    sleep 0.05
  done
  local t0; t0=$(date +%s%N)
  # shellcheck disable=SC2086 # WEB_ARGS is a list of flags, split on purpose
  docker create --name "$web" --network "$back" --cpuset-cpus "$WEB_CPUS" \
    --memory "$WEB_MEMORY" --ulimit nofile="$NOFILE:$NOFILE" \
    -e PGHOST="$db" -e PGUSER=study -e PGPASSWORD="$DBPASS" -e PGDATABASE=study \
    -e STUDY_DB_POOL="$DB_POOL" -e STUDY_PROTOCOL="$PROTO" \
    "$WEB_IMAGE" $WEB_ARGS >/dev/null
  docker network connect "$front" "$web"
  docker start "$web" >/dev/null
  local code; code=$(docker wait "$ready")
  local out; out=$(docker logs "$ready" 2>&1)
  docker rm "$ready" >/dev/null
  if [ "$code" != 0 ]; then
    echo "$out" >&2
    docker logs "$web" >&2
    echo "chain: web never answered ok" >&2
    return 1
  fi
  echo "$t0 $(jq -r '"\(.ready_unix_ns) \(.attempts)"' <<<"$out")"
}

# web starts a fresh server: a repetition is one process from its warm-up
# to its last stage (§7.1).
web() {
  launch >/dev/null
  docker logs "$web" 2>&1 | head -1
}

# boot measures a start (§1.4) on the chain up built: from docker create to
# the first answer ok (the cold start, never the first log line), the RSS
# at rest right after it, then the time to steady state — a plateau at RATE
# read in WINDOW windows, steady from the first of three windows whose p99
# hold within ±15 % of one another (max ≤ 1.15 min), within SETTLE (5 min),
# "never" past it. That time runs from the start of the load, to the
# window: the generator's own start in between (load_after_ready_ms, the
# same for every contender) is idle, and warms nothing. One JSON line.
boot() {
  local window=${WINDOW:-5s} settle=${SETTLE:-300s}
  local t0 ready attempts rss
  local l; l=$(launch) || return 1
  read -r t0 ready attempts <<<"$l"
  rss=$(in_web 'awk "/^VmRSS/ {print \$2}" /proc/1/status')
  local OUT=${OUT:-$(mktemp -d)}
  docker rm -f "$tester" >/dev/null 2>&1 || true
  docker run -d --name "$tester" --network "container:$tnet" --cpuset-cpus "$TESTER_CPUS" \
    --ulimit nofile="$NOFILE:$NOFILE" \
    "$TESTER_IMAGE" -url "https://$web:8443" -proto "$PROTO" -scenario "$SCENARIO" \
    -profile plateau -window "$window" -rate "$RATE" -duration "$settle" -warmup 0s \
    -keyspace "$ROWS" -rtt "$RTT" -seed "$SEED" -conns "$CONNS" >/dev/null
  docker wait "$tester" >/dev/null
  docker logs "$tester" > "$OUT/settle.jsonl" 2> "$OUT/tester.err"
  docker rm "$tester" >/dev/null
  local start0; start0=$(jq -r 'select(.window == 0) | .start' "$OUT/settle.jsonl")
  if [ -z "$start0" ]; then
    cat "$OUT/tester.err" >&2
    echo "chain: the plateau wrote no window" >&2
    return 1
  fi
  start0=$(date -d "$start0" +%s%N)
  jq -sc --arg image "$WEB_IMAGE" --arg args "$WEB_ARGS" --argjson t0 "$t0" --argjson ready "$ready" \
    --argjson attempts "$attempts" --argjson rss "$rss" --argjson start0 "$start0" \
    --arg window "$window" --arg settle "$settle" '
    [.[] | select(.window != null)] | sort_by(.window) as $w |
    ($w | map(.ttlb_ms.p99)) as $p |
    ($window | rtrimstr("s") | tonumber) as $ws |
    ([range(0; ($p | length) - 2) | select(($p[.:.+3] | max) <= 1.15 * ($p[.:.+3] | min))] | first) as $i |
    {image: $image, args: $args, scenario: $w[0].scenario, proto: $w[0].proto, rate: $w[0].rate,
     cold_start_ms: (($ready - $t0) / 1e6), ready_attempts: $attempts, rss_kb_rest: $rss,
     settle: {window: $window, within: $settle, p99_ms: $p, held: ($w | map(.held)),
              steady_from_window: $i,
              load_after_ready_ms: (($start0 - $ready) / 1e6),
              steady_ms: (if $i == null then null else $i * $ws * 1000 end),
              never: ($i == null)}}' "$OUT/settle.jsonl"
}

# Before the stage (§2.2): the write table emptied, everything flushed, and
# the counters a checkpoint or an autovacuum would move read.
steady() {
  psql -At <<'SQL'
SELECT (SELECT num_requested FROM pg_stat_checkpointer) || ' ' ||
       (SELECT coalesce(sum(autovacuum_count), 0) FROM pg_stat_user_tables);
SQL
}

stage() {
  local OUT=${OUT:-$(mktemp -d)}
  trap 'docker rm -f "$tester" "$sampler" "$watch" "$burn" >/dev/null 2>&1 || true' EXIT
  docker rm -f "$tester" "$sampler" "$watch" "$burn" >/dev/null 2>&1 || true
  psql <<'SQL'
TRUNCATE comparison_writes;
VACUUM ANALYZE comparison_writes;
CHECKPOINT;
SQL
  local before; before=$(steady)

  # netem's counters are the campaign's since up: the stage reads their
  # difference. The largest queue netem held during the stage is sampled
  # every second.
  declare -A q0
  local name ctr dev lossy
  while read -r name ctr dev lossy; do
    q0[$name]=$(stat "$ctr" "$dev")
  done < <(qdiscs)
  docker run -d --name "$sampler" --network "container:$tnet" "$TC_IMAGE" \
    sh -c 'while :; do tc -s qdisc show; sleep 1; done' >/dev/null

  # Before the stage: web, the tester's cores, the metrics point, web's
  # identity (a crash is the contender's, never an invalidation, §4).
  local w0 c0 m0 id0
  w0=$(probe_web) c0=$(host_cpu "$TESTER_CPUS") m0=$(metrics_point)
  id0=$(docker inspect "$web" --format '{{.State.Pid}} {{.RestartCount}}')
  # web's memory and connections, every second through the stage.
  docker run -d --name "$watch" --pid "container:$web" --network "container:$web" "$TC_IMAGE" \
    sh -c 'while :; do echo $(awk "/^VmRSS/ {print \$2}" /proc/1/status) \
      $(ss -Htn state established "( sport = :8443 )" | wc -l) \
      $(ss -Hltn "( sport = :8443 )" | awk "{q = \$2} END {print q + 0}"); sleep 1; done' >/dev/null
  # FORCE invalidates the stage on purpose, to prove the controls see it: a
  # checkpoint requested halfway, or the tester's cores taken by a burner.
  case ${FORCE:-} in
    checkpoint) (sleep "$(( ${DURATION%s} / 2 + ${WARMUP%s} ))"; psql -c CHECKPOINT) & ;;
    tester-cpu)
      docker run -d --name "$burn" --cpuset-cpus "$TESTER_CPUS" "$ALPINE" \
        sh -c "for i in \$(seq $(tr ',' '\n' <<<"$TESTER_CPUS" | wc -l)); do (while :; do :; done) & done; wait" >/dev/null ;;
    '') ;;
    *) echo "chain: FORCE=$FORCE: want checkpoint or tester-cpu" >&2; exit 1 ;;
  esac

  docker run -d --name "$tester" --network "container:$tnet" --cpuset-cpus "$TESTER_CPUS" \
    --ulimit nofile="$NOFILE:$NOFILE" \
    "$TESTER_IMAGE" -url "https://$web:8443" -proto "$PROTO" -scenario "$SCENARIO" \
    -rate "$RATE" -duration "$DURATION" -warmup "$WARMUP" -keyspace "$ROWS" -rtt "$RTT" \
    -seed "$SEED" -conns "$CONNS" >/dev/null
  local code; code=$(docker wait "$tester")
  local c1 w1 m1 id1 oom
  c1=$(host_cpu "$TESTER_CPUS") w1=$(probe_web) m1=$(metrics_point)
  id1=$(docker inspect "$web" --format '{{.State.Pid}} {{.RestartCount}}')
  oom=$(docker inspect "$web" --format '{{.State.OOMKilled}} {{.State.Running}}')
  docker rm -f "$burn" >/dev/null 2>&1 || true
  local watched backlog
  watched=$(docker logs "$watch" 2>&1 | awk 'NF >= 2 {n++; rss += $1; if ($1 > mr) mr = $1; if ($2 > me) me = $2; if ($3 > mq) mq = $3}
    END {printf "%d %d %d %d", (n ? rss / n : 0), mr, me, mq}')
  docker rm -f "$watch" >/dev/null 2>&1 || true
  backlog=$(docker logs "$sampler" 2>&1 | awk '/backlog/ {sub(/p$/, "", $3); if ($3+0 > m) m = $3+0} END {print m+0}')
  docker rm -f "$sampler" >/dev/null 2>&1 || true

  # netem's own counters (§0, §8.4): with no loss injected, one drop
  # invalidates the stage; with loss, the rate dropped must sit within 10 %
  # of the rate injected — or within three standard deviations of the
  # binomial draw, when that is wider: over 20 000 packets, 10 % of a 1 %
  # loss is 1.4 of them, and a correct setting would fail one stage in six
  # by chance. A loss not applied, or applied twice, still fails. And the
  # queue stays under its limit.
  local netem_ok=yes qd="{}" s sent dropped sent0 dropped0
  while read -r name ctr dev lossy; do
    s=$(stat "$ctr" "$dev")
    [ -z "$s" ] && continue
    read -r sent dropped <<<"$s"
    read -r sent0 dropped0 <<<"${q0[$name]:-0 0}"
    sent=$(( sent - sent0 )) dropped=$(( dropped - dropped0 ))
    qd=$(jq -c --arg k "$name" --argjson s "$sent" --argjson d "$dropped" '. + {($k): {sent: $s, dropped: $d}}' <<<"$qd")
    if [ "$lossy" = lossy ]; then
      if ! awk -v s="$sent" -v d="$dropped" -v w="${LOSS%\%}" 'BEGIN {
          n = s + d; if (n == 0) exit 1; r = 100 * d / n; p = w / 100
          tol = 0.1 * w; sd = 300 * sqrt(p * (1 - p) / n); if (sd > tol) tol = sd
          exit !(r >= w - tol && r <= w + tol) }'; then
        netem_ok="no: $name dropped $dropped of $((sent + dropped)), want ${LOSS} ± max(10 %, 3 sd)"
      fi
    elif [ "$dropped" != 0 ]; then
      netem_ok="no: $name dropped $dropped with no loss injected"
    fi
  done < <(qdiscs)
  if (( backlog >= NETEM_LIMIT )); then
    netem_ok="no: netem's queue reached its limit ($backlog packets)"
  fi
  docker logs "$tester" > "$OUT/stage.jsonl" 2> "$OUT/tester.err"
  if [ ! -s "$OUT/stage.jsonl" ]; then
    cat "$OUT/tester.err" >&2
    echo "chain: the generator wrote no line (exit $code)" >&2
    exit 1
  fi
  local after hit
  after=$(steady)
  hit=$(psql -At <<'SQL'
SELECT round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 3)
FROM pg_stat_database WHERE datname = 'study';
SQL
)

  jq -n --arg db "$DB_CPUS" --arg web "$WEB_CPUS" --arg tester "$TESTER_CPUS" \
    --arg nofile "$NOFILE" --arg memory "$WEB_MEMORY" --arg maxconn "$MAX_CONNECTIONS" \
    --arg webimage "$WEB_IMAGE" --arg webargs "$WEB_ARGS" --arg dbimage "$DB_IMAGE" --arg hit "$hit" \
    --arg steady "$([ "$before" = "$after" ] && echo yes || echo "no: $before -> $after")" \
    --arg rtt "$RTT" --arg dbrtt "$DB_RTT" --arg link "$LINK" --arg loss "$LOSS" \
    --argjson qdiscs "$qd" --argjson backlog "$backlog" --arg netemok "$netem_ok" \
    '{cpus: {db: $db, web: $web, tester: $tester}, nofile: $nofile, web_memory: $memory,
      max_connections: $maxconn, web_image: $webimage, web_args: $webargs, db_image: $dbimage,
      cache_hit_pct: $hit, db_steady: $steady,
      network: {rtt: $rtt, db_rtt: $dbrtt, link: $link, loss: $loss},
      netem: {qdiscs: $qdiscs, max_backlog_pkts: $backlog, ok: $netemok}}' > "$OUT/chain.json"
  local db_extra
  db_extra=$(psql -At <<'SQL'
SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname = 'study') || ' ' ||
       (SELECT max(age(datfrozenxid)) FROM pg_database);
SQL
)

  # One line per stage: the generator's, and what the three roles did
  # while it ran (§1.2), every counter as the difference across the stage;
  # then the controls of §4 that invalidate it — the apparatus, never the
  # contender: the injected network, the tester's CPU past 70 %, a
  # checkpoint or an autovacuum, a connection the tester could not open. A
  # crash of web is the contender's: recorded, never an invalidation.
  local ut0 rss0 hwm0 wb0 est0 q0_ rb0 rp0 tb0 tp0 lo0 ld0 sc0
  local ut1 rss1 hwm1 wb1 est1 q1_ rb1 rp1 tb1 tp1 lo1 ld1 sc1
  local rssmean rssmax estmax qmax busy0 total0 busy1 total1 dbconns frozen
  read -r ut0 rss0 hwm0 wb0 est0 q0_ rb0 rp0 tb0 tp0 lo0 ld0 sc0 <<<"$w0"
  read -r ut1 rss1 hwm1 wb1 est1 q1_ rb1 rp1 tb1 tp1 lo1 ld1 sc1 <<<"$w1"
  read -r rssmean rssmax estmax qmax <<<"$watched"
  read -r busy0 total0 <<<"$c0"
  read -r busy1 total1 <<<"$c1"
  read -r dbconns frozen <<<"$db_extra"
  local seconds cpu_s m
  seconds=$(( ${DURATION%s} + ${WARMUP%s} ))
  cpu_s=$(awk -v t=$(( ut1 - ut0 )) 'BEGIN {printf "%.2f", t / 100}')
  for m in m0 m1; do jq -e . <<<"${!m}" >/dev/null 2>&1 || printf -v "$m" null; done
  jq -c --slurpfile chain "$OUT/chain.json" \
    --argjson web "{\"cpu_s\": $cpu_s, \"rss_kb_mean\": $rssmean, \"rss_kb_max\": $rssmax,
      \"rss_kb_peak\": $hwm1, \"write_bytes\": $( (( wb0 < 0 )) && echo null || echo $(( wb1 - wb0 )) ), \"established_max\": $estmax, \"listen_recvq_max\": $qmax,
      \"rx_bytes\": $(( rb1 - rb0 )), \"rx_packets\": $(( rp1 - rp0 )),
      \"tx_bytes\": $(( tb1 - tb0 )), \"tx_packets\": $(( tp1 - tp0 )), \"listen_overflows\": $(( lo1 - lo0 )),
      \"listen_drops\": $(( ld1 - ld0 )), \"syncookies_sent\": $(( sc1 - sc0 )),
      \"pid_restarts\": \"$id0 -> $id1\", \"oom_killed_running\": \"$oom\"}" \
    --argjson tester "{\"cores_busy_pct\": $(awk -v b="$((busy1 - busy0))" -v t="$((total1 - total0))" 'BEGIN {printf "%.1f", t ? 100 * b / t : 0}'), \"seconds\": $seconds}" \
    --argjson db "{\"connections\": $dbconns, \"max_frozen_xid_age\": $frozen}" \
    --argjson m0 "${m0:-null}" --argjson m1 "${m1:-null}" \
    --arg crash "$([ "$id0" = "$id1" ] && [ "$oom" = "false true" ] && echo no || echo "yes: pid/restarts $id0 -> $id1, oom/running $oom")" \
    '. as $g | $chain[0] as $c |
     ([ (if $c.netem.ok != "yes" then "network: \($c.netem.ok)" else empty end),
        (if $tester.cores_busy_pct > 70 then "tester: its cores \($tester.cores_busy_pct) % busy, over 70 %" else empty end),
        (if $c.db_steady != "yes" then "db: a checkpoint or an autovacuum ran (\($c.db_steady))" else empty end),
        (if ($g.tester // 0) > 0 then "tester: \($g.tester) connections it could not open" else empty end)
      ]) as $invalid |
     $g + {chain: $c, roles: {web: $web, db: $db, tester: $tester},
           metrics: {start: $m0, end: $m1}, crash: $crash,
           valid: ($invalid | length == 0), invalid: $invalid}' \
    "$OUT/stage.jsonl" > "$OUT/stage-merged.jsonl"
  jq -c '{scenario, proto, rate, held, cause, sent, ok, bad_key, errors, timed_out,
          p50: .ttlb_ms.p50, p99: .ttlb_ms.p99, valid, invalid, crash,
          tester_busy: .roles.tester.cores_busy_pct, web_cpu_s: .roles.web.cpu_s, rss_kb_max: .roles.web.rss_kb_max}' "$OUT/stage-merged.jsonl"
  echo "chain: merged line in $OUT/stage-merged.jsonl, tester exit $code" >&2
  # A wrong answer is the contender's and ends the campaign; a stage that
  # broke (errors, requests past their deadline) is a measurement, which
  # the generator's own exit 1 does not tell apart from it.
  if ! jq -e '.bad_key == 0' "$OUT/stage.jsonl" >/dev/null; then
    echo "chain: an answer broke the response contract" >&2
    exit 1
  fi
  if ! jq -e .valid "$OUT/stage-merged.jsonl" >/dev/null; then
    echo "chain: the stage is invalid: $(jq -r '.invalid | join("; ")' "$OUT/stage-merged.jsonl")" >&2
    exit 2
  fi
}

case ${1:-} in
  up) up ;;
  web) web ;;
  stage) stage ;;
  boot) boot ;;
  down) down ;;
  *) echo "usage: chain.sh up|web|stage|boot|down" >&2; exit 1 ;;
esac
