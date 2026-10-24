# comparison — the study's own apparatus

This directory holds what the comparison study runs, starting with the
Sluice.go server it measures. It is **not** a result: nothing measured on a
developer's machine is published here, not even a ratio. The figures of the
study come from the campaign the study's protocol describes, on the machines
it names, and go to `docs/COMPARISON.md` when they exist.

## `sluice/` — the server of the internal study

One binary serves the four scenarios of level 1 in the reference
configuration **R** — `web` over `net/http`, HTTP/1.1 with TLS,
`database/postgres`, a `gateway` of 64 keys or 2 ms, buffered responses, no
compression — and every axis of the internal study is a flag:

| Axis | Flag | Today |
|---|---|---|
| A — transport | `-transport nethttp-h1` (R), `nethttp-h2`, `httpstream-h1`, `httpstream-h2`, `httpstream-h3` | all five; `httpstream-h3` listens on UDP (`net/quic`), TLS always |
| B — driver | `-driver sluice` (R), `pgx` | both, under the same gateway and connection budget |
| C — grouping | `-gateway-size 64 -gateway-within 2ms`, `-gateway-eager` | any positive values; eager: no window, batches form while the pipeline works |
| D — transport batch | `-max-requests-per-batch`, `-max-requests-per-conn` (`-1` unbounded) | httpstream transports only |
| E — response | `-response buffered` (R), `stream` | the list is written as the store delivers it, one flush per batch of rows, no `Content-Length`; with `-compression`, the encoder is flushed per batch too; the other scenarios stay buffered |
| F — compression | `-compression none` (R), `gzip1`, `gzip6`, `deflate6` | stdlib encoders on the buffered body when the client accepts it (`deflate` in its zlib wrapper, RFC 9110 §8.4.1.2); `Vary: Accept-Encoding` |
| — JSON encoder (§2.4, optimised version) | `-json std` (R), `append` | the list document written by hand (`strconv.AppendInt`, the same escaping as `encoding/json`), buffered or streamed, compressed like the others; the other scenarios stay under `encoding/json` |

A variant the code does not implement is refused before the server listens,
with the axis that will add it. Nothing falls back to R silently: a flag
that did would publish R's figure under another name.

The scenarios, and the response contract every dynamic answer honours —
the key asked for is in the body, so a load generator can check every
response under load:

| Scenario | Request | Key in the answer |
|---|---|---|
| static small | `GET /static` | — (a fixed ~1 KB document) |
| unit read | `GET /orders/{id}` | `id` |
| list | `GET /orders?from=K&limit=1000` | `from`; `items` strictly increasing above it |
| write | `POST /orders` with `{"key":K,"total":T}` | `key` |

### The TLS every contender runs under

What a TLS handshake costs weighs more than the framework once a tenth of
the connections are fresh (§3.6), so it is fixed, and published, for every
contender alike (§4 "TLS fixed"):

- TLS 1.3 only, on an ECDSA P-256 certificate (`-cert`/`-key`, or one
  generated at startup for a local run). TLS 1.3's cipher suites are the
  stack's own, which Go's `crypto/tls` does not let a configuration set;
  the generator publishes the one negotiated (`tls_suite`), and a
  contender that negotiates another is compared knowing it.
- ALPN names the one protocol of the run: `http/1.1`, `h2` or `h3`.
- Session resumption off in R: a fresh connection is a new visitor, who
  holds no ticket. `-tls-resume` on the server issues tickets (Go's
  `crypto/tls` issues them by default; R turns that off), and
  `-tls-resume` on the generator keeps the session cache that uses them;
  `resumed` counts the handshakes that did.
- In clear (level 2, "TLS off"): HTTP/1.1 only, `-tls=false` on the server
  and an `http://` URL for the generator; h2c is no configuration of the
  study, and QUIC is always TLS.

The server writes this at startup, `sluice-study: nethttp-h1 on
127.0.0.1:8443 (TLS 1.3+, ECDSA P-256 certificate, ALPN http/1.1,
resumption off)`; the generator writes what it negotiated on every stage
line.

The mixed scenario (80 % unit, 15 % list, 5 % write) is the generator's
business. Unit reads go through the gateway and leave as one `= ANY($1)`
per batch; under `httpstream` the requests of one transport batch are
submitted to the gateway together. Lists and writes take a connection from
a small pool (`-db-conns`). Writes land in `comparison_writes`, a table no
read ever touches, created at startup with autovacuum off; a campaign
truncates it between repetitions.

### Running it

It is its own Go module, like `benchmarks/`: it replaces the root with the working tree, so a run measures the commit it
is run from, and the root's `go test ./...` never sees it.

```
export SLUICE_PG=host:5432 SLUICE_PG_USER=sluice SLUICE_PG_DB=sluice SLUICE_PG_PASSWORD=…
go run ./internal/bench/cmd/pgfixtures        # bench_orders, 100 000 rows
go run -C comparison/sluice . -h              # every flag, R as the default
go run -C comparison/sluice . -listen 127.0.0.1:8443
curl -k https://127.0.0.1:8443/orders/42
```

Without `-cert`/`-key` the server generates a self-signed ECDSA P-256
certificate at startup; the campaign passes a fixed one.

### In a container

```
docker build -f comparison/sluice/Dockerfile --target opt -t sluice-study:opt .   # from the repository root
docker build -f comparison/sluice/Dockerfile --target doc -t sluice-study:doc .
docker build -f comparison/loadgen/Dockerfile -t sluice-loadgen .
docker run -d --name study --network <net> -e PGHOST=… -e PGUSER=… -e PGPASSWORD -e PGDATABASE=… sluice-study:opt
docker run --rm --network <net> sluice-loadgen -conformance https://<reference>:8443 -url https://study:8443
```

Two versions of the server's image (§1.4, §2.4): `doc` is what Docker's
guide for Go writes, one stage on the Go image (1.6 GB); `opt` is the
static binary alone on a distroless image, run as a user with no rights
(34 MB). Both are configured by the contract's variables alone and listen
on `0.0.0.0:8443`, TCP and UDP; the generator's image is the static
binary on the same base. Every base is pinned by digest, as
`interop/Dockerfile`'s, and Dependabot moves the pins. Both server images
answer like `go run` does: `-conformance` against it, 300 requests under
gzip, conformant.

A short container's output can be lost on the way back from `docker run`
on some engines; `docker run -d`, `docker wait`, then `docker logs` reads
it whole.

What a developer's machine under WSL2 offers the local chain (§0),
probed on a 6.18 kernel with Docker 29: `--cpuset-cpus` holds (a container
on core 2 sees one CPU, `cpuset.cpus.effective` 2); `netem` works from a
container started with `--cap-add NET_ADMIN` as root (a 20 ms delay on
`eth0` reads +20 ms on a TCP connect); so do the `ingress` qdisc, `u32`,
`mirred` and an `ifb` device carrying `netem` loss, which the inbound
direction needs; a sibling container on the same Docker network is
reachable by name. The kernel loads these modules on first use.

### The local chain

```
comparison/chain/run.sh                                  # unit, h1, 1 000 req/s, 10 s
SCENARIO=mixed PROTO=h2 RATE=500 comparison/chain/run.sh
```

The study's three roles (§0) — db, web, tester — as containers on one
machine, to validate the chain before the campaign runs it on three VMs;
what it measures is never published, not even a ratio. It reads the
physical cores from `lscpu`, keeps hyperthread siblings together, leaves
core 0 to the system, and gives a third to each role (the tester the
rest, since it will also carry netem's softirq). Two Docker networks keep
the tester from reaching the database: db and web on one, web and tester
on the other. PostgreSQL is tuned so that nothing fires during a stage
(§2.2): `bench_orders` loaded, frozen (`VACUUM FREEZE`) and left out of
autovacuum, which its load's statistics would otherwise fire a minute
later, mid-stage; checkpoints by time only (`checkpoint_timeout` 1 h,
`max_wal_size` 8 GB), `max_connections` above any pool, and before the
stage the write table emptied, vacuumed and a checkpoint taken. Web and
tester get the same `nofile`; web a memory limit; the tester a wide port range and
`tcp_tw_reuse` (§3.6). The script plays one stage with the generator's
image against the server's (`WEB_IMAGE`, `TESTER_IMAGE`; `SCENARIO`,
`PROTO`, `RATE`, `DURATION`…), writes `chain.json` — the cores of each
role, the limits, the images, the cache hit rate and whether a checkpoint
or an autovacuum ran during the stage (`db_steady`) — and the stage's
line, and fails unless every answer was valid. Everything it created is
removed on exit, error or not. On a developer's machine it holds under
h1, h2 and h3, the mixed scenario included, with the cache at 99.9 % and
the database steady.

`run.sh` is four verbs of `comparison/chain/chain.sh` in a row, which a
campaign calls itself: `up` (the networks, the database loaded, the
injected network), `web` (a fresh server, the previous one removed: a
repetition starts there, §7.1), `stage` (one stage on that server, as
many times as the repetition has stages), `down`. Every verb reads the
same environment, so `stage` reads back the network `up` shaped. A
campaign's stage exits 0 for a measurement, held or broken, since a ramp
breaks by design past the capacity; `run.sh` alone also fails on any
answer not ok. The generator's connection budget is the C = 2 R (RTT + X)
of §3.5 at the stage's rate (`-conns 0`), as in a ramp. `CORES="db web
tester"` counts the physical cores of each role otherwise than by thirds.

```
RTT=20ms SCENARIO=static RATE=200 comparison/chain/run.sh       # latency felt (§3.4)
LOSS=1% LINK=100mbit RTT=20ms comparison/chain/run.sh           # §8.4's lossy link
```

The network of §3.4 is injected with `netem`, never on web: the whole
client round trip on the tester's egress (`RTT`, 0 by default, the
capacity condition), the whole web↔db one on the database's (`DB_RTT`,
1 ms by default); `LINK` caps the rate web → tester and `LOSS` drops in
both directions, the inbound one on an `ifb` device at the tester's
entrance. Every queue is `limit 1000000` (§0). `tc` runs from a small
Alpine image of its own (`iproute2`), in the network namespace of the
container it shapes, so neither the server's image nor the generator's
carries it; the tester's namespace is held by a container of its own,
shaped before the generator starts, so that its first request already
crosses the injected network, and the generator is told the round trip
(`-rtt`). After the stage, `netem`'s counters decide (`chain.json`,
`netem`): with no loss injected, one drop invalidates the stage; with
loss, each direction's rate must sit within 10 % of the one injected, or
within three standard deviations of the binomial draw when that is wider
(over 20 000 packets, 10 % of 1 % is 1.4 of them: a correct setting would
fail one stage in six); the largest queue, sampled every second, must stay
under its limit. Measured here: a 20 ms RTT moves the static p50 from
0.37 ms to 20.39 ms; a 1 % loss reads 1.08 % and 1.07 % on the two
directions over 14 000 packets each, the queue at most 22 packets.

At the end of the stage the script writes `stage-merged.jsonl`: the
generator's line, and what the three roles did while it ran (§1.2), every
counter the difference across the stage. web is read from inside its own
namespaces by the tools image (its server is PID 1 there): CPU seconds,
RSS mean, max and peak, bytes written, established connections and the
listen queue at most, the interface's bytes and packets, the accept
queue's kernel counters (`ListenOverflows`, `ListenDrops`,
`SyncookiesSent`), its PID, restarts and OOM; the database: connections
and the oldest unfrozen transaction; the tester: its cores' busy share as
the host counts it, softirq included; and the contract's metrics point
at the start and the end, read by the generator itself (`loadgen
-metrics`, in the stage's protocol). Then the controls of §4, all on the
apparatus, never the contender: the injected network, the tester's cores
past 70 %, a checkpoint or an autovacuum during the stage, a connection
the tester could not open. `valid` and `invalid` (the causes) are on the
line; the exit status is 0 for a valid stage, 2 for an invalid one, 1 for
an answer that was wrong. A crash of web is the contender's (§1.2): it is
recorded (`crash`), never an invalidation. `FORCE=checkpoint` and
`FORCE=tester-cpu` invalidate a stage on purpose, a checkpoint requested
halfway or the tester's cores taken by a burner; both come out invalid
with their cause, the generator's own line still reading `held`. Not
collected on this kernel: `write_bytes`, which needs per-task I/O
accounting (`/proc/<pid>/io`), absent from WSL2's (`null`).

### A campaign on the chain

```
CORES="2 1 4" go run -C comparison/loadgen ./campaign -matrix ../chain/trial.json -out /tmp/trial
go run -C comparison/loadgen . -verdicts /tmp/trial/rounds.jsonl -compare R:httpstream-h1
```

`loadgen/campaign` plays a matrix on the chain (§3.3, §6, §7.1, §7.2,
§7.6). The matrix is a JSON file: the contenders (an image, and the flags
that make a variant of the study's server), the families they meet in (a
scenario × a protocol, the survey's first rate, the scenario's absolute
fixed points), and the protocol's numbers, each defaulting to the
protocol's value — 21 rounds, 40 at most, 5 more at a time, 7 full rounds,
a minute of warm-up, stages of 30 s with their first 5 s not counted,
fixed points of 90 s, 5 s of drain, capacity at 0 ms and fixed points at
20 ms of client round trip, 1 ms to the database. Per family, in this
order:

- **survey** (§3.3): per contender, in an order drawn, a repetition of
  its own from the first rate up by ×1.25 until a stage breaks, which
  gives the last stage held, L, and the first broken, H. One broken at its
  first stage starts again four times lower;
- **capacity** (§7.6): rounds, each in an order drawn at random; in each,
  every contender plays one repetition — a fresh server (`chain.sh web`),
  its warm-up at the first stage's rate, then the common grid
  `100 × 1.05^k` from its largest point at or below 0.9 L to its smallest
  at or above 1.1 H, stopped at the first broken stage. The capacity is
  the stage before it; no broken stage is replayed. A ramp broken at its
  first stage, or held to its last, is censored: a new survey widens the
  window and the repetition is played again;
- **fixed points** (§7.1, §7.6), on a chain rebuilt at the points' round
  trip: a contender is measured at the points at most 0.8 × the low bound
  of its capacity's interval (§3.2), and the designated point is the
  highest one eligible for all. The full rounds are drawn among the rounds
  after the first and written down; in those, every eligible point, in
  one process, ascending; in the others, the designated point alone.

A stage the apparatus invalidates (exit 2 of `chain.sh stage`, §4)
replays its whole repetition, a fresh server again, never the stage; five
invalidated in a row stop the campaign, which is then the chain's to fix.
A wrong answer stops it at once. A stage is lengthened until it counts
the requests its percentile needs (§7.3: 1 000 for a capacity stage,
10 000 for a fixed point, whose p99.9 is published only from those that
did), and refused if the keyspace (`rows`) cannot hold it without a
repeated key (§2.1); a warm-up is cut into stages the keyspace holds. The
adaptive stop (§7.2) runs on the width of the intervals, never on a
test's result: past the minimum, while the 95 % interval of one
contender's median covers more than one step on either side (capacity)
or reaches past ±5 % (the designated point's p99), everyone plays five
rounds more, up to the maximum, where the one still wide is published
unstable. The generator's draw is the same for every contender of a
round.

It writes `matrix.json` (the matrix as played, defaults filled in),
`plan.jsonl` (every draw: survey order, each round's order, the full
rounds, from the matrix's `seed`), `raw.jsonl` (every stage's merged
line, with its family, measure, round, contender, attempt and kind),
`rounds.jsonl` (what `-verdicts` reads) and `summary.json` (windows,
medians and intervals, the unstable, the points eligible, the designated
one, the descriptive points with their median and the interval from
their lowest to their highest value, 98 % at 7 rounds). A matrix with
fewer rounds than a verdict needs must say `"trial": true`: its rounds
carry the mark, its summary says it is not publishable, and `-verdicts`
refuses them for want of rounds.

`trial.json` is the brief's reduced matrix: R against `httpstream-h1`
on the 1 000-row list under h1, three rounds, short stages. Played on a
developer's machine with web on one core (`CORES="2 1 4"`: with a third
each, the tester's cores pass 70 % before web breaks), it runs end to end
in 23 minutes, 81 stages, none invalidated, and nothing it measures is a
figure.

### An image, built and started

```
comparison/chain/image.sh comparison/sluice/Dockerfile opt
SCENARIO=unit RATE=1000 TOUCH=main.go comparison/chain/image.sh comparison/sluice/Dockerfile doc
```

The containerisation of one image (§1.4), one JSON line. The build
context is the committed tree (`git archive HEAD`) in a directory of its
own, so the working tree is never touched and the build is the one a CI
would run; the image is tagged `study-measured:<target>`. It reports:

- **size**: the image's bytes as Docker counts them, the gzip of `docker
  save` (no registry here; the size a registry stores is of that order),
  and its layers;
- **build**: from nothing (`--no-cache`, the bases already pulled), then
  after one line of code added to `TOUCH` (under the Dockerfile's
  directory, `main.go` by default), the cache of the first build warm;
- **cold start**, by `chain.sh boot` on the chain `up` built: from the
  instant before `docker create` — the server then gets both networks and
  starts — to its **first answer ok**, never its first log line. The
  generator's `-ready` polls from the tester's side every 10 ms, a fresh
  connection each time, with the scenario's requests, their keys checked;
  a name not resolved yet or a port not listening is one more attempt.
  `chain.sh web` waits for a server the same way, whatever the contender
  writes in its logs;
- **RSS at rest**, read in the server's namespaces right after that
  answer;
- **time to steady state**: a plateau at `RATE` (unit, 1 000 req/s by
  default) read in 5 s windows (`WINDOW`), steady from the first of three
  windows whose p99 hold within ±15 % of one another, at most 5 min
  (`SETTLE`), `never` past it. It is counted from the start of the load:
  the generator's own start in between (`load_after_ready_ms`, the same
  for every contender) is idle and warms nothing. Its resolution is one
  window.

### Validating it

```
go test -C comparison/sluice ./...
```

needs the PostgreSQL the variables name and skips without it. It runs R and
every implemented variant in-process, sends the same sample of keys drawn
without repetition (one seed), 64 requests in flight, and checks for each
answer that the key is the one asked for, that the normalised JSON — parsed
and re-encoded, so key order and whitespace cannot fail it — matches R's
with the same status and content type, and that every write landed. A
variant that answers differently from R is a bug to fix before it is
measured, never a figure.

The benchmarks in the same file are a load for a CPU profile, not a
measurement: client and server share the process and the machine.

```
go test -C comparison/sluice -run '^$' -bench . -cpuprofile cpu.out
go tool pprof -top cpu.out
```

## The contract every contender honours

Every server the study measures, Sluice.go and the others alike, keeps the
same outside: whatever it is built with, the campaign starts it, loads it
and reads it the same way. What it must honour:

- **Routes and documents**: the four of the table above, with the
  response contract (the key asked for is in the body), JSON as R writes
  it: `/orders/{id}` answers `{"id":…,"total":…,"label":…}`, a list
  `{"from":…,"limit":…,"items":[…]}`, a write `{"key":…}`, the static
  document R's own. An unknown order is a 404 with a JSON body. Key order,
  spaces and the way a number is written are free; values are not.
- **Environment**: it reads these variables, and nothing else decides how
  it runs.

  | Variable | Meaning |
  |---|---|
  | `STUDY_LISTEN` | address to listen on, `host:port` |
  | `STUDY_PROTOCOL` | `h1`, `h2` or `h3` |
  | `STUDY_TLS` | `on` (default) or `off`, HTTP/1.1 in clear (level 2) |
  | `STUDY_TLS_CERT`, `STUDY_TLS_KEY` | PEM files of the certificate (ECDSA P-256) and its key |
  | `STUDY_TLS_RESUME` | `off` (default) or `on`: issue session tickets |
  | `STUDY_COMPRESSION` | `off` (default), `gzip` (level 1), `deflate`, `br`, `zstd`, as far as the contender offers them |
  | `STUDY_DB_POOL` | size of the PostgreSQL pool |
  | `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | the database, under libpq's names, which every driver knows |

  The study's server maps them onto its flags, which still win:
  `STUDY_PROTOCOL` picks R's transport for the protocol (`nethttp-h1`,
  `nethttp-h2`, `httpstream-h3`), `-transport` the study's other variants.
- **Logs, declared** (§4): its log level and whether it writes an access
  log are published with its figures. The standard version keeps its
  framework's default, the optimised one may cut it as a listed
  optimisation. The study's server writes one startup line and its errors
  to stderr, no access log.
- **A metrics point**, `GET /study/metrics`, JSON, read at the start and
  the end of every stage (the campaign takes the difference): it explains
  a p99, it is never a verdict (§1.3).

  ```
  {
    "runtime": "go1.26.0",
    "cpu_seconds": 812.4,                      // the process's CPU so far
    "gc": {                                    // null for a runtime with no GC
      "cpu_seconds": 21.7,                     // CPU the collector took so far
      "pauses": {"count": 5310, "seconds": 0.41,
                 "buckets": [{"le": 0.0001, "count": 4800}, …]}
    },
    "batches": {                               // null for a variant without grouping
      "count": 120400, "keys": 2891000,        // SQL batches of unit reads, keys in them
      "sizes": [{"le": 1, "count": 300}, {"le": 2, "count": 900}, …]
    }
  }
  ```

  Every number is cumulative since the server started; a bucket counts
  what is at most `le` and above the bucket before, and only the buckets
  that counted something are written. The study's server reads its part
  from `runtime/metrics`: `cpu_seconds` is the runtime's own estimate
  (`/cpu/classes/total` minus `idle`), so that the collector's share
  (`/cpu/classes/gc/total`) is a ratio in one unit, the pauses are
  `/sched/pauses/total/gc:seconds` and their total the pause CPU over
  `GOMAXPROCS`; `batches` counts the SQL queries of the unit reads, one
  per gateway batch, by size in powers of two.

### Checking a contender against R

```
go run -C comparison/loadgen . -conformance https://127.0.0.1:8443 -url https://127.0.0.1:9443 -checks 300
```

Before a series, every contender answers the requests a seed draws — unit
reads, lists and writes in the mixed scenario's proportions, a static
request every twentieth — one at a time, and so does the reference named
by `-conformance`. The two answers are compared as §4 asks: the status, the
`Content-Type` (media type and parameters, `charset=utf-8` counting as
JSON's own), and the JSON after parsing — keys in any order, numbers by
their exact value (`7`, `7.0` and `7e0` are one number; `0.1` and
`0.10000000000000001` are two), spaces ignored. It stops at the first
difference and writes it on its JSON line, with the request and the place
in the document (`$.items[3].label`); the exit status is then 1. Under
`-encoding`, both are asked for that coding and decoded first. What a
write does in the database is not seen from here: the campaign checks it
on the database's side.

On a developer's machine with PostgreSQL, R against R, against
`-transport httpstream-h1`, `-json append` and `-compression gzip1`, 300
requests each under `-encoding gzip`: conformant; the trivial server
against R parts at the first static request (`$.service`), as it should,
and agrees on everything else it was asked.

## `loadgen/` — the load generator

The study's own client (§3 and §4 of its protocol; "home-made" is a
recorded decision: no tool on the market checks the key in the body of
every answer). It sends requests at a **constant interval** and clocks each
one from the instant it was *due*, not the instant it left: a request that
waited because every connection was busy carries that wait as latency.
That is the correction of the coordinated omission, and what makes a
closed-loop generator's figures too good. Everything it prints is a JSON
line per stage, never a result of the study. It shares no code with
Sluice.go: a bug symmetric between client and server would mask itself.

What one stage does, over TLS (or HTTP/1.1 in clear for an `http://`
URL) under `-proto h1` (HTTP/1.1, the default),
`h2` or `h3`:

- opens its connection budget before the clock starts (the TLS handshakes
  never fall inside the measurement): one virtual client is one connection
  whatever the protocol, with one request in flight on it under HTTP/1.1
  and at most 8 streams under HTTP/2 and HTTP/3 (§3.5). Each connection is
  a transport of its own, so that `net/http` and `quic-go`, which both
  share one connection per host, never coalesce two virtual clients;
  `-warmup` seconds are then sent at the stage's rate and never counted;
- draws one key per request from a permutation of `1..-keyspace` under
  `-seed`, so no key repeats within a stage and every contender of a round
  sees the same sequence; a stage longer than its keyspace is refused;
- checks **every** answer: body complete (`Content-Length`), JSON parsed to
  the end, key served = key asked (`id`, `from`, `key` by scenario), a
  list's ids strictly increasing above its bound; and on 1 % of them,
  drawn by `(seed, index)`, the whole document against what the fixtures
  predict (`total = 7·id`, `label = "order-<id>"`, a write answers its key);
- gives every request `deadline_ms` from its due instant to be answered to
  the end, `10 × X + 4 × RTT` (§1.2: four round trips cover a fresh
  connection's TCP and TLS 1.3 handshakes, the request and a margin);
- publishes TTFB and TTLB apart (p50, p90, p99, p99.9, max, in ms, over the
  answers that passed every check) and what became of every other
  request, one class each, `sent = ok + bad_key + timed_out + errors +
  tester`: `bad_key`, a wrong answer (the contract broken: one fails the
  stage), never counted as an error; `timed_out`, no complete answer by
  the deadline; `errors`, the contender's, by nature — `status_4xx`,
  `status_5xx`, `refused` (`ECONNREFUSED`), `reset` (the connection ended
  under a request already sent: `ECONNRESET`, EOF, a stream reset),
  `connect_timeout` (the deadline passed while the connection was
  opening), `other` (a failed handshake, any other status); `tester`, a
  connection the tester could not open for want of a local port or a
  descriptor (`EADDRNOTAVAIL`, `EMFILE`, §4: one invalidates the stage).
  Beside them, what the server did to the budget's connections: `closed`,
  the ones it ended (`Connection: close` under HTTP/1.1, `GOAWAY` under
  HTTP/2, a FIN or a reset on an idle one, the end of the QUIC connection
  under HTTP/3), and `reconnects`, the ones the generator opened again,
  after a closure or after giving up on a request under HTTP/1.1, which
  closes its connection; past zero, the stage no longer holds "0 % fresh
  connections" (§3.6). Then the internal queue: `queue_max`, the most
  requests due at once with no connection free, `queued`, how many were
  due while another still waited, and `wait_ms`, the queue seen from one request. `late_ms` is the
  dispatcher's own lateness after its due instant, the generator's noise
  floor on the median: it sleeps until 2 ms before, then spins;
- gives its verdict under the scenario's SLO (§3.2: `p99 ≤ X + RTT` and
  errors and timeouts under 0.1 %; X is `-slo`, by default static 10 ms,
  unit and write 25 ms, list 100 ms, mixed 50 ms, fixed net of the round
  trip `-rtt` injects): `held`, or `cause` by precedence
  — `key` (the contract broken, a bug of the server), `tester` (a
  connection the tester could not open, §4; a refusal or a silence of the
  server is the contender's, and counts against the SLO), `queue` (the
  generator's queue was not empty while p99 sat under X: the generator
  limits, §3.5), `slo`;
- with `-encoding gzip` or `deflate` (`identity` by default, nothing
  asked), sends that one coding in `Accept-Encoding` and decodes every
  answer before checking it: a body is complete once the decoder reaches
  its end (a gzip member's trailer, a zlib stream's checksum), with or
  without a `Content-Length`, so an answer streamed in batches is read
  the same way. HTTP's `deflate` is a deflate stream in the zlib format
  (RFC 9110 §8.4.1.2); a raw one, which some servers send and browsers
  accept, is decoded and counted in `deflate_raw`. An answer in clear is
  no fault (§2.3 bis): `compressed_pct` says how many came in the coding
  asked for. A body that does not decode, or in a coding not asked for,
  is a wrong answer (`bad_key`). `wire_bytes` is the body's size on the
  wire per answer, `ratio` the decoded size over it, over the compressed
  answers. `br` and `zstd` are refused until their decoders are in
  `loadgen/go.mod`;
- publishes the TLS it negotiated, `tls_version`, `tls_suite`, `alpn`
  (`none` in clear; two values joined by ` | ` would mean the connections
  did not all agree), and `tls_resume`, whether it kept a session cache;
- with `-new-conns 10`, a tenth of the requests, drawn by `(seed, index)`,
  open a fresh connection whose handshake falls inside their own delays and
  close it, so that `TIME_WAIT` lands on the tester (§3.6); `handshakes`
  counts them, with the reconnections' own, and `resumed` those that
  resumed a session (`-tls-resume`).

```
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario list -rate 200 -duration 5s
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario list -rate 200 -encoding gzip
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -proto h3 -rates 1000,4000 -duration 3s
go run -C comparison/loadgen . -h       # every flag; -ca verifies the server, empty trusts it
```

The exit status is 1 when the stage is not a measurement: any request
that is not ok. The mixed scenario is 80 % unit, 15 % list, 5 % write by
position in the sequence, exact whatever the seed. HTTP/1.1 and HTTP/2 are
`net/http`'s client; HTTP/3 is `quic-go`'s, required in `loadgen/go.mod`
only, the version `interop/` and `sluice/` pin: the root module's graph
is untouched (ADR 0002), and the generator never measures Sluice.go against
itself.

### The ramp

```
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -ramp -rate 4000 -duration 30s
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -rates 500,2000,8000
```

`-ramp` walks the study's common grid `100 × 1.05^k` from `-rate` up
(§3.3, §7.6), `-rates` a list of its own; both stop at the first stage
that breaks, with its cause on that stage's line, or before a stage the
keyspace could not hold. Between two stages every request of the first is
answered, then `-drain` passes. Each stage gets the budget
`C = 2 × R × (RTT + X)` of §3.5 (`-rtt` is the round trip injected on the
wire, 0 on a loopback; the budget depends on the scenario and the rate
only, the same for every contender), capped by `-max-conns` when the
tester has a ceiling; the connections it lacks are opened before its
clock starts, the ones a smaller stage leaves idle never age out. The exit
status is 0 when every stage is a measurement and the last broke on the
SLO alone, or none broke.

### Plateau, peak, and the browser's shape

```
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -profile plateau -rate 3000 -duration 10m -window 30s
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -profile peak -rate 1000 -duration 60s -peak-for 30s
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario unit -proto h2 -shape browser -rate 2000
```

`-window` cuts a run into lines of that length, written before the run's
own line: each carries `window` (its index), its counts and quantiles, and
`start`, the wall-clock instant it began, which a record taken on the
server's side (RSS, CPU, §1.2) joins on, since the generator does not see
the server's memory. `-profile plateau` is one rate held long (§3.3: about
70 % of the contender's capacity, 10 minutes) to read the drift of p99
and memory, window by window (10 s unless `-window` says otherwise).
`-profile peak` holds `-rate` for `-duration`, `-peak-factor` (5) times it
for `-peak-for` (30 s), then `-rate` again for `-duration`: absorption and
recovery, each window tagged `base`, `peak` or `back` with its own `rate`.
Its budget C is the peak's, `2 × 5R × (RTT + X)` (§3.5), opened before the
clock, unless `-conns` is given. A window never straddles two phases:
`-window` must divide `-duration` and `-peak-for`. Both run longer than a
table holds keys: for reads only (static, unit, list), the draw walks the
keyspace again, each pass in an order of its own, and `keyspace_passes`
says how many; a write never repeats its key, so a plateau of writes is
bounded by the keyspace like any stage.

`-shape browser` (§8.4) keeps the requests in flight the API shape has and
groups them as a browser does: six connections to a virtual client under
HTTP/1.1, and under HTTP/2 and HTTP/3 the `8 × C` streams on connections
that carry up to 50 each — fewer connections, more streams on each, where
HTTP/2's head-of-line blocking shows. `conns` and `clients` are published.

### Calibrating it

```
go test -C comparison/loadgen -race ./...
```

runs the calibration the protocol demands: against a server of the test
that sleeps exactly 5 ms, p50 must read 5 ms above what the same server
sleeping 0 ms costs (the apparatus' own floor, under a millisecond), and
under 5.5 ms absolute; p99 and p99.9 are printed, not asserted, since they
belong to the machine. A second stage asks 2 000 req/s of 4 connections
that carry 800: the queue climbs, the delays climb with it, and the
achieved rate says what the budget carried. A third answers two requests
with another key and the stage fails with `bad_key` 2. A ramp of three
stages under a ceiling of 4 connections holds the first two and stops at
the third with the cause `queue`; a stage with a tenth of fresh
connections counts their handshakes apart and fails no connect. A 404, a
503 and a key never answered land in `status_4xx`, `status_5xx` and
`timed_out`, under each protocol; a server that ends every connection
after 10 requests, by `Connection: close` under HTTP/1.1 and the `GOAWAY`
it becomes under HTTP/2, shows 10 `closed` and 10 `reconnects` and loses
no request; a port nothing listens on is `refused` over TCP and a
`connect_timeout` over QUIC. A plateau of 2 s in four windows of 500 ms that walks its keyspace
twice writes four lines of 500 requests each, half a second apart; a peak
writes its six windows at 100, 500 and 100 req/s on the peak's budget,
and no other connection opens; the browser shape is seen from the server
as 12 connections under HTTP/1.1, and 2 connections carrying 50 requests
at once under HTTP/2 and HTTP/3. Under each protocol and each coding, buffered
and streamed, a server that answers one key in clear, cuts one body, uses
an unasked coding for another and raw deflate for a fourth sees two wrong
answers and the rest decoded.

### Not the bottleneck

```
comparison/loadgen/selftest.sh                                   # 4 scenarios × h1 h2 h3
ENCODINGS="identity gzip deflate" SCENARIOS=list comparison/loadgen/selftest.sh   # per coding
go run -C comparison/loadgen . -trivial 127.0.0.1:8444 -keyspace 300000   # the server alone
```

A capacity measured against a contender means nothing unless the generator
could have gone much further (§4): the campaign opens with a ramp per
scenario against a **trivial server**, which answers the four routes with
documents of the scenario's shape and size — ~1 KB for `/static`, one row
for `/orders/{id}`, 1 000 rows and ~50 KB for `/orders?from=&limit=`, the
key echoed for `POST /orders` — serialised once at startup (the table's
rows are one byte array, a list is a slice of it between two offsets), the
generator's validation at 100 % on. It is the same binary under `-trivial`,
run as a process of its own so that its goroutines never share the
generator's scheduler, serving HTTP/1.1 and HTTP/2 on TCP and HTTP/3 on UDP
at the same port, under a certificate generated at startup (`-ca` empty
trusts it). Asked for gzip or deflate, it compresses every 200 on the fly
at level 1, the level the study compares at (§2.3 bis), with encoders
reused from a pool: ~90 µs for a list page, a third of what the generator
spends decoding and checking it. A page cannot be compressed once at
startup, since every list starts at another key. `selftest.sh` builds it,
starts it, runs one ramp per scenario, protocol and coding on a coarse
`×1.25` grid from 4 000 req/s (`RATES`, `DURATION`, `SCENARIOS`, `PROTOS`,
`ENCODINGS` override it; the coding is `identity` by default) and prints,
per cell, the last stage held, its p99, the stage that broke and its
cause, with a CPU profile of the generator per cell (`-cpuprofile`) for
when a ceiling needs explaining, and the trivial server's own in
`trivial.prof`, written when the script stops it: the generator, not the
server, must be what limits.
The study asks for a ceiling of at least twice the fastest contender's
capacity in that scenario; otherwise the tester gets more cores before
anything else is measured.

Not yet here: the `oha` cross-check.

### From rounds to verdicts

```
go run -C comparison/loadgen . -verdicts rounds.jsonl -compare sluice-std:go-std,sluice-opt:go-opt
```

A verdict comes out of a program, never out of a reading (§7.4). The
campaign writes one JSON line per contender per round,
`{"scenario":"unit","proto":"h1","contender":"sluice-std","round":3,"capacity_rps":8000,"p99_ms":11.2}`;
`-verdicts` reads them, and for every comparison declared with `-compare`
(A against B, fixed before the campaign) writes one line per primary
metric. A family is a scenario × a protocol over both metrics, and every
comparison in it is judged together:

- **paired by round**: the first round is dropped (it absorbs the
  machine's own ramp), and a comparison needs 21 rounds (§7.2: below that,
  the sign test cannot pass Holm's first threshold in a family of 46);
- **the gap**: for capacity, the difference in whole steps of the grid
  `100 × 1.05^k` per round (a rate off the grid is refused, no float
  decides a verdict), shown as a percentage; for p99, the median of the
  per-round ratios A/B, never the ratio of the medians; both with their
  95 % interval by ranks;
- **the test**: the exact two-sided sign test over the untied rounds, its
  p-value published, then Holm's correction over the family (the smallest
  p must pass 0.05/n, the next 0.05/(n−1)…);
- **the verdict**, in that order: `faster` or `slower` when the test
  passes Holm and the gap reaches the floor (one step, 3 % of p99, raised
  by `-floor-steps` and `-floor-p99` once the A/A tests have measured the
  noise, §7.5); `equivalent` when the interval lies inside the margin (the
  floor, at least ±5 % for p99, one step either way for capacity);
  `inconclusive` otherwise.

Holm holds the risk of a false `faster` to 5 % per family, not to zero:
on 400 synthetic A/A families under a laptop's noise, 4 read a difference,
and none past its floor by more than a step. That is the rate §7.5 expects
of its A/A tests, and the reason the floors are raised by them.
