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
| C — grouping | `-gateway-size 64 -gateway-within 2ms` | any positive values |
| D — transport batch | `-max-requests-per-batch`, `-max-requests-per-conn` (`-1` unbounded) | httpstream transports only |
| E — response | `-response buffered` (R), `stream` | the list is written as the store delivers it, one flush per batch of rows, no `Content-Length`; with `-compression`, the encoder is flushed per batch too; the other scenarios stay buffered |
| F — compression | `-compression none` (R), `gzip1`, `gzip6`, `deflate6` | stdlib encoders on the buffered body when the client accepts it; `Vary: Accept-Encoding` |
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

The mixed scenario (80 % unit, 15 % list, 5 % write) is the generator's
business. Unit reads go through the gateway and leave as one `= ANY($1)`
per batch; under `httpstream` the requests of one transport batch are
submitted to the gateway together. Lists and writes take a connection from
a small pool (`-db-conns`). Writes land in `comparison_writes`, a table no
read ever touches, created at startup with autovacuum off; a campaign
truncates it between repetitions.

### Running it

It is its own Go module, like `benchmarks/` on the `benchmarks` branch:
it replaces the root with the working tree, so a run measures the commit it
is run from, and the root's `go test ./...` never sees it.

```
export SLUICE_PG=host:5432 SLUICE_PG_USER=sluice SLUICE_PG_DB=sluice SLUICE_PG_PASSWORD=…
internal/bench/scripts/pgdev.sh fixtures      # bench_orders, 100 000 rows
go run ./comparison/sluice -h                 # every flag, R as the default
go run ./comparison/sluice -listen 127.0.0.1:8443
curl -k https://127.0.0.1:8443/orders/42
```

Without `-cert`/`-key` the server generates a self-signed ECDSA P-256
certificate at startup; the campaign passes a fixed one.

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

What one stage does, HTTP/1.1 over TLS:

- opens its connection budget before the clock starts (the TLS handshakes
  never fall inside the measurement), one request in flight per
  connection — the ceiling of 8 is for HTTP/2 and HTTP/3, which come next;
  `-warmup` seconds are then sent at the stage's rate and never counted;
- draws one key per request from a permutation of `1..-keyspace` under
  `-seed`, so no key repeats within a stage and every contender of a round
  sees the same sequence; a stage longer than its keyspace is refused;
- checks **every** answer: body complete (`Content-Length`), JSON parsed to
  the end, key served = key asked (`id`, `from`, `key` by scenario), a
  list's ids strictly increasing above its bound; and on 1 % of them,
  drawn by `(seed, index)`, the whole document against what the fixtures
  predict (`total = 7·id`, `label = "order-<id>"`, a write answers its key);
- publishes TTFB and TTLB apart (p50, p90, p99, p99.9, max, in ms, over the
  answers that passed every check), the errors by class — `bad_key`
  (the contract broken: one fails the stage), `connect` (one invalidates
  it) — and the internal queue: `queue_max`, the most requests due at once
  with no connection free, `queued`, how many were due while another still
  waited, and `wait_ms`, the queue seen from one request. `late_ms` is the
  dispatcher's own lateness after its due instant, the generator's noise
  floor on the median: it sleeps until 2 ms before, then spins;
- gives its verdict under the scenario's SLO (§3.2: `p99 ≤ X` and errors
  under 0.1 %; X is `-slo`, by default static 10 ms, unit and write 25 ms,
  list 100 ms, mixed 50 ms): `held`, or `cause` by precedence — `key` (the
  contract broken, a bug of the server), `connect` (a failed connect,
  the tester's fault, §3.6), `queue` (the generator's queue was not empty
  while p99 sat under X: the generator limits, §3.5), `slo`;
- with `-new-conns 10`, a tenth of the requests, drawn by `(seed, index)`,
  open a fresh connection whose handshake falls inside their own delays and
  close it, so that `TIME_WAIT` lands on the tester (§3.6); `handshakes`
  counts them.

```
go run -C comparison/loadgen . -url https://127.0.0.1:8443 -scenario list -rate 200 -duration 5s
go run -C comparison/loadgen . -h       # every flag; -ca verifies the server, empty trusts it
```

The exit status is 1 when the stage is not a measurement: any error, or a
failed connect. The mixed scenario is 80 % unit, 15 % list, 5 % write by
position in the sequence, exact whatever the seed.

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
connections counts their handshakes apart and fails no connect.

Not yet here: HTTP/2 and HTTP/3, the `oha` cross-check, the trivial server
per scenario that proves the generator is not the bottleneck (§4).
