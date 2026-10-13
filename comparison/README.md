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
| A — transport | `-transport nethttp-h1` (R), `nethttp-h2`, `httpstream-h1`, `httpstream-h2`, `httpstream-h3` | h3 refused until axis A |
| B — driver | `-driver sluice` (R), `pgx` | both, under the same gateway and connection budget |
| C — grouping | `-gateway-size 64 -gateway-within 2ms` | any positive values |
| D — transport batch | `-max-requests-per-batch`, `-max-requests-per-conn` (`-1` unbounded) | httpstream transports only |
| E — response | `-response buffered` (R), `stream` | stream refused until axis E |
| F — compression | `-compression none` (R), `gzip1`, `gzip6`, `deflate6` | refused until axis F |

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
