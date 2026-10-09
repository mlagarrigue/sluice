# benchmarks — measuring against the libraries people actually use

This directory is a **second Go module**. It depends on the framework; nothing
in the framework depends on it. That is what lets a project with one production
dependency (`golang.org/x/crypto`) be compared against `pgx`, `gin`, `echo`,
`chi` and `fiber` without acquiring any of them.

```
cd benchmarks
go test ./...                       # the wire-compatibility checks
go test -run='^$' -bench=. ./...    # the comparisons
```

The root's `go test ./...` does not see this directory, and three tests in the
root module keep the arrangement honest: `go.mod` must require nothing outside
the allowlist (`golang.org/x/crypto` and the `golang.org/x/sys` it brings), its
`go` line must match the oldest version CI builds, and this module must still
`replace` the root with the working tree rather than a published tag.

## Two rules

**Nothing here may be imported by the framework.** Not a helper, not a fixture.
A nested module is excluded from the root's `./...`, so such an import would
have to become a real `require` in the root `go.mod` to compile at all — where
both the CI `deps` job and `TestGoModRequiresOnlyTheAllowlist` catch it.

**Versions are pinned, recorded, and never bumped by a bot.** A figure without
the version it was measured against is not a claim. Updates are deliberate and
they re-run the comparison.

## What is pinned

| | Version |
|---|---|
| `github.com/jackc/pgx/v5` | v5.10.0 |
| `github.com/lib/pq` | v1.12.3 |
| `github.com/gin-gonic/gin` | v1.12.0 |
| `github.com/labstack/echo/v4` | v4.15.4 |
| `github.com/go-chi/chi/v5` | v5.3.2 |
| `github.com/gofiber/fiber/v2` | v2.52.15 |

Six direct dependencies pull **51 modules** of transitive graph. That number is
not an argument against any of them — it is what a comparison costs, and it is
the reason it lives here rather than in the module that promises one.

**Both modules declare Go 1.26.** This module once needed a newer toolchain than
the framework, because `pgx` required Go 1.25 while the framework promised
1.24; since the framework moved to 1.26 the two lines agree, and the
comparisons build on any toolchain the framework itself supports.

## Where the numbers live

**[`docs/BENCHMARK-CONSOLIDATED.md`](../docs/BENCHMARK-CONSOLIDATED.md) is the
table of record.** It is produced by `internal/bench/scripts/consolidate.sh` in
one session, on one machine, with the conditions printed at the top — Go
version, CPU, kernel, PostgreSQL, benchtime, what was pinned.

That file exists because everything below it was assembled from a dozen
separate runs over several hours. Every figure was honestly obtained and no two
of them were comparable, which is precisely what this repository's own rule
about absolute numbers says not to do. The sections below keep the *reasoning*
— what each comparison means, where the harness was wrong, which caveat
attaches to which figure — and the consolidated file keeps the figures.

Regenerate it with:

```
eval "$(internal/bench/scripts/pgdev.sh start)"
internal/bench/scripts/pgdev.sh fixtures
internal/bench/scripts/consolidate.sh > docs/BENCHMARK-CONSOLIDATED.md
```

## What is measured, and what each figure means

### `bigint[]` codec, against pgx

The claim the connector is built around: a thousand keys leave as one binary
parameter, a thousand values come back out of one array.

Measured on one pinned core, `GOMAXPROCS=1`, six runs of 300 ms, variance under
2%:

| | sluice | pgx v5.10.0 | |
|---|---|---|---|
| encode 1000×`int8` | **464 ns**, 0 B, 0 allocs | 12 450 ns, 7 761 B, 966 allocs | ×26.8 |
| decode 1000×`int8` | **655 ns**, 0 B, 0 allocs | 4 580 ns, 8 272 B, 5 allocs | ×7.0 |

**Read the caveats before quoting these.**

*Only the codec is measured.* No socket, no server, no round trip. Against the
~500 µs a round trip costs, everything in this table is noise. It matters
because a batched `= ANY($1)` read encodes one array per query and decodes one
per result, so this sits on the path every batched read takes — once.

*The gap is not a setup artefact.* 966 allocations to encode a thousand
integers looked like the wrong entry point, so `pgtype.FlatArray[int64]` — the
type pgx's own documentation reaches for when the shape is known — was measured
too: 12 580 ns and 965 allocations. Same figure. The difference is pgx's design
here, not a hand-crippled competitor.

*Both sides produce identical bytes.* `TestInt8ArrayWireCompatibility` asserts
that the two encoders emit the same wire form and that each decodes the other's
— because an encoder that is faster while emitting something subtly different
is not faster, it is wrong, and a benchmark cannot tell the difference. That
test also makes pgx a genuine differential oracle for the codec, which is what
the root module's dependency rule forbids there and this module exists to
allow.

### Web pipeline, in process

`PATCH /orders/{order}/lines/{line}`: two path parameters read as integers, a
JSON body decoded under a byte limit, one business rule, a JSON answer — 200
with the amended quantity, or 422 with the problem *and* the remedy for it.
Same statuses and same body on every side, asserted by
`TestWebContendersAgree` before any figure is believed. Each framework is
written the way its own documentation writes it.

One pinned core, `GOMAXPROCS=1`, three runs of 300 ms:

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| `net/http` (no framework) | **1 745** | 1 928 | 21 |
| `gin` v1.12.0 | 1 770 | 1 864 | 19 |
| **sluice** | 1 880 | 1 952 | 22 |
| `echo` v4.15.4 | 1 970 | 1 896 | 21 |
| `chi` v5.3.2 | 2 075 | 2 584 | 23 |

Nobody wins by much, which is the honest shape of this comparison: all five are
doing the same JSON decode and encode, and that is most of the time. sluice
sits 8% above hand-written `net/http` and between `gin` and `echo`.

**The harness was wrong first.** Measured through `httptest.NewRecorder`, all
five came out within 15% of each other at ~3 400 ns and 8 184 B/op — forty
times the size of the response. That was the recorder being measured, not the
frameworks. §2 says to look for the harness being wrong before believing a
result; this is what that looked like, and the table above is from a
`discardWriter` and a rewindable body instead.

### Over a real socket, where most of the difference disappears

The same handlers, served on a listener and driven by a real client with
keep-alive. This is the row that says how much the table above matters.

| | per request | B/op | allocs |
|---|---|---|---|
| `net/http` | **176 µs** | 10 060 | 111 |
| `gin` | 178 µs | 10 100 | 109 |
| **sluice** | 180 µs | 10 200 | 112 |
| `chi` | 180 µs | 10 800 | 113 |
| `echo` | 186 µs | 10 200 | 111 |
| `fiber` (fasthttp) | 190 µs | **6 720** | **82** |

**Everything is within 8% of everything else.** The in-process table spreads
those same five frameworks over 18% — 1 745 ns to 2 075 ns — and a loopback
socket costs 175 µs, so the whole spread lands inside the noise of one round
trip. On a real network it would disappear entirely.

And "within 8%" understates it. Run through the paired protocol — thirteen
interleaved rounds, a sign test on the per-round ratio, the same one the
regression gate uses — the ordering above turns out not to exist:

    versus.sh BenchmarkSocketNetHTTP BenchmarkSocketSluice   → noise, wins 7/12
    versus.sh BenchmarkSocketGin     BenchmarkSocketSluice   → noise, wins 8/12
    versus.sh BenchmarkSocketNetHTTP BenchmarkSocketFiber…   → SLOWER +6.2%, wins 1/12

sluice is **statistically indistinguishable** from hand-written `net/http` and
from `gin` over a socket. Not "close to" — indistinguishable: the rounds split
seven-five and eight-four, which is what two identical things look like. The
only contender the test separates is fiber, and it separates it in the
direction its reputation does not predict.

That is what the paired protocol is for. A single run of each produced 176,
178, 180, 180, 186, 190 — six numbers in an order, inviting a reader to rank
them. Five of those six differences are not there.

That is not an argument for ignoring the in-process figures: they measure
something real, and a handler that allocated ten times more would show up here
too. It is an argument against choosing a Go web framework on its
requests-per-second chart, which is what those charts are usually for.

Concurrently, 32 requests at once:

| | 32 requests | allocs |
|---|---|---|
| `net/http` | 1.14 ms | 3 583 |
| **sluice** | 1.16 ms | 3 614 |
| `fiber` (fasthttp) | **1.03 ms** | **2 643** |

**fiber is on its own row and it is the row this project loses.** It runs on
`fasthttp`, its own HTTP implementation, so this compares two parsers as much
as two pipelines — which is exactly why it gets a label rather than a rank. It
allocates 26% less and, under concurrency, is 11% faster. Sequentially it is
the slowest of the six, which is the part its reputation does not predict.

*What this harness does not measure.* The client runs on the same machine and
takes CPU from the server. Absolute numbers are therefore pessimistic for
everyone equally, and `TestSocketHarnessIsNotTheBottleneck` fails if a request
exceeds a millisecond on loopback — a floor, not a proof.

### Where batching wins, which is what it was built for

Sixty-four concurrent HTTP requests, each needing one row from the database.
The gateway groups them into one batch and one `= ANY($1)`; the per-request
drivers issue sixty-four queries. `TestGatewayIssuesOneQueryPerBatch` asserts
that the sixty-four really do reach the database as **one**, counted where the
query is issued — an earlier version counted `pg_stat_database`, whose
collection lags, and reported "0 queries for 64 requests" while passing.

| | 64 requests | connections | per request |
|---|---|---|---|
| **sluice + gateway** | **0.97 ms** | **1** | 15 µs |
| pgx | 12.9 ms | 1 | 201 µs |
| lib/pq | 24.1 ms | 1 | 377 µs |
| pgx, pool of 16 | 1.95 ms | 16 | 30 µs |
| pgx, pool of 64 | 1.33 ms | 64 | 21 µs |

At equal connections — the configuration the brief demands — batching is **×13**
over pgx. But that row alone would be winning against a handicapped opponent,
because nobody deploys pgx with `MaxConns(1)`. So the pooled rows are here too,
and they are the ones that say what this is worth:

**sluice on one connection beats pgx on sixty-four.** Not by much — 0.97 ms
against 1.33 — and that is the point. The claim is not "faster"; it is *the same
throughput on one connection instead of sixty-four*, which is what matters when
connections are the scarce resource: a managed Postgres with a hard connection
cap, a serverless runtime that cannot pool, a service fanning out to a database
someone else also uses.

Read it beside the rows below rather than instead of them. The same mechanism
that wins here costs ×600 when there is no round trip to amortise, and this
table is the other half of that sentence.

### Where batching loses, which is the row that matters

Without a database there is nothing for the gateway to amortise. Its whole
thesis is that sixty-four callers waiting on one round trip beat sixty-four
round trips; with no round trip in the picture, what is left is a channel, a
hand-off, and a batch that usually holds one element.

| | per request |
|---|---|
| sluice, direct handler | 1 880 ns |
| sluice through a gateway, **one caller at a time** | 1 140 000 ns |
| sluice through a gateway, **64 concurrent callers** | ~19 300 ns |

**Read the serial figure carefully — it is mostly not the framework.** The
gateway was configured with `Within: 200µs`, and a lone call waits for that
delay before its batch is served. It does not: on this machine anything below
roughly a millisecond is rounded up to **~1.12 ms** by the platform's timer
granularity. `TestGatewayTimerFloor` measures it directly —

    Within=100µs  observed=1.11ms   ratio=11.1
    Within=200µs  observed=1.12ms   ratio=5.6
    Within=1ms    observed=1.12ms   ratio=1.1
    Within=5ms    observed=5.26ms   ratio=1.05

— so a `Within` shorter than the floor is a setting with no effect, which
anyone tuning latency needs to know, and the serial number above is measuring
the clock rather than the code. Observed under WSL2, where Linux neither sees
nor controls the host clock; it may differ elsewhere, which is why it is a test
that reruns rather than a number written into a document.

The mechanism's own cost is what is left: **five more allocations per request**
(27 against 22) and, under the concurrency it is designed for, ~19 µs per
request against 1.9 µs direct. Whether that pays is arithmetic this table
cannot finish: it pays when the round trips saved exceed it, and the round trip
is exactly what has not been measured yet.

### Connector, against a live server

One connection each — this connector has no pool, so a comparison against a
pooled driver would measure pooling. Same SQL, same values into the same Go
types, and `TestPGContendersAgree` asserts sluice and pgx read identical rows
before any figure is believed. A loopback round trip measures **130 µs** on this
machine, which is the unit every figure below is really counting.

Figures from [`docs/BENCHMARK-CONSOLIDATED.md`](../docs/BENCHMARK-CONSOLIDATED.md)
(2026-08-21, one session, medians of three 1 s runs):

| | sluice | pgx v5.10.0 | pgx/`database/sql` | lib/pq |
|---|---|---|---|---|
| point read | 292 µs · 10 allocs | 140 µs · 10 | 147 µs · 19 | 308 µs · 26 |
| point read, cache on | 242 µs · 11 | — | — | — |
| point read, cache + `AllRows` | **139 µs** · 11 · 248 B | 140 µs · 10 · 664 B | — | — |
| 1000-key `= ANY` | 1.01 ms · **36 allocs** | 742 µs · 2 770 | 764 µs · 3 486 | — |
| 1000-key `= ANY`, `AllRows` | **697 µs** · **37 allocs** | — | — | — |
| wide scan, 20k×11 | 13.6 ms · **60 allocs** | **9.1 ms** · 280 008 | 13.9 ms · 478 513 | 15.8 ms · 478 505 |
| bulk COPY, 50k rows | **11.5 ms** · **5 allocs** | 12.9 ms · 99 799 | — | — |

**sluice loses on reads and wins on writes**, and both halves have an
explanation rather than an excuse.

*The point read used to cost two round trips where pgx costs one*, and now it
does not. The default sequence is Execute-with-a-row-limit, Flush to deliver
without closing the portal, then a Sync to end it — and on a result that fit in
one batch that Sync is a pure round trip. `QueryConfig.AllRows` sends the Sync
with the query instead: **139 µs against pgx's 140**, at parity, with 248 B
against 664.

What it gives up is a cheap early stop. With a row limit an abandoned result
costs at most BatchRows to drain; with AllRows the server was told to produce
everything, so abandoning a ten-million-row scan drains ten million rows. It is
for reads that are small and always consumed whole, which a point read is and a
scan is not — so it is an option rather than the new default.

*The wide scan was 4× slower and it was not the decode — that reading was
wrong, and a profile said so.* Raising BatchRows from 1 024 to 32 768 removed
nineteen of the twenty round trips and changed nothing, which was taken as
proof that the transport was innocent and the decode guilty. It proved only
that *round trips* were innocent. A CPU profile put **79% of the time in
`Syscall6`** and 3% in decoding: the reader was going to the kernel twice per
protocol message — once for the five-byte header, once for the body — which is
forty thousand system calls for twenty thousand rows.

Buffering the socket took the scan from **33.5 ms to 13.5 ms** and the
thousand-key `= ANY` from **1.97 ms to 0.69 ms**, which puts that row ahead of
pgx. What remains on the wide scan is a genuine 1.5× on decode, and it is the
design trade the allocation column shows from the other side: sluice copies
each row into one flat buffer with offsets, pgx references its read buffer and
allocates per value — 60 allocations against 280 008.

The buffer is applied to `net.Conn` and to nothing else. Buffering a reader
already in memory is a pure copy, and it measured +5% on the in-memory replay
benchmark: making a harness slower to make a socket faster is a trade nobody
asked for.

*COPY is where the batch model pays.* Five allocations for fifty thousand rows,
against 99 799 — one buffer filled in one pass, versus a slice per row — and it
is also faster.

**A round-trip trap worth knowing.** The point read first measured 405 µs
because it was written with `BatchRows: 1`, asking for exactly the one row it
expected. The server answers PortalSuspended in that case — it produced
everything asked for and cannot know whether more follow — and the client sends
another Execute to be told there are none. One whole round trip, bought by
asking for one row less than you could have. `QueryConfig.BatchRows` now says
so.

## What is not measured yet

The connector against pgx / lib/pq / `database/sql`, the web comparison with
the database behind it, and `fiber` on its own labelled row were on this list;
each now has its section above.

**The latency distribution over a socket.** `TestWebLatencyDistribution`
reports p50, p99 and p99.9 in process (`SLUICE_LATENCY=1 go test -run
TestWebLatencyDistribution`), and those are the tail of the *handler*. A real
service's p99.9 is dominated by queueing at the accept loop, which is exactly
what running in process removes. Both readings belong in a published
comparison, labelled as what they are.
