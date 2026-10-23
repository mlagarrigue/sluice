# Measurements

Every performance figure in the repository has a benchmark and a
denominator. This page says how the figures are produced, what they are read
against, and why a figure taken alone is worth nothing on the machine that
produces them. The tables of "Measured in one session" are regenerated in one
session by `internal/bench/scripts/consolidate.sh`, which rewrites only what
lies between their `consolidate` markers; the session's conditions are
written with them, and every other line of this page is edited by hand.
The figures the README quotes are copied from those tables by
`internal/bench/scripts/readme-figures.sh`, run after a regeneration; a test
refuses a README figure that differs, to the character, from the median of
its row.

## Contents

- **How to read this page**
  - [The ceiling and the budget](#the-ceiling-and-the-budget)
  - [Measuring on a laptop](#measuring-on-a-laptop)
  - [Reading the tables](#reading-the-tables)
  - [Not yet measured](#not-yet-measured)
- **HTTP servers**
  - [HTTP/2 and HTTP/3, from a real client](#http2-and-http3-from-a-real-client)
  - [Web pipeline, from a real client](#web-pipeline-from-a-real-client)
  - [Web pipeline, in process](#web-pipeline-in-process)
  - [Web pipeline, over a socket](#web-pipeline-over-a-socket)
  - [The gateway, with a database behind it](#the-gateway-with-a-database-behind-it)
  - [Partitioning by principal](#partitioning-by-principal)
  - [Paired verdicts](#paired-verdicts)
- **QUIC**
  - [QUIC, on an emulated path](#quic-on-an-emulated-path)
- **The PostgreSQL connector**
  - [Connector, against pgx / lib/pq / database/sql](#connector-against-pgx--libpq--databasesql)
  - [Remote reads](#remote-reads)
  - [Codec, in isolation](#codec-in-isolation)
  - [Statement cache](#statement-cache)
- **The core and the architecture's decisions**
  - [Joins and hydration, in isolation](#joins-and-hydration-in-isolation)
  - [The architecture's figures](#the-architectures-figures)

`consolidate.sh` regenerates the tables under
[Measured in one session](#measured-in-one-session); the sections before it
are measurements of their own, each dated.

## The ceiling and the budget

**A native `for range` loop over `[]int64`: 0.219 ns per element, zero
allocations** ([measured](#the-architectures-figures)). That is the ceiling;
every measurement of the core is expressed as a multiple of it. A core
operator has a budget of **about 1.5 ns per stage per element**;
`TestStageBudget` in `internal/bench` guards it in CI, with a tolerance.

The batch is what makes that budget reachable. Measured: a batched pipeline
is ~2× faster than an element-wise one for the same useful work from two
stages on, and the gap reaches ×180 for the operators that must pull from
several streams at once, because `iter.Pull` costs 53.3 ns per value pulled
— a coroutine and a context switch — and at batch grain that cost is paid
once per thousand elements. The batch-size plateau starts at 8 elements and
stays flat up to a million; 1024 by default is safe, not critical.

## Measuring on a laptop

The figures are taken on a laptop under WSL2, in a devcontainer. That is not
the machine a benchmark wants, and the protocol exists because of it.

**The noise is larger than most changes worth making.** The decisive test
is the null experiment: run the *same binary* on both sides of an A/B
comparison. Any difference it reports is fabricated. Under a sequential
median-of-medians, the same binary "compared" to itself at −14%, +13%, +21%
depending on the benchmark. **A single-run comparison resolves nothing below
~15%.**

**The usual fixes do not work here.** `cpufreq` does not exist under WSL2:
Windows owns the clock, the `performance` governor is out of reach. A longer
`benchtime` makes it worse: at 3 s per iteration the pinned core heats and
drops back. `GOMAXPROCS=1` alone doubles the spread; it helps only with
pinning, where it brings the spread to 25%. The remaining floor, pinned to
one core with a warm clock, is about 40% between the fastest and the slowest
of twenty consecutive runs. That is the machine, not the code.

**What works: pair the rounds, then count wins.**
`internal/bench/scripts/compare.sh` runs both binaries back to back *within
each round*, on one pinned core, and takes the ratio *inside* the round.
Drift that moves both sides together — a clock ramp, a Windows background
task, throttling — cancels out of a ratio instead of landing on whichever
side ran first. The verdict is a **sign test** over the paired rounds: did
"after" win enough rounds that chance is no longer a credible explanation?
It assumes nothing about the noise's distribution, and one wild round cannot
swing it. Two things pairing does not cancel are alternated instead: the
order inside a round, and the path each binary starts from. The same bytes
of `BenchmarkHPACKDecode` measured 292 ns from one file name and 230 ns from
another, so each side runs from two copies under names of different
lengths, swapped every other round; an effect of one name then lands in
half the rounds, which the sign test reads as noise. Validated in both directions: the same binary on both sides reads
`noise` where raw medians showed up to +21%; a real change reads
`faster -52.7%`, 10 wins out of 10; an untouched benchmark reads `noise`.

```bash
internal/bench/scripts/compare.sh 'BenchmarkFlatMap$' 15 HEAD
```

Two limits to know. Below eleven paired rounds the test produces false
positives — an untouched `Peek` came out "faster −8.1%" at n=8 — so the
script refuses to rule under ten rounds and takes fifteen by default. And a
real gain under ~10% may still read `noise`: the test is honest about what
it does not know; raise the rounds, or accept that the finding is not
established on this hardware.

**The CI gate** (`internal/bench/scripts/gate.sh`) applies the same protocol
to every pull request against its merge base, and additionally requires a 3%
effect: the sign test controls the error rate of *one* comparison, and a
suite of twenty produces a spurious verdict about two times in three. A
benchmark lives in `internal/bench`, or it cannot be paired; an operator
added is added to the gated set, or nobody watches its regressions. It
measures only the benchmarks whose code changed: a benchmark is kept when the
diff touches a package its code reaches — its body and the helpers it calls,
read by `internal/bench/cmd/benchdeps` — or a dependency of one. Read by
file instead, a benchmark whose stages are helpers of its own file was
measured on a diff to `database/postgres` alone, and declared slower. The test
binary links every package `internal/bench` imports, so a change to
`net/quic` moves the code of all the others within the binary, and that move
alone has declared an untouched benchmark 4 to 19% slower, always in the same
direction, which the sign test takes for a regression. Both binaries are
therefore linked with every function aligned on 64 bytes
(`-ldflags=-funcalign=64`, measured 2026-10-23 on an AMD EPYC 7763 runner):
on a change that moves code and nothing else, seven benchmarks called
SLOWER by 3 to 32% became one at 4%, the same in two passes of one job, and a
15% slowdown injected into `Scan` was still SLOWER. The alignment is that of
a function's entry, not of the loops inside it, which is what the remaining
case shows.

**Host settings that raise the floor**, none of them required: disabling
Core Performance Boost in the BIOS, which removes the clock ramp at the cost
of about 15% absolute speed; `powercfg` with `PROCTHROTTLEMIN 100` on
Windows; `autoMemoryReclaim=disabled` and `pageReporting=false` in
`.wslconfig`.

## Reading the tables

- **Read ratios, not absolutes.** Nanoseconds do not transfer between
  sessions or machines; ratios within a session do.
- **A spread above 15% is flagged `**`** and the row must not be quoted.
- **Prefer the paired verdicts** at the end of the document to any single
  row above them: a row is an order that invites ranking; the sign test says
  which differences survive interleaving.
- **A figure without its row is not quoted.** The README quotes only figures
  present here, and a test checks it.

## Not yet measured

- Cache-driven sizing with several batches alive at once: the existing
  measurements cover a single `int64` stream.
- The cost of diagnostics carried per element — probably the model's
  biggest regression risk.
- `distinct.By`'s map, 90% of a saturated window: only another data
  structure moves it.
- An end-to-end round trip with a real remote client: this document's
  network figures are over loopback, and [Remote reads](#remote-reads)
  emulates the latency alone.
- The comparison with ecosystems other than Go, framed separately and
  published with its scenarios and method.

## HTTP/2 and HTTP/3, from a real client

This measures `net/httpstream` end to end over HTTP/2 and HTTP/3. The
package's micro-benchmarks measure each layer without a socket; here it is the whole
path, seen by a client that shares no code with sluice: the standard
library's HTTP/2 client (`net/http`, ALPN `h2` over TLS 1.3) and quic-go's
HTTP/3 client (`quic-go/http3`). Server and client run on the same machine,
over loopback, with one connection and requests in sequence. ns/op is the
round trip as the client sees it, from the request to the last byte of the
body; MB/s counts the bytes of the body.

There are four bodies. `plain` is a response rendered whole by the handler
(20 bytes). `stream` is three one-line batches pulled one at a time
(24 bytes). `bulk` is sixteen 4 KiB batches pulled one at a time (64 KiB),
several times the data a sender puts in flight before its first
acknowledgements come back (the congestion window), though it stays well
inside the flow-control windows (the limit the receiver announces on how
much data the sender may have in flight). `paced` is the same body as
`bulk` from a producer that pauses one millisecond before every batch after
the first, as a database reading the next rows would.

```
cd interop && go test -run xxx -bench 'H2|H3' -benchtime 3s -count 3
```

Median of three passes on 2026-10-10, on the same machine as the rest of
this document (laptop, WSL2, not pinned: a socket bench cannot be pinned),
after the commit `perf(httpstream): coalesce streamed batches into one
write` (1ed3551). This session is slower than the one of the morning
(`plain` over HTTP/2 at 196 µs against 118): only the ratios can be compared
from one session to the next, which is why the earlier figures are recalled.

| benchmark | median | spread | B/op | allocs/op | before 1ed3551 |
|---|---|---|---|---|---|
| `BenchmarkH2FromNetHTTP/plain` | 196 µs | ±9% | 5.8 KB | 56 | 118 µs |
| `BenchmarkH2FromNetHTTP/stream` | 309 µs | ±2% | 5.4 KB | 59 | 334 µs (2.8 × plain) |
| `BenchmarkH2FromNetHTTP/bulk` | 516 µs · 127 MB/s | ±2% | 5.4 KB | 78 | 1184 µs · 55 MB/s |
| `BenchmarkH3FromQuicGo/plain` | 378 µs | ±3% | 11 KB | 163 | 345 µs |
| `BenchmarkH3FromQuicGo/stream` | 406 µs | ±2% | 12 KB | 208 | 348 µs |
| `BenchmarkH3FromQuicGo/bulk` | 1179 µs · 56 MB/s | ±2% | 120 KB | 860 | 1053 µs · 62 MB/s |

B/op and allocs/op count both sides, the third-party client included: read
them as an order of magnitude, not as the cost of the server.

**What the figures say.** Before 1ed3551, a streamed HTTP/2 response of three
batches cost nearly three times the plain response: about 70 µs per batch
pulled, whatever its size, which HTTP/3 did not pay for small batches. The
mechanism is traced in `h2Writer.extend`: handing a batch to the HTTP/2 writer
blocked the producer until the bytes were on the wire, so one TLS write, two
hand-overs between goroutines and one TCP segment per batch, plus an empty
DATA frame for END_STREAM; quic-go, for its part, copies and gives control
back, and its send loop gathers what has piled up into one packet. Since
then, the producer appends its bytes to the job still in the queue and only
waits if that job holds one round's budget (`MaxWriteBufferBytes`) of unframed
bytes; END_STREAM rides on the last DATA frame. Three batches now cost 1.6
times the plain response (about 38 µs per batch) and `bulk` goes from 55 to
127 MB/s, twice the throughput of HTTP/3 over loopback.

What remains is a cost of rhythm, not of code: the first batch leaves as soon
as it arrives — holding it back would be delay added to a response that
streams — and those that arrive while it is being written leave together on
the next pass, so at most one write per round of the writer instead of one per
batch; on the client side, each DATA frame received wakes up the body reader.
HTTP/3 does not know this step for three batches of eight bytes because its
first packet waits for its turn in the send loop.

**Why `bulk` is slower over HTTP/3 (measured 2026-10-11).** Not the QUIC
windows. Server side, `Config.TransportParameters` scaled by 1, 4 and 16
leaves `bulk` at 47–56, 51–59 and 40–50 MB/s: the spread of one setting is
wider than the gap between settings. Client side, quic-go's receive windows
at 512 KiB (its default), 4 MiB and 16 MiB give 48, 50–54 and 53–55 MB/s; only
64 KiB, the size of the whole body, is slower (41–43 MB/s). The body fits in
the default windows, so none of them is ever the limit. The CPU profile of the
server during the transfer points elsewhere: a fifth of the samples sit in
`sendto`, once per packet, called from `Stream.Write` itself
(`writePacketLocked`), and a quarter in the scheduler parking and waking
goroutines. 64 KiB is some fifty packets, each its own system call, with no
batching of datagrams and no pacing of their departure. That is the observed
shape, not a proven split of the 1179 µs.

**Batching the datagrams (measured 2026-10-13).** The packets one
`Stream.Write` builds are sealed back to back and leave in one `sendmsg`
carrying `UDP_SEGMENT` (UDP segmentation offload, Linux 4.18): the kernel
cuts the buffer into the same datagrams a write per packet gave, so the peer
sees no difference, and a 4 KiB batch costs one system call instead of four.
The standard library is enough (`WriteMsgUDPAddrPort` with a control
message); no module was added. Paired A/B against one write per datagram,
eight rounds alternated back to back, `-benchtime 2s`, medians:

| benchmark | before | after | rounds won |
|---|---|---|---|
| `BenchmarkH3FromQuicGo/plain` | 454 µs | 464 µs | ~ (p=0.80) |
| `BenchmarkH3FromQuicGo/stream` | 464 µs | 470 µs | ~ (p=0.51) |
| `BenchmarkH3FromQuicGo/bulk` | 1447 µs · 45 MB/s | 1128 µs · 58 MB/s | 8 of 8 |

`bulk` also loses 27 allocations per request (867 to 840). The session ran
slower than the one of the table above (`bulk` at 45 MB/s before the change
against 56): in the same session, `BenchmarkH2FromNetHTTP/bulk` measured
97 MB/s, so HTTP/3 went from under half of HTTP/2 to six tenths. macOS
keeps one write per datagram — it offers no public interface for
segmentation —, as does a system that refuses the option (the first refusal
turns batching into a loop of writes for the connection).

**The same batching on Windows (measured 2026-10-19).** The one call carries
`UDP_SEND_MSG_SIZE`, the Windows counterpart of `UDP_SEGMENT` (UDP
segmentation offload: the system cuts the buffer into datagrams of the
given size). Before the first send, sluice asks the system, once per
process, whether it knows the option (`getsockopt`, as msquic does); if it
does not, each datagram stays its own write. A laptop under Linux cannot
measure this path: the figure comes from a GitHub `windows-latest` runner
(4 cores, Go 1.27.2), through the hand-started `ab` workflow, with the
protocol of the local A/B runs (`ab.sh`, two binary names, alternated
order, sign test). `BenchmarkStreamWriteBurst` is one 16 KiB `Stream.Write`
on a loopback socket, fourteen datagrams; twelve rounds of 2 s, medians:

| benchmark | before | after | rounds won |
|---|---|---|---|
| `BenchmarkStreamWriteBurst` (Windows) | 169 µs · 97 MB/s | 97 µs · 169 MB/s | 11 of 11 |

The benchmark measures the cost of sending one flight, not an HTTP/3
exchange: the benchmarks against a third-party client
(`BenchmarkH3FromQuicGo`) run under Linux only.

**Where the rest goes (measured 2026-10-13).** Two causes, neither of them
the QUIC protocol.

- *Dropped datagrams under load.* A `Listener` reads every connection's
  datagrams off one socket and queues them per connection; when a queue is
  full it drops the datagram rather than stall every other connection. The
  queue held 8 datagrams. A 64 KiB response draws back a burst — the
  client's acknowledgements and its next request — and on a busy machine the
  connection's read loop drained it too slowly: 55 datagrams dropped in
  1,200 requests, each loss stalling its request for the client's loss timer
  (about 27 ms), nearly half the time between sending and receiving. The
  queue now holds 64 (32 already dropped nothing). Paired A/B, eight rounds,
  `-benchtime 2s`: neutral on a quiet machine (83 against 84 MB/s, 5 rounds
  of 8) and on two cores (4 of 8); with fifteen CPU-bound processes running
  beside it, 23.8 to 25.7 MB/s, 7 rounds of 8. The change buys steadiness
  under load, not throughput at rest.
- *The Go scheduler.* With every core available (`-cpu 15`), `bulk` reaches
  81 MB/s against 143 over HTTP/2; on two (`-cpu 2`), 167 against 157:
  HTTP/3 catches up with HTTP/2, and both run faster. A request crosses
  several goroutines (the stream writer, the connection's read loop, its
  timer loop, and quic-go's on the client side), and each hand-off wakes a
  goroutine, often on another core. The mutex profile names who makes the
  others wait on the connection's lock: `Stream.Write` for 54% of the
  waiting, the read loop for 12%, the timer loop for 9%.

**What holds the connection's lock (measured 2026-10-18).** Since the
batched send and the queue of 64, `bulk` reaches 118 MB/s over HTTP/3
against 213 over HTTP/2 (three passes of `-benchtime 3s`, every core, same
session): HTTP/3 stays at a little over half. The benchmark now reports
`discarded`, the datagrams the `Listener`'s front door refused during the
run: zero on all four routes, and a single refusal during `bulk` fails the
benchmark. Queue losses are out of the picture.

The server's profile during `bulk` (`-cpuprofile`, `-mutexprofile`) names
the next item. `Stream.Write` keeps the connection's lock (`mu`, a *mutex*:
one goroutine at a time holds it) from one end of the write to the other:
building the packets, sealing them, then handing them to the kernel in one
`sendmsg` (`flushBatchLocked`). That system call weighs 13% of the
process's CPU samples, about two thirds of the time spent in
`Stream.Write`, and it runs with the lock held. The mutex profile says who
makes the others wait on `mu`: `Stream.Write` for 50% of the waiting, the
read loop for 13%, the timer loop for 10% — the same split as on
2026-10-13, batched send or not. While the kernel copies the batch, the
read loop cannot process the acknowledgements arriving, and they are what
opens the window for the next batch. The scheduler remains the other item
(a quarter of the samples, both sides together); taking the system call
out of the lock is the first one the server can change on its own.

**Taking the system call out of the lock (measured 2026-10-19).** It was
tried: the batches sealed under `mu` went into an ordered queue that one
goroutine at a time wrote with the lock released, the others queuing
behind it so that datagrams left in the order of their packet numbers. The
mutex profile changed as expected: the process's total wait on
`sync.Mutex` fell from 1.02 s to 0.26 s over one pass of `-benchtime 3s`,
and `Stream.Write`'s share from 49% to 16%; the read loop waited a third as
long, the timer loop hardly at all. Throughput did not follow: in a paired
A/B (`internal/bench/scripts/ab.sh`, ten rounds of `-benchtime 2s`, every
core), the version outside the lock was 2.2% slower in geometric mean, and
slower in nine rounds of ten. The waiting left the connection's lock for
the runtime's internal locks (352 ms → 623 ms in the same profile),
`sendmsg` kept its share of the CPU (20%, then 22%) and the scheduler its
own (a quarter of the samples). `bulk` sends its requests one at a time,
with one writer per connection: what bounds its throughput is the round
trip between goroutines, not mutual exclusion. The change was not kept;
contention on `mu` is no longer an item to treat until a benchmark with
several writers per connection shows it.

**One datagram per exchange (measured 2026-10-23).** Three commits made a
small HTTP/3 exchange cost the server one datagram where it used to cost
several: the response's data and the end of its stream (FIN) leave in one
frame (`feat(quic): WriteClose sends data and FIN together`), the
acknowledgements and the MAX_STREAMS credit ride the packets that carry
data instead of leaving on their own (`fix(quic): ACKs ride data,
MAX_STREAMS by eighths`), and a response rendered whole is written in one
call (`perf(httpstream): one write for a buffered h3 response`). The tests
`TestASmallExchangeCostsTheServerOneDatagram` and
`TestH3SmallResponseIsOneDatagram` hold the count; this is what it is
worth in time. Paired A/B of the commit before the first (bbf4745) against
the last (ab5665f), `internal/bench/scripts/ab.sh`, twelve rounds of
`-benchtime 2s` alternated back to back, every core, the first round
discarded as warm-up, medians:

| benchmark | before | after | rounds won |
|---|---|---|---|
| `BenchmarkH3FromQuicGo/plain` | 294 µs | 274 µs (−10%) | 11 of 11 |
| `BenchmarkH3FromQuicGo/stream` | 325 µs | 301 µs (−8%) | 10 of 11 |
| `BenchmarkH3FromQuicGo/bulk` | 766 µs · 86 MB/s | 739 µs · 89 MB/s | 8 of 11, noise |

The gain is where the datagrams went: a response of one or a few packets
had one or two more to send, and `bulk`, some fifty packets, had the same
one or two. This session ran faster than the one of the first table (`plain`
at 294 µs before the change against 378): compare the ratios, not the
figures.

**First and last byte (measured 2026-10-13).** ns/op is a mean, and it
only counts the last byte. The benchmark also records, request by request,
two delays counted from the moment the request is sent: the first byte of
the body (TTFB, *time to first byte*: the client sees the response start)
and the last (TTLB, *time to last byte*: it has all of it). It reports them
as a distribution — p50, p90, p99, p99.9, the delay under which 50%, 90%,
99% and 99.9% of the requests fall — because a mean hides the slow
requests. Median of three passes, `-benchtime 3s`, quiet machine, in µs:

| benchmark | TTFB p50 | p90 | p99 | p99.9 | TTLB p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|---|---|
| `BenchmarkH2FromNetHTTP/plain` | 138 | 186 | 354 | 610 | 138 | 186 | 357 | 614 |
| `BenchmarkH2FromNetHTTP/stream` | 208 | 256 | 432 | 670 | 209 | 257 | 435 | 671 |
| `BenchmarkH2FromNetHTTP/bulk` | 226 | 327 | 510 | 824 | 283 | 433 | 621 | 938 |
| `BenchmarkH2FromNetHTTP/paced` | 279 | 501 | 658 | 740 | 18,918 | 19,546 | 20,640 | 20,940 |
| `BenchmarkH3FromQuicGo/plain` | 211 | 331 | 563 | 1,030 | 214 | 337 | 575 | 1,051 |
| `BenchmarkH3FromQuicGo/stream` | 222 | 349 | 590 | 1,099 | 231 | 367 | 611 | 1,146 |
| `BenchmarkH3FromQuicGo/bulk` | 312 | 627 | 1,113 | 1,578 | 543 | 1,022 | 1,597 | 1,999 |
| `BenchmarkH3FromQuicGo/paced` | 304 | 615 | 987 | 1,008 | 18,609 | 19,227 | 19,810 | 20,395 |

On `paced`, the first byte arrives after 0.3 ms and the last after 19 ms,
over HTTP/2 as over HTTP/3: the first batch leaves as soon as it is ready,
and the coalescing of batches does not hold it back. That is the README's
promise — the client receives the start while the server produces the
rest — put in figures. On `plain` and `stream`, the two delays merge: the
body fits in one write. On `bulk`, the gap between first and last byte is
57 µs over HTTP/2 and 231 µs over HTTP/3: that is HTTP/3's lower throughput
seen above, not a late start. HTTP/3's tails (p99.9 around a millisecond
from `plain` on) are nearly twice those of HTTP/2. A pass of `paced` counts
only about 160 requests: its p99.9 is the slowest of them.

**Verdict.** The cost per batch is explained and what remains is owned:
`docs/guide/experimental.md` says it in one sentence for whoever sizes their
batches. A bigger batch still divides the cost by as much.

## Web pipeline, from a real client

These are the third-party figures `docs/guide/limits.md` cites for
`web/stream`, whose label leaves with a first real consumer. A client that shares no
code with sluice — `net/http` over HTTP/1.1 and HTTP/2, quic-go over
HTTP/3 — requests one order (`GET /orders/42`, with a bearer token) from two
handlers that answer the same bytes:

- `stages` is the handler the package documentation sketches, filled in:
  routing (`Router`), authentication (`Authenticate`), authorization
  (`Authorize`), the path parameter (`PathInt`), the JSON render (`Render`);
- `bare` is a plain `httpstream` handler doing the same work by hand: a
  prefix cut for the route, a byte comparison for the token, the body
  appended byte by byte.

The gap between the two is what the stages cost on a real round trip;
everything else — TLS, framing, the client — is common to both. The verifier
compares the token with a constant: the stages are measured, not a signature
check, which a real verifier would add to both sides. Before measuring, each
benchmark reads the response once and compares status, `Content-Type` and
body byte for byte: two handlers that answered differently would compare
nothing.

> **In plain words.** A round trip over loopback costs about 160 to 235 µs.
> The `web/stream` stages cost 0.28 µs of it. They do not show over the
> network, so they are read from a second benchmark, with no network, that
> calls both handlers directly.

```
cd interop && go test -run '^$' -bench 'WebStream' -count 6
```

On 2026-10-19, on the same machine as the rest of this document (laptop, AMD
Ryzen AI 9 465, WSL2, `GOMAXPROCS` 15, not pinned: a socket bench cannot be
pinned), six passes of `-benchtime 1s`, one connection, requests in sequence.
Median and spread (fastest to slowest, over the median) are computed as in
`internal/bench/scripts/consolidate.sh`. `bare` and `stages` run back to back
on each protocol, so that a machine drifting during the series moves both
figures of a pair together.

| benchmark | median | spread | p99 (TTLB) | B/op | allocs/op |
|---|---|---|---|---|---|
| `BenchmarkWebStream/h1/bare` | 165 µs | ±8% | 328 µs | 5.4 KB | 68 |
| `BenchmarkWebStream/h1/stages` | 162 µs | ±4% | 338 µs | 5.8 KB | 71 |
| `BenchmarkWebStream/h2/bare` | 170 µs | ±4% | 355 µs | 6.5 KB | 62 |
| `BenchmarkWebStream/h2/stages` | 163 µs | ±6% | 376 µs | 6.9 KB | 65 |
| `BenchmarkWebStream/h3/bare` | 227 µs | ±4% | 512 µs | 11 KB | 151 |
| `BenchmarkWebStream/h3/stages` | 235 µs | ±3% | 515 µs | 12 KB | 154 |
| `BenchmarkWebStreamHandler/bare` | 84 ns | ±4% | — | 136 B | 4 |
| `BenchmarkWebStreamHandler/stages` | 363 ns | ±2% | — | 568 B | 7 |

B/op and allocs/op of the socket benchmarks count both sides, the
third-party client included: read them as an order of magnitude, not as the
cost of the server. Their difference, though, is the server's alone, since
the client does the same thing in both cases.

**What the figures say.** Over a socket, the gap between `bare` and `stages`
is smaller than the gap from one pass to the next, on all three protocols:
`stages` is even faster over HTTP/1.1 and HTTP/2, which is only noise. The
difference that does not move is the allocations: three more and 430 to 470
bytes per request, on all three protocols. With no network, the stages cost
279 ns, 432 bytes and three allocations more than the bare handler. Against
the shortest round trip measured here (162 µs), that is 0.17%. Nothing to
optimise at this point: the cost of a request is the transport's, not the
stages'.

**What remains for the exit from experimental.** This measurement was one of
the two conditions; the other, a first real consumer building its request
path on these stages, stays open (`docs/guide/limits.md`).

## QUIC, on an emulated path

`net/quic`'s path benchmarks (`net/quic/netem_bench_test.go`) move one
stream — two, on the fairness profile — through an in-process middlebox
that charges every datagram a one-way delay and, per profile, draws a loss
(independent or in bursts), holds it back so that it arrives after its
successors, or queues it behind a bottleneck that the flows share. They
measure the congestion controller, not the CPU: a transfer takes as long as
the path lets it, so the figure is goodput — bytes the receiver got per
second of wall clock — and the machine's load barely moves it. What moves
it is how fast the window grows (`Clean`), how loss is detected and growth
resumed (`Loss1pct`, and `BurstLoss` for the same loss rate in runs of four),
whether a late datagram is mistaken for a lost one (`Reorder`, which has no
loss at all), whether bursts overrun a shallow buffer (`Bottleneck`, whose
line rate is 6.25 MB/s), how much of a deep one the controller fills
(`DeepBuffer`, read through `rtt_x`), how two copies of it split a line
(`TwoFlows`, read through `share`) and how it grows a window back on a long
round trip (`RTT40`, `RTT80`). Each benchmark also prints `rtt_x`, the
sender's final smoothed RTT over the smallest it sampled: one when the
controller keeps the queue empty, six when it keeps 200 ms of buffer full.
Each benchmark also prints `retx_B` and `lost`: the bytes the sender put
back in its queue for the losses it declared, and the packets it declared
lost. On a path that loses nothing, they count only spurious losses: a
datagram that arrives late and that the sender took for lost.

| Benchmark | Path | Transfer | Median | Goodput | Spread |
|---|---|---|---|---|---|
| `BenchmarkTransferClean` | 20 ms RTT, no loss | 8 MiB | 255.8 ms | 32.8 MB/s | ±6% |
| `BenchmarkTransferLoss1pct` | 20 ms RTT, 1% loss each way | 4 MiB | 4.40 s | 0.95 MB/s | ±4% |
| `BenchmarkTransferBurstLoss` | 20 ms RTT, 1% loss in bursts of 4 | 4 MiB | 755 ms | 5.55 MB/s | ±1%, one round at +42% |
| `BenchmarkTransferReorder` | 20 ms RTT, 2% of datagrams up to 2 ms late | 4 MiB | 4.89 s | 0.86 MB/s | ±12%, not gated |
| `BenchmarkTransferBottleneck` | 20 ms RTT, 50 Mbit/s, 5 ms of buffer | 4 MiB | 865 ms | 4.86 MB/s | ±2%, one round at +23% |
| `BenchmarkTransferBottleneck16MiB` | the `Bottleneck` link | 16 MiB | 3.33 s | 5.04 MB/s | −1% to +3%, not gated |
| `BenchmarkTransferDeepBuffer` | 20 ms RTT, 50 Mbit/s, 200 ms of buffer | 4 MiB | 788 ms | 5.33 MB/s, `rtt_x` 5.9 | ±0.2% |
| `BenchmarkTransferTwoFlows` | 20 ms RTT, 50 Mbit/s, 20 ms of buffer, two flows | 2 × 4 MiB | 1.49 s | 5.63 MB/s, `share` 0.47–0.60 | ±1%, one round at +17% |
| `BenchmarkTransferTwoFlowsVsReno` | the `TwoFlows` link, the second flow on Reno | 2 × 4 MiB | 1.49 s | 5.63 MB/s, `share` 0.42–0.54 | ±5%, not gated |
| `BenchmarkTransferRTT40` | 40 ms RTT, 100 Mbit/s, 20 ms of buffer | 8 MiB | 971 ms | 8.64 MB/s | ±1% |
| `BenchmarkTransferRTT80` | 80 ms RTT, 100 Mbit/s, 20 ms of buffer | 8 MiB | 1.35 s | 6.21 MB/s | ±2% |
| `BenchmarkTransferRTT80Shallow` | 80 ms RTT, 100 Mbit/s, 5 ms of buffer | 32 MiB | 8.80 s | 3.81 MB/s | ±2%, not gated |

```
go test -run=XXX -bench='BenchmarkTransfer' -benchtime=1x -count=8 ./net/quic/ | tee after.txt
benchstat before.txt after.txt
```

**These rows are ceilings.** Eight rounds on 2026-10-14, medians, go1.26.9,
the same laptop as the rest of this document, not pinned: an emulation that
sleeps on timers in several goroutines must not be. The spread is the
farthest round from the median; where one round of eight sits far above
it, the row says so, and that is why the statistic is the median. A change
to `net/quic` that puts a median more than **15% below** its row is a
regression until it is explained; one that puts it above replaces the row.

`RTT80Shallow` is the congestion-avoidance benchmark: the 8 MiB of `RTT80`
are gone before avoidance matters (the handshake and a slow start doubling
from 12 kB floor it near 1.2 s whatever the controller, and its 20 ms
buffer leaves a halved window at 0.78 of the path), while 32 MiB behind
5 ms of buffer leave Reno at half of the path after each loss, a datagram
per round trip from full. It is not gated: its figure moves with where the
slow start's exit loss lands (CUBIC's eight rounds spread ±25%).

`BenchmarkTransferBottleneckAfterSmall` is the 4 MiB of `Bottleneck`
written after 5 MiB of small messages on the same connection: a connection
that has served requests, then a download. It asks two questions. Did the
messages leave the window larger than they needed? `cwnd_x`, the window at
the start of the transfer over the largest flight the messages reached,
is 2.00 to 2.02: one doubling of a slow start not yet ended, the
bound the benchmark sets. Does the transfer then lose more than
`Bottleneck` alone? No: 47 to 58 packets lost, against 51 to 61. HyStart++
(`SLUICE_QUIC_SS`) changes neither figure. Not gated: the figure comes
behind two seconds of messages.

The six profiles below `Loss1pct`, plus `RTT80Shallow`, are the ones the
congestion-control palette has to prove itself on, and the Reno rows are
the starting ceiling, not a target. Reno fills a 200 ms buffer to the
brim, declares loss on the reordering profile that the path never had,
and on the shallow 80 ms path reaches three tenths of the line — those
are the margins a new controller is measured against. The first one is
CUBIC (RFC 9438, `CongestionCubic` in `TransportParameters.Congestion`;
NewReno — the classic congestion control of RFC 9002, which slows down on
loss — stays the default until a controller has been measured against a
real client):

| Benchmark | Reno, same pass | CUBIC | Reno / CUBIC | CUBIC goodput | Spread |
|---|---|---|---|---|---|
| `BenchmarkTransferClean` | 284 ms | 288 ms | 0.98 | 29.1 MB/s | ±17% |
| `BenchmarkTransferLoss1pct` | 4.47 s | 3.60 s | 1.24 | 1.17 MB/s | ±2% |
| `BenchmarkTransferBurstLoss` | 766 ms | 483 ms | 1.59 | 8.68 MB/s | ±21% |
| `BenchmarkTransferReorder` | 4.42 s | 3.72 s | 1.19 | 1.13 MB/s | ±31%, not gated |
| `BenchmarkTransferBottleneck` | 883 ms | 905 ms | 0.98 | 4.63 MB/s | ±3% |
| `BenchmarkTransferDeepBuffer` | 788 ms | 789 ms | 1.00 | 5.32 MB/s, `rtt_x` 8.1 (6.0 before HyStart++) | ±1% |
| `BenchmarkTransferTwoFlows` | 1.51 s | 1.50 s | 1.01 | 5.59 MB/s, `share` 0.46–0.56 | ±13% |
| `BenchmarkTransferRTT40` | 980 ms | 964 ms | 1.02 | 8.70 MB/s | ±22% |
| `BenchmarkTransferRTT80` | 1.38 s | 1.33 s | 1.04 | 6.30 MB/s | ±1% |
| `BenchmarkTransferRTT80Shallow` | 8.80 s | 5.04 s | 1.7 or 2.3 (two modes) | 6.66 MB/s | ±25%, not gated |

Eight rounds on 2026-10-14, interleaved — one Reno transfer, one CUBIC
transfer, eight times, the same test binary with `SLUICE_QUIC_CC=cubic`
set on the CUBIC side — medians, go1.26.9, the same laptop (`rtt_x` and
`share` for CUBIC: three rounds). The `rtt_x` of `DeepBuffer` comes from a
pass on 2026-10-16, after the loss-detection fixes: median 8.1 (7.6–9.2),
9.3 on 2026-10-14. The Reno column is the interleaved pass's own Reno,
which is what CUBIC is compared with; the rows above are the ceilings the
gate watches, and the two agree within their spread except `Clean`, which
is the one profile the machine's load moves (its 284 ms here against
256 ms above). What CUBIC proved: the 1.7× to 2.3× on the shallow 80 ms path
(two modes, depending on where the loss that ends the slow start lands) is
its window curve — a loss costs it three tenths of the window instead of
half, and the curve refills in seconds what Reno refills a datagram per
round trip; the 1.24× on `Loss1pct` and 1.59× on `BurstLoss` are the same
two facts on a short path. On the clean and bottleneck profiles it sits
within 2.5% of Reno, which is the Reno-friendly region (RFC 9438 §4.3)
doing its job; on the deep buffer it fills the queue exactly as Reno does
(`rtt_x` 6.0 against 5.9): CUBIC reads loss and nothing else, and a buffer
that does not drop is a buffer it fills. `RTT80` itself moves 4%: an 8 MiB
transfer is slow start and the handshake, not avoidance.

**HyStart++** (RFC 9406) ends CUBIC's initial slow start on the round
trip rising instead of on loss. Measured the same way, CUBIC against
CUBIC without it, eight rounds on 2026-10-14: the eight gated medians
within ±4%, `RTT80Shallow` unchanged (bimodal on both sides, 3.8 s or
5.2 s depending on where the exit loss lands). What it changes is the
loss count: on `DeepBuffer` the loss-based exit lost 779 kB in 171
batches and HyStart++ loses nothing; on `TwoFlows` half as much (181 kB
against 366 kB). The price is on `DeepBuffer` too: the five rounds of
Conservative Slow Start at a quarter growth triple the window after the
exit, and the queue ends fuller than β would have left it (`rtt_x` 9.3
against 6.0 that day; 8.1 since the loss fixes of 2026-10-16, from 7.6 to
9.2). On the 20 ms and 5 ms buffers of the other profiles it
never fires: the detector compares one round's minimum with the previous
round's, so it reads a queue one round after it starts, and a doubling
slow start overflows a buffer no deeper than the bandwidth-delay product
within that round — the loss comes first, and the exit is the loss as
before.

```
for r in 1 2 3 4 5 6 7 8; do for cc in newreno cubic; do
  SLUICE_QUIC_CC=$cc go test -run=XXX -bench='BenchmarkTransfer' -benchtime=1x -count=1 ./net/quic/ \
    | sed "s/^/$cc /" >> ab.txt
done; done
```

**BBR version 1** (draft-cardwell-iccrg-bbr-congestion-control-00; BBR is a
congestion control that measures the path's rate and delay instead of
waiting for loss) is no
longer in the palette: the IETF draft below beats it on every benchmark of
this file, and version 1 never went past an individual draft, now expired.
The figures that follow are the ones that decided it. It was the first
model-based controller of the palette:
it paces at the highest delivery rate measured over the last ten round
trips and keeps about two bandwidth-delay products of window, probing
for more bandwidth one round in eight and draining the path for 200 ms
every ten seconds to re-measure its propagation delay; loss is not a
signal to it. Measured the same way as CUBIC, eight rounds on
2026-10-14, interleaved with the same binary and `SLUICE_QUIC_CC=bbr1`,
on the four profiles the controller was written for:

| Benchmark | Reno, same pass | BBRv1 | Reno / BBRv1 | BBRv1 goodput | Spread |
|---|---|---|---|---|---|
| `BenchmarkTransferLoss1pct` | 4.31 s | 376 ms | 11.5 | 11.2 MB/s | ±13% |
| `BenchmarkTransferBottleneck` | 881 ms | 818 ms | 1.08 | 5.13 MB/s (0.82 of the line), `rtt_x` 1.22 | ±4% |
| `BenchmarkTransferDeepBuffer` | 788 ms | 782 ms | 1.01 | 5.37 MB/s, `rtt_x` 1.8 (Reno 5.9) | ±0.3% |
| `BenchmarkTransferTwoFlows` | 1.51 s | 1.52 s | 0.99 | 5.53 MB/s, `share` 0.47–0.54 | ±2% |

Three things it proved and two it did not. `Loss1pct` is the point of a
model: a path that drops one datagram in a hundred still delivers at
its rate, and BBRv1 sends at that rate where Reno halves its window at
every loss (0.98 MB/s, the Mathis bound). On `DeepBuffer` it holds the
queue instead of filling it — `rtt_x` 1.6 to 2.0 across the rounds
against Reno's 5.9 and CUBIC's 9.3 — at the same goodput. On
`Bottleneck` it moves 4 MiB in 818 ms against 881, with less of the
spread Reno shows there (786–844 ms against 853–1195). What it did not
reach: the 85% of the line the palette plan set for this profile (0.82
here), and a smoothed round trip within 1.5× of the path's on the deep
buffer (1.8). Both are the version's documented behaviour, read in the
controller's trace. Startup detects the full pipe three rounds after the
fact, pacing at 2/ln 2 of the estimate meanwhile, which on a 5 ms
buffer loses (gain − 1) × BDP a round, 60 to 90 kB each, before Drain;
and the max filter keeps the highest sample of ten rounds, which the
acknowledgement timing of the emulated path inflates by 5 to 7% above
the line, so the cruise paces above the link and only the window's gain
of two bounds the queue — one bandwidth-delay product of it on a buffer
that never drops, the 5 ms buffer's worth of loss on one that does.
The ceiling of any controller on this transfer is 0.83 to 0.84 of the
line (671 ms of line time, 80 to 90 ms of fixed cost and about 50 ms of
ramp from 12 kB, worked out under the draft's table below), so the 85%
the plan set is out of the measurement's reach, not the version's; what
is version 1's is one or two hundredths under that ceiling (the Startup
overshoot just described) and the spread, and the draft's later
revisions (a Startup that exits on loss, an in-flight bound from the
loss rate) are the next step of the palette, not a tuning of this one.

Against a Reno flow on the same bottleneck (the `TwoFlows` link, one
flow each, started together, four rounds, not a benchmark of the suite)
BBRv1 takes 0.60 to 0.69 of the delivered rate — the version's known
appetite, with no bound on what it leaves a loss-based flow; the
draft's later revisions add one, and that figure is the next step's
criterion. Reno stays the default: nothing here is a measurement
against a real client.

**BBR as the IETF drafts it** (draft-ietf-ccwg-bbr-06, the version Linux
ships as BBRv3, `CongestionBBR`) keeps version 1's model of the path and
reads loss as evidence about how much the path holds: a round that loses
more than 2% while probing bounds the volume in flight, with 15% of that
bound left free while cruising; a loss between probes cuts the rate and
the volume to 0.7 of what they were until the next probe, two to three
seconds later or as soon as a Reno flow of the same window would have
grown back; Startup ends on six loss events in a round as well as on a
plateau. Measured the same way, eight rounds on 2026-10-14, interleaved
with the same binary and `SLUICE_QUIC_CC=bbr`, on the four profiles of
BBRv1's table, the burst-loss one, and a new benchmark,
`BenchmarkTransferTwoFlowsVsReno` — the `TwoFlows` link with the second
flow on Reno whatever the variable says, `share` the first flow's:

| Benchmark | Reno, same pass | BBR | Reno / BBR | BBR goodput | Spread |
|---|---|---|---|---|---|
| `BenchmarkTransferLoss1pct` | 4.45 s | 277 ms | 16.1 | 15.2 MB/s | 266–440 ms |
| `BenchmarkTransferBurstLoss` | 757 ms | 256 ms | 2.96 | 16.4 MB/s | 253–324 ms |
| `BenchmarkTransferBottleneck` | 876 ms | 797 ms | 1.10 | 5.26 MB/s (0.84 of the line), `rtt_x` 1.22 | 795–824 ms (Reno 854–1031) |
| `BenchmarkTransferDeepBuffer` | 790 ms | 787 ms | 1.00 | 5.33 MB/s, `rtt_x` 1.77 (Reno 5.9) | `rtt_x` 1.19–2.09 |
| `BenchmarkTransferTwoFlows` | 1.49 s | 1.53 s | 0.97 | 5.48 MB/s, `share` 0.45–0.55 | ±5% |
| `BenchmarkTransferTwoFlowsVsReno` | 1.49 s | 1.62 s | 0.92 | 5.18 MB/s, `share` 0.56 (0.46–0.61) | ±8% |

What changed from version 1. `Loss1pct` goes from 11.5× Reno to 16×,
and `BurstLoss` — a profile BBRv1 was not measured on — from Reno's
757 ms to 256 ms: a loss between probes costs a round at 0.7 of the
rate and the volume, not a halved window. `Bottleneck` moves 4 MiB in
797 ms against 876, in a 4% band where Reno spreads over 21%: Startup
leaves on the first round that loses (one round of overshoot, 126 kB,
where version 1 lost three), and the short-term bound then holds the
flight at the product plus the buffer. On `DeepBuffer` the queue is
what version 1 held, 1.8× the path's round trip at the end against
Reno's 5.9, at the same goodput. Against a Reno flow BBR takes 0.56 of
the line (0.46 to 0.61 across the rounds) where version 1 took 0.60 to
0.69: the loss-rate bound and the headroom are what the draft added for
this, and the criterion was 0.70 at most.

The two figures the palette plan set and version 1 missed are missed
again, by the draft rather than by this reading of it. 0.85 of the line
on `Bottleneck`: 797 ms is 0.84, and the ceiling of the measurement is
there — 671 ms of line time, 80 to 90 ms of fixed cost (a 64 kB transfer
takes 91 to 104 ms on this profile, 25 ms of handshake included) and
about 50 ms of ramp (four rounds under the line rate: 12, 24, 48, 96 kB
against 125 kB a round) make 800 to 810 ms, 0.83 to 0.84. On a 16 MiB
transfer (`BenchmarkTransferBottleneck16MiB`, eight rounds on
2026-10-16 interleaved with Reno), out of that ceiling, BBR delivers
0.93 of the line (2.88 s median, 2.870 to 2.886 s) against Reno's 0.81
(3.33 s, 3.29 to 3.43 s). A smoothed round trip
within 1.5× of the path's on `DeepBuffer`: 1.77 here, and 2.25 as the
median over a 16 MiB transfer. With no loss and no ECN (the mark a router
sets on a packet when its queue builds, before it drops any) the draft bounds
the queue by the window's gain alone (§5.3.1.1: (gain − 1) × the
estimated product, plus `extra_acked`), and the estimate carries +5.6%:
the emulated path's delivery-rate samples are noisy by ±3% either way
(median 0.99 of the line, 1.01 at the ninetieth percentile, 1.04 at
most in a round) and the max filter keeps the highest over two ProbeBW
cycles, so the cruise paces at 0.99 × 1.056 of the line and adds 4% of
a product to the queue every round, with nothing to drain it before the
next probe (two to three seconds, or 63 rounds) or ProbeRTT (five
seconds). That figure is not a property of the draft on a path without
a signal; it asks for ECN (§3.7: measured below) or an estimator that
does not keep upward noise. Both criteria are rewritten as measured;
nothing in the controller is tuned toward them. Reno stays the default:
nothing here is a measurement against a real client.

**BBR with ECN.** draft-ietf-ccwg-bbr-06 §3.7 specifies no response to
CE marks, only that a connection that marks its packets ECT MUST treat
them as congestion. The response here reads a mark as a loss without the
retransmission: a round that saw one between probes cuts the short-term
model to 0.7, and while probing the marks count with the losses against
the 2% that bounds `inflight_longterm`. Reno and CUBIC hear the same
signal as RFC 9002 §7.3.2 and B.7 have it, a congestion event dated by
the frame's largest acknowledged packet: the window halves (Reno) or
takes β (CUBIC), once per recovery period.
`BenchmarkTransferDeepBufferECN` is `DeepBuffer` behind an AQM that marks
a datagram CE once it waits more than 5 ms for the line — the depth of
`Bottleneck`'s buffer — and still drops at 200 ms. It runs over four UDP
sockets on the loopback interface rather than in-memory pairs, the only
transport here that carries the ECN field, and its server is a
`Listener`, which reads each datagram's ECN field with its bytes. Eight
rounds per controller on 2026-10-14, one binary,
`SLUICE_QUIC_CC` the variable; `rtt_x` is the figure to read, the times
are not `DeepBuffer`'s to the millisecond:

| Controller | Transfer, median | Spread | `rtt_x`, median | Spread | Same controller on `DeepBuffer` |
|---|---|---|---|---|---|
| BBR | 791 ms, 5.3 MB/s | 789–795 ms | 1.09 | 1.07–1.21 | 784 ms, `rtt_x` 1.71 (1.64–2.00) |
| Reno | 1.18 s, 3.5 MB/s | 0.86–1.36 s | 1.09 | 1.06–1.18 | 790 ms, `rtt_x` 5.9 |
| CUBIC | 883 ms, 4.7 MB/s | 0.84–1.09 s | 1.09 | 1.08–1.10 | `rtt_x` 8.1 (HyStart++ row above) |

The queue BBR lets build falls from 1.7× the path's round trip to 1.1×
at the same goodput: the marked rounds cut `inflight_shortterm` to 0.7
of the window, the queue drains under the marking threshold, and the
next probe finds the threshold again and goes DOWN on it. This is the
`rtt_x` ≤ 1.5 the palette plan asked of `DeepBuffer`, met with the
signal the draft names and not without it. Reno and CUBIC hold the same
queue and pay in goodput, as a loss-driven controller does on a shallow
buffer: every marked round is a window cut, and a 5 ms threshold on a
20 ms path is a quarter of a product. Nothing here is a measurement
against a real client, and the marking AQM is the emulator's: a step at
a threshold, not CoDel's or PIE's.

The gate (`gate-path.sh`) runs once per controller: CI sets
`SLUICE_QUIC_CC` to reno, cubic and bbr in three parallel jobs, and
both sides of the A/B run the same controller. A controller nobody
measures is one whose regressions pass green. On an identical tree
(2026-10-16, eight rounds each) the eight rows hold within 0.978–1.012 for
Reno, 0.991–1.011 for CUBIC and 0.990–1.013 for BBR.

Under CUBIC, two rows still turned the job red on CI on commits that
touched no Go code: `Bottleneck` (1.076) and `TwoFlows` (1.083). Eighty
interleaved rounds on an identical tree, on four cores, cut into ten passes
of eight (2026-10-19), show why: `Bottleneck` holds within 0.948–1.059, one
transfer landing anywhere between 797 and 954 ms; `TwoFlows` holds within
0.992–1.015 but is bimodal — four transfers in five near 1.46 s, the rest
at 1.53–1.64 s — and the median of eight changes mode when four land high.
Resampled from those 160 transfers, the chance that one pass reads more
than 6% slower is 3.9% for `Bottleneck` and 1.1% for `TwoFlows`; at 10%,
0.03% and 0.01%. Those two rows are therefore judged at 10% under CUBIC,
the others stay at 6%. Sixteen rounds at 6% would have left 1.2% and
doubled the job. The initial window halved still turns the cubic job red,
on rows that keep 6% (`Clean` 1.140, `RTT80` 1.077, `BurstLoss` 1.069);
under CUBIC it moves neither `Bottleneck` (0.957) nor `TwoFlows` (1.038).

A second script, `gate-cc.sh`, holds the three controllers other than
Reno to their own criteria, on one tree and without a base: the median of
`share` on `TwoFlowsVsReno` is 0.70 at most for all three (measured: CUBIC
0.51, BBRv1 0.61, BBR 0.50); the median round of `RTT80Shallow` is at
least 1.3 times faster on CUBIC than on Reno in the same round (measured:
1.42 to 1.48, median 1.44, 2026-10-21); every round of `DeepBufferECN`
keeps `rtt_x` at 1.5 at most on BBR (measured: 1.05 to 1.09). Both sides of
`RTT80Shallow` are bimodal, by where the slow start's exit loss lands —
CUBIC 3.7 s or 5.1 s, Reno 5.3 s or 8.6 s on 2026-10-21, where Reno read
8.8 s every round when the bound was set (2026-10-14) — and a round that
pairs CUBIC's slow mode with Reno's fast one reads 1.03 and says nothing
about the controller: the worst round, the criterion until then, failed
three of the sixteen CI runs of 2026-10-21. The median falls
under the bound only when the slow mode becomes the common case. `gate-path.sh` sees a controller
that gets worse from one commit to the next; `gate-cc.sh` sees one that
never met its criterion, or drifts from it by less than 6% a commit.
CUBIC's β brought from 0.7 to 0.5 turns the cubic job red in both scripts
— `BurstLoss` 1.566 and `Loss1pct` 1.255 in `gate-path.sh`, every round
of `RTT80Shallow` within 1.09–1.12 in `gate-cc.sh` — and leaves the reno
job green (0.991 to 1.007).

**The CI gate watches the rows of its `GATED` list per controller** —
eight on each — through a second script,
`internal/bench/scripts/gate-path.sh`, run by its own CI job beside
`gate.sh`'s on every pull request and every push to `main`. It differs
from `gate.sh` in the three ways these rows require: it builds
`net/quic`'s test binary rather than `internal/bench`'s; it runs the two
binaries unpinned, with the runtime's own `GOMAXPROCS`, since the emulation
sleeps on timers in several goroutines; and it rules on the median of
eight transfers a side, interleaved before/after so drift cancels, red
when the median after is more than 6% above the median before (10% for
`Bottleneck` and `TwoFlows` under CUBIC, above) rather than on a 3% effect
by sign test. Ten passes on an identical tree kept every
ratio within 0.970–1.015; an initial window halved put four rows 11–24%
above. One transfer per round (`-benchtime=1x`): the figure is a
transfer's wall clock, not a rate the harness scales. `Reorder` stays out of the gate:
on an identical tree its median moved 7% between the two sides (the eight
gated profiles, under 2.1%), because a spurious loss is declared or not by
a few microseconds of scheduling. Its `lost` is no steadier: 21 to 55
spurious losses over 24 transfers: a packet threshold lowered from three
to two moves its median from 34 to 40.5, inside that spread, so a gate on
the count would not see it either. To run the gate by
hand against the tree before a change:

```
internal/bench/scripts/gate-path.sh main
```

## Joins and hydration, in isolation

The figures the guides quote for choosing a join and for the generated
hydration. Six runs of one second, median, 2026-10-23, go1.26.9, 15 cores,
under WSL2, not pinned. `JoinMerge` and `JoinHash` join 65,536 rows a side,
sorted, through `join.Merge` and through the hash table (`internal/bench`);
`GeneratedHydrate` decodes 1,024 rows of six columns into a struct through
the code `sluicegen` writes, a nullable `*string` and a copied string
included (`database/postgres/internal/entity`).

| benchmark | ns per element or row | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkJoinMerge` | 18.5 | 18.3–19.3 | 25,465 | 20 |
| `BenchmarkJoinHash` | 83.0 | 81.9–88.7 | 8,422,070 | 600 |
| `BenchmarkGeneratedHydrate` | 53.8 | 52.8–55.7 | 21,846 | 2,048 |

## The architecture's figures

The figures [the architecture](design/architecture.md) quotes for its
decisions, each from the benchmark named beside it. Six runs of one second,
median, 2026-10-23, go1.26.9, 15 cores, under WSL2, not pinned. Each table
counts in the unit its header names; the spread is the lowest and highest of
the six runs.

Per element, over 1,048,576 `int64` (`internal/bench`). `BaselineLoop` is
the ceiling, a native `for range`; `SeqStages/stages=0` is the same traversal
pushed through an `iter.Seq`; the two `MergeTwoStreams` rows merge two halves
by `iter.Pull`, one value or one batch of 1024 at a time.

| benchmark | ns per element | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkBaselineLoop` | 0.219 | 0.215–0.222 | 0 | 0 |
| `BenchmarkSeqStages/stages=0` | 1.05 | 1.03–1.05 | 72 | 3 |
| `BenchmarkPullPerElement` | 53.3 | 52.9–58.9 | 267 | 7 |
| `BenchmarkPullPerBatch` | 0.299 | 0.293–0.300 | 272 | 7 |
| `BenchmarkMergeTwoStreams/element` | 53.2 | 53.0–53.7 | 559 | 14 |
| `BenchmarkMergeTwoStreams/batch1024` | 0.295 | 0.291–0.300 | 544 | 14 |
| `BenchmarkMergeOperator` | 0.305 | 0.300–0.327 | 568 | 16 |
| `BenchmarkMergeOperatorEight` | 0.349 | 0.319–0.392 | 2,305 | 57 |
| `BenchmarkWindowWide` | 5.43 | 5.32–5.67 | 8,413,960 | 1,038 |

The joins, 65,536 rows a side. The interval join and the two joins it is
compared with are measured in the same runs here; the hash join's spread is
wide on this machine, and the [section above](#joins-and-hydration-in-isolation)
is the one the guides quote.

| benchmark | ns per element | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkIntervalJoinNarrow` (zero-width window) | 82.5 | 81.4–83.1 | 24,248 | 56 |
| `BenchmarkIntervalJoinWide` (±2 s window) | 141 | 139–141 | 92,538 | 105 |
| `BenchmarkJoinHash` | 93.5 | 76.9–101.6 | 8,421,980 | 599 |
| `BenchmarkJoinMerge` | 21.8 | 21.0–22.1 | 25,464 | 20 |

Per row, six columns decoded into a struct (`database/postgres`), over a
batch of 1,024 rows or of one.

| benchmark | ns per row | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkHydrateRowMajor` | 29.6 | 29.1–30.5 | 15,584 | 1,024 |
| `BenchmarkHydrateReflect` | 56.7 | 56.2–59.3 | 15,608 | 1,025 |
| `BenchmarkHydrateReflectBoxed` | 187.5 | 185.4–190.5 | 163,764 | 8,192 |
| `BenchmarkHydrateColumnar` | 36.7 | 36.1–36.9 | 15,589 | 1,024 |
| `BenchmarkHydrateRowMajorBatchOne` | 26.1 | 25.9–26.4 | 8 | 1 |
| `BenchmarkHydrateColumnarBatchOne` | 40.5 | 39.8–42.0 | 8 | 1 |

Per call: one request through the six steps of the
[request path](design/architecture.md#the-request-path), written by hand,
through a pipeline built per request, or through one built once
(`internal/bench`); the first `Add` to a diagnostics collector, and an `Add`
past its ceiling.

| benchmark | ns per call | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkWebHandwritten` | 1.98 | 1.95–2.02 | 0 | 0 |
| `BenchmarkWebBatchOne` | 280 | 277–288 | 368 | 13 |
| `BenchmarkWebBatchOneReused` | 142 | 140–147 | 128 | 6 |
| `BenchmarkDiagnosticsFirstAdd` | 553 | 512–584 | 2,048 | 1 |
| `BenchmarkDiagnosticsOverCeiling` | 1.24 | 1.24–1.25 | 0 | 0 |

Per run of a whole pipeline. The four `Scan` rows read a simulated table
through [`pushdown`](design/architecture.md#enriched-stop-signal--pushdown) with no demand, with one
that never changes, and with a consumer that skips ahead, publishing its
position or not (`pushdown`). The four `Parallel` rows run 65,536 elements
through `parallel.Ordered` with one worker or four, on an `f` that does
about 34 µs of work per batch or only passes its batch through
(`internal/bench`).

| benchmark | µs per run | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkScanNoDemand` | 1,908 | 1,868–1,947 | 8,192 | 1 |
| `BenchmarkScanQuietDemand` | 1,868 | 1,854–1,936 | 8,192 | 1 |
| `BenchmarkScanSkipAheadControl` | 445 | 441–452 | 8,192 | 1 |
| `BenchmarkScanSkipAhead` | 64.4 | 63.2–66.9 | 9,344 | 138 |
| `BenchmarkParallelSerialBaseline` | 2,160 | 2,131–2,229 | 524,430 | 69 |
| `BenchmarkParallelHeavyWork` | 2,051 | 2,003–2,172 | 558,552 | 92 |
| `BenchmarkParallelTrivialSerial` | 16.0 | 15.9–16.1 | 136 | 5 |
| `BenchmarkParallelTrivialWork` | 56.6 | 55.4–58.0 | 34,247 | 28 |

Where four workers start to pay: `BenchmarkParallelWorkSize` runs the same
65,536 elements with the work per element doubled from one row to the next,
one worker against `parallel.Ordered` and `parallel.Unordered` on four
(`internal/bench`, six runs of one second, medians). The work per batch is
the serial run divided by its 64 batches; a gain is the serial run divided
by the parallel one.

| rounds | work per batch, µs | serial, µs per run | `Ordered` ×4, µs per run | `Unordered` ×4, µs per run | `Ordered` gain | `Unordered` gain |
|---|---|---|---|---|---|---|
| 64 | 34 | 2,148 | 2,063 | 1,356 | ×1.04 | ×1.58 |
| 128 | 83 | 5,328 | 3,131 | 2,275 | ×1.70 | ×2.34 |
| 256 | 192 | 12,307 | 5,139 | 4,432 | ×2.39 | ×2.78 |
| 512 | 404 | 25,868 | 9,693 | 8,473 | ×2.67 | ×3.05 |
| 1024 | 850 | 54,390 | 18,487 | 17,076 | ×2.94 | ×3.19 |

---

## Measured in one session

Produced by `internal/bench/scripts/consolidate.sh` in one session, on one
machine. That is the point of this file: the repository's rule is that absolute
figures do not transfer across sessions, and the tables it replaced had been
assembled from a dozen runs over several hours — each honest, none comparable
to the next.

### Conditions

<!-- consolidate: conditions -->
| | |
|---|---|
| Date | 2026-08-21 12:05 UTC |
| Go | go1.26.6 |
| CPU | AMD Ryzen AI 9 465 w/ Radeon 880M |
| Cores | 15 |
| Kernel | Linux 6.18.33.2-microsoft-standard-WSL2 |
| PostgreSQL | 15.19 |
| Server | 127.0.0.1:5433 (loopback) |
| benchtime | 1s × 3, median reported, spread = (max-min)/median |
| Pinned | core 14, GOMAXPROCS=1, except where concurrency is the point |
<!-- /consolidate -->

**This is a laptop under WSL2, not a measurement rig.** Linux neither sees nor
controls the host clock here; two sequential runs of the same binary differ by
up to 14%, and the network-bound rows are worse than that at a short benchtime.
Read ratios rather than absolutes, and prefer the paired verdicts at the end
over any single row above them.

A **spread** over 15% is flagged with `**`. Those rows should not be quoted:
the run-to-run variation is larger than most of the differences the table is
being read for. Raise `BENCHTIME` and re-run before believing them — a point
read measured 127 µs in one run and 318 µs in another at `BENCHTIME=300ms`,
and settled at 285-298 µs across five runs of 1s.

### Connector, against pgx / lib/pq / database/sql

One connection each, since this connector has no pool and a comparison that
ignores that measures pooling.

**Re-measured on 2026-10-19, outside the session above.** The conditions
differ from the "Conditions" table: Go 1.26.9, kernel
6.18.54.1-microsoft-standard-WSL2, the same CPU (AMD Ryzen AI 9 465),
PostgreSQL 17.11 in the devcontainer's `postgres` container (a Docker
bridge, not loopback; data on a 512 MB tmpfs, `fsync` off). Same protocol:
1 s × 3, median, core 14, `GOMAXPROCS=1`. The absolute values in this table
do not compare with the other sections of this file; the ratios within it
do. `bench_copy` is now `UNLOGGED` and emptied after every COPY, outside the
timer: the two COPY rows no longer measure the server writing the WAL, which
is the same work for both clients. The tables are loaded by
`go run ./internal/bench/cmd/pgfixtures`, which reads `SLUICE_PG*`.

<!-- consolidate: connector -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGAnyPgxSQL` | 326525 ns | ±3% | 96169 B | 3486 |
| `BenchmarkPGAnyPgx` | 362928 ns | ±8% | 114321 B | 2770 |
| `BenchmarkPGAnySluiceFetchAll` | 347519 ns | ±2% | 471 B | 12 |
| `BenchmarkPGAnySluiceCached` | 345199 ns | ±1% | 471 B | 12 |
| `BenchmarkPGAnySluice` | 348790 ns | ±1% | 473 B | 12 |
| `BenchmarkPGCopyPgx` | 8344756 ns | ±3% | 3492470 B | 99800 |
| `BenchmarkPGCopySluice` | 7322436 ns | ±1% | 2367172 B | 14 |
| `BenchmarkPGPointLibPQ` | 302271 ns | ±1% | 944 B | 26 |
| `BenchmarkPGPointPgxSQL` | 146520 ns | ±5% | 872 B | 19 |
| `BenchmarkPGPointPgx` | 141672 ns | ±4% | 664 B | 10 |
| `BenchmarkPGPointSluiceFetchAll` | 146497 ns | ±11% | 424 B | 12 |
| `BenchmarkPGPointSluiceCached` | 155305 ns | ±5% | 424 B | 12 |
| `BenchmarkPGPointSluice` | 154913 ns | ±1% | 424 B | 12 |
| `BenchmarkPGWideLibPQ` | 14126229 ns | ±1% | 5819629 B | 478505 |
| `BenchmarkPGWidePgxSQL` | 12990871 ns | ±2% | 5819996 B | 478513 |
| `BenchmarkPGWidePgx` | 9382291 ns | ±2% | 6740578 B | 280008 |
| `BenchmarkPGWideSluiceFetchAll` | 9777159 ns | ±3% | 6394 B | 23 |
| `BenchmarkPGWideSluice` | 10178372 ns | ±3% | 6711 B | 23 |
<!-- /consolidate -->

**Point read with no option (measured 2026-10-13).** A connection now keeps
its prepared statements by default (up to 512) and asks for the whole result
of a statement whose last execution fit in one batch (`FetchAuto`).
`BenchmarkPGPointSluice`, which sets nothing, therefore pays a single round
trip from its second execution on: one write and one read per query
(`writes/op` = 1, `reads/op` = 1 with `SLUICE_PG_COUNT=1`), as pgx does.

- Against pgx, in the same binary, 15 alternating rounds (plus one
  discarded), 2 s each, core 6, `GOMAXPROCS=1`: medians 196 µs against
  197 µs, sluice slower in 7 rounds of 15. No detectable difference (sign
  test, p = 1).
- Against the same read in two round trips, paired over 8 rounds:
  184 µs against 367 µs (−50%), faster in 8 rounds of 8.

Server: the `postgres` container (PostgreSQL 17.11, Docker bridge). The
table above, re-measured on 2026-10-19 against the same server, measures the
new default (155 µs against 142 for pgx in that unpaired run; the paired
verdict above is the one to read).

**First and last batch of a wide read (measured 2026-10-13).** The table
above gives a mean per complete read. The wide read (`bench_wide`, 20,000
rows of eleven columns, about 2.5 MB on the wire) also records, query by
query, two delays counted from the call: the first batch in the caller's
hands (TTFB, *time to first batch*) and the end of the result (TTLB, *time
to last batch*). pgx hands over one row at a time: its first batch is its
first row. sluice hands over batches of 1,024 rows.
`BenchmarkPGWideSluiceFetchAll` is the same read with `FetchAll`: one
`Execute` for the whole result, the server sends every row while the client
decodes, and the caller receives the same batches of 1,024 rows.

Fifteen paired rounds (the first one dropped), the three benchmarks back to
back in one process each round, `-benchtime 2s`, core 6, `GOMAXPROCS=1`.
Server: PostgreSQL 17.11 in the container next to the development container
(a Docker bridge, as for the table above, on another day). Median of the
rounds, in µs; a round counts 150 to 200 reads, so its p99.9 is the slowest
one:

| benchmark | ns/op | TTFB p50 | p90 | p99 | p99.9 | TTLB p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|---|---|---|
| `BenchmarkPGWidePgx` | 9.97 ms | 255 | 419 | 730 | 1,033 | 9,927 | 11,058 | 13,202 | 14,087 |
| `BenchmarkPGWideSluice` | 13.45 ms | 711 | 818 | 1,331 | 1,428 | 13,275 | 14,335 | 18,132 | 19,474 |
| `BenchmarkPGWideSluiceFetchAll` | 10.42 ms | 724 | 846 | 1,408 | 1,723 | 10,334 | 10,931 | 13,821 | 16,616 |

Sign-test verdicts, 14 rounds:

- `FetchAll` against the default: −22.5% in ns/op, 14 rounds of 14; TTLB
  p50 −22.4%, 14 of 14; TTFB unchanged (noise). The 3.5 ms gap with pgx is
  therefore first the round trip per batch: at the time, sluice sent the
  `Execute` for the next batch only after delivering the previous one
  (`database/postgres/conn.go`), and the server waited while the client
  decoded. Twenty batches, twenty waits: 3.0 ms of the 3.5. Fixed since:
  see the next paragraph.
- `FetchAll` against pgx: +4.2% in ns/op (13 rounds of 14), TTLB p50 +4.1%
  (12 of 14). 0.4 ms remain. It is not decoding: on a CPU profile, the
  sluice client spends 31% of the read idle, waiting on the socket, and
  decoding is a quarter of its busy time; pgx is busy throughout, mostly
  allocating (`mallocgc` 43%). Nor is it the number of reads: sluice makes
  about 300 `read` calls per query, 8 KiB each, pgx about 540. The
  remainder is named in the next paragraph: an idle client is slow to wake.
- sluice's TTFB (711 µs against 255) is the batch size, not a delay: 1,024
  rows at pgx's pace take about 510 µs, plus its first byte. A smaller batch
  arrives sooner; that is what `BatchRows` sets.

```
cd benchmarks && go test -run '^$' -bench 'PGWide(Sluice|SluiceFetchAll|Pgx)$' -benchtime 2s
```

**Two batches asked for ahead (measured 2026-10-13).** The default read now
sends the `Execute` for the next two batches as soon as the portal
suspends, before the batch in hand reaches the consumer: the server
produces batch k+1 while the consumer works through batch k, and finds the
request for k+2 waiting. Same protocol as above, fifteen paired rounds,
the binary before the change and after it back to back in each round. The
machine was slower than for the table above, so the absolute figures do not
compare with it; the ratios within a round do:

| benchmark | ns/op | TTFB p50 | p90 | p99 | p99.9 | TTLB p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|---|---|---|
| `BenchmarkPGWidePgx` | 13.11 ms | 359 | 545 | 951 | 1,236 | 12,764 | 14,953 | 20,811 | 23,249 |
| `BenchmarkPGWideSluice`, before | 18.53 ms | 973 | 1,288 | 1,836 | 1,965 | 17,914 | 21,431 | 27,440 | 28,609 |
| `BenchmarkPGWideSluice`, after | 14.64 ms | 1,044 | 1,437 | 1,904 | 2,001 | 14,192 | 16,698 | 21,153 | 23,748 |
| `BenchmarkPGWideSluiceFetchAll` | 13.85 ms | 1,009 | 1,299 | 1,946 | 2,171 | 13,380 | 15,195 | 22,990 | 24,138 |

- After against before: −22.9% in ns/op, 15 rounds of 15; TTLB p50
  −21.9%, 15 of 15. The round trip per batch is gone.
- The first batch pays for it: TTFB p50 +5.6% (worse in 14 rounds of 15),
  about 70 µs, the write of the two `Execute` messages, which now goes out
  before the first batch is handed over.
- After against `FetchAll`: +6.1% in ns/op (worse in 13 of 15). The cause is
  the write once per batch, where `FetchAll` writes once per query. Counted
  on the socket (a `net.Conn` that reads and writes through
  `syscall.RawConn` and counts every call and every `EAGAIN`, the "not
  ready, wait" answer of a non-blocking socket): the default makes 21
  writes per query and spends 365 µs inside them, `FetchAll` one write and
  41 µs; a write costs about 16 µs on this machine (a Docker bridge under
  WSL2). Everything else is equal: 157 parked reads per query on both
  sides, 7.5 ms waiting for the server on both, so the server is no slower
  to resume a suspended portal than to continue one. With a consumer that
  works (300 µs of CPU per batch, next point), the gap is 1.7% (median of
  six rounds), within the 3% the writes cost. Asking for batches two at a
  time would halve the writes (about 160 µs, 1.5%) and lengthen an early
  stop's drain to three batches; not done.
- `FetchAll` against pgx: sluice waits for the server, pgx does not. sluice
  decodes faster than the server sends, empties the socket and parks its
  read 157 times per query (7.5 ms waiting, 5.3 ms of system CPU); pgx,
  busy allocating, parks 15 times (0.95 ms). Each park puts the core to
  sleep and the next segment has to wake it. A consumer that works shows
  it: 300 µs of CPU per batch, 6 ms per query, makes the read *faster*,
  10.75 ms to 9.88 ms (median of three rounds), 16 parks per query, below
  pgx's 10.6 ms the same session. The 0.4 ms are the cost of waking an
  idle client on this machine, paid only by a consumer that does nothing
  with its rows; the connector has nothing to fix there.
- Two variants measured and rejected: one batch asked for ahead (−4.7%, 9
  rounds of 15, not significant: the server still waits for batch k+1 to
  be read before it hears of k+2), and two batches kept asked for but
  topped up after delivery (−13.6%, 15 of 15, TTFB unchanged: the server
  idles while the consumer works).
- An early stop now drains up to two batches instead of none
  (`QueryConfig.BatchRows`). A `Demand` keeps one request per batch, after
  delivery, because the request depends on what the consumer says.

`SLUICE_PG_COUNT=1` adds the socket counts to the output of the wide reads,
`SLUICE_PG_SPIN=300us` the consumer's work per batch.

### Remote reads

**Measured on 2026-10-19**, in the conditions of the "Connector" re-measure
above (the same PostgreSQL 17.11 container, core 14, `GOMAXPROCS=1`), with a
round trip — the time a message takes to reach the server and come back —
added by a proxy. The proxy is `TestPGDelayProxy`
(`benchmarks/pgdelay_test.go`): a process of its own, pinned to cores 0-5,
that holds every segment half the round trip in each direction. It emulates
latency alone; the bandwidth stays loopback's.

Wide read (`bench_wide`, 20,000 rows of eleven columns, batches of 1,024),
paired A/B by `ab.sh`, 14 rounds of 1 s, medians in ms. Before: the
adaptive ask-ahead depth alone; after: the end-of-read `ReadyForQuery` also
read behind the next query. `FetchAll` and pgx do not run the changed code:
they are the controls.

| benchmark | loopback | 1 ms | 10 ms |
|---|---|---|---|
| `PGWideSluice` (default) | 10.21 → 10.06 | 15.24 → 14.90 (−2.5%, 11/13) | 34.04 → 22.97 (−32.2%, 13/13) |
| `PGWideSluiceFetchAll` | 10.13 → 9.86 | 14.22 → 14.21 (noise) | 21.83 → 21.75 (noise) |
| `PGWidePgx` | 10.09 → 10.04 (noise) | 13.63 → 13.66 (noise) | 21.22 → 21.24 (noise) |
| `PGWideSluiceDemand` | 13.90 → 14.10 (noise) | 38.07 → 36.70 (−3.2%, 12/13) | 230.96 → 221.04 (−4.4%, 13/13) |

- On loopback, the default and `FetchAll` both gain 2.6 to 2.7% (12/13).
  `FetchAll` does not run the changed code, so that is a bias the two
  binaries share, not a gain; a second pass puts the default at noise
  (7/13). `Demand` read `SLOWER` +2.3% (1/13) on the first pass and noise
  on the second (+1.4%, 8/13); the table gives the second.
- At a distance, the default's gap to `FetchAll` goes from 7.2% to 4.8% at
  1 ms and from 56% to 5.6% at 10 ms. No structural round trip is left
  between the two: the closing `Sync` leaves without being waited for, and
  a statement already seen sends its Executes ahead with the query. The
  1.2 ms left at 10 ms came from the depth: see "Asking ahead by the
  shortest batch" below.
- `Demand` still kept one request per batch here, after delivery: twenty
  round trips per read, ten times `FetchAll` at 10 ms. The next section
  measures the fix.
- What the change lets through: a SELECT, VALUES or TABLE read outside a
  transaction block is reported complete at its last row, before its
  commit, and a commit that fails then — a function it calls that writes,
  a deferred constraint — is logged, not returned. A read inside a block
  and a statement that may write still wait for their `ReadyForQuery`.

#### Under a demand that holds

**Measured on 2026-10-19**, same conditions, same proxy. Before: a demand
asked one batch at a time; after: a demand whose bound does not move and
that has no limit asks ahead like a query without one. The default does not run the
changed code: it is each series' control.

| benchmark | loopback | 1 ms | 10 ms |
|---|---|---|---|
| `PGWideSluiceDemand` | 13.68 → 10.81 (−20.8%, 13/13) | 38.92 → 16.01 (−58.6%, 13/13) | 220.87 → 24.60 (−88.8%, 13/13) |
| `PGWideSluice` (default, control) | 10.68 → 10.65 (noise) | 16.29 → 16.36 (noise) | 23.99 → 24.81 (noise) |

- The demand is now within 2% of the default at all three distances. The
  1 ms and 10 ms series are slower than the table above for both
  benchmarks (the default at 24.81 against 22.97): that is the machine's
  state on the day, which the control carries too.
- The top-up leaves before the batch is delivered, as without a demand.
  Sent after delivery, a first version stayed 15% behind the default on
  loopback with as many Executes in flight; sent before, the gap is gone.
- What the demand pays: a bound that moves while batches are asked ahead
  makes them worthless (they continue the old portal), so they are read
  and dropped up to the re-bind's `BindComplete` — about one round trip of
  the server's work. After a move nothing is asked ahead until the bound
  holds for a batch: `AdvanceBy`, which moves on every batch, pays that
  once per read. A limit asks for one batch at a time, sized to it.

#### Asking ahead by the shortest batch

**Measured on 2026-10-19**, same conditions, same proxy. Before: the depth
came from the statement's latest first batch; after: a statement's depth (⌈RTT / T₁⌉ + 1, the number of
batches asked ahead) comes from the shortest first batch the statement has
timed rather than from its latest. T₁ is the time of the first batch, from
the `BindComplete` to the `PortalSuspended`: the server's work plus
whatever delayed this side, and only the second varies. Timed on its latest
read alone, the same statement was given depths from 11 to 31 at 10 ms for
one result of 20 batches.

The long read (`PGLongSluice`) is the wide read five times over: 100,000
rows, 98 batches, more than one depth covers at 10 ms; the 20-batch read
fits whole in one depth and hides the steady state. `PGStopSluiceBatched`
stops at the first batch of the long read: the batches asked ahead are read
and dropped, then the closing `Sync`'s `ReadyForQuery`; that is what the
depth costs a consumer that stops. `FetchAll` and pgx are the controls.

| benchmark | loopback | 1 ms | 10 ms |
|---|---|---|---|
| `PGWideSluice` (default) | 10.66 → 10.65 (noise) | 14.90 → 14.23 (−5.5%, 13/13) | 23.55 → 22.68 (−3.0%, 13/13) |
| `PGWideSluiceFetchAll` | 10.50 → 10.62 (+1.3%, 0/13) | 14.57 → 14.31 (noise) | 21.89 → 21.84 (noise) |
| `PGWidePgx` | — | 13.71 → 13.73 (noise) | 21.40 → 21.53 (noise) |
| `PGLongSluice` | 60.34 → 61.54 (noise) | 73.60 → 71.76 (noise) | 91.55 → 82.34 (−9.3%, 11/11) |
| `PGLongSluiceFetchAll` | — | 75.24 → 75.38 (noise) | 81.75 → 81.65 (noise) |
| `PGStopSluiceBatched` | 2.36 → 2.36 (noise) | 5.07 → 6.46 (+27.6%, 0/13) | 28.08 → 33.29 (+19.6%, 0/13) |

- The default's gap to `FetchAll` on the wide read goes from 7.6% to 3.9%
  at 10 ms, and from 2.3% to none at 1 ms. On the long read at 10 ms, from
  12% to 0.8%: with the previous depth, the server waited for part of every
  round trip. At 1 ms the batched long read is already ahead of `FetchAll`.
- The long read on loopback and at 10 ms ran under the `PGTmpLong…` names
  of a temporary file with the same code; the 10 ms series has 12 rounds
  (11 pairs judged), the others 14. At 10 ms each row comes from its own
  series, paired within itself. `FetchAll`'s +1.3% on
  loopback is a bias the two binaries share: it does not run the changed
  code.
- What the change costs: a consumer that stops at the first batch waits
  1.4 ms longer at 1 ms and 5.2 ms longer at 10 ms, the extra batches it
  drains. That is the round trip of the server's work a stop was meant to
  cost (see `depth` in `database/postgres`); the previous depth undercut
  it. On loopback nothing moves.
- A margin on the formula (×1.5 or ×2 on ⌈RTT / T₁⌉) was measured first:
  ×2 brought the wide read to 4.0% of `FetchAll` at 10 ms, as close as the
  shortest batch, but by asking for two round trips of the server's work,
  which every stop would drain, instead of correcting the estimate.

### Codec, in isolation

<!-- consolidate: codec -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGDecodeInt8ArrayPgxFlat` | 5204 ns | ±2% | 8256 B | 4 |
| `BenchmarkPGDecodeInt8ArrayPgx` | 5330 ns | ±3% | 8272 B | 5 |
| `BenchmarkPGDecodeInt8ArraySluice` | 687 ns | ±0% | 0 B | 0 |
| `BenchmarkPGEncodeInt8ArrayPgxFlat` | 13933 ns | ±0% | 7736 B | 965 |
| `BenchmarkPGEncodeInt8ArrayPgx` | 14484 ns | ±1% | 7760 B | 966 |
| `BenchmarkPGEncodeInt8ArraySluice` | 492 ns | ±4% | 0 B | 0 |
<!-- /consolidate -->

### Web pipeline, in process

No socket, no kernel. See the socket table below before drawing anything from
the ordering here.

<!-- consolidate: web -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkWebChi` | 1927 ns | ±4% | 2584 B | 23 |
| `BenchmarkWebEcho` | 1810 ns | ±2% | 1896 B | 21 |
| `BenchmarkWebGin` | 1642 ns | ±0% | 1864 B | 19 |
| `BenchmarkWebNetHTTP` | 1676 ns | ±1% | 1928 B | 21 |
| `BenchmarkWebSluice` | 1764 ns | ±0% | 1952 B | 22 |
<!-- /consolidate -->

### Web pipeline, over a socket

Not pinned: a socket benchmark needs a client and a server at once, and pinning
both to one core measures their contention.

<!-- consolidate: socket -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkSocketChi` | 187419 ns | ±4% | 10902 B | 113 |
| `BenchmarkSocketConcurrentFiberFasthttp` | 1024082 ns | ±3% | 214592 B | 2644 |
| `BenchmarkSocketConcurrentNetHTTP` | 1179177 ns | ±4% | 353284 B | 3585 |
| `BenchmarkSocketConcurrentSluice` | 1143356 ns | ±3% | 339770 B | 3613 |
| `BenchmarkSocketEcho` | 185395 ns | ±2% | 10125 B | 111 |
| `BenchmarkSocketFiberFasthttp` | 183669 ns | ±7% | 6707 B | 82 |
| `BenchmarkSocketGin` | 183640 ns | ±1% | 10171 B | 109 |
| `BenchmarkSocketNetHTTP` | 170371 ns | ±14% | 10113 B | 111 |
| `BenchmarkSocketSluice` | 184708 ns | ±1% | 10120 B | 112 |
<!-- /consolidate -->

#### The connection budget and the batch bound (2026-10-14)

`net/httpstream` closes an HTTP/1.1 connection after
`Config.MaxRequestsPerConn` requests (1024 by default), and a client that
does not pipeline — `net/http`'s own client, most load generators — dials
again: a TCP handshake and a full TLS 1.3 handshake that `net/http`'s server
never asks for. `BenchmarkConnLimitTLS*` (`internal/bench/connlimit_test.go`)
prices it on this machine (Ryzen AI 9 465 under WSL2, loopback, eight clients
with one connection each, no session resumption, client and server on the
same CPU), 15 paired rounds by `versus.sh`: 1024 against unbounded, **+3.7%
per request** (the budget won 0 of 14 rounds); 16 against unbounded, +224%,
which gives the handshake itself: (115.1 − 34.9) µs × 16 ≈ 1.3 ms for both
halves, and 1.3 ms ÷ 1024 ≈ 1.25 µs predicts +3.6% of a 35 µs round trip. The
measured gap is the handshake and nothing else. On a deployment only the
server's half lands on its CPU. The default stays. What this does not
measure is the tail: over a 20 ms network each reconnection holds one request
in 1024 for two extra round trips, which is exactly the p99.9 the comparison
study measures (axis D).

`Config.MaxRequestsPerBatch` only binds for a client that pipelines.
`BenchmarkBatchLimitTLS*`, one connection with 256 requests written before
any answer is read: 8 against 64, +83% per request (0 of 14); 256 against
64, −16% (12 of 14), one response write per batch instead of four. The
default stays at 64, the bound on what one burst may allocate; a consumer
measured pipelining more than 64 deep is the condition to revisit it.

### The gateway, with a database behind it

Sixty-four concurrent requests, each needing one row. Not pinned, for the same
reason.

<!-- consolidate: gateway-db -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkWebDBLibPQOneConn` | 24689962 ns | ±2% | 483135 B | 3264 |
| `BenchmarkWebDBPgxOneConn` | 12926242 ns | ±3% | 457682 B | 2150 |
| `BenchmarkWebDBPgxPool16` | 1915795 ns | ±3% | 455170 B | 2094 |
| `BenchmarkWebDBPgxPool64` | 1279128 ns | ±8% | 449370 B | 1962 |
| `BenchmarkWebDBSluiceOneConn` | 898204 ns | ±6% | 470318 B | 2251 |
<!-- /consolidate -->

#### The gateway, against a bounded backend

The benchmarks of `gateway/`, outside the paired gate: they measure the
scheduler as much as the code. A resource serves 16 queries at once and each
sleeps 100 µs; 256 callers. Under WSL2 that sleep lasts close to a
millisecond, the same rounding that hits `Within`, so the ratios below are
those of a query of about 1 ms. Three runs of 2 s, median, 2026-10-22,
15 cores.

| benchmark | ns per call | throughput ratio |
|---|---|---|
| `BenchmarkPerCallRoundTrip` | 63,988 | 1 |
| `BenchmarkBatchedRoundTrip` | 5,389 | 11.9 |
| `BenchmarkBatchedParallelRoundTrip` | 4,142 | 15.4 |
| `BenchmarkBatchedNothingBehind` | 3,688 | — |
| `BenchmarkLoneCall` | 1,110,493 | — |

`BatchedNothingBehind` is the gateway's own price, with nothing to
amortise. `LoneCall` is one caller under a `Within` of 500 µs: it waits for
the clock, not for company. The "65 times slower" of earlier versions came
from a first benchmark with no bound on the resource, since removed; the
benchmark's comment tells why.

### Partitioning by principal

<!-- consolidate: partitioning -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPartitionAllDistinct` | 1243 ns | ±2% | 0 B | 0 |
| `BenchmarkPartitionSingleTenant` | 454 ns | ±3% | 0 B | 0 |
<!-- /consolidate -->

### Statement cache

<!-- consolidate: statement-cache -->
| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGPrepareHit` | 786 ns | ±0% |  B |  |
| `BenchmarkPGPrepareMissUnbounded` | 1669 ns | ±25% ** |  B |  |
| `BenchmarkPGPrepareOff` | 783 ns | ±1% |  B |  |
| `BenchmarkStmtCacheChurn` | 136 ns | ±2% | 55 B | 3 |
| `BenchmarkStmtCacheHit` | 16 ns | ±0% | 0 B | 0 |
<!-- /consolidate -->

### Paired verdicts

A single run of each contender produces numbers in an order, which invites
ranking. The sign test says which of those differences survive interleaving.

<!-- consolidate: verdicts -->
| `BenchmarkSocketNetHTTP` vs `BenchmarkSocketSluice` | 183857 ns | 183752 ns | noise | 5/12 wins, 10 needed |
| `BenchmarkSocketGin` vs `BenchmarkSocketSluice` | 177060 ns | 177126 ns | noise | 8/12 wins, 10 needed |
| `BenchmarkSocketNetHTTP` vs `BenchmarkSocketFiberFasthttp` | 175211 ns | 191202 ns | SLOWER +8.8% | 0/12 wins, 10 needed |
<!-- /consolidate -->
