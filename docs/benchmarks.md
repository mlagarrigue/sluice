# Measurements

Every performance figure in the repository has a benchmark and a
denominator. This page says how the figures are produced, what they are read
against, and why a figure taken alone is worth nothing on the machine that
produces them. The tables after this head are regenerated in one session by
`internal/bench/scripts/consolidate.sh`; the session's conditions are written
with them.

## The ceiling and the budget

**A native `for range` loop over `[]int64`: 0.310 ns per element, zero
allocations.** That is the ceiling; every measurement of the core is
expressed as a multiple of it. A core operator has a budget of **about 1.5 ns
per stage per element**; `TestStageBudget` in `internal/bench` guards it in
CI, with a tolerance.

The batch is what makes that budget reachable. Measured: a batched pipeline
is ~2× faster than an element-wise one for the same useful work from two
stages on, and the gap reaches ×179 for the operators that must pull from
several streams at once, because `iter.Pull` costs 68.6 ns per value pulled
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
swing it. Validated in both directions: the same binary on both sides reads
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
added is added to the gated set, or nobody watches its regressions.

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
  network figures are over loopback.
- The comparison with ecosystems other than Go, framed separately and
  published with its scenarios and method.

---

## Measured in one session

Produced by `internal/bench/scripts/consolidate.sh` in one session, on one
machine. That is the point of this file: the repository's rule is that absolute
figures do not transfer across sessions, and the tables it replaced had been
assembled from a dozen runs over several hours — each honest, none comparable
to the next.

## Conditions

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

## Connector, against pgx / lib/pq / database/sql

One connection each, since this connector has no pool and a comparison that
ignores that measures pooling.

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGAnyPgxSQL` | 763743 ns | ±0% | 96170 B | 3486 |
| `BenchmarkPGAnyPgx` | 741611 ns | ±2% | 114322 B | 2770 |
| `BenchmarkPGAnySluiceAllRows` | 697092 ns | ±1% | 102703 B | 37 |
| `BenchmarkPGAnySluiceCached` | 800525 ns | ±1% | 102704 B | 37 |
| `BenchmarkPGAnySluice` | 1011804 ns | ±4% | 102698 B | 36 |
| `BenchmarkPGCopyPgx` | 12871464 ns | ±7% | 3492658 B | 99799 |
| `BenchmarkPGCopySluice` | 11484811 ns | ±1% | 2370686 B | 5 |
| `BenchmarkPGPointLibPQ` | 308396 ns | ±1% | 944 B | 26 |
| `BenchmarkPGPointPgxSQL` | 146960 ns | ±5% | 872 B | 19 |
| `BenchmarkPGPointPgx` | 139916 ns | ±6% | 664 B | 10 |
| `BenchmarkPGPointSluiceAllRows` | 138623 ns | ±2% | 248 B | 11 |
| `BenchmarkPGPointSluiceCached` | 241789 ns | ±1% | 248 B | 11 |
| `BenchmarkPGPointSluice` | 291786 ns | ±3% | 240 B | 10 |
| `BenchmarkPGWideLibPQ` | 15820111 ns | ±2% | 5819627 B | 478505 |
| `BenchmarkPGWidePgxSQL` | 13901578 ns | ±2% | 5820000 B | 478513 |
| `BenchmarkPGWidePgx` | 9091637 ns | ±2% | 6740547 B | 280008 |
| `BenchmarkPGWideSluice` | 13609370 ns | ±2% | 601564 B | 60 |

## Codec, in isolation

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGDecodeInt8ArrayPgxFlat` | 5204 ns | ±2% | 8256 B | 4 |
| `BenchmarkPGDecodeInt8ArrayPgx` | 5330 ns | ±3% | 8272 B | 5 |
| `BenchmarkPGDecodeInt8ArraySluice` | 687 ns | ±0% | 0 B | 0 |
| `BenchmarkPGEncodeInt8ArrayPgxFlat` | 13933 ns | ±0% | 7736 B | 965 |
| `BenchmarkPGEncodeInt8ArrayPgx` | 14484 ns | ±1% | 7760 B | 966 |
| `BenchmarkPGEncodeInt8ArraySluice` | 492 ns | ±4% | 0 B | 0 |

## Web pipeline, in process

No socket, no kernel. See the socket table below before drawing anything from
the ordering here.

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkWebChi` | 1927 ns | ±4% | 2584 B | 23 |
| `BenchmarkWebEcho` | 1810 ns | ±2% | 1896 B | 21 |
| `BenchmarkWebGin` | 1642 ns | ±0% | 1864 B | 19 |
| `BenchmarkWebNetHTTP` | 1676 ns | ±1% | 1928 B | 21 |
| `BenchmarkWebSluice` | 1764 ns | ±0% | 1952 B | 22 |

## Web pipeline, over a socket

Not pinned: a socket benchmark needs a client and a server at once, and pinning
both to one core measures their contention.

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

## The gateway, with a database behind it

Sixty-four concurrent requests, each needing one row. Not pinned, for the same
reason.

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkWebDBLibPQOneConn` | 24689962 ns | ±2% | 483135 B | 3264 |
| `BenchmarkWebDBPgxOneConn` | 12926242 ns | ±3% | 457682 B | 2150 |
| `BenchmarkWebDBPgxPool16` | 1915795 ns | ±3% | 455170 B | 2094 |
| `BenchmarkWebDBPgxPool64` | 1279128 ns | ±8% | 449370 B | 1962 |
| `BenchmarkWebDBSluiceOneConn` | 898204 ns | ±6% | 470318 B | 2251 |

## Partitioning by principal

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPartitionAllDistinct` | 1243 ns | ±2% | 0 B | 0 |
| `BenchmarkPartitionSingleTenant` | 454 ns | ±3% | 0 B | 0 |

## Statement cache

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkPGPrepareHit` | 786 ns | ±0% |  B |  |
| `BenchmarkPGPrepareMissUnbounded` | 1669 ns | ±25% ** |  B |  |
| `BenchmarkPGPrepareOff` | 783 ns | ±1% |  B |  |
| `BenchmarkStmtCacheChurn` | 136 ns | ±2% | 55 B | 3 |
| `BenchmarkStmtCacheHit` | 16 ns | ±0% | 0 B | 0 |

## Paired verdicts

A single run of each contender produces numbers in an order, which invites
ranking. The sign test says which of those differences survive interleaving.

```
BenchmarkSocketNetHTTP vs BenchmarkSocketSluice                                     183857.0000 -> 183752.5000  noise            wins=5/12 (need 10)
BenchmarkSocketGin vs BenchmarkSocketSluice                                     177060.0000 -> 177126.0000  noise            wins=8/12 (need 10)
BenchmarkSocketNetHTTP vs BenchmarkSocketFiberFasthttp                                     175211.0000 -> 191202.0000  SLOWER +8.8%     wins=0/12 (need 10)
```
