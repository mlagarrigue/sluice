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
