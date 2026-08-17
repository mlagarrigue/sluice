# Step 0 — Measured performance ceiling

> Project: **sluice** (`github.com/mlagarrigue/sluice`)

> Run on 2026-08-17 · Go 1.26.6 · linux/amd64
> AMD Ryzen AI 9 465 (15 visible cores) · L1d 48 KiB/core · L2 1 MiB/core · L3 16 MiB
> `-benchtime=300ms -count=3`, median of 3 · code: [internal/bench/](../internal/bench/)

> **Reading these figures.** The first benchmark of a process absorbs the CPU
> frequency ramp and the first traversal of a freshly allocated dataset. Left
> alone, that inflated the baseline — the denominator of every other figure — by
> ~24%, and made the same measurement land at ×225 on one run and ×183 on the
> next. `TestMain` now spins for a second and faults in a dataset before any
> measurement runs; the baseline holds 0.305 ns ± 1%.
>
> A second effect survives the warm-up: in a full-suite run, a fast benchmark
> placed right after a slow one (`iter.Pull` at 68 ns/element) starts on a
> down-clocked core and decays over its repetitions. **Figures below 1 ns are
> therefore taken from runs restricted to the fast benchmarks**, where they are
> stable to within a few percent. A whole-suite median is not trustworthy at that
> scale.

Without a measured denominator, "high performance" is not falsifiable
([ARCHITECTURE.md §13](ARCHITECTURE.md)). This document establishes that denominator and
confirms — or refutes — the assumptions in the spec.

---

## The ceiling

**Native `for range` loop over `[]int64`: 0.310 ns/element, 0 allocations.**

All measurements below are expressed as a multiple of this ceiling.

---

## Result 1 — All-batch is confirmed

Assumption from §2.1, borrowed from the database literature. **Confirmed**, with strictly
identical useful work:

| Stages | Element-wise | Batch of 1024 | Gain |
|---|---|---|---|
| 1 | 3.78 ns (×12.2) | 2.19 ns (×7.1) | **×1.72** |
| 2 | 7.19 ns (×23.2) | 3.66 ns (×11.8) | **×1.97** |
| 4 | 12.92 ns (×41.6) | 6.65 ns (×21.4) | **×1.94** |

The gain stabilizes around **×1.9–2.0** from two stages onward. That is less than
the order of magnitude reported by X100 (~50× over MySQL), but the comparison is not
the same: X100 measured a complete SQL engine where 62% of the time went into tuple
navigation, not a `Map` loop over integers already in memory. **In the case least
favorable to batching — near-zero useful work — the gain is already ×2.**

### Marginal cost per stage

| Model | Slope |
|---|---|
| Element-wise | **~2.6 ns per stage per element** |
| Batch of 1024 | **~1.5 ns per stage per element** |

The per-stage cost is **linear** in both models: no collapse with depth,
a 10-stage pipeline stays predictable. Batching reduces the slope by
~42%.

> Takeaway: entering `iter.Seq` already costs ×5.5 the ceiling (1.72 ns) before any
> operator. The first stage is the most expensive; the following ones are marginal.

---

## Result 2 — `iter.Pull` costs 3.5× more than announced

**The spec quoted ~20 ns per value, from a community source. That is wrong here.**

| Measurement | Cost |
|---|---|
| `iter.Pull`, pulled element-wise | **68.6 ns/element** (×221) |
| Verification on a minimal harness | **70.0 ns/element** |
| Same source in direct push | 1.44 ns/element |
| `iter.Pull`, pulled **by batch of 1024** | **0.49 ns/element** (×1.6) |

The overhead of pull versus push is about **×48** on the same source. The
figure was verified on a second harness stripped of all indirection to
rule out a measurement artifact.

> **Consequence for the spec.** §2.4 must carry 68 ns, not 20 ns. The rule
> "`iter.Pull` only on batches" goes from recommendation to **absolute
> obligation**: at 68.6 ns/element, a tuple-at-a-time `Pull` is 221× the ceiling and
> disqualifies any hot path that would use it.

---

## Result 3 — Batched merge is indeed negligible

Direct validation of §7.3, which was an arithmetic projection:

| Merge of 2 streams | Cost |
|---|---|
| Element-wise (2 × `iter.Pull`) | **69.8 ns/element** (×225) |
| By batch of 1024 | **0.39 ns/element** (×1.3) |

**×179 difference.** The `Merge` operator sits at 1.3× the ceiling in batch mode — that is,
essentially free — whereas it is unusable tuple-at-a-time.

§7.3 stated "it is all-batch that makes the operator possible, not merely
faster". **That is now measured, and the statement is stronger than expected**: the spec's
calculation projected ~0.02 ns/element from a cost of 20 ns; the real cost being
68.6 ns, we get 0.39 ns — still negligible, but 20× more than projected.

---

## Result 4 — Batch size: the plateau starts at 8

This is the most unexpected result.

| Size | ns/element |
|---|---|
| 1 | 7.005 |
| **8** | **3.515** |
| 64 | 3.790 |
| 256 | 3.687 |
| 512 | 3.604 |
| **1024** | **3.558** |
| 2048 | 3.561 |
| 8192 | 3.555 |
| 65536 | 3.551 |
| 1048576 (all) | 3.509 |

**The plateau is reached from 8 elements onward, and it is perfectly flat up to 1M.**
The gap between 8 and 1024 is 1.2% — within the noise. Between 1024 and 8192, 0.1%.

Two consequences:

1. **The choice of 1024 is safe, but not critical.** No reason to change it for
   2048 (DuckDB) or 8192 (DataFusion), and no reason to worry about a batch that
   drops to 64 or 128 after a filter.
2. **The right-hand collapse predicted by X100 does not occur here** — because the
   work measured is an in-place `Map` on a single array, with no intermediate
   materialization. The X100 curve measured *multiple* vectors alive
   simultaneously. **The assumption "the set of live batches must fit in cache"
   is therefore neither confirmed nor refuted by this test** — it will have to be re-measured on a
   multi-column pipeline.

> **Honest caveat.** This benchmark tests only a single stream of `int64`. The
> cache-based sizing remains to be verified on a realistic case (join,
> several live columns).

---

## Result 5 — A selective filter does not degrade batching

Case at 1% selectivity, the one that motivates `Coalesce` (§7.5):

| Model | Cost |
|---|---|
| Element-wise | 2.58 ns (×8.3) |
| Batch of 1024, sparse batches, **without** `Coalesce` | **0.71 ns (×2.3)** |

Batching remains **3.6× faster** even with batches reduced to ~10 useful elements,
and **with no allocation at all**. Batched filtering into a reused buffer avoids the
per-element indirection cost.

> `Coalesce` remains justified to avoid propagating sparse batches **downstream over
> several stages**, but it is not a performance emergency on an isolated stage.

---

## Result 6 — `Split` in lock-step: the O(1) promise holds

§6.3 claimed a fan-out with an O(1) backlog. It was theoretical — this is the
measurement, taken against a plain batched traversal of the same data
(`BenchmarkSplitBaseline`, 0.325 ns/element).

| Mode | Cost | vs ceiling | Allocations |
|---|---|---|---|
| Baseline (no `Split`) | 0.324 ns/element | ×1.1 | 0 |
| Partition, one branch consumed | **0.272 ns/element** | ×0.9 | 1 per batch, from `route` |
| Broadcast, two branches in alternation | **0.929 ns/element** | ×3.0 | 1 per batch, from `route` |

**Partition costs nothing** — it lands below the baseline because half the
batches are dropped rather than summed. **Broadcast costs ~2.9× the baseline**,
which is expected: every element is visited twice, once per branch, and the
`iter.Pull` alternation sits on top.

Both stay far under the 1.5 ns per-stage budget, and `Split` itself allocates
nothing: the allocations counted above are the `[]int` that the benchmark's own
`route` returns per batch. A caller who returns a preallocated slice drops them
to zero — measured at 29 allocations per million elements, i.e. setup only.

> **Caveat.** `route` is called **once** per batch, by construction. That matters
> for round-robin routing, which is stateful: an implementation calling it twice
> would double-advance the counter and misroute the data. Pinned by
> `TestSplitRoutesOncePerBatch`.

---

## Result 7 — `Merge`: the §7.3 projection holds on the real operator

Result 3 measured a hand-rolled merge harness. This is the shipped operator,
against a plain back-to-back traversal of the same two halves:

| Measurement | Cost | Overhead |
|---|---|---|
| Baseline: both halves traversed, no merge | 0.328 ns/element | — |
| `Merge` of 2 sources | **0.398 ns/element** | +0.07 ns |
| `Merge` of 8 sources | **0.429 ns/element** | +0.10 ns |

**The cost does not scale with the source count.** Four times as many sources
costs 8% more, because the per-source expense is one `iter.Pull` — a fixed
setup, amortized over every batch that source contributes. Allocations behave
the same way: 16 for two sources, 59 for eight, all of it construction, none of
it per batch.

At 0.4 ns/element the operator sits at ×1.3 the ceiling, matching the 0.39 ns
that §7.3 projected from the step 0 harness.

---

## Result 8 — `MergeJoinBy` does not fit the 1.5 ns budget, and cannot

This is the one operator so far that misses the per-stage ceiling. The figure is
recorded rather than smoothed over, because the reason is structural.

| Measurement | Cost |
|---|---|
| Hand-written merge join, no output materialized | 1.10 ns/element |
| Hand-written, materializing `EitherOrBoth` into a batch | **3.40 ns/element** |
| `MergeJoinBy`, no keys match | 11.6 ns/element |
| `MergeJoinBy`, every key matches | 14.7 ns/element |
| `MergeJoinBy`, 8×8 duplicate keys (cross product) | 24.2 ns/element |

**The floor is 3.4 ns, not 1.5.** Two reasons, both inherent:

1. **A merge join is element-at-a-time by nature.** `Map` runs a tight loop the
   compiler can vectorize; a merge join compares one key pair, decides which side
   advances, and cannot know the next step until it has. There is no tight loop
   to optimize.
2. **Each output row is 32 bytes** (two values plus two flags) against 8 for an
   `int64`. Materializing them costs 2.3 ns of the 3.4 — measured by writing the
   same rows from hand-written code.

The operator sits ~3.4× above that floor. Two hypotheses for the gap were tested
and **both were wrong**:

- *Memoizing the key in the cursor.* Profiling put 44% of runtime in `peek`,
  which re-extracted the key on every call. Caching it gained 33% (16.6 → 11.1
  ns) — real, and kept — but `peek` still holds 39%.
- *The indirect `cmp` call.* The line profile attributed 320 ms to `cmp(lk, rk)`,
  suggesting an unelidable indirect call. Measured against a hand-written join
  calling `cmp` through a function value: **1.45 ns vs 1.50 ns direct — no
  difference.** Go devirtualizes a locally-known closure. Those 320 ms are memory
  stall attributed to the line, not call overhead.

A third attempt — splitting `peek` so the hot path would inline — made it
*slower* (12.2 vs 11.1) and was reverted.

> **Where this leaves the operator.** It is correct, O(1) in memory on unique
> keys, and works on infinite streams. It is also the wrong tool for a hot inner
> loop over millions of rows. What remains unexplained is the gap between 3.4 and
> 11.6 ns; closing it needs a different approach from the three tried here, not
> another round of micro-optimization.

### The sort check costs nothing measurable

§7.2 wanted the monotonicity check behind a debug flag. Measured, one comparison
per element against the several the merge already performs is lost in the noise —
and the failure it prevents is a silently wrong result. **The check is always
on**, and the debug mode the spec asked for is not worth building.

---

## Synthesis — what the measurements change in the spec

| # | Finding | Effect |
|---|---|---|
| 1 | All-batch: **×1.9 to ×2.0** from 2 stages onward | §2.1 **confirmed** |
| 2 | `iter.Pull`: **68.6 ns**, not 20 ns | §2.4 and §7.3 to be **corrected** |
| 3 | Batched `Merge`: **0.39 ns** (×1.3) | §7.3 **confirmed**, more strongly than expected |
| 4 | Batch-size plateau from **8** elements onward | §2.3: 1024 confirmed, but **not critical** |
| 5 | 1% filter: batch stays ×3.6 faster, 0 alloc | §7.5: `Coalesce` **useful, not urgent** |
| 6 | Per-stage cost **linear** (2.6 ns element / 1.5 ns batch) | Deep pipelines **predictable** |
| 7 | `Split` lock-step: **0.27 ns** partition, **0.94 ns** broadcast, 0 alloc | §6.3 **confirmed**, no longer theoretical |
| 8 | `Merge` operator: **0.40 ns** for 2 sources, **0.43 ns** for 8 | §7.3 **confirmed** on the real operator, and flat in source count |
| 9 | `MergeJoinBy`: **11.6 ns**, floor 3.4 ns — misses the budget | §7.2: the ceiling **does not apply** to an element-at-a-time operator |
| 10 | Sort check: **no measurable cost** | §7.2: the debug mode is **unnecessary**, the check is always on |

### The most important point going forward

The gap between the ceiling (0.31 ns) and a 4-stage batch pipeline (6.65 ns) is
**×21**. This is not "zero-cost" — the spec is right to promise nothing of the sort.
But most of that gap is the useful work itself (4 additions and 4
memory writes), not the stream machinery.

**The machinery costs ~1.5 ns per stage per element.** That is the figure to remember,
and the budget not to exceed for any new core operator.

---

## Not yet measured

1. **Cache-based sizing** with several live batches (cf. Result 4).
2. **The cost of diagnostics** carried per element (§4) — probably the biggest
   regression risk in the model.
3. **The bounded hash join** (§9.1) and its materialization cost.
4. **Columnar decoding** versus row-by-row decoding (§8.6).
