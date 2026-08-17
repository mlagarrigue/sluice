# Step 0 — Measured performance ceiling

> Project: **sluice** (`github.com/mlagarrigue/sluice`)

> Run on 2026-08-17 · Go 1.26.6 · linux/amd64
> AMD Ryzen AI 9 465 (15 visible cores) · L1d 48 KiB/core · L2 1 MiB/core · L3 16 MiB
> `-benchtime=300ms -count=3`, median of 3 · code: [internal/bench/](../internal/bench/)

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

## Synthesis — what the measurements change in the spec

| # | Finding | Effect |
|---|---|---|
| 1 | All-batch: **×1.9 to ×2.0** from 2 stages onward | §2.1 **confirmed** |
| 2 | `iter.Pull`: **68.6 ns**, not 20 ns | §2.4 and §7.3 to be **corrected** |
| 3 | Batched `Merge`: **0.39 ns** (×1.3) | §7.3 **confirmed**, more strongly than expected |
| 4 | Batch-size plateau from **8** elements onward | §2.3: 1024 confirmed, but **not critical** |
| 5 | 1% filter: batch stays ×3.6 faster, 0 alloc | §7.5: `Coalesce` **useful, not urgent** |
| 6 | Per-stage cost **linear** (2.6 ns element / 1.5 ns batch) | Deep pipelines **predictable** |

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
3. **`Split` and lock-step** (§6.3), whose O(1) promise remains theoretical.
4. **The bounded hash join** (§9.1) and its materialization cost.
5. **Columnar decoding** versus row-by-row decoding (§8.6).
