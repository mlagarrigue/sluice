# Architecture — sluice

> Status: **spec v0.3**
> Module: `github.com/mlagarrigue/sluice`
> Go 1.26 · 0 production dependencies · pull-based, all-batch
>
> v0.3 incorporates seven research efforts and the review feedback on v0.2.
> Revised decisions and sources in §14.

---

## 1. Founding principle

The framework has **a single concept**: the `Stream`, a potentially infinite
sequence of **batches** of values, whose throughput is controlled by the consumer.

**Everything is a stream operation.** This is not a figure of speech: HTTP
parsing, routing, security, business rules, SQL reads, object hydration and
serialization are **all** operators of the same algebra. There is no "handler" in
the middle of the pipeline that would be of some other nature — business
processing is an operator just like `Map` or `Filter`.

### 1.1 A complete web example — a PATCH

Every arrow is a stream operator, without exception:

```
Stream[Batch[byte]]        ← socket read, pooled buffers
  → Stream[Batch[Frame]]      protocol parsing
  → Stream[Batch[Request]]    decoding, limits
  → Stream[Batch[Route]]      routing
  → Stream[Batch[Params]]     parameter extraction + validation
  → Stream[Batch[Params]]     security (authn/authz)          ← operator
  → Stream[Batch[Params]]     business security middleware    ← operator
  → Stream[Batch[Entity]]     DB read from the batch of params
  → Stream[Batch[Entity]]     data amendment                  ← operator
  → Stream[Batch[Entity]]     business rules (line.saleQty > 0)
  → Stream[Batch[Entity]]     save
  → Stream[Batch[Response]]   result projection
  → Stream[Batch[byte]]       serialization
```

Diagnostics (§4) travel **alongside** the entities through this whole pipeline —
an element can be valid, carry three warnings, and keep moving forward.

### 1.2 A database example

The connector follows exactly the same shape:

```
Stream[Batch[byte]]     ← PostgreSQL wire protocol
  → Stream[Batch[Row]]      DataRow decoding, binary format
  → Stream[Batch[Entity]]   generated hydration, column by column
```

This implies **reimplementing the connectors**: `database/sql` cannot batch
(§8.1). That is an accepted and budgeted cost.

| Domain | Reading it in stream terms |
|---|---|
| Web | `byte → Frame → Request → … → Response → byte` |
| Database | `byte → Row → Entity`, joins = operators |
| ETL | source → transformations → sink, the literal definition |
| Messaging | `Message` with a time-interval join |

**Verified positioning.** No Go web framework is built on an end-to-end stream
abstraction; no framework, in any language, unifies web + DB + ETL on a single
stream. Akka Streams is the closest precedent without covering all three. The
ground is clear — that is neither a guarantee nor a proof that the idea is good.

---

## 2. The central type — all-batch

### 2.1 Decision: a single shape, the batch

```go
// Stream is a sequence of batches whose throughput is controlled by the consumer.
type Stream[T any] iter.Seq[Batch[T]]

type Batch[T any] struct {
    Items []T          // variable length, stable capacity (pool)
    Ctx   *Context     // context carried AT THE BATCH LEVEL, not the element level
}
```

> **Divergence from the implementation — undecided.** The core as shipped has a
> bare `Batch` holding only `Items`. `Ctx` was left out on the dependency test:
> nothing in the core reads it, and a pure in-memory ETL pipeline has no use for
> it, so carrying it would make every user pay for a field most do not need.
>
> Against that: §7.6 leans on `Batch.Ctx` to make context merging trivial, and
> without the field, context has to travel as an ordinary payload — which pushes
> the problem into every N→1 operator instead of solving it once.
>
> The cost is one pointer per batch, amortised over ~1024 elements, so this is
> not a performance question. It is a question of what the core is allowed to
> assume. **To be settled before the core API is frozen.**

**v0.2 proposed two shapes (`Stream[T]` and `BatchStream[T]`). That was a
mistake**, for three reasons:

1. **A batch of size 1 is a degenerate case, not a distinct type.** If a function
   processes N elements efficiently, it processes 1 without effort.
2. **Two shapes = every operator written twice**, a boundary to arbitrate
   everywhere, and the permanent temptation to stay on the slow shape "for
   readability".
3. **The algorithm is far more optimizable in batch form**: columnar decoding,
   amortized context checking, batch-granularity pushdown, one network round-trip
   per batch.

This is also what X100 does: `next()` **always** returns a vector.

### 2.2 Beneficial side effect: the stop signal stops being ambiguous

With `iter.Seq[T]`, the signature `func(yield func(T) bool)` mixes two channels:
the value being carried and the continuation signal. On a `Stream[bool]`, they
can no longer be told apart, and a generic operator inspecting the return value
of `yield` cannot know whether it is reading data or a signal.

In all-batch form, the signature becomes `func(yield func(Batch[T]) bool)`: **the
control `bool` is never of the same type as the elements being carried.** The
ambiguity disappears by construction.

### 2.3 Batch size: ~1024 by default

Criterion: *the set of batches alive simultaneously* must fit in CPU cache.

**Measured** (stage 0): the plateau is reached from **8 elements** on and stays
flat up to 1M — 1.2% spread between 8 and 1024. Choosing 1024 is safe but **not
critical**, and a batch that drops to 64 after a filter degrades nothing.

*Caveat: this test measures only a single `int64` stream. Cache-driven sizing
remains to be verified with several batches alive simultaneously.*

### 2.4 Real cost — what is promised and what is not

- `iter.Seq` is **push-style at the machine level**: the Go compiler inlines the
  `yield` closure, making `for v := range seq` close to a native loop.
- Go has **no** monomorphization. Rust is a conceptual model, **not a cost
  model** — no "zero-cost" promise is made here.
- `iter.Pull` costs a runtime coroutine and a context switch **per value
  pulled**: **68.6 ns/element measured** (×221 the ceiling), against 1.4 ns in
  direct push. It is reserved for lockstep joins and merge, and then **on
  batches** — never on elements. This is not a recommendation but an
  **obligation**: see [BENCHMARK-STEP-0.md](BENCHMARK-STEP-0.md).

All-batch amortizes the indirect call over ~1024 elements: the per-stage cost
becomes negligible next to the useful work.

---

## 3. Context, cancellation and stopping

### 3.1 Correction to v0.2

v0.2 claimed that a `Stream` "carries neither runtime, nor context, nor global
registry". **That was badly written and false as stated.** What is true: there is
neither a global registry nor an injection container to configure. But an HTTP
context, a user context, a tenant — of course those travel.

**Context is carried at the batch level** (`Batch.Ctx`), not repeated on each
element. That is a direct benefit of all-batch.

Contexts are **stratified**: the HTTP context exists up to the HTTP layer, the
user/business context beyond it. An operator sees only the stratum that concerns
it.

### 3.2 Three causes of stopping, not to be confused

| Cause | Origin | Mechanism |
|---|---|---|
| Normal end | the source is exhausted, or LIMIT reached | the generator returns |
| Consumer stop | downstream no longer needs anything | `yield` → `false` |
| **Internal stop** | a stage fails (HTTP parsing KO) | `yield` → `false` **+ cause** |
| **External cancellation** | the user cancels | `ctx` captured at the source |

**The pattern adopted is that of `context`**: a fast binary signal in the hot
loop, **cause consulted after the fact** via an `Err() error` accessor.
`errors.Is` is enough to discriminate `nil` (normal end) / business error /
`context.Canceled`.

This is necessary because a **middle** stage deciding to stop can cut upstream
(by returning), but can only signal downstream by ending its sequence — which is
indistinguishable from a normal end. None of the systems studied mixes error and
cancellation in the same signal.

> **Runtime guarantee.** The `iter` contract requires `yield` to **panic** if it
> is called after having returned `false`. Stop propagation is therefore not a
> convention: it is checked by the runtime.

### 3.3 Checking frequency

**One `ctx` check per batch, never per element.** This is the universal pattern
of DB engines (PostgreSQL scatters its `CHECK_FOR_INTERRUPTS()` "at safe
places"). Hoist `done := ctx.Done()` out of the loop.

`context.Context` is a **parameter of the constructor function**, never a hidden
field that is never read.

### 3.4 Enriched stop signal — pushdown

A consumer may want to say something better than "stop": *"skip ahead to key X"*,
*"I no longer need column Y"*. This mechanism exists and has a name: **sideways
information passing**.

The modern reference implementation is DataFusion's *dynamic filters*: a
downstream `TopK` operator passes the current threshold to the upstream scan,
**which tightens as execution proceeds**; the scan then skips rows, then whole
files. Gains measured up to **22×** (ClickBench Q23) and **25×** on joins.

The mechanism is transposable and 0dep-compatible: a shared demand object plus an
**atomic generation counter** that the reader compares without a lock. The hot
path pays only one atomic read **per batch**.

```go
type Demand struct {
    generation atomic.Uint64   // the scan compares, without a lock
    // thresholds, required columns, remaining limit…
}
```

> See §13 for the attachment to the implementation stage. This is the
> highest-leverage performance lead identified by the research.

---

## 4. Diagnostics — errors, warnings and affordances

This is the richest topic in v0.3, and the real need goes far beyond a
`Result[T]`.

### 4.1 The need

In a PATCH stream, we want to be able to produce, **without interrupting the
stream**:

```
Error    1000  QteMustBeOverZero  commande[42].ligne[7].qteVente
Critical 0001  ClientMustExists   commande[42].idClient
Warning  2010  PrixInhabituel     commande[42].ligne[3].prixUnitaire
```

with, in the logs, the readable trace allowing one to go from
`Critical 0001 ClientMustExists` back to its real origin — a `NOT NULL` SQL
constraint at read time, or a hydration failure.

### 4.2 This model already exists — twice

**FHIR OperationOutcome** (HL7) carries exactly this shape: `severity`
(`fatal | error | warning | information`), `code`, `details`, and `expression` — a
path with indices, `Patient.identifier[2].value`. A point that settles the
hesitation between "code 1000" and "classification Sales\Order": FHIR uses
**both**, a code from a closed vocabulary *and* an application code in `details`.

**SARIF 2.1.0** (OASIS) brings the two missing pieces:

- **i18n**: a `message` object with `id` (the key) and `arguments` (the values);
  the rule catalog is separate from the occurrences. **The key travels, not the
  rendered text.**
- **causal traceability**: `codeFlows` / `threadFlows` with execution order and
  importance level.

A useful counter-example: `ValidationProblemDetails` (.NET) has the right path
shape (`Order.Lines[3].Quantity`) but too poor a payload — an array of
already-rendered messages, with no level, no code, no key. **Take only the path
from it.**

### 4.3 The model adopted

```go
type Severity uint8   // Info, Warning, Error, Critical

type Diagnostic struct {
    Severity  Severity
    Code      string          // "Sales.Order.InvalidQty" — hierarchical
    RuleID    string          // key in the rule catalog
    MessageID string          // i18n key: "QteMustBeOverZero"
    Args      map[string]any  // NAMED arguments, not positional
    Path      Path            // structured, NOT a string
    Origin    Origin          // SQL | Hydration | BusinessRule | Protocol
    Affords   []Affordance    // what is modifiable, by which action
    cause     error           // NOT exported — never serialized
}
```

Four decisions, each motivated:

**`Path` is a typed structure, not a string.** Concatenating strings traps you on
escaping (JSON Pointer mandates `~0`/`~1`) and precludes grouping by subtree. It
is then rendered as FHIRPath **or** JSON Pointer depending on the consumer. The
path is that of the **business model**, not of the incoming JSON document — two
namespaces not to be confused. FHIR in fact deprecated its `location` field, tied
to the serialization format, in favor of `expression`, tied to the model.

**Named arguments.** This is the acknowledged weakness of SARIF with its
`{0}`/`{1}`: languages reorder, and a translator cannot manage without ambiguity.

**The cause stays unexported**, with `Unwrap() error`. The HTTP serializer is then
**structurally unable** to leak internals: the S7 guarantee becomes impossible to
violate instead of being recommended. At the network boundary, the cause is
replaced by a correlation identifier that only the log resolves.

**Severity is not the flow decision.** FHIR distinguishes `fatal` and `error`;
SARIF separates `level` and `kind`. "How serious is it" and "should we stop" are
two orthogonal axes.

### 4.4 Affordances — the gap to fill

No format merges validation and affordances. The building blocks exist
separately: HAL-FORMS has `readOnly`, `regex`, `required` per property; Siren has
`method`, `href` and `fields` for the action.

**The join nobody has standardized: attaching the affordance to the same `Path`
as the diagnostic.** The same `commande[42].ligne[7].qteVente` carries both "here
is why this is invalid" and "here is how to fix it, by which action, with which
type". That is the model's added value, and it is a bet — no precedent to copy.

### 4.5 Diagnostics in a stream: what no standard does

SARIF, FHIR and Problem Details are all **finite documents**. On 1000 entities ×
N diagnostics, memory blows up.

> **Invariant.** A diagnostics ceiling per element **and** per batch, with stable
> ordering. Beyond it, a truncation counter. Without this, a pathological batch
> drowns the signal.

**Conditional trace capture**: `runtime.Callers` is expensive and Go's `errors`
package captures nothing. Trace only if `Severity >= Error` or in debug mode.

### 4.6 Articulation with `Result[T]`

`Diagnostic` **does not replace** the stream's error handling, it complements it:

| Mechanism | Role |
|---|---|
| `[]Diagnostic` carried by the element | **business** diagnostic — never interrupts |
| `Result[T]` in the stream | the element **could not** be produced |
| `Source.Err()` outside the stream | **setup / teardown** failure (§4.7) |
| `Stream.Err()` | **stop cause** of the stream (§3.2) |

Do not reuse `error` for a business diagnostic: a warning is not a processing
error.

### 4.7 Errors outside the stream

A `Result[T]` **inside** the stream can carry neither the failure to open a file
(no element ever existed) nor the failure of a `Close()` (after the last
element).

```go
type Source[T any] struct {
    Stream Stream[T]
    Err    func() error   // setup + teardown; consulted AFTER consumption
}
```

> **Terminality invariant.** A `Result` carrying an error **does not stop** the
> stream. An operator that must stop at the first error declares it via `OrFail`.
> Since this rule is not expressible in the signature, it is **checked by test**.

---

## 5. Operator taxonomy

Classifying by **memory behavior** is the foundation: it is what determines
whether a pipeline can process an infinite stream.

**Stateless — O(1).** `Map` · `Filter` · `FlatMap` · `Peek` · `Scan` · `Take` ·
`Drop` · `TakeWhile` · `DropWhile` · `Concat` · `Merge` · `Interleave` ·
`MergeJoinBy` · `ZipLongest`. Composable indefinitely.

The last five are the N→1 operators (§7): they merge several streams **without
state**, `MergeJoinBy` and `ZipLongest` on condition that inputs are sorted or
consumed in lockstep.

**Bounded state — O(k), k fixed at construction.** `Rebatch(n)` · `Coalesce(n)` ·
`Window(duration)` · `Distinct(n)` · `Buffer(n)` · `Split` · `Parallel`. Each one
**declares its overflow policy** (block, drop oldest, drop newest, error) —
back-pressure does not remove pressure, it propagates it up to a point where it
can be handled, and these operators *are* that point.

**Blocking — O(n).** `Sort` · `GroupBy` · `Collect` · `Join` (build side) ·
`Materialize`. **Incompatible with an infinite stream.**

> **Java 8 lesson.** Never let a parallelism constraint amputate the sequential
> API: Java deprived itself of `zip`, `foldLeft` and `takeWhile` to preserve
> parallelizability, and a study over 5.5 MLOC shows parallelism is very rarely
> used there. Everyone paid for almost nobody.

---

## 6. Split — a single primitive

### 6.1 Three uses, one function

Partition (successes/errors), parallel fan-out and cloning are **the same
operation** parameterized by a routing function. This is the Akka Streams model:

| Mode | Who receives | Order | Route |
|---|---|---|---|
| **Partition** | one branch, chosen | preserved per branch | `[i]` |
| **Balance** | one branch, the first free one | **no guarantee** | round-robin |
| **Broadcast** | all branches | preserved per branch | `[0..N-1]` |

```go
func Split[T any](s Stream[T], n int, route func(Batch[T]) []int) []Stream[T]
```

> **Caveat to document.** Partition and Broadcast preserve order per branch;
> **Balance guarantees no order** and is non-deterministic. Unifying the three is
> justified, but the guarantees **differ** and must be stated per mode.

### 6.2 The impossibility theorem

With a non-replayable source, you cannot have simultaneously:
**independent branches**, **source read only once**, **bounded memory**.
One must be sacrificed.

| Strategy | Sacrifice | Who |
|---|---|---|
| Unbounded buffer | **memory** | Web Streams, Python `tee`, Rust `itertools::tee` |
| Bounded buffer + blocking | **independence** | Akka `Broadcast` |
| Bounded buffer + drop | **completeness** | Akka `OverflowStrategy` |
| Re-running the source | **CPU/IO ×N** | `iter.Seq` replay |
| O(n) materialization | **memory = total** | Python recommends `list()` |

MDN is explicit about the Web Streams choice: *"unread data is enqueued
internally on the slower consumed ReadableStream without any limit or
backpressure"*. **That is the anti-pattern** with respect to our S1 guarantee.

### 6.3 The fifth option — the one we adopt

**The buffer is not the price of duplication, it is the price of misalignment.**

In a single goroutine, if `Split` **pushes** to N handlers in lock-step instead of
exposing N independent `iter.Seq`, the backlog stays **O(1)** without Akka's
pathological coupling. This is the lever that reconciles our three objectives, and
it is only available because we are pull-based and single-goroutine.

Corollary for cloning (§10.1): if the source is replayable, re-run it; otherwise
**explicit** `Materialize`. Never build an implicit unbounded tee.

### 6.4 `Parallel` — structural limit

Rayon (Rust) parallelizes because its sources are *splittable* (`split_at`). An
opaque `iter.Seq` **is not**. Our only option is per-batch fan-out, hence:

- either loss of ordering (`Parallel`), or an O(k) reordering buffer;
- local loss of native traces → enrich the error context at the crossing;
- `Parallel(1)` must be an explicit no-op, never a silent concurrent path.

The batch is an advantage here: fan-out happens per batch of ~1024, so the
coordination cost is amortized.

---

## 7. N→1 operators — merging several streams

`Split` (§6) decomposes one stream into N. This section covers the inverse
operation. These are two distinct families, and **merging is not joining**:
joining pairs elements on a key, merging combines sequences.

### 7.1 Taxonomy

| Operator | Semantics | Memory | Order |
|---|---|---|---|
| `Concat` | all of A, then all of B | O(1) | deterministic |
| `Merge` | interleaving, per batch | O(1) | non-deterministic |
| `Interleave` | strict alternation, N from A, N from B | O(1) | strictly alternating |
| `MergeJoinBy` | ordered merge of sorted streams | **O(1)** | sorted |
| `ZipLongest` | positional pairs, goes to the longest | O(1) | positional |
| `Coalesce` | recombines batches to a target size | O(k) | preserved |
| ~~unsorted `Union`/`Intersect`/`Except`~~ | HashSet deduplication | **O(distinct)** | — |

The last three **do not exist** as stream operators: an unbounded HashSet
violates S1. They are provided only in a sorted variant, derived from
`MergeJoinBy` (§7.2) — this is the sort-merge strategy of SQL engines.

### 7.2 `MergeJoinBy` — the central primitive

A single operator subsumes six semantics, in O(1) memory, over **potentially
infinite** streams provided they are sorted on the key.

```go
type EitherOrBoth[L, R any] struct {
    Left  *L   // nil if absent
    Right *R   // nil if absent
}

func MergeJoinBy[L, R any](
    left  Stream[L],
    right Stream[R],
    cmp   func(L, R) int,
) Stream[EitherOrBoth[L, R]]
```

Each semantics is a **downstream filter**, not a distinct operator:

| Semantics | Filter |
|---|---|
| Inner join | keep `Both` |
| Left join | keep `Both` + `Left` |
| Full outer join | keep everything |
| Intersect | keep `Both` |
| Except (A \ B) | keep `Left` |
| Union | keep everything |

This is the best power-to-code ratio in the entire taxonomy, and the only binary
operator that merges two infinite streams without state.

> **Articulation with §8.** `MergeJoinBy` **is** the merge-join primitive; the
> "merge join" entry in the strategy table (§8.2) refers to it and is not a
> separate implementation. The hash join (§8.1) remains a distinct primitive: it
> does not assume sorted inputs, but materializes one side.

> **Documented pitfall.** On inputs that are **not actually sorted**,
> `MergeJoinBy` produces a wrong result **with no error whatsoever**. A debug mode
> must check key monotonicity. This is a silent failure, of the same kind as Kafka
> co-partitioning (§8.1) — unacceptable as it stands.

### 7.3 `Merge` — why the batch redeems the cost

In single-goroutine pull, a fair merge **at the element level** is impossible
without a buffer or `iter.Pull`. At **batch** grain, it becomes trivial: one
`iter.Pull` per source, and you alternate.

**Measured** (stage 0): merging two streams costs **69.8 ns/element** tuple-at-a-
time against **0.39 ns/element** per batch of 1024 — a factor of **179**. At 1.3×
the ceiling, the operator is essentially free in batch, and unusable without it.

> Tuple-at-a-time, this cost is prohibitive and `Merge` would require an inversion
> of control. **It is all-batch that makes the operator possible**, not merely
> faster — measured, not projected.

**Completion condition: an explicit parameter.** Does the merged stream end when
*one* source ends, or when *all* of them do? Akka makes it a parameter
(`eagerComplete`, default "all"). Never an implicit choice.

### 7.4 `ZipLongest` primitive, `Zip` derived

A `Zip` that silently stops at the shortest **hides bugs**. Rust deemed it
necessary to add `zip_eq`, which **panics** on unequal lengths — the sign that the
default semantics is considered dangerous.

**Decision: `ZipLongest` is the primitive**, `Zip` a `Both` filter. The user who
wants stopping at the shortest asks for it explicitly.

> **Structural advantage to document.** In push, `Zip` is a memory bomb: RxJava
> has a dedicated ticket showing that an unbounded queue per source is necessary,
> and that **a single slow producer forces all the others to buffer**. Our pull
> model is immune to this by construction.

### 7.5 Rebatching — batches do not align

Two streams produce batches of different, unaligned sizes. **Nobody tries to
align them** — neither DuckDB, nor DataFusion, nor Arrow.

The established model: binary operators work on a **`(batch, offset)` cursor per
input**, consume batches partially, and produce output at a target size.

DataFusion has a dedicated physical operator for recomposition,
`CoalesceBatchesExec`, visible in execution plans, whose documented role is to
recombine small batches after a selective filter or a join.

```go
func Coalesce[T any](s Stream[T], targetSize int) Stream[T]
```

> **Rule.** Place a `Coalesce` after any selective operator — `Filter`, inner
> join, `Split`/Partition — on pain of propagating tiny batches that cancel out
> the benefit of vectorization.

**Target size: measured.** The plateau starts at 8 elements and does not degrade
up to 1M (§2.3). The gap with DuckDB (2048) and DataFusion (8192) has no effect
here. On a filter at 1% selectivity, the batch remains **3.6× faster** than the
element even without `Coalesce`, and without allocation — `Coalesce` is therefore
useful to avoid propagating sparse batches across several stages, but **it is not
urgent**.

### 7.6 Merging contexts

There is no `context.Merge` in the Go stdlib. The proposal
[golang/go#36503](https://github.com/golang/go/issues/36503) was **closed**.

*Caveat: the discussion thread could not be read, so the exact reason for the
rejection is not established.*

The hard point is known: merging **cancellation and deadline** is well defined
(the earliest wins), but merging **values** is not — third-party implementations
arbitrarily take those of a single parent.

**Our `Batch.Ctx` (§3.1) makes the question moot**: each batch coming out of a
merge **keeps the context of its originating batch**. Nothing to merge, O(1),
semantically correct.

| Aspect | Rule |
|---|---|
| Batch context | kept as is — never merged |
| Pipeline cancellation | separate channel; union of sources, via `context.AfterFunc` (no goroutine) |
| Deadline | the minimum |
| Values | **never merged** |

### 7.7 Errors and diagnostics at merge time

Rx explicitly distinguishes two strategies, and the distinction is the right one:

| Strategy | Behavior |
|---|---|
| `merge` | the first error propagates immediately, the other sources are abandoned |
| `mergeDelayError` | the other sources run to completion, errors are aggregated |

**Default adopted: `mergeDelayError`.** Our per-element diagnostics (§4) make
aggregation natural — a failing source produces `Diagnostic`s of severity
`Critical` without interrupting the others, which is consistent with the
terminality invariant (§4.7).

---

## 8. Database connectors

### 8.1 `database/sql` does not fit — a verified finding

- **No batching**: each statement is executed serially, one network round-trip
  each.
- `rows.Next()` is **intrinsically row by row**.
- Boxing into `interface{}` per scanned value — that is the real cost, even more
  than reflection.

Incompatible with the vectorized model. **The connector therefore speaks the
PostgreSQL v3 protocol directly.**

### 8.2 Honest budget for 0dep

Framing is trivial: ~20 useful message types, 1 type byte + an int32 length.
**The real budget is elsewhere**:

- **SCRAM-SHA-256** (PBKDF2-HMAC-SHA256 + HMAC) — feasible with stdlib
  `crypto/*`;
- **the per-type binary codecs** (numeric, timestamptz, arrays) — long and
  treacherous work.

Budget the codecs, not the parsing.

### 8.3 The "LINQ-style trick": `= ANY($1)`

`IN` requires a list of scalar expressions: N values → N placeholders → **a
distinct plan per value of N**. On batches from 1 to 1024, that is up to 1024
plans for a single logical query.

`= ANY($1)` takes **an array in a single parameter**: a single query shape
whatever N is, hence **a single prepared plan**.

> **Rule.** The builder produces **one canonical shape per logical query, never
> parameterized by N.**

| Backend | Strategy |
|---|---|
| PostgreSQL | `= ANY($1)` — one array parameter, one plan |
| SQL Server | TVP, with caveats (§8.4) |
| MySQL | `JSON_TABLE` — one placeholder, joinable with an index |
| Without arrays | padding to a power of 2 (~11 variants for N ≤ 1024) |

Padding duplicates the **last bound value** (`(1,2,3,3)`), not text: security is
intact. To be avoided on DBMSs without a plan cache, where it is a net overhead.

### 8.4 Caveat on SQL Server TVPs

Cardinality estimation of **10% for equality, 30% for inequality, 9% for a
range**, independent of the actual number of rows; no per-column statistics; and
on **joins**, bidirectional parameter sniffing — a first plan built on 100k rows
persists until recompilation. Mitigations: `OPTION (RECOMPILE)` or trace flag
2453.

### 8.5 Pipelining and COPY

**Pipelining**: send Parse/Bind/Execute for the whole batch, then **a single
`Sync`**. On error the backend skips all messages up to the next Sync — hence a
**natural per-batch error boundary**. Gain documented on the pgx side: 11
round-trips reduced to 2.

**Binary COPY**: an 11-byte signature, then per tuple a 16-bit field count and per
field a 32-bit length (`-1` = NULL), trailer `-1`. Message boundaries **need not
coincide with rows**: a `Batch[Row]` maps onto a `CopyData`. ~200 lines of code
for the best effort-to-gain ratio in the project.

### 8.6 Hydration

The decisive argument for codegen is **not** "`reflect` is slow": it is that
reflection works **row by row and field by field**, which breaks vectorization.

- a generated hydration function **per entity**, looping over the 1024 rows
  internally — one indirection per batch, not per row;
- **columnar decoding**: the loop over 1024 values of the same type is monomorphic
  and predictable — the X100 principle applied to hydration;
- **zero `interface{}`** in the hot path; **binary** format mandatory;
- pre-allocation at batch size, buffer reuse between batches.

### 8.7 Collections: never a cartesian product

Two collection `Include`s **at the same level** produce a cross product (10 posts
× 10 contributors = 100 rows for a blog). In a batch engine, we already have the
parent keys: **separate queries + in-memory join**. Sort order must be made
deterministic by construction.

### 8.8 Security by construction

1. **Values never travel through SQL text** — only in Bind. With `= ANY($1)`,
   1024 values remain **one parameter**: zero injection surface.
2. **Identifiers come from a catalog generated** at compile time — a column name
   is a *Go symbol*, not a string.
3. **Typed builder**: an invalid state is not representable. No public
   `Raw(string)` in the nominal path.

---

## 9. Joins

### 9.1 Hash join — the build side is a table

The build side **is** a table, the probe side drives the join and alone produces
the outputs (Kafka Streams' stream/table duality). Naming both sides in the API
removes the ambiguity about which one is materialized.

```go
func JoinStreamTable[A, B any, K comparable, R any](
    probe Stream[A],        // streamed — may be infinite
    build Stream[B],        // materialized — MUST be finite
    keyA  func(A) K,
    keyB  func(B) K,
    merge func(A, B) R,
    limit BuildLimit,       // MANDATORY — required positional parameter
) Stream[R]
```

**`limit` is required, not an option.** Making unbounded state *inexpressible* is
better than detecting it at runtime. Spark **refuses at planning time** a
stream-stream outer join without a watermark; Kafka **deprecated** its implicit
24 h grace period (KIP-633). The safe default is zero tolerance, widened on
request.

**Overflow is loud**: a `Diagnostic` of severity `Critical` and an observable
counter — never silence, never an OOM. Breaking co-partitioning in Kafka Streams
raises no exception and produces no output: that is the worst failure mode, and
the counter-example not to reproduce.

**The limit is global to the plan**, not local to the operator: when several joins
coexist, the sum of the tables must fit. A shared budget.

**Storage is an interface** — an in-memory map by default, optional spill. Never
hardwire a persistence policy into an operator: Kafka Streams ended up mandating
RocksDB in its foreign-key join, in contradiction with its own agnosticism goal.
For spill, the reference is radix partitioning with recursive bit increase
(DuckDB), with its known guardrail: on very skewed data, recursive
sub-partitioning causes *I/O thrashing*.

### 9.2 Strategies

| Strategy | Condition | Memory |
|---|---|---|
| Hash join | `comparable` keys | O(build) |
| Merge join (§7.2) | both streams sorted on the key | O(1) |
| Interval join | ordered temporal streams | O(throughput × interval) |
| Nested loop | last resort, replayable right side | O(1), CPU O(n·m) |

**Keys of identical type, enforced at compile time.** Materialize documents that
implicit casts in join constraints are very expensive in memory; generic typing
makes the problem non-existent — a clear advantage over SQL engines.

### 9.3 Infinite streams: interval join

State is bounded by a **temporal predicate**, not by a slicing — a sliding window
duplicates each element into N windows.

```go
// a joins b iff: a.ts + lower <= b.ts <= a.ts + upper
func IntervalJoin[A, B any, K comparable, R any](
    left, right Stream[A], /* ... */
    lower, upper time.Duration,
) Stream[R]
```

**Accepted restrictions, taken from Flink: inner join and event time only.** A
windowed outer join requires knowing *when to give up finding a partner*, hence a
watermark. Without one the semantics is **undefined**, and exposing it would be
lying to the user.

> If watermarks are introduced: plan from the design stage for a **per-source idle
> timeout**. An inactive source freezes the watermark, windows never close, state
> grows without end.

### 9.4 n-ary joins

**Never materialize intermediate binary results** — that is the state
amplification that forces Materialize and RisingWave into *delta join*. A
`.Join().Join()` chain **lies about the real cost**: either an n-ary join, or a
documented cost.

### 9.5 Scope: no retractions

Three accumulation modes exist (Dataflow Model); *accumulating & retracting* — the
revision of an already-emitted result — is by far the most expensive. **This
framework does `discarding` only.** That is what blows up Beam's complexity, and
it is out of reach for a single-process engine without durable state.

*Caveat: no source shows the Beam team calling retractions a design error. It is
an accepted cost on their part, not a documented regret — our choice is a scope
choice, not a correction.*

---

## 10. API pitfalls

### 10.1 A stream is not re-read — it is cloned

A `Stream` consumed twice runs twice, or yields zero elements if it captures a
`*bufio.Scanner`. **No signal at all.** Java raises `IllegalStateException`; .NET
created a dedicated analysis rule (CA1851). **Go does neither.**

Response: no implicit `Memoize` that would hide the cost, but `Split` in Broadcast
mode (§6) for cloning, **explicit** `Materialize` when the source is not
replayable, and a naming convention for single-pass sources.

### 10.2 Name of the central type — settled

Rust RFC 2996 renamed `Stream` to `AsyncIterator` because "stream" is too generic
— `io.Reader`, `net.Conn` and network streams are all "streams".

**Decision: we keep `Stream`.** The collision is less troublesome in Go than in
Rust, because the package qualifies the usage at the point of reading:
`sluice.Stream[T]` is unambiguous where Rust imported `Stream` into a shared
namespace. `AsyncIterator` would moreover be a misnomer — our model is
**synchronous**: no `Future`, no suspension point, everything happens on the same
stack. That is precisely what gives native execution traces and the `defer`
executed on early stop.

### 10.3 Chaining: wrapper adopted

Go has no generic methods. Two options:

```go
Map(Filter(s, pred), f)        // nested: read backwards
s.Filter(pred).Map(f)          // chained: requires a struct wrapper
```

**Decision: wrapper.** All-batch reduces the number of operators to write, which
makes the wrapper's cost easier to absorb.

> **Cross-cutting lesson (conduit, pipes, Node.js).** Theoretical elegance never
> compensates for an unfamiliar API, and API debt accumulates fast. **Freeze the
> core early** (`Stream`, `Batch`, `Diagnostic`), keep the rest as replaceable
> operators. And **never an implicit mode**: a stream whose behavior changes
> depending on whether a consumer has subscribed cost Node.js three major
> versions.

---

## 11. Security — verifiable properties

| # | Guarantee | Verification |
|---|---|---|
| S1 | No unbounded allocation. Every bound is a required parameter. | Overflow → error, not OOM |
| S2 | No `panic` crosses a public boundary. | Fuzzing of the parsers |
| S3 | Restrictive default limits. | Review of the defaults |
| S4 | `unsafe` forbidden except under a named audit. | `go vet` + review |
| S5 | Mandatory timeouts on all I/O. | Silent connection → released |
| S6 | No goroutine leak. | In-house detector |
| S7 | Internal errors do not leak to the client. | **Guaranteed by structure**: `cause` unexported (§4.3) |
| S8 | Every network input is bounded before allocation. | Fuzzing |
| S9 | Every internal `iter.Pull` calls `stop()` on all paths. | Test with injected panic |
| S10 | Every operator propagates `false` and lets the generator unwind. | Prompt finalization test |
| S11 | No unbounded buffer between two operators. | Review + pressure test |
| **S12** | **Diagnostics ceiling per element and per batch.** | Pathological batch → counted truncation |
| **S13** | **No value travels through SQL text.** | Builder review + fuzzing |

**S9** — if the sequence is not exhausted and `stop()` is not called, the
coroutine **never** terminates.

**S10** — this is streaming's problem #1, ahead of performance. The author of
`conduit` documented his own failure: with a `take 4`, the file handle **stayed
open** for the whole rest of the pipeline. `iter.Seq` handles this case well — a
`defer f.Close()` runs as soon as there is an early stop — **but only if all
operators honestly propagate the `false`**. A tested invariant, not a convention.

**S11** — before 1.5, Flink relied on TCP for its back-pressure: one slow consumer
blocked *all* the logical connections of the multiplex. Our `yield` **is** the
credit; any unbounded buffer reintroduces this bug.

### 0dep — the exact rule

- **Production: zero dependencies.** Stdlib only, no exception.
- **Tests/tooling: allowed** if they cannot end up in the final binary.

---

## 12. Application architecture

The framework imposes no project structure. A `Stream` is a function: nothing to
inject, no global registry, no implicit runtime (but a stratified context, see
§3.1).

- **DDD / Clean** — the domain manipulates `Stream[Entity]` without importing the
  web package; adapters live at the periphery.
- **Monolith** — in-memory composition, without serialization.
- **Microservices** — the network boundary is one more operator; the logical
  pipeline is identical, only the transport changes.

---

## 13. Implementation plan

| Stage | Content | Validates |
|---|---|---|
| 0 | ✅ **Done** — [BENCHMARK-STEP-0.md](BENCHMARK-STEP-0.md): ceiling 0.31 ns, batch ×1.9, `iter.Pull` 68.6 ns | §2.4 — the denominator |
| 1 | `Stream`, `Batch`, `Diagnostic`, `Path`, `Source`, O(1) operators, terminals | The core |
| 2 | Single `Split` + `Parallel` + O(k) operators + S9/S10/S11 tests | §6, S1, S6 |
| 3 | N→1 operators: `Concat`, `Merge`, `MergeJoinBy`, `ZipLongest`, `Coalesce` | §7 |
| 4 | `Join` (bounded hash, interval) — merge join derives from §7.2 | §9 |
| 5 | End-to-end HTTP vertical, diagnostics included | §1.1 |
| 6 | PostgreSQL connector: v3 protocol, `= ANY`, COPY, generated hydration | §8 |
| 7 | Dynamic pushdown (`Demand` + atomic generation) | §3.4 |

**Stage 0 is done.** All-batch is validated (×1.9 to ×2.0 from two stages on), the
cost of `iter.Pull` corrected (68.6 ns and not 20 ns), and the budget of a core
operator established: **~1.5 ns per stage and per element**. Every new operator is
measured against this budget.

**Stage 7 is deliberately late**: pushdown is the highest-leverage lead (20×+),
but it presupposes stabilized operators to make sense.

---

## 14. Revision log v0.2 → v0.3

| # | Change | Origin |
|---|---|---|
| 1 | **All-batch: `Stream[T] = iter.Seq[Batch[T]]`, single shape** | Review — "if a method processes N elements, it processes 1" |
| 2 | **§1: everything is a stream operation**, complete PATCH example | Review — the point was not stated |
| 3 | **§1.2: the DB connector follows the same shape**; connectors to reimplement | Review |
| 4 | **§2.2: the stop-signal ambiguity disappears** with the batch | Review — `Stream[bool]` case |
| 5 | **§3.1: correction — context does travel**, stratified, carried by the batch | Review — §6 of v0.2 badly written |
| 6 | **§3.2: four stop causes distinguished**, `context` pattern | Review (internal stop) + research |
| 7 | **§4: complete `Diagnostic` model** | FHIR OperationOutcome + SARIF 2.1.0 |
| 8 | **§4.4: affordances joined to the same `Path`** | Gap identified in review — without precedent |
| 9 | **§6: single `Split` with three modes** | Review + Akka Streams |
| 10 | **§6.3: single-goroutine lock-step → O(1) buffer** | The 5th option, not identified before |
| 11 | **§8.3: `= ANY($1)`** — one canonical shape, one plan | Review ("LINQ-style trick") |
| 12 | **§8.1: `database/sql` ruled out** — no batching possible | Research |
| 13 | **§3.4: dynamic pushdown** (sideways information passing) | Research — gains 22×/25× |
| 14 | **§10.1: a stream is cloned, not re-read**; implicit `Memoize` removed | Review |
| 15 | **§10.3: wrapper adopted for chaining** | Review |
| 16 | **S12, S13 added** | §4.5, §8.8 |
| 17 | **§7: complete N→1 family** (Concat, Merge, Interleave, ZipLongest) | Review — "2 streams, how to make just one" |
| 18 | **§7.2: `MergeJoinBy` central primitive** — 6 semantics by filtering | Rust `itertools::merge_join_by` |
| 19 | **Unsorted `Union`/`Intersect`/`Except` ruled out** — unbounded HashSet, violates S1 | Sort-merge of SQL engines |
| 20 | **§7.5: `Coalesce`** — batches do not align, we recompose | DataFusion `CoalesceBatchesExec` |
| 21 | **§7.6: contexts never merged**, each batch keeps its own | `context.Merge` rejected in Go |
| 22 | **§7.7: `mergeDelayError` by default** | Rx `merge` vs `mergeDelayError` |

### What did not change

The **pull-based** choice comes out reinforced: Kersten et al. establish that
pull/push and granularity are orthogonal and that neither direction dominates;
DuckDB moved to push for architectural reasons, **without invoking pull
performance**; and the Timely Dataflow README proposes, as a fix for its unbounded
push output… having operators return an iterator.

---

## Appendix — sources

**Query engines**
[MonetDB/X100 (CIDR 2005)](https://www.cidrdb.org/cidr2005/papers/P19.pdf) ·
[Kersten et al., PVLDB 11(13), 2018](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) ·
[Neumann, PVLDB 4(9), 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) ·
[Saving Private Hash Join, PVLDB 18](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf) ·
[Graefe, Volcano, IEEE TKDE 6(1), 1994](https://dl.acm.org/doi/10.1109/69.273032) ·
[DuckDB — push-based execution](https://github.com/duckdb/duckdb/issues/1583) ·
[DataFusion — Dynamic Filters](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/) ·
[Sideways Information Passing, ICDE 2008](https://dl.acm.org/doi/10.1109/ICDE.2008.4497486)

**Stream engines**
[Flink — Network Stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/) ·
[Flink — Joining](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/joining/) ·
[Kafka Streams — Core Concepts](https://kafka.apache.org/42/streams/core-concepts/) ·
[The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) ·
[Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow/blob/master/README.md) ·
[Materialize — Four Thoughts](https://materialize.com/blog/four-thoughts-four-years-materialize/) ·
[Akka Streams — Graphs](https://doc.akka.io/libraries/akka-core/current/stream/stream-graphs.html)

**Iterators, split, cancellation**
[Go Blog — Range Over Function Types](https://go.dev/blog/range-functions) ·
[Go Blog — Pipelines](https://go.dev/blog/pipelines) ·
[Russ Cox — Coroutines for Go](https://research.swtch.com/coro) ·
[pkg.go.dev/iter](https://pkg.go.dev/iter) ·
[Sinclair Target — Error Handling with Iterators](https://sinclairtarget.com/blog/2025/07/error-handling-with-iterators-in-go/) ·
[Snoyman — The core flaw of pipes and conduit](https://www.yesodweb.com/blog/2013/10/core-flaw-pipes-conduit) ·
[Denicola — On the Streams Standard](https://domenic.me/streams-standard/) ·
[Rust RFC 2996](https://rust-lang.github.io/rfcs/2996-async-iterator.html) ·
[WHATWG Streams — tee](https://streams.spec.whatwg.org/#rs-tee) ·
[MDN — ReadableStream.tee()](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream/tee) ·
[Python itertools.tee](https://docs.python.org/3/library/itertools.html#itertools.tee) ·
[CA1851 — Multiple enumerations](https://learn.microsoft.com/en-us/dotnet/fundamentals/code-analysis/quality-rules/ca1851)

**Diagnostics**
[FHIR R4 OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) ·
[SARIF 2.1.0 (OASIS)](https://docs.oasis-open.org/sarif/sarif/v2.1.0/errata01/os/sarif-v2.1.0-errata01-os-complete.html) ·
[RFC 9457 — Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html) ·
[RFC 6901 — JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) ·
[JSON:API](https://jsonapi.org/format/) ·
[ValidationProblemDetails](https://learn.microsoft.com/en-us/dotnet/api/microsoft.aspnetcore.mvc.validationproblemdetails?view=aspnetcore-9.0) ·
[Siren](https://github.com/kevinswiber/siren) ·
[ICU MessageFormat](https://unicode-org.github.io/icu/userguide/format_parse/messages/)

**Databases**
[PG protocol-flow](https://www.postgresql.org/docs/current/protocol-flow.html) ·
[PG message-formats](https://www.postgresql.org/docs/current/protocol-message-formats.html) ·
[PG COPY](https://www.postgresql.org/docs/current/sql-copy.html) ·
[Crunchy — ANY vs IN](https://www.crunchydata.com/blog/postgres-query-boost-using-any-instead-of-in) ·
[Mihalcea — parameter padding](https://vladmihalcea.com/improve-statement-caching-efficiency-in-clause-parameter-padding/) ·
[Brent Ozar — TVP sniffing](https://www.brentozar.com/archive/2018/03/table-valued-parameters-unexpected-parameter-sniffing/) ·
[EF Core — split queries](https://learn.microsoft.com/en-us/ef/core/querying/single-split-queries) ·
[go-database-sql — surprises](http://go-database-sql.org/surprises.html) ·
[pgx](https://github.com/jackc/pgx) · [sqlc](https://docs.sqlc.dev/en/latest/) ·
[fasthttp](https://github.com/valyala/fasthttp)
