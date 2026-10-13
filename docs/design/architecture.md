# Architecture

The reasoning behind Sluice.go: what the library is, the one type at its
centre, how a stream stops, how it reports, and why its operators are shaped
the way they are. Each structural decision has an [ADR](adr/) with the
alternatives it rejected; this document is the reasoning that connects them.
Figures quoted here were produced with the method in
[Measurements](../benchmarks.md).

## How to read this document

This document explains **why** Sluice.go has the shape it has. It does not
teach you to use it — that is what the [guides](../guide/) are for — and it
assumes you have read [Getting started](../guide/getting-started.md). It is
long because it keeps the reasons, the sources and the measurements behind
each choice: that is what lets a choice be contested with an argument
rather than a preference.

Read it in order to understand everything; otherwise each section stands
alone. The quoted blocks (`>`) are what the implementation learned while
building the section: figures, traps, decisions taken against intuition.
The **ADR** links lead to the formal decision (the problem, the decision,
the alternatives rejected).

## Glossary

The terms that recur throughout, one sentence each.

- **Stream** — a sequence of batches of values, produced only when someone
  reads it.
- **Batch** — a slice of values processed together; the unit of transport
  of the whole library, a thousand by default.
- **Operator** — a function that takes a stream and returns another
  (`Filter`, `Map`, a join).
- **Pull / push** — in pull, the reader asks for the next batch; in push,
  the source sends it when it has it. Sluice.go is pull-based.
- **Back-pressure** — what slows a source down when the reader cannot keep
  up; in pull, it follows from the very shape of the type.
- **Iterator** — in Go, a function that `for range` can walk; `iter.Seq` is
  its standard type.
- **Goroutine** — one of Go's lightweight threads; several run at once on
  the available cores.
- **Allocation** — reserving memory for a new value; expensive to repeat
  per element, which is what buffer reuse avoids.
- **Bound** — the limit an operator that retains state must receive as a
  parameter; without it, memory can grow without end.
- **Overflow policy (`Overflow`)** — what an operator does when its bound
  is reached: drop the oldest, the newest, or fail.
- **Diagnostic** — what can be said about a value without stopping the
  stream: an error, a warning, where, for what reason.
- **Affordance** — the remedy attached at the same place as a diagnostic:
  what the client can do to fix it.
- **Round trip** — a question sent to a server and its answer; one network
  journey, often the dominant cost.
- **Join** — pairing the elements of two streams that share a key.
- **Build side** — in a table join, the side loaded entirely into memory;
  the other is the *probe* side.
- **Window** — a group of elements from one slice of time.
- **Connector** — the code that speaks an external system's protocol
  (PostgreSQL) to read it as a stream.
- **Hydration** — filling a Go struct from a result row.
- **Wire protocol** — the exact shape of the bytes exchanged with a server.
- **Sentinel** — a named error, raised by panic for lack of another channel
  in an iterator, which `Try` knows how to convert.
- **S1 … S14** — the fourteen verifiable security guarantees listed in the
  Security section; the document refers to them by number.
- **Nanoseconds per element (ns/element)** — the unit of the core's
  measurements; the ceiling is 0.310 ns, an operator's budget 1.5 ns per
  stage.

Every technical term not in this glossary is explained in one sentence,
between dashes or in parentheses, the first time it appears below. The
original term is always kept, because it is what you search for in the
code and in the sources.

---

## Founding principle

The library has **a single concept**: the `Stream`, a potentially infinite
sequence of **batches** of values, whose throughput is controlled by the consumer.

**Everything is a stream operation.** This is not a figure of speech: HTTP
parsing, routing, security, business rules, SQL reads, object hydration and
serialization are **all** operators of the same algebra. There is no "handler" in
the middle of the pipeline that would be of some other nature — business
processing is an operator just like `Map` or `Filter`.

### What this is — and is not

**A library, not a framework.** The distinction is inversion of control: a
library is called by your code; a framework calls your code and owns the
loop. Here the caller owns `main`, its goroutines, its sockets and its
lifecycle. Nothing requires a global registry, a mandatory init, or a
callback to enroll in; every blocking convenience (`httpstream.Serve`,
`httpstream.ServeH3`) has the direct alternative under it (`quic.Dial` and
`quic.Accept` return a `*quic.Conn` you drive yourself; `httpstream.Requests`
reads a connection you opened). Composition is with the standard library —
`net.PacketConn`, `tls.Config`, `io`, `iter.Seq` — never a substitute for
it. What the library implements itself — QUIC, and experimentally the HTTP
transports — is what the standard library does not offer, or offers only in
a shape that cannot batch; it is built on the standard library's primitives,
not beside them. An API that breaks this rule is a defect, whatever it saves
its implementation.

**A narrow surface over a deep implementation.** What is built underneath —
path validation, connection-ID rotation, flow-control crediting, loss
recovery — surfaces in the API only when a caller needs it to act. The
common case must hold in a few lines: usable zero values, no implicit call
order, tuning optional and off the nominal path. The test is written before
the implementation: if the common case's usage example is laborious, the
API is wrong, not the documentation.

**Implemented is not recommended.** Some of what this library ships exists
because an RFC or backward compatibility demands it, not because it should
be chosen: such code is labelled at its point of use in the godoc —
*deprecated*, *kept for backward compatibility*, *interop-only* — and the
library's recommendation is always the performant and secure option:
HTTP/3 or HTTP/2 over cleartext HTTP/1.1, AES-GCM where hardware
accelerates it, Retry-based address validation under forged-address
pressure. A label at the use site beats a policy page nobody reads next to
the code; this section states the policy once so the labels have a spine.

### A complete web example — a PATCH

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

Diagnostics ([Diagnostics](#diagnostics--errors-warnings-and-affordances)) travel **alongside** the entities through this whole pipeline —
an element can be valid, carry three warnings, and keep moving forward.

> **Working, in `example/orders`.** A PATCH served by a pipeline built once at
> startup, with requests entering it as elements. `net/http` stays the protocol
> parser — rewriting one exposed to hostile input is how CVEs are made — so
> what the example builds is everything above it: validation, a batched read, a
> batched write, and the answer.
>
> Three of its decisions come from measurements rather than from taste. **The
> pipeline is built once**, because building six operators per request is half
> the 249 ns and thirteen allocations a request costs. **The store
> is asked once per batch**, which is the whole thesis and measured at ×6.9
> against a bounded backend. And **a rejected request is answered,
> never filtered out**: the gateway routes replies through the element itself,
> so an element removed from the stream is a caller left waiting for its
> context to expire.
>
> That last one is where the sentence above stops being an intention. A refusal
> travels as a `Diagnostic` attached to the element, which keeps its place in
> the batch — and beside it an `Affordance` at the **same `Path`**, so the HTTP
> layer renders `422` with both the problem and the action that fixes it, and a
> client pairs them by comparing paths rather than by convention. The example's
> handler is eight lines and there is still no adapter in the library for it:
> the status code and the encoding are the two decisions that matter, and an
> adapter would hide them.

#### Two paths: supported and experimental

The flow above is the **supported path**: `net/http` supplies the protocol
parsing (byte → Frame → Request), `web` and the example's pipeline build the
rest, and nothing about the HTTP parsing is this library's responsibility. An
experimental alternative exists: `net/httpstream` and `net/quic` are the package names,
and they bring the byte → Frame → Request boundary into the library's own
table — for measurement rather than for production, and under the caveat that
writing protocol parsers exposed to hostile input is how CVEs are made, which
the supported path is built to avoid.

[example/vertical](../../example/vertical) runs this experimental path end to
end: every arrow is a stream stage — the transport from `net/httpstream`, whose
one Handler serves HTTP/1.1, /2 and /3 (the example's own tests drive /1.1),
and the routing and middle stages from `web/stream` — meeting the same
credential and grant vocabulary as `web`'s pipeline. Nothing here is on the
supported path. `net/httpstream` is not fuzzed against a public corpus, not run
in production, and not reviewed by security specialists; the experiment
claims only that the table is realisable, that building it can be measured,
and that the claimed batch semantics hold end-to-end.

### A database example

The connector follows exactly the same shape:

```
Stream[Batch[byte]]     ← PostgreSQL wire protocol
  → Stream[Batch[Row]]      DataRow decoding, binary format
  → Stream[Batch[Entity]]   generated hydration, column by column
```

This implies **reimplementing the connectors**: `database/sql` cannot batch
([database/sql does not fit](#databasesql-does-not-fit--a-verified-finding)). That is an accepted and budgeted cost.

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

**Pull, not push.** Kersten et al. establish that pull/push and granularity
are orthogonal and that neither direction dominates; DuckDB moved to push for
architectural reasons without invoking pull performance; and the Timely
Dataflow README proposes, as a fix for its unbounded push output, having
operators return an iterator. Pull is what makes the consumer's refusal reach
the source and run its `defer`, which is the property everything below leans
on.

---

## The central type — all-batch

### Decision: a single shape, the batch

```go
// Stream is a sequence of batches whose throughput is controlled by the consumer.
type Stream[T any] iter.Seq[Batch[T]]

type Batch[T any] struct {
    Items []T          // variable length, stable capacity (pool)
}
```

> **The batch carries no context.** Context travels in the **element type**,
> through a wrapper defined outside the core.
>
> The deciding argument is structural, not cost. Of the operators in the core,
> none reads a batch's context, and half of them **rebuild** the batch because
> they change its contents. A `Ctx` field would have to be copied at each of
> those sites and at every one added later; the omission would compile, run,
> and silently drop the context from that stage onward — the failure mode the
> [merge join](#joinmerge--the-central-primitive) went out of its way to turn
> into a panic.
>
> Cost was measured and is not the reason: a `context.Context` is an interface,
> so the batch goes from 24 to 40 bytes, and a traversal of 1M elements is
> unchanged at 343 vs 346 ns — including with cancellation checked per batch.
> What a caller pays instead is 16 bytes per **element** when they do need
> per-element context. That is a real trade, made where the caller knows they
> need it. Rationale, alternatives, and what would reverse it:
> [ADR 0005](adr/0005-batch-carries-no-context.md).

**Two shapes — a `Stream[T]` of elements and a `BatchStream[T]` of batches —
would be a mistake**, for three reasons:

1. **A batch of size 1 is a degenerate case, not a distinct type.** If a function
   processes N elements efficiently, it processes 1 without effort.
2. **Two shapes = every operator written twice**, a boundary to arbitrate
   everywhere, and the permanent temptation to stay on the slow shape "for
   readability".
3. **The algorithm is far more optimizable in batch form**: columnar decoding,
   amortized context checking, batch-granularity pushdown, one network round-trip
   per batch.

This is also what X100 does: `next()` **always** returns a vector.

### Beneficial side effect: the stop signal stops being ambiguous

With `iter.Seq[T]`, the signature `func(yield func(T) bool)` mixes two channels:
the value being carried and the continuation signal. On a `Stream[bool]`, they
can no longer be told apart, and a generic operator inspecting the return value
of `yield` cannot know whether it is reading data or a signal.

In all-batch form, the signature becomes `func(yield func(Batch[T]) bool)`: **the
control `bool` is never of the same type as the elements being carried.** The
ambiguity disappears by construction.

### Batch size: ~1024 by default

Criterion: *the set of batches alive simultaneously* must fit in CPU cache.

**Measured**: the plateau is reached from **8 elements** on and stays
flat up to 1M — 1.2% spread between 8 and 1024. Choosing 1024 is safe but **not
critical**, and a batch that drops to 64 after a filter degrades nothing.

*Caveat: this test measures only a single `int64` stream. Cache-driven sizing
remains to be verified with several batches alive simultaneously.*

### Real cost — what is promised and what is not

- `iter.Seq` is **push-style at the machine level**: the Go compiler inlines (copies the body of, in place of a call) the
  `yield` closure, making `for v := range seq` close to a native loop.
- Go has **no** monomorphization — generating a specialised copy of a generic function per type, which is how Rust makes generics free. Rust is a conceptual model, **not a cost
  model** — no "zero-cost" promise is made here.
- `iter.Pull` (the standard function that turns an iterator into a `next()` call) costs a runtime coroutine — a suspended function the runtime resumes on demand — and a context switch **per value
  pulled**: **68.6 ns/element measured** (×221 the ceiling), against 1.4 ns in
  direct push. It is reserved for lockstep joins and merge, and then **on
  batches** — never on elements. This is not a recommendation but an
  **obligation**: see [Measurements](../benchmarks.md).

All-batch amortizes the indirect call over ~1024 elements: the per-stage cost
becomes negligible next to the useful work.

---

## Context, cancellation and stopping

### Context travels in the element

A `Stream` carries neither runtime nor global registry: there is nothing to
inject and no container to configure. But an HTTP context, a user context, a
tenant — of course those travel.

**Context travels in the element type**, through a wrapper defined outside the
core — see [the central type](#decision-a-single-shape-the-batch) and
[ADR 0005](adr/0005-batch-carries-no-context.md). Not on the batch: a field
that half the operators must remember to copy is a field that gets silently
dropped.

Contexts are **stratified**: the HTTP context exists up to the HTTP layer, the
user/business context beyond it. An operator sees only the stratum that concerns
it.

### Three causes of stopping, not to be confused

| Cause | Origin | Mechanism |
|---|---|---|
| Normal end | the source is exhausted, or LIMIT reached | the generator returns |
| Consumer stop | downstream no longer needs anything | `yield` → `false` |
| **Internal stop** | a stage fails (HTTP parsing KO) | `yield` → `false` **+ cause** |
| **External cancellation** | the user cancels | `ctx` captured at the source |

**The pattern adopted is that of `context`**: a fast binary signal in the hot
loop, **cause consulted after the fact** via an `Err() error` accessor —
`Source.Err` ([Errors outside the stream](#errors-outside-the-stream)), since a `Stream` is a bare `iter.Seq` and carries no
methods.
`errors.Is` is enough to discriminate `nil` (normal end) / business error /
`context.Canceled`.

This is necessary because a **middle** stage deciding to stop can cut upstream
(by returning), but can only signal downstream by ending its sequence — which is
indistinguishable from a normal end. None of the systems studied mixes error and
cancellation in the same signal.

> **Runtime guarantee.** The `iter` contract requires `yield` to **panic** if it
> is called after having returned `false`. Stop propagation is therefore not a
> convention: it is checked by the runtime.

### Checking frequency

**One `ctx` check per batch, never per element.** This is the universal pattern
of DB engines (PostgreSQL scatters its `CHECK_FOR_INTERRUPTS()` "at safe
places"). Hoist `done := ctx.Done()` out of the loop.

`context.Context` is a **parameter of the constructor function**, never a hidden
field that is never read.

### Enriched stop signal — pushdown

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

> **`pushdown` is that mechanism, measured on both sides.** The cost first,
> because a mechanism that taxes every pipeline to help a few is not worth
> having: a scan consulting a demand that never changes runs at **1.699 ms
> against 1.699 ms** without one — the atomic load per batch is not
> measurable. The gain, on the shape the mechanism is for — a consumer walking
> in key order publishing where it has got to, so the scan skips whole pages
> instead of decoding rows it is about to discard: **58.2 µs against 403.8**,
> **×6.9**. Smaller than DataFusion's ×22 because the simulated per-row work is
> smaller; the ratio is what transposes.
>
> Two decisions shape it. **The demand only tightens** — a limit falls, a bound
> rises, a column set shrinks, never the reverse. A source that has already
> skipped a thousand rows cannot un-skip them, so a loosening is a programming
> error rather than a case to handle: it is ignored and counted where a test
> can see it, instead of quietly yielding a result missing rows. And
> **`pushdown` lives outside the core**: an operator publishing its remaining
> count would make `Stream` and `Batch` know about pushdown, and a core that
> knows its extensions can no longer evolve. The demand-aware operators
> compose with the core ones instead.
>
> **One operator was written, measured and removed.** A `Take` publishing its
> remaining count measured 8.63 µs against the core operator's 8.35 — a 3% cost
> and no gain — and the reason is structural: a source whose unit of work *is*
> the batch it yields cannot do less than one batch for the last batch, and
> checking a limit per row is the per-element cost this design refuses. Its
> absence is documented in the package with the number that decided it. The
> limit itself stays useful where a source runs *ahead* of its consumer, behind
> an `parallel.Async` — but that is a caller's judgement about a pipeline it can see,
> not an operator measurement justifies.

---

## Diagnostics — errors, warnings and affordances

This is the richest topic in the design, and the real need goes far beyond
an error value per element.

### The need

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

### This model already exists — twice

**FHIR OperationOutcome** (HL7) carries exactly this shape: `severity`
(`fatal | error | warning | information`), `code`, `details`, and `expression` — a
path with indices, `Patient.identifier[2].value`. A point that settles the
hesitation between "code 1000" and "classification Sales\Order": FHIR uses
**both**, a code from a closed vocabulary *and* an application code in `details`.

**SARIF 2.1.0** (OASIS) brings the two missing pieces:

- **i18n** (internationalisation: rendering the same message in any language): a `message` object with `id` (the key) and `arguments` (the values);
  the rule catalog is separate from the occurrences. **The key travels, not the
  rendered text.**
- **causal traceability**: `codeFlows` / `threadFlows` with execution order and
  importance level.

A useful counter-example: `ValidationProblemDetails` (.NET) has the right path
shape (`Order.Lines[3].Quantity`) but too poor a payload — an array of
already-rendered messages, with no level, no code, no key. **Take only the path
from it.**

### The model adopted

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

> **`Affords` is not a field.** Every other field above exists as sketched,
> in `diagnostics`, with `NewDiagnostic(severity, code, path)` for the three a
> diagnostic cannot be useful without and `With…` methods returning copies for
> the rest. The value receiver is what makes a diagnostic safe to hold as a
> template — measured at 5.69 ns either way, so the size the linter flags is
> not a cost.
>
> Affordances are a separate type ([below](#affordances--the-gap-to-fill)):
> an `Affordance` carries its own `Path`, and a remedy is matched to a problem
> by comparing paths, so neither value needs to hold the other.

Four decisions, each motivated:

**`Path` is a typed structure, not a string.** Concatenating strings traps you on
escaping (JSON Pointer, the standard notation for a location in a JSON document, mandates `~0`/`~1`) and precludes grouping by subtree. It
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

### Affordances — the gap to fill

No format merges validation and affordances. The building blocks exist
separately: HAL-FORMS has `readOnly`, `regex`, `required` per property; Siren has
`method`, `href` and `fields` for the action.

**The join nobody has standardized: attaching the affordance to the same `Path`
as the diagnostic.** The same `commande[42].ligne[7].qteVente` carries both "here
is why this is invalid" and "here is how to fix it, by which action, with which
type". That is the model's added value, and it is a bet — no precedent to copy.

> **The bet is taken as stated.** `Affordance` carries a `Path`,
> an `Action` code in the same hierarchical style as `Diagnostic.Code`, an
> optional method and target, and the inputs the action accepts. Matching a
> remedy to a problem is a prefix comparison on the path (`Path.HasPrefix`),
> which is the reason `Path` keeps its segments instead of rendering to a
> string: `lines[70]` must not match a query for `lines[7]`. There is no
> bounded collector for affordances: nothing collects them, and
> `stream.Exchange.Affordances` is a plain slice.
>
> **It describes, it does not execute**, and that is the line that keeps the
> type honest. The producer of a diagnostic knows what *could* be done; whether
> it *may* be done is an authorisation question answered elsewhere, and how it
> is done belongs to the transport. A type that executed would have to know all
> three, and would drag the domain into both.
>
> **Affordances have no collector of their own**, unlike diagnostics ([Diagnostics in a stream](#diagnostics-in-a-stream-what-no-standard-does)).
> A batch offers a handful of remedies where it may raise thousands of
> diagnostics, so the S12 ceiling has nothing to bound on that side; the
> consumer carries them as a plain slice. A zero `Affordance` is the ordinary
> "no remedy to offer" value and reports `IsZero`. `WithInput` copies rather
> than appending in place, because an affordance is precisely the kind of
> value held as a template and extended along two branches.

### Diagnostics in a stream: what no standard does

SARIF, FHIR and Problem Details are all **finite documents**. On 1000 entities ×
N diagnostics, memory blows up.

> **Invariant.** A diagnostics ceiling per element **and** per batch, with stable
> ordering. Beyond it, a truncation counter. Without this, a pathological batch
> drowns the signal.

**`diagnostics.Collector`** is that ceiling, built by `NewCollector(limit)`, whose limit is a required argument —
S1 says every bound is stated by the caller, and a limit nobody chose is a limit
nobody owns. It keeps the first *n* in arrival order rather than the most severe:
stable ordering is what makes a report reproducible, and a collector that
reshuffles under load answers differently for the same input depending on what
came before. `Truncated()` reports what was dropped, because a truncation that is
not counted is indistinguishable from a clean run.

`Reset` clears the kept entries rather than merely shortening the slice. A
`Diagnostic` holds an `Args` map and a wrapped cause, so a reused collector that
only truncated would pin the first batch's memory for the life of the process —
the unbounded retention S1 exists to prevent, arriving through the back door.

**The ceiling bounds what is kept, not what is allocated.** Claiming the whole
limit on the first diagnostic turns a cautious configuration value into a
125 MB allocation — measured, for four diagnostics under a 1M ceiling. Claiming
nothing costs 12 allocations and 3× the time to fill a 1000-entry report,
because every doubling copies. The storage is therefore reserved up to a small
cap and grows into its limit beyond it: 2 KB and one allocation for the first
`Add` at any ceiling, and past the ceiling a counter increment at 1.9 ns with no
allocation at all.

**Conditional trace capture**: `runtime.Callers` is expensive and Go's `errors`
package captures nothing. Trace only if `Severity >= Error` or in debug mode.

### Articulation with the stream's error handling

`Diagnostic` **does not replace** the stream's error handling, it complements it:

| Mechanism | Role |
|---|---|
| `[]Diagnostic` carried by the element | **business** diagnostic — never interrupts |
| the element type itself | the element **could not** be produced — the caller's own field, see below |
| `Source.Err()` outside the stream | **setup / teardown** failure and **stop cause** ([Three causes of stopping, not to be confused](#three-causes-of-stopping-not-to-be-confused), [Errors outside the stream](#errors-outside-the-stream)) |

> **No `Result[T]` wrapper, no `OrFail` operator, no `Stream.Err()`.**
> `Stream[T]` is a named `iter.Seq[Batch[T]]` with no methods, so the stop
> cause lives on `Source` ([below](#errors-outside-the-stream)). A per-element
> failure travels in the caller's element type — a struct with an error field
> is a `Result[T]` the caller owns — and nothing in the core inspects it, so
> the terminality rule (an element carrying an error does not stop the stream)
> holds without a type.

Do not reuse `error` for a business diagnostic: a warning is not a processing
error.

> **Enforced, not merely stated.** `Diagnostic` renders through `String`, not an
> `Error` method, so it does not satisfy the `error` interface. With one, a
> `Warning` — "unusual price, accepted" — could be returned where a failure is
> expected and would fire every `if err != nil` it reached; the rule above would
> be decorative. The internal cause stays reachable as
> `errors.Is(d.Unwrap(), target)`, which reads as what it is: asking for the
> error behind the diagnostic rather than treating the diagnostic as one. A test
> asserts the type does not implement `error`, because the mistake this guards
> against is adding the method back later.

### Errors outside the stream

An error value carried **inside** the stream can carry neither the failure to open a file
(no element ever existed) nor the failure of a `Close()` (after the last
element).

```go
type Source[T any] struct{ /* both fields unexported */ }

func NewSource[T any](s Stream[T], err func() error) Source[T]
func (s Source[T]) Stream() Stream[T]
func (s Source[T]) Err() error
func (s Source[T]) Consume(f func(Batch[T]) bool) error
func (s Source[T]) Collect() ([]T, error)
```

> **Both fields are unexported, reached through methods.** An exported func
> field would make `src.Err()` a nil call on a source built without one — and
> a zero value has to be usable. An exported stream field would let
> `src.Stream = other` leave the source reporting data from one stream and an
> error from another, which is this type's own purpose turned against itself.
> `NewSource` is the only route in, so no incoherent state is reachable.
>
> `Stream()` keeps the composition the exported field provided —
> `Take(src.Stream(), 10)` — and substitutes an empty stream for a zero Source,
> so the result is always safe to range over. The error stays on the `Source`:
> consuming a derived stream does not carry it along.
>
> `Consume` and `Collect` exist so that consuming and checking are one step. The
> failure this type guards against is a caller who reads every element and never
> asks why the stream was short; a shape that returns the error makes forgetting
> it a visible omission rather than the default.

> **Terminality invariant.** An element carrying an error **does not stop** the
> stream: the core never looks inside elements. A pipeline that must stop at
> the first failed element says so itself — `TakeWhile`, or a consumer
> returning `false`
> ([above](#articulation-with-the-streams-error-handling)).

---

## Operator taxonomy

Classifying by **memory behavior** is the foundation: it is what determines
whether a pipeline can process an infinite stream. The package layout follows
it — an operator lives in the package named after **what it retains between
two batches** — so that the import path says the cost before the
documentation does ([ADR 0001](adr/0001-package-map.md)).

**Retains nothing — the root package, O(1).** `Map` · `Convert` · `Filter` ·
`FlatMap` · `Peek` · `Scan` · `Take` · `Drop` · `TakeWhile` · `DropWhile` ·
`Split` · `Concat` · `Merge` · `Coalesce`. Composable indefinitely. `Scan`
keeps an accumulator, not elements; `Split` keeps one batch of slack per
branch ([Split](#split--a-single-primitive)); `Coalesce` keeps at most one
batch while it fills it — the one documented exception, and the core's own
tool for restoring full batches.

**Retains one side — `join`.** `join.Merge` and `join.ZipLongest` walk two
inputs in lockstep and retain only the current equal-key run; `join.Merge`
is O(1) while keys repeat on at most one side — a key repeated on both buffers
its right run to emit the cross product, O(m) over that run, so a stream that
is one long many-to-many run materializes its right input
([the merge join](#joinmerge--the-central-primitive)). `join.Interval`
retains a bounded window of both sides; `join.StreamTable` materializes its
build side entirely, under a required bound.

**Retains time — `window`.** `window.Tumbling` holds one open window;
`window.CoalesceWithin` holds a partial batch and a clock, which costs a
goroutine.

**Retains goroutines — `parallel`.** `parallel.Ordered`, `parallel.Unordered`,
`parallel.WithState` and `parallel.Async` own the goroutines they start and
stop them before returning, on every path out.

**Retains keys — `distinct`.** `distinct.By(n)` remembers the last *n* keys.

Every operator that retains elements **declares its overflow policy** (drop
oldest, drop newest, fail) through its bound — back-pressure does not remove
pressure, it propagates it up to a point where it can be handled, and these
operators *are* that point.

> **Two names deliberately absent.**
>
> `Rebatch(n)` would be a second name for `Coalesce(n)`, which regroups
> batches to a target size ([Rebatching](#rebatching--batches-do-not-align)).
> One operator, two names, and nothing distinguishing them.
>
> `Buffer(n)` cannot do anything in this model, and shipping it would be worse
> than its absence — a caller would believe they had decoupling. A buffer exists
> to let a producer run ahead of a consumer, which requires them to advance
> independently, which requires two goroutines. In synchronous pull the `yield`
> **is** the credit: interposing a buffer only delays delivery and forces a copy,
> since the batches it holds are reusable buffers. The decoupling a caller wants
> from `Buffer` is what `parallel.Async` provides, with the goroutine that
> makes it meaningful. This is also S11 read forwards rather than backwards.
>
> The policy itself is one shared `Overflow` type — `DropOldest`, `DropNewest`,
> `Fail` — rather than a per-operator spelling. Three vocabularies for one
> decision would be three things to learn, and there is no default: a policy
> nobody chose is a policy nobody owns.

> **What `window.Tumbling` emits is not a batch.** Making a
> window a batch — window frontiers as batch frontiers — would let semantics
> dictate transport granularity, which is exactly the fragmentation `Coalesce`
> exists to repair: a stream of one-element windows would become a stream of
> one-element batches. So `window.Tumbling` emits `Stream[Pane[T]]`, where a `Pane`
> carries its `Start`, its `End` and its elements. Windows tumble, aligned on
> absolute time rather than on the first element, so the same data yields the
> same windows however the stream is cut; ordering closes them, exactly as in
> [Infinite streams](#infinite-streams-interval-join), which keeps the state at **one open window** — element times never
> decrease, so window indices never decrease either. Empty windows are not
> emitted: a one-second window over a stream with a one-year gap would turn
> bounded input into thirty million empty panes.
>
> `Pane.Items` is **owned, not borrowed** — the one place in the package that
> copies by default. A `Pane` is an element containing a slice, and every
> generic operator copies elements shallowly, so a borrowed `Items` would make
> `Collect(window.Tumbling(...))` return panes that all read back as the last one, with
> no error anywhere. Documentation cannot prevent that, because the operators
> doing the copying are correct.
>
> Measured at 4.5 ns/element (1024-element windows), above the per-stage
> budget for the reason `distinct.By` is: every element pays a caller callback
> returning a 24-byte `time.Time` and two time comparisons. Calling
> `time.Truncate` per element — a 64-bit division — costs 10.5; testing the
> open window's end first answers for every element but the first of each
> window, for −56.7% (14/14 paired rounds).

**Blocking — O(n).** `Collect` · `join.StreamTable` (build side).
**Incompatible with an infinite stream.**

> **No `Sort`, `GroupBy` or `Materialize`.** A caller who needs one collects
> (`Collect`) and sorts or groups the slice, which states the O(n) cost at the
> call site — the same reason the [joins](#joins) take the build side's bound
> positionally.

> **Java 8 lesson.** Never let a parallelism constraint amputate the sequential
> API: Java deprived itself of `zip`, `foldLeft` and `takeWhile` to preserve
> parallelizability, and a study over 5.5 MLOC shows parallelism is very rarely
> used there. Everyone paid for almost nobody.

---

## Split — a single primitive

### Three uses, one function

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

### The impossibility theorem

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

### The fifth option — the one we adopt

**The buffer is not the price of duplication, it is the price of misalignment.**

In a single goroutine, `Split` drives **one** traversal of the source and hands
each branch a slot of **one batch**, instead of exposing N independent
`iter.Seq`. The backlog stays **O(1)** without Akka's pathological coupling, and
it is only available because we are pull-based and single-goroutine.

This does **not** escape the theorem in [The impossibility theorem](#the-impossibility-theorem) — nothing does. The sacrifice is
**branch independence**: a branch whose slot is still full blocks the shared
pull, so branches read at the same time must be advanced in alternation. What
the fifth option buys is the *size* of that sacrifice: one batch of slack rather
than an unbounded queue, and independence lost only between branches that are
genuinely concurrent.

Draining one branch while another lags panics with `ErrSplitStalled` rather than
yielding a silent prefix of the data. Rationale, alternatives and cost:
[ADR 0003](adr/0003-split-reads-once.md).

Corollary for cloning ([A stream is not re-read](#a-stream-is-not-re-read--it-is-cloned)): if the source is replayable, re-run it; otherwise
materialize it **explicitly** — `Collect` into a slice, re-served with `Of`.
Never build an implicit unbounded tee.

### `parallel.Ordered` — structural limit

Rayon (Rust) parallelizes because its sources are *splittable* (`split_at`). An
opaque `iter.Seq` **is not**. Our only option is per-batch fan-out, hence:

- either loss of ordering (`parallel.Ordered`), or an O(k) reordering buffer;
- local loss of native traces → enrich the error context at the crossing;
- `parallel.Ordered` with one worker must be an explicit no-op, never a silent concurrent path.

The batch is an advantage here: fan-out happens per batch of ~1024, so the
coordination cost is amortized.

> **Ordered by default, and the speed-up is smaller than the word suggests.**
> `parallel.Ordered` takes the ordering side of the choice above: an operator inserted
> into an existing pipeline must not silently change what that pipeline means,
> and nothing at the call site would reveal that it had. The reordering buffer
> is O(workers), which keeps it in the bounded-state family.
>
> That ordering caps the gain, and the measurement says so: the emitter waits
> for the oldest batch, so one slow worker holds the queue whatever the others
> are doing. On work heavy enough to distribute, four workers came out at
> **4.10 ms against 5.27 ms serial — ×1.29, not ×4**. On a pass-through `f`,
> four workers were **3.5× slower** than none. The operator earns its place only
> where `f` is genuinely expensive, and the numbers are in its documentation so
> that "add `parallel.Ordered`" is a claim to check rather than an assumption.
>
> `parallel.Ordered` with one worker is the explicit no-op this section
> requires: `f` applied inline, no goroutine, no channel, no reordering buffer.
>
> **The unordered side is named so it cannot be picked by accident.**
> `parallel.Unordered` emits batches as they complete; the concern
> above — two operators differing in a way most callers would get wrong — is
> answered by the name being the contract. Measured on work skewed 8× between
> batches: ordered fan-out ×1.03 over serial (the fan-out nearly cancelled by
> its own ordering), unordered ×2.75. Uniform work gains ×1.59 too, since
> ordered emission turns scheduling jitter into window stalls. Ordered stays
> the default. `parallel.WithState` completes the family: per-worker private
> state, built up front on the consuming goroutine, closed through `io.Closer`
> on every exit path — the operator for the decoder table or reader cache that
> is neither shareable nor worth a mutex (−32% time, −45% memory against
> allocating the workspace per batch). All three share one worker core and so
> one ordering, shutdown and panic contract.
>
> **The input batch is copied at the hand-off (S14).** The operator retains
> each incoming batch while a worker reads it, and upstream operators
> legitimately reuse their output buffers — composed, those two facts are a
> data race with silent value corruption. `Items` is copied into a per-slot
> buffer at the retention point; the slots are recycled, so the copy costs no
> allocation in steady state. Measured: ~0.2 ns/element on a pass-through `f`,
> in the noise on a heavy one.

---

## N→1 operators — merging several streams

`Split` ([Split](#split--a-single-primitive)) decomposes one stream into N. This section covers the inverse
operation. These are two distinct families, and **merging is not joining**:
joining pairs elements on a key, merging combines sequences.

### Taxonomy

| Operator | Semantics | Memory | Order |
|---|---|---|---|
| `Concat` | all of A, then all of B | O(1) | deterministic |
| `Merge` | round-robin, one batch per source in turn | O(1) | strictly alternating rotation |
| `join.Merge` | ordered merge of sorted streams | **O(1)**, O(m) per run repeated on both sides | sorted |
| `join.ZipLongest` | positional pairs, goes to the longest | O(1) | positional |
| `Coalesce` | recombines batches to a target size | O(k) | preserved |
| ~~unsorted `Union`/`Intersect`/`Except`~~ | HashSet deduplication | **O(distinct)** | — |

The last three **do not exist** as stream operators: an unbounded HashSet
violates S1. They are provided only in a sorted variant, derived from
`join.Merge` ([join.Merge](#joinmerge--the-central-primitive)) — this is the sort-merge strategy of SQL engines.

### `join.Merge` — the central primitive

A single operator subsumes six semantics, in O(1) memory — with the equal-key
caveat below — over **potentially infinite** streams provided they are sorted
on the key.

```go
type EitherOrBoth[L, R any] struct {
    Left     L
    Right    R
    HasLeft  bool
    HasRight bool
}

func Merge[L, R any, K any](       // package join
    left  Stream[L],
    right Stream[R],
    keyL  func(L) K,
    keyR  func(R) K,
    cmp   func(K, K) int,
) Stream[EitherOrBoth[L, R]]
```

> Not `Left *L` / `Right *R` with a single `cmp func(L, R) int`: a pointer
> into a reused batch buffer dangles, and a cross-side comparator cannot
> compare two elements of the same side, which both the sort check and the
> cross product need. See [ADR 0004](adr/0004-merge-join-signature.md).

Each semantics is a **downstream filter**, not a distinct operator:

| Semantics | Filter |
|---|---|
| Inner join | keep `Both()` |
| Left join | keep `HasLeft` |
| Right join | keep `HasRight` |
| Full outer join | keep everything |
| Intersect | keep `Both()` |
| Except (A \ B) | keep `HasLeft && !HasRight` |
| Union | keep everything |

This is the best power-to-code ratio in the entire taxonomy, and the only
binary operator that merges two infinite streams without state beyond the
current equal-key run.

> **The one place memory grows.** Rows sharing a key on both sides produce
> their full cross product, as SQL requires: a key repeated n > 1 times on the
> left and m > 1 times on the right buffers the right run, O(m) for that key,
> and streams the left run against it — an input that is a single
> many-to-many run materializes its right side. A key held once on either
> side costs O(1) whatever the length of the other side's run: the repeated
> side streams against the single row, across batch boundaries. That covers
> one-to-one and one-to-many joins on an identifier, and is the figure the
> tables quote.

> **Articulation with [Database connectors](#database-connectors).** `join.Merge` **is** the merge-join primitive; the
> "merge join" entry in the strategy table ([Strategies](#strategies)) refers to it and is not a
> separate implementation. The hash join ([Hash join](#hash-join--the-build-side-is-a-table)) remains a distinct primitive: it
> does not assume sorted inputs, but materializes one side.

> **The order is always checked.** On inputs that are **not actually sorted**,
> `join.Merge` would produce a wrong result with no error whatsoever — a silent
> failure of the same kind as Kafka co-partitioning
> ([hash join](#hash-join--the-build-side-is-a-table)), and unacceptable.
>
> So key monotonicity is checked **always**, not behind a debug flag: the
> check costs nothing measurable against the comparisons the merge already
> performs, and a check that only runs in debug builds is absent exactly when it
> matters. Out-of-order input panics with `ErrUnsorted`.

### `Merge` — why the batch redeems the cost

In single-goroutine pull, a fair merge **at the element level** is impossible
without a buffer or `iter.Pull`. At **batch** grain, it becomes trivial: one
`iter.Pull` per source, and you alternate.

**Measured**: merging two streams costs **69.8 ns/element** tuple-at-a-
time against **0.39 ns/element** per batch of 1024 — a factor of **179**. At 1.3×
the ceiling, the operator is essentially free in batch, and unusable without it.

> Tuple-at-a-time, this cost is prohibitive and `Merge` would require an inversion
> of control. **It is all-batch that makes the operator possible**, not merely
> faster — measured, not projected.

**Completion condition: an explicit parameter.** Does the merged stream end when
*one* source ends, or when *all* of them do? Akka makes it a parameter
(`eagerComplete`, default "all"). Never an implicit choice. An undeclared
`Completion` value panics at construction rather than reading as `WhenAll`.

> **The rotation is strict, and that is the contract.** In single-goroutine
> pull nothing runs ahead, so `Merge` cannot "emit whatever is ready": it
> pulls its sources round-robin, one batch per source in turn — with sources
> yielding a, b and 1, 2 the output is a, 1, b, 2 — which is what makes it
> usable in a test and worth relying on when a fair share per source is what
> the caller needs. Under `WhenAll`, an exhausted source drops out of the
> rotation and the rest carry on. Measured flat at ~0.40 ns/element from two
> sources to eight: the pull count per element does not change with the
> source count.

### `join.ZipLongest` primitive, `Zip` derived

A `Zip` that silently stops at the shortest **hides bugs**. Rust deemed it
necessary to add `zip_eq`, which **panics** on unequal lengths — the sign that the
default semantics is considered dangerous.

**Decision: `join.ZipLongest` is the primitive**, `Zip` a `Both` filter. The user who
wants stopping at the shortest asks for it explicitly.

> **Structural advantage to document.** In push, `Zip` is a memory bomb: RxJava
> has a dedicated ticket showing that an unbounded queue per source is necessary,
> and that **a single slow producer forces all the others to buffer**. Our pull
> model is immune to this by construction.

### Rebatching — batches do not align

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
up to 1M ([Batch size](#batch-size-1024-by-default)). The gap with DuckDB (2048) and DataFusion (8192) has no effect
here. On a filter at 1% selectivity, the batch remains **3.6× faster** than the
element even without `Coalesce`, and without allocation — `Coalesce` is therefore
useful to avoid propagating sparse batches across several stages, but **it is not
urgent**.

### Merging contexts

There is no `context.Merge` in the Go stdlib. The proposal
[golang/go#36503](https://github.com/golang/go/issues/36503) was **closed**.

*Caveat: the discussion thread could not be read, so the exact reason for the
rejection is not established.*

The hard point is known: merging **cancellation and deadline** is well defined
(the earliest wins), but merging **values** is not — third-party implementations
arbitrarily take those of a single parent.

**The sidestep: nothing is merged.** With context in the element type
([the central type](#decision-a-single-shape-the-batch)), every element keeps
its own context; a merge pairs elements without combining their contexts,
still O(1), still semantically correct. The N→1 operators need to know nothing
about it, since they pass elements through without inspecting them.

| Aspect | Rule |
|---|---|
| Element context | kept as is — never merged |
| Pipeline cancellation | separate channel; union of sources, via `context.AfterFunc` (no goroutine) |
| Deadline | the minimum |
| Values | **never merged** |

### Errors and diagnostics at merge time

Rx explicitly distinguishes two strategies, and the distinction is the right one:

| Strategy | Behavior |
|---|---|
| `merge` | the first error propagates immediately, the other sources are abandoned |
| `mergeDelayError` | the other sources run to completion, errors are aggregated |

**Default adopted: `mergeDelayError`.** Our per-element diagnostics ([Diagnostics](#diagnostics--errors-warnings-and-affordances)) make
aggregation natural — a failing source produces `Diagnostic`s of severity
`Critical` without interrupting the others, which is consistent with the
terminality invariant ([Errors outside the stream](#errors-outside-the-stream)).

---

## Database connectors

### `database/sql` does not fit — a verified finding

- **No batching**: each statement is executed serially, one network round-trip
  each.
- `rows.Next()` is **intrinsically row by row**.
- Boxing — wrapping a value in an interface, which allocates — into `interface{}` per scanned value: that is the real cost, even more
  than reflection.

Incompatible with the vectorized model. **The connector therefore speaks the
PostgreSQL v3 protocol directly.**

### Honest budget for 0dep

Framing is trivial: ~20 useful message types, 1 type byte + an int32 length.
**The real budget is elsewhere**:

- **SCRAM-SHA-256** (PBKDF2-HMAC-SHA256 + HMAC) — feasible with stdlib
  `crypto/*`;
- **the per-type binary codecs** (numeric, timestamptz, arrays) — long and
  treacherous work.

Budget the codecs, not the parsing.

> **The estimate held.** `postgres.Startup` is the startup packet
> and the SASL exchange over `crypto/hmac`, `crypto/sha256`, `crypto/subtle`,
> `crypto/rand` and `crypto/pbkdf2` — the last of which arrived in the standard
> library in Go 1.24, so even the key derivation this section budgeted for did
> not have to be written. The work that was actually expensive was not the
> cryptography, which is prescribed to the byte by RFC 5802 and pinned by RFC
> 7677's published vector: it was deciding what to *refuse* — a nonce that does
> not extend the client's, an iteration count chosen by the server and paid for
> by the client, a mechanism list offering only channel binding to a client
> without TLS, `md5`. Channel binding itself, `SCRAM-SHA-256-PLUS` over
> `tls-server-end-point`, is in. Each of those refusals is one line of code
> and a paragraph of reasoning, which is the ratio this section did not
> predict.

### The "LINQ-style trick": `= ANY($1)`

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
| SQL Server | TVP, with caveats ([Caveat on SQL Server TVPs](#caveat-on-sql-server-tvps)) |
| MySQL | `JSON_TABLE` — one placeholder, joinable with an index |
| Without arrays | padding to a power of 2 (~11 variants for N ≤ 1024) |

Padding duplicates the **last bound value** (`(1,2,3,3)`), not text: security is
intact. To be avoided on DBMSs without a plan cache, where it is a net overhead.

### Caveat on SQL Server TVPs

Cardinality estimation of **10% for equality, 30% for inequality, 9% for a
range**, independent of the actual number of rows; no per-column statistics; and
on **joins**, bidirectional parameter sniffing — a first plan built on 100k rows
persists until recompilation. Mitigations: `OPTION (RECOMPILE)` or trace flag
2453.

### Pipelining and COPY

**Pipelining** — sending several protocol messages before waiting for any answer: send Parse/Bind/Execute for the whole batch, then **a single
`Sync`**. On error the backend skips all messages up to the next Sync — hence a
**natural per-batch error boundary**. Gain documented on the pgx side: 11
round-trips reduced to 2.

**Binary COPY**: an 11-byte signature, then per tuple a 16-bit field count and per
field a 32-bit length (`-1` = NULL), trailer `-1`. Message boundaries **need not
coincide with rows**: a `Batch[Row]` maps onto a `CopyData`. ~200 lines of code
for the best effort-to-gain ratio in the project.

> **The ~200 lines are the easy part.** The framing above is exactly right.
> What it does not say is that **the transaction boundary is a policy the
> caller must choose**, and that the default everybody writes first is a
> defect rather than a slow option.
>
> `CopyConfig.RowsPerTx` is **required and has no default**, for the reason
> `Overflow` has none: a policy nobody chose is a policy nobody owns. One
> transaction around a whole bulk load costs three things, and only the first
> is obvious.
>
> 1. **Nothing is durable until the end** — eight hours of work, one network
>    blip, all of it lost.
> 2. **A long transaction pins the vacuum horizon for the entire database** — the point before which PostgreSQL may reclaim dead rows —,
>    not just the table being loaded. Dead rows from unrelated workloads
>    cannot be reclaimed while it runs, so a bulk load degrades every other
>    workload on the server. This is the one nobody predicts: it is not a
>    property of the pipeline, it is a property of the database the pipeline is
>    talking to, and no amount of in-pipeline correctness addresses it.
> 3. **Restart is all or nothing.**
>
> The parameter is not a tuning knob laid over one long stream: a COPY cannot
> be committed part-way, so a transaction boundary is a whole cycle — BEGIN,
> COPY, rows, CopyDone, COMMIT — and `RowsPerTx` drives a loop that re-issues
> the COPY. That consequence is worth stating because it is what makes the
> parameter structural rather than cosmetic.
>
> **Delivery is at-least-once, and this is stated rather than implied.** A copy
> interrupted after its third commit leaves three transactions' rows and no
> record of where it stopped; the caller owns the idempotence. The connector
> deliberately does **not** persist a watermark: one that is not committed in
> the same transaction as the data is a second source of truth, and one that is
> would have to know the caller's schema. Where a resumable load is needed, a
> contiguous-prefix watermark is the mechanism, and its counter-example is the
> part to keep — resuming from the *highest* committed key silently loses
> everything still in flight below it, and the failure only appears with more
> than one writer and an unlucky interleaving.
>
> **Index after COPY, not during**: measured at ~2× on that workload. The
> connector cannot do it for the caller, since only the caller knows which
> indexes may be dropped — but the sentence belongs here, where it costs one
> line and otherwise costs a rediscovery.

> **The framing layer is where S8 is kept or lost.** `internal/pgwire` turns
> a byte stream into messages and back, on the standard library alone.
> One ordering decision carries the guarantee: a message's length prefix is
> attacker-controlled, so it is compared to the reader's limit **before** any
> buffer is grown for it — a four-byte header claiming a gigabyte costs a
> comparison and an error, not a gigabyte. The limit cannot be removed, only
> raised: `SetMaxMessage(0)` restores the default rather than meaning
> "unbounded", because there is no legitimate way to ask this package for an
> unbounded read (S3). Verified by fuzzing, as S8 requires rather than
> suggests: ~30 million executions across the reader, the startup reader and a
> writer/reader round trip, no crash and no allocation past the limit.
>
> Message bodies borrow the reader's buffer and are valid until the next read
> — the `Batch` rule again, and for the same reason: a connection that
> allocates per message allocates per row. Two facts a lazier reader would
> merge are kept apart: a clean end returns `io.EOF`, a stream cut mid-message
> returns `io.ErrUnexpectedEOF`, because a truncated response is not an empty
> one. And the length prefix is read as the signed `int32` the protocol
> declares, so a negative length is reported as malformed rather than as an
> implausibly large message — a test caught that, and the fix was to the
> diagnostic rather than to the safety, which was already correct.
>
> **The message layer above it decodes results flat, which is
> [hydration](#hydration)'s layout arriving early.** `Rows` packs every field of a batch end to end in one buffer with a
> table of offsets beside it, rather than a slice per row or per field: a
> thousand-row batch costs a handful of allocations that the next batch reuses,
> where the obvious `[][]byte`-per-row shape costs fifteen hundred. It is also
> the layout a generated hydration function needs to walk one column across
> every row. Two distinctions are kept that a lazier decoder would lose: NULL
> is not the empty value — the protocol separates them and merging them turns a
> missing value into an empty string with no way back — and a `DataRow` whose
> width disagrees with the `RowDescription` is refused rather than interpreted,
> since it means the stream is being read at the wrong offset and every value
> after it would be plausible nonsense. The row count is bounded per batch
> (S1): a result larger than the caller budgeted fails instead of growing.
>
> A server error is a struct with its **SQLSTATE** — PostgreSQL's standard five-character error code — not a sentence. That code
> is the only part stable across versions, locales and rewordings; a caller
> given only a message ends up matching on text, which is how a "duplicate key"
> check breaks the day the server runs in another language. Unknown error
> fields are skipped rather than refused, so a server upgrade does not become an
> outage.
>
> All eight parsers and decoders are fuzz targets — framing, startup framing,
> writer/reader round trip, `ErrorResponse`, `RowDescription`, `DataRow`, the
> scalar decoders and the array decoder — run at ~62 million executions with no
> crash and no allocation past the limit.
>
> **The binary codecs are checked by golden vectors, and by `pgx` from the
> separate `benchmarks` module.** Round-tripping every codec against `pgx`
> as a test-only import of the root module would put `pgx` in every
> consumer's `go.mod`, so that comparison lives in `benchmarks`
> (`TestPGContendersAgree`); inside the module, golden vectors are the
> oracle, and they are better than a fallback. **Golden vectors written from the
> specification** catch the failure a differential oracle is wanted for and
> that a round trip structurally cannot: encode-then-decode agrees with itself
> about a mistake — a codec truncating floats at six decimals passes every
> round-trip test ever written. A vector stating what the bytes must be does
> not.
>
> Two vectors disagreed with the encoder on their first run, and **both times
> the encoder was right**: an IEEE 754 mantissa and a microsecond count across
> twenty-four years are not values a person derives by eye. The lesson is in
> the test file: a golden vector is only as good as the independence of its
> derivation, and one that cannot be derived structurally must say where it
> came from.
>
> **`= ANY($1)` works end to end, and the pull reaches the server.** `Conn.Query`
> sends Parse, Bind, Describe and Execute together — [Pipelining and COPY](#pipelining-and-copy)'s pipelining, two
> round trips rather than eleven — and returns its rows as a
> `sluice.Source[Row]`. `Source` rather than a bare `Stream` because a query has
> exactly the two failures [Errors outside the stream](#errors-outside-the-stream) built it for: one that fails to start produced
> no row to attach an error to, and one that fails at the end has already
> produced all of them.
>
> The decision worth carrying: **Execute names a row count, so the consumer's
> pull becomes back-pressure on the server.** The backend sends that many rows,
> replies `PortalSuspended` and waits; a consumer that stops pulling stops
> PostgreSQL. Memory is bounded by the batch size rather than by the size of
> the result, which is what makes a ten-million-row result readable at all. The
> price is one round trip per batch instead of one for the whole result, taken
> deliberately — and it pays a second time on an early stop, where the drain
> back to `ReadyForQuery` is bounded by one batch rather than by everything the
> server still had to send.
>
> That resynchronisation is not optional. A connection left mid-result hands
> the *next* query this one's rows — a wrong answer with no error anywhere, and
> a symptom no one traces back to its cause — so the drain runs on every path
> out, including an early stop and a panic in the consumer (S10 meeting the
> protocol's own requirement). The distinction it rests on: a **query** error
> leaves the connection usable, because the server skips to the next Sync and
> carries on, while a **framing** error does not, because the position in the
> stream is no longer known. The second marks the connection broken and every
> later query refuses rather than pretends.
>
> **`numeric` is exact.** Go has no decimal type and every approximation
> loses what the column was chosen to keep — both true, and neither an
> obstacle once a representation is chosen deliberately. The value is kept
> the way the wire keeps it — sign,
> weight, display scale, base-10000 digits — which is exact, and **every lossy
> reading has to be named**, so `Float64` says in its own signature what it
> does. `ParseNumeric` is how a value gets in without ever passing through a
> float.
>
> The display scale is the part a rational would have lost: `10.00` and `10`
> are the same number and different numerics, and the difference is what
> `numeric(12,2)` declares and what an invoice prints. Keeping the wire's shape
> keeps it. A thirty-eight-digit value round-trips through the encoder and
> comes back as the same text, which is the property the type exists for and
> the one no float64 can offer.

### Hydration

The decisive argument for codegen is **not** "`reflect` is slow": it is that
reflection works **row by row and field by field**, which breaks vectorization.

- a generated hydration function **per entity**, looping over the 1024 rows
  internally — one indirection per batch, not per row;
- ~~**columnar decoding**: the loop over 1024 values of the same type is
  monomorphic and predictable — the X100 principle applied to hydration~~
  **— measured false, see the note below;**
- **zero `interface{}`** in the hot path; **binary** format mandatory;
- pre-allocation at batch size, buffer reuse between batches.

> **Measured, and the columnar half is wrong.** The argument against
> reflection holds, though it is smaller than it looks: a naive measurement
> that boxes every field through `reflect.Value.Interface` charges the boxing
> to reflection (×5.6). Written the way a careful mapper writes it — a
> per-type plan, typed setters, nothing boxed — reflection costs **×1.8** the
> same hydration written by hand, with the same one allocation per row: 67
> against 37 ns/row (`BenchmarkHydrateReflect` against
> `BenchmarkHydrateRowMajor` in `database/postgres`). ×1.8 on the hydration
> step, with by-name binding and bind-time type checks on top, still justifies
> a generator. The argument *for* columnar decoding does not hold. One scanner
> per column then a pass to assemble measures **34.7 ns/row against row-major's 31.2**,
> and at batch = 1 — the web vertical's shape — 37.6 against 26.3. The columnar
> form makes two passes over the values, and the extra memory traffic costs
> more than the branch predictability buys; a row-major loop over five
> repeating fields is a pattern a processor predicts perfectly well, and there
> is nothing to vectorize when the per-value work is a bounds check and an
> eight-byte load.
>
> So the generator this section asks for is still worth building, and **it
> should emit row-major code**. The column scanners ship anyway, for what they
> are genuinely for: extracting a column, which is what feeds the next
> `= ANY($1)` in a batched pipeline. Their documentation says which of the two
> they are good at.
>
> **The generator is `cmd/sluicegen`**, and two properties of its output
> matter more than its speed. **Columns bind by name, never by position**: a
> SELECT whose two `bigint` columns are reordered would otherwise write `total`
> into `id` — right types, wrong data, no error — and by-name binding turns
> that into a failure when the result opens. **Type OIDs are checked at bind
> time**, so a column altered from `int4` to `bigint` in a migration is caught
> there rather than by decoding eight bytes as four in the middle of a batch.
> Both checks run once per result, not once per row.
>
> The generated file is **committed**, and a test regenerates it and compares:
> generated code that lives only on the machine that ran the generator is code
> nobody reviews and nothing compiles. Drift fails the build.
>
> Measured at 47 ns/row on a six-column entity with a nullable string — a
> different entity from the figures above, so not comparable to them. What the
> number does say directly: **a nullable `*T` field costs an
> allocation per row**, because the pointer the generated code takes escapes to
> the heap. A `postgres.Null[T]` field says "may be NULL" without the pointer
> and without the allocation (measured as a difference, present against
> absent: zero). Pointers are for columns that are genuinely nullable and
> callers that want a pointer, not for optionality in the Go sense.

### Collections: never a cartesian product

Two collection `Include`s **at the same level** produce a cross product (10 posts
× 10 contributors = 100 rows for a blog). In a batch engine, we already have the
parent keys: **separate queries + in-memory join**. Sort order must be made
deterministic by construction.

### Security by construction

1. **Values never travel through SQL text** — only in Bind. With `= ANY($1)`,
   1024 values remain **one parameter**: zero injection surface.
2. **Identifiers come from a catalog generated** at compile time — a column name
   is a *Go symbol*, not a string.
3. **Typed builder**: an invalid state is not representable. No public
   `Raw(string)` in the nominal path.

---

## Joins

### Hash join — the build side is a table

The build side **is** a table, the probe side drives the join and alone produces
the outputs (Kafka Streams' stream/table duality). Naming both sides in the API
removes the ambiguity about which one is materialized.

```go
func StreamTable[A, B any, K comparable, R any](   // package join
    probe Stream[A],        // streamed — may be infinite
    build Stream[B],        // materialized — MUST be finite
    keyA  func(A) K,
    keyB  func(B) K,
    merge func(A, B) R,
    limit BuildLimit,            // MANDATORY — required positional parameter
) Stream[R]
```

**`limit` is required, not an option.** Making unbounded state *inexpressible* is
better than detecting it at runtime. Spark **refuses at planning time** a
stream-stream outer join without a watermark; Kafka **deprecated** its implicit
24 h grace period (KIP-633). The safe default is zero tolerance, widened on
request.

**Overflow is loud**: a `Diagnostic` of severity `Critical` and an observable
counter — never silence, never an OOM. Breaking co-partitioning in Kafka Streams — the requirement that both sides of a join be split the same way — raises no exception and produces no output: that is the worst failure mode, and
the counter-example not to reproduce.

**The limit is global to the plan**, not local to the operator: when several joins
coexist, the sum of the tables must fit. A shared budget.

**Storage is an interface** — an in-memory map by default, optional spill. Never
hardwire a persistence policy into an operator: Kafka Streams ended up mandating
RocksDB in its foreign-key join, in contradiction with its own agnosticism goal.
For spill (continuing on disk when memory is full), the reference is radix partitioning with recursive bit increase
(DuckDB), with its known guardrail: on very skewed data, recursive
sub-partitioning causes *I/O thrashing*.

> `join.StreamTable` takes `join.BuildLimit` positionally, and its zero value
> is refused: a limit of zero under the zero policy would drop every row,
> which is what the type exists to prevent. Overflow is loud — `Fail` panics
> with `ErrOverflow`; `DropNewest` joins against the truncated table and calls
> the limit's `Report` once, which is where the caller raises its `Critical`
> diagnostic — `join` itself knows nothing of diagnostics. The bound counts
> **values, not distinct keys**, since one skewed key is exactly the shape a
> key-based bound would not see. See [ADR 0016](adr/0016-hash-join-bound.md).
>
> `DropOldest` is **rejected rather than accepted** for this operator. A hash
> table has no oldest entry, and quietly behaving as `DropNewest` would offer a
> choice that does nothing.
>
> `join.StreamTableWith` is the storage seam: a caller supplying their own
> `BuildTable` gets spill, or any other policy, without forking the operator.
>
> **The default table is not `map[K][]B`.** That shape allocates a slice per
> distinct key, which dominates on a build side with many keys: measured at
> 66,000 allocations and 17.7 ms over 65536 keys, against 597 and 8.0 ms for a
> flat value slice with an index chain — 2.6× the time and 110× the
> allocations. The chain addresses values with `int32`, so `MaxEntries` is
> capped accordingly and a larger one is refused at construction rather than
> left to overflow.

### Strategies

| Strategy | Condition | Memory |
|---|---|---|
| Hash join | `comparable` keys | O(build) |
| Merge join ([join.Merge](#joinmerge--the-central-primitive)) | both streams sorted on the key | O(1), O(m) per run repeated on both sides |
| Interval join | ordered temporal streams | O(throughput × interval) |
| Nested loop | last resort, replayable right side | O(1), CPU O(n·m) |

**Keys of identical type, enforced at compile time.** Materialize documents that
implicit casts in join constraints are very expensive in memory; generic typing
makes the problem non-existent — a clear advantage over SQL engines.

> **The gap between the first two rows is larger than the table conveys.** On
> 65536 rows a side, joining one-to-one, the merge join measured **24
> ns/element and 25 KB** against the hash join's **99 ns and 8.4 MB** — four
> times the speed and three hundred times less memory (`BenchmarkJoinMerge`
> and `BenchmarkJoinHash`, ten interleaved rounds pinned to one core, medians;
> the per-round ratio held at ×3.5 to ×4.7 in nine rounds of ten). "O(1)
> versus O(build)"
> understates it, because the hash join also pays a map lookup per probe row
> where the merge join advances a cursor. Materialising a side is what you do
> when you cannot sort, not a default to reach for.

### Infinite streams: interval join

State is bounded by a **temporal predicate**, not by a slicing — a sliding window
duplicates each element into N windows.

```go
// a joins b iff: a.ts + lower <= b.ts <= a.ts + upper
func Interval[A, B any, K comparable, R any](      // package join
    left, right Stream[A], /* ... */
    lower, upper time.Duration,
) Stream[R]
```

**Accepted restrictions, taken from Flink: inner join and event time only.** A
windowed outer join requires knowing *when to give up finding a partner*, hence a
watermark — a moving mark that says "nothing older than this will arrive". Without one the semantics is **undefined**, and exposing it would be
lying to the user.

> If watermarks are introduced: plan from the design stage for a **per-source idle
> timeout**. An inactive source freezes the watermark, windows never close, state
> grows without end.

> **What replaces the watermark is ordering.** `join.Interval`
> requires both inputs non-decreasing on their extracted `time.Time`, checked
> as elements are first seen — the same discipline, machinery (the merge
> join's cursor, with the timestamp as its key) and sentinel (`ErrUnsorted`)
> as `join.Merge`. Ordering is what makes eviction safe without guessing
> lateness: a row dies the moment the other side's clock passes the last
> instant that could match it. This also settles **what a timestamp is in
> this library** — an event time extracted per element as `time.Time`, with
> ordering as the operator's requirement — and `window.Tumbling` inherits
> that convention. Two additions: the S1 bound is a required
> `join.BuildLimit` counting both sides' retained rows (`Fail` | `DropNewest`,
> the first drop reported once through `Report`), because
> O(throughput × interval) is a class, not a bound; and the measured class is
> the hash join's, not the merge join's — **~100 ns/element on a zero-width
> window against 99 for the hash join and 24 for the merge join** in the same
> session (the [Strategies](#strategies) measurement, `BenchmarkIntervalJoinNarrow` in the same
> interleaved rounds; ~170 ns on a ±2 s window) — since the per-element map
> is the same dominant term. Verified
> against a brute-force oracle over pseudo-random ordered inputs, eviction
> and compaction included.

### n-ary joins

**Never materialize intermediate binary results** — that is the state
amplification that forces Materialize and RisingWave into *delta join*. A
`.Join().Join()` chain **lies about the real cost**: either an n-ary join, or a
documented cost.

### Scope: no retractions

Three accumulation modes exist (Dataflow Model); *accumulating & retracting* — the
revision of an already-emitted result, "what I told you before was wrong" — is by far the most expensive. **This
library does `discarding` only.** That is what blows up Beam's complexity, and
it is out of reach for a single-process engine without durable state.

*Caveat: no source shows the Beam team calling retractions a design error. It is
an accepted cost on their part, not a documented regret — our choice is a scope
choice, not a correction.*

---

## The request path

The supported path serves HTTP over `net/http`: `web` reads a request into a
batch element and renders what comes back, and `gateway` turns concurrent
request/reply calls into batched stream processing — one pipeline, built at
startup, serving many callers a batch at a time. Three decisions in the
gateway are worth carrying ([ADR 0012](adr/0012-gateway-batch-submission.md)).

**The element is the correlation.** A batched request/reply layer must route
each answer back to the goroutine that asked, and a correlation key carried
alongside the data would make every stage responsible for preserving it. The
element handed to the pipeline is therefore the call itself, carrying the
channel its answer returns through. The cost is a rule: **never drop a call**
— reject by replying with a rejection, which is what the diagnostics model
exists for.

**Nothing checks, per batch, that every call was answered.** Returning from a
yield means the batch was accepted downstream, not handled — `parallel.Async`
and `parallel.Unordered` return as soon as a worker takes it. A check there
would fail calls the pipeline is about to answer correctly, and the real
answer would then arrive as a second one. What is guaranteed instead is that
no caller waits on a pipeline that is gone.

**The value is I/O amortization, and it only exists against a bounded
backend.** Measured: **×6.9** against one query per request, **×8.2** with
`parallel.Unordered` inside the pipeline. The same benchmark against an
unbounded backend reports the gateway ×65 *slower* — correctly, and that
figure is recorded beside the other, because it is the one that says when not
to use this.

**Batch = 1 is measured, not assumed.** A pipeline built per request costs
249 ns and 13 allocations against 1.88 ns and none for the same six steps
hand-written: about 1% of the cheapest possible request, so the claim
survives, and all-batch buys nothing at that size. Two findings follow: the
per-traversal output buffers of `Convert` and `Filter` — the economy that
makes the library fast over large batches — become one allocation per stage
per element when the batch holds one; and building the pipeline costs as much
as running it, 122 ns of the 249. The shape that pays neither is a pipeline
built once at server start with requests entering it as elements, which is
also the only shape where the batch can hold more than one request. The
per-request-pipeline shape is the expensive one, and it is the one everybody
writes first.

No HTTP adapter ships with the gateway, deliberately: the handler that joins
`net/http` to it is eight explicit lines, and an adapter taking decode and
encode functions would save three of them while hiding the two decisions that
matter — the status code and the encoding. `net/http` also stays the protocol
parser: rewriting a parser exposed to hostile input is how CVEs are
manufactured, and the stream layer has no business there.

---

## API pitfalls

### A stream is not re-read — it is cloned

A `Stream` consumed twice runs twice, or yields zero elements if it captures a
`*bufio.Scanner`. **No signal at all.** Java raises `IllegalStateException`; .NET
created a dedicated analysis rule (CA1851). **Go does neither.**

Response: no implicit `Memoize` that would hide the cost, but `Split` in Broadcast
mode ([Split](#split--a-single-primitive)) for cloning, **explicit** materialization — `Collect`, then `Of` —
when the source is not replayable, and a naming convention for single-pass
sources.

### Name of the central type — settled

Rust RFC 2996 renamed `Stream` to `AsyncIterator` because "stream" is too generic
— `io.Reader`, `net.Conn` and network streams are all "streams".

**Decision: we keep `Stream`.** The collision is less troublesome in Go than in
Rust, because the package qualifies the usage at the point of reading:
`sluice.Stream[T]` is unambiguous where Rust imported `Stream` into a shared
namespace. `AsyncIterator` would moreover be a misnomer — our model is
**synchronous**: no `Future`, no suspension point, everything happens on the same
stack. That is precisely what gives native execution traces and the `defer`
executed on early stop.

### Chaining: wrapper not adopted

Go has no generic methods. Two options:

```go
Map(Filter(s, pred), f)        // nested: read backwards
s.Filter(pred).Map(f)          // chained: requires a struct wrapper
```

**No wrapper.** Operators are free functions over `Stream[T]`, itself a
named `iter.Seq[Batch[T]]` with no methods. The absence of generic methods is
what decides it: a wrapper can only
chain the operators that keep `T` — `Filter`, `Take`, `Peek` — while `Map`,
`FlatMap`, `Scan` and every join change the element type and would still be
nested calls, so a chain would break exactly where a pipeline does its work.

> **Cross-cutting lesson (conduit, pipes, Node.js).** Theoretical elegance never
> compensates for an unfamiliar API, and API debt accumulates fast. **Freeze the
> core early** (`Stream`, `Batch`, `Diagnostic`), keep the rest as replaceable
> operators. And **never an implicit mode**: a stream whose behavior changes
> depending on whether a consumer has subscribed cost Node.js three major
> versions.

---

## Security — verifiable properties

| # | Guarantee | Verification |
|---|---|---|
| S1 | No unbounded allocation. Every bound is a required parameter. | Overflow → error, not OOM |
| S2 | No `panic` crosses a public boundary, other than the documented sentinels that `Try` converts into an error ([ADR 0006](adr/0006-sentinel-panics-and-try.md)). | Fuzzing of the parsers — `Path.JSONPointer` covered; every sentinel goes through one constructor |
| S3 | Restrictive default limits. | Review of the defaults |
| S4 | `unsafe` forbidden except under a named audit. | `go vet` + review |
| S5 | Mandatory timeouts on all I/O. | Silent connection → released |
| S6 | No goroutine leak. | In-house detector |
| S7 | Internal errors do not leak to the client. | **Guaranteed by structure**: `cause` unexported ([The model adopted](#the-model-adopted)) |
| S8 | Every network input is bounded before allocation. | Fuzzing |
| S9 | Every internal `iter.Pull` calls `stop()` on all paths. | Test with injected panic |
| S10 | Every operator propagates `false` and lets the generator unwind. | Prompt finalization test |
| S11 | No unbounded buffer between two operators. | Review + pressure test |
| **S12** | **Diagnostics ceiling per element and per batch.** | Pathological batch → counted truncation |
| **S13** | **No value travels through SQL text.** | Builder review + fuzzing |
| **S14** | **An operator that retains a batch beyond the call that received it copies it first.** | `-race` composition test with a buffer-reusing upstream (`Convert \|> X`), per goroutine-crossing operator |

**S9** — if the sequence is not exhausted and `stop()` is not called, the
coroutine **never** terminates.

**S10** — this is streaming's problem #1, ahead of performance. The author of
`conduit` documented his own failure: with a `take 4`, the file handle **stayed
open** for the whole rest of the pipeline. `iter.Seq` handles this case well — a
`defer f.Close()` runs as soon as there is an early stop — **but only if all
operators honestly propagate the `false`**. A tested invariant, not a convention.

> **Tested per operator.** One table drives every operator that takes a
> stream and returns one and asserts the three parts: the refusal reaches the
> source, the source unwinds so its deferred cleanup runs, and no operator
> swallows the refusal to carry on. An operator correct by accident of how it
> is written is not the same thing as guaranteed. Adding an operator without
> adding it to that table is the omission the test makes visible.

**S11** — before 1.5, Flink relied on TCP for its back-pressure: one slow consumer
blocked *all* the logical connections of the multiplex. Our `yield` **is** the
credit; any unbounded buffer reintroduces this bug.

### Dependencies — the exact rule

[ADR 0002](adr/0002-one-dependency.md) records the decision: every
production dependency is a recorded decision, and the graph is as small as
the job allows. Today:

- **Production: the standard library plus one module, `golang.org/x/crypto`.**
  It supplies ChaCha20-Poly1305, which `crypto/tls` negotiates in TLS 1.3 and
  cannot be deselected, but does not export; the QUIC transport needs the raw
  AEAD and keystream to protect packets (RFC 9001 §5). Maintained by the Go
  team, pinned in `go.sum`, watched by Dependabot and `govulncheck`. No other
  module enters the graph: CI checks the allowlist on every push.
- **Tests/tooling: allowed** if they cannot end up in the final binary; tests
  that compare against other libraries live in the separate `benchmarks`
  module, so they never reach a consumer's `go.mod`.

---

## Application architecture

The library imposes no project structure. A `Stream` is a function: nothing to
inject, no global registry, no implicit runtime (but a stratified context, see
[Context travels in the element](#context-travels-in-the-element)).

- **DDD / Clean** — the domain manipulates `Stream[Entity]` without importing the
  web package; adapters live at the periphery.
- **Monolith** — in-memory composition, without serialization.
- **Microservices** — the network boundary is one more operator; the logical
  pipeline is identical, only the transport changes.

### A stage is not a phase

The README names three domains, and the third needs a word this library does
not have. Web and database work is served by **stages** — the dataflow
operators everything above is about. An ETL job that runs for hours needs
something else on top of them, and conflating the two is how a pipeline ends
up unable to resume.

| | Stage | Phase |
|---|---|---|
| What it is | dataflow operator | unit of restart |
| Lifetime | in-process | survives process death |
| Composition | pipelined, concurrent | sequential, barrier between |
| State | in memory | persisted, validated on resume |
| Failure | unwinds the pipeline | marked failed; the rest is resumable |
| Provided here | `Stream[T]` | *nothing, deliberately* |

sluice has an excellent model of the left column and none of the right. For a
request/response cycle that is correct — there is nothing to resume. For an
eight-hour ingest it is the difference between a job you can run overnight and
one you cannot. The distinction is named here so that the two are not
conflated; the machinery is deliberately absent, because a phase runner needs
somewhere to persist its state and that somewhere is the application's schema,
not the library's.

Three properties are what make a phase runner work rather than merely exist,
and they are recorded because each was learned expensively in production
pipelines.

**Skips are re-validated, never trusted.** A phase marked done still has its
recorded counts checked against reality before it is skipped. The failure this
catches is real: staging tables are often `UNLOGGED`, so a crash truncates
them while the checkpoint row survives saying `done`.

**Resume is only valid against the same input**, which means hashing what the
run depends on — the source's identity, the domain parameters, and a
**`logicVersion`**: an integer bumped if and only if the same input can now
produce different output. That last one is four lines of code and it is the
difference between a resume you can trust and a **silently hybrid result** —
half the rows written by last week's code, half by today's, no error, no way
to tell afterwards. A framework offering resume without an identity check
offers that failure as a feature. Note also what such a fingerprint must
*exclude*: operator gates — worker counts, verbosity, `--force` — because the
resuming run's flags should apply to the phases that remain. Getting that
boundary right is most of the work; the hash is trivial.

**Concurrency is refused loudly.** Two processes sharing staging tables corrupt
each other, so the second must fail immediately rather than produce a
plausible wrong answer — the same instinct as S1's "overflow → error, not
OOM".

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
