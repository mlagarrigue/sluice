# sluice

A dataflow engine for Go: one data model — the `Stream` — for web serving,
database access and ETL.

```go
import "github.com/mlagarrigue/sluice"
```

> **Status: early design.** The core compiles, is tested and benchmarked, but
> the API is not stable and most of the framework is still specification. See
> [Roadmap](#roadmap).

A sluice regulates flow, and that is what this package does. The consumer sets
the pace; the producer only produces what is pulled from it. This back-pressure
is not a mechanism bolted on — it follows from the shape of the type.

## The idea

There is no separate "web engine" and "database engine". There is one flow
engine, and adapters at the edges:

| Domain | Read as a stream |
|---|---|
| Web | `byte → Frame → Request → … → Response → byte` |
| Database | `byte → Row → Entity` |
| ETL | source → transforms → sink, the literal definition |

Parsing, routing, security, business rules, SQL reads, object hydration and
serialization are **all operators in the same algebra**. There is no special
"handler" in the middle of the pipeline.

## The core

Two types, and nothing else:

```go
type Stream[T any] iter.Seq[Batch[T]]   // a sequence of batches, pulled
type Batch[T any]  struct { Items []T } // the unit of transport
```

Everything else — diagnostics, joins, connectors, HTTP — is built on top
without the core knowing about it. That is deliberate: a core that knows its
extensions can no longer evolve.

```go
s := sluice.Of(orders, sluice.DefaultBatchSize)
s = sluice.Filter(s, func(o Order) bool { return o.Total > 0 })
s = sluice.Map(s, applyDiscount)

for b := range s {
    for _, o := range b.Items {
        // ...
    }
}
```

## Why batches

The batch is the unit of transport, never the element. A function that handles
N elements efficiently handles 1 without effort; the converse does not hold.

Measured on this repository ([full results](docs/BENCHMARK-STEP-0.md)):

| | Element-wise | Batched | Gain |
|---|---|---|---|
| 2-stage pipeline | 7.19 ns/elem | 3.66 ns/elem | **×1.97** |
| 4-stage pipeline | 12.92 ns/elem | 6.65 ns/elem | **×1.94** |
| Merging two streams | 69.8 ns/elem | 0.39 ns/elem | **×179** |

The baseline — a native `for range` loop — costs 0.310 ns/element. Batching
does not make merge *faster*; it makes it *possible*. Element-wise, `iter.Pull`
costs 68.6 ns per value, which disqualifies any hot path that uses it.

## Design principles

- **Zero production dependencies.** Standard library only, no exceptions.
- **Bounded memory.** Every blocking operator takes its bound as a required
  parameter, so unbounded state is not expressible rather than merely
  discouraged.
- **Loud failure.** A silent wrong answer is worse than a crash. Overflowing a
  bound raises a diagnostic and a visible counter, never an OOM.
- **Prompt finalization.** An early stop unwinds the generator immediately, so
  `defer f.Close()` in a source runs right away. This is an invariant under
  test, not a convention.
- **Measured, not asserted.** Performance claims come with a benchmark and a
  denominator.

## Roadmap

| Step | Scope | Status |
|---|---|---|
| 0 | Performance ceiling, measured | ✅ done |
| 1 | Core: `Stream`, `Batch`, base operators | 🚧 in progress |
| 2 | `Split`, `Parallel`, bounded operators | planned |
| 3 | N→1 operators, `MergeJoinBy` as the central primitive | planned |
| 4 | Joins: bounded hash join, interval join | planned |
| 5 | HTTP vertical, end to end | planned |
| 6 | PostgreSQL connector: wire protocol, `= ANY`, COPY | planned |
| 7 | Dynamic pushdown | planned |

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — the reference specification, with the
  reasoning, the sources and the trade-offs behind each decision.
- [Benchmarks](docs/BENCHMARK-STEP-0.md) — the measured ceiling every figure is
  expressed against.
- [Contributing](CONTRIBUTING.md)

## Prior art

The design draws on the Volcano iterator model, MonetDB/X100 vectorization,
Akka Streams junctions, Flink's interval joins, and the diagnostic models of
FHIR and SARIF. `docs/ARCHITECTURE.md` cites its sources throughout, including
the ones that argue against the choices made here.

## License

[Apache 2.0](LICENSE)
