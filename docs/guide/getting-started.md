# Getting started

## What you will learn

In twenty minutes: write a first program with Sluice.go, understand what a
*stream* and a *batch* are, and know the four rules that avoid the early
pitfalls. You need Go installed (version 1.26 or newer) and to know how to
write a `for` loop. Nothing else.

## The idea in one picture

Picture a river of data: orders, lines of a file, answers from a database.
You do not want to load all of it into memory at once — the river may be
huge, or never end. You want to process it **as it comes**, by the bucket.

Sluice.go (a *sluice* is a gate that regulates the flow of a waterway)
gives you exactly that:

- a **stream** (`Stream`): the river, a sequence of values arriving in order;
- a **batch** (`Batch`): the bucket, a bundle of values processed together,
  a thousand by default;
- **operators**: the stations along the river, which filter, transform,
  regroup.

The important point, the one that sets Sluice.go apart: **the reader sets
the pace**. Until you ask for the next bucket, nothing is produced. A
program that reads slowly does not overflow memory. That is called
*back-pressure*; here it is not a mechanism to turn on, it is the very
shape of the object.

## Installing

```sh
go get github.com/mlagarrigue/sluice
```

## A first program

We have orders; we want the sum of their totals, ignoring the ones at zero
and applying a 10% discount.

```go
package main

import (
    "fmt"

    "github.com/mlagarrigue/sluice"
)

type Order struct {
    ID    int
    Total float64
}

func main() {
    orders := []Order{{1, 100}, {2, 0}, {3, 50}}

    // 1. Build a stream from a slice.
    s := sluice.Of(orders, sluice.DefaultBatchSize)

    // 2. Describe the stages. Careful: nothing runs yet.
    s = sluice.Filter(s, func(o Order) bool { return o.Total > 0 })
    s = sluice.Map(s, func(o Order) Order { o.Total *= 0.9; return o })

    // 3. Read. This is where everything happens, bucket by bucket.
    sum := 0.0
    for b := range s {              // b is a batch
        for _, o := range b.Items { // b.Items is an ordinary slice
            sum += o.Total
        }
    }
    fmt.Println(sum) // 135
}
```

Three things to see in this program.

**`Of` builds the stream.** It cuts the slice into batches of
`DefaultBatchSize` elements (1024). With three orders there will be one
batch; with a million, a thousand.

**`Filter` and `Map` are operators.** Each takes a stream and returns
another. `Filter` keeps the elements for which your function answers
`true`; `Map` replaces each element with what your function returns.
Neither does any work when you call it: they *describe* what will have to
happen. That is called *lazy* evaluation.

**`for range` sets everything off.** A `Stream` is a standard Go iterator
(a function that `for range` knows how to walk, since Go 1.23). On each
loop iteration the stream gives you a batch; to produce it, it asks `Map`,
which asks `Filter`, which asks `Of`. Then you receive the next batch, and
so on.

> **In plain terms.** If you write `s := sluice.Filter(...)` and never walk
> `s`, none of your functions is ever called. That is not a bug, it is the
> principle: a stream only works when it is read.

## When the result is simple: the terminals

You do not have to write the loop yourself. A few *terminal* functions do
it for you and return a result directly:

```go
all   := sluice.Collect(s)                               // everything into a slice
n     := sluice.Count(s)                                  // how many elements
total := sluice.Reduce(s, 0.0, func(acc float64, o Order) float64 { return acc + o.Total })
first, ok := sluice.First(s)                              // the first one, and whether there is one
```

> **In plain terms.** `Collect` loads everything into memory — that is its
> job. On an endless river it never returns. The other operators keep one
> bucket at a time.

## The four rules of a batch

A `Batch` is only a struct with an `Items` field, a slice:

```go
type Stream[T any] iter.Seq[Batch[T]]   // "a sequence of batches of T"
type Batch[T any]  struct { Items []T } // "a batch of T"
```

The `[T any]` is a *generic type*: `T` is the type of your elements, `Order`
in the example. The whole point is in four rules.

### Rule 1 — A batch is only valid while you receive it

To go fast without allocating memory for every bucket, operators **reuse**
their buffers: the batch you receive on the next iteration is often
written *in the same place* as the previous one. Consequence: if you set a
batch aside (in a variable outside the loop, in a slice of batches) it will
be overwritten without warning.

```go
var kept [][]Order
for b := range s {
    kept = append(kept, b.Items)               // WRONG: they will all end up identical
    kept = append(kept, slices.Clone(b.Items)) // RIGHT: a copy is yours
}
```

> **In plain terms.** Process the batch during the loop iteration, or copy
> it. It is the one rule that surprises, and it is the reason Sluice.go
> costs almost nothing per element.

### Rule 2 — `Of` shares your slice, and `Map` writes into it

`Of` does not copy your slice: the batches point at your data. And `Map`
modifies elements *in place*, in the batch it receives. So after
`Map(Of(orders, n), f)`, the `orders` slice itself holds the transformed
values. That is deliberate — it is what allows zero allocations — but it
can surprise.

To transform into **another type** (an `Order` into a `Line`), use
`Convert`: it writes into its own buffer and leaves the input alone.

```go
labels := sluice.Convert(s, func(o Order) string { return fmt.Sprintf("#%d", o.ID) })
```

### Rule 3 — `break` stops everything, immediately

If you leave the loop with `break`, or a terminal stops before the end, the
signal travels back up the whole chain to the source, which runs its
`defer`s right away. A file closes, a connection is released, the moment
you stop reading — not at the end of the program.

```go
for b := range sluice.Of(bigSlice, 1024) {
    if found(b) {
        break // the source is released here, not later
    }
}
```

> **In plain terms.** In many stream libraries, stopping reading leaves
> resources open behind you; it is the number one bug of the genre. Here it
> is a property tested for every operator.

### Rule 4 — A batch can be nearly empty

After a `Filter` that keeps one order in a hundred, a batch of 1024 holds
only ten. The following stages pay their small fixed cost for ten elements
instead of a thousand. It is not wrong, just less efficient. `Coalesce`
glues small batches back into full ones:

```go
s = sluice.Filter(s, rare)
s = sluice.Coalesce(s, sluice.DefaultBatchSize) // full batches again
```

> **In plain terms.** Think of it after a very selective `Filter` or a
> join. When in doubt, it is not urgent: a sparse batch is still faster than
> element-by-element processing.

## Errors: two families, two places

### What happens *around* the stream

Opening a file can fail before the first line; closing it can fail after
the last. No element is there to carry those errors. Sluice.go puts them
beside the stream, in a `Source`:

```go
src := openCSV("orders.csv")        // returns a sluice.Source[Order]
for b := range src.Stream() {
    // ...
}
if err := src.Err(); err != nil {   // to consult AFTER reading
    return fmt.Errorf("reading orders: %w", err)
}
```

To write your own source, `sluice.NewSource(stream, errFn)` pairs a stream
with the function that will report the error. And so that you do not
forget to read it, `src.Consume(f)` and `src.Collect()` read and then
return the error in one step.

### What happens *to an element*

A malformed line in a file, an order that breaks a business rule: that is
not a failure of the stream, it is information about *that* element. It
travels with it — in a field of your struct, or in an attached
*diagnostic* (the [Web](web.md) guide shows that model). The stream never
looks inside your elements: an element "in error" keeps moving, unless you
decide to stop (`TakeWhile`, or a terminal returning `false`).

> **In plain terms.** `error` for "the stream cannot continue", a
> diagnostic for "this element has a problem". A warning on one order must
> not crash the processing of the thousand others.

## When a panic is really an error

A Go iterator has no way to return an error in the middle of a walk.
Sluice.go therefore uses a *panic* (Go's abrupt stop) for a few precise,
named situations: a bounded operator overflowing under the "fail" policy
(`ErrOverflow`), data that should have been sorted and is not
(`ErrUnsorted`), the branches of a `Split` read in the wrong order
(`ErrSplitStalled`).

In a script run overnight, crashing is the right behaviour: you want to
know. In a server serving a thousand users, it is not. `Try` converts those
panics — and only those — into an ordinary error:

```go
err := sluice.Try(func() {
    sluice.ForEach(pipeline, handle)
})
if errors.Is(err, sluice.ErrOverflow) {
    // a pathological batch; the server carries on
}
```

> **In plain terms.** `Try` does not hide your own bugs: a nil pointer in
> your `Map` function keeps crashing the program, loudly, with its trace.
> That is deliberate.

## Several streams at once

- `Split(s, n, route)` sends each batch to one or more of the `n` branches,
  according to what your `route` function returns: one branch number
  (partition), a number in turn (balance), all the numbers (broadcast). The
  source is read once. Read the branches in alternation, not one after the
  other.
- `Merge(sluice.WhenAll, a, b)` brings two streams together, strictly
  alternating: one batch from `a`, one from `b`. `Concat(a, b)` puts `b`
  after `a`.
- *Joins* (pairing the elements of two streams by a common key, as in SQL)
  live in the `join` package, because they must keep elements in memory —
  the next guide explains what that costs.

## What next

- [Choosing an operator](operators.md) — the full map, grouped by what each
  operator keeps in memory.
- [PostgreSQL](postgres.md) — read a table as a stream, write in batches.
- [Web](web.md) — serve HTTP requests that share their round trips.
