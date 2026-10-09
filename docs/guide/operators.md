# Choosing an operator

## One question: what does it keep in memory?

An operator receives batches and produces batches. Some only need the batch
in front of them: `Filter` looks at each element and keeps it or not, and
that is all. Others must **remember** something from one batch to the
next: a join must have seen the other stream to know what to pair with, a
time window must accumulate until the minute has passed.

What an operator retains is what decides whether it can run on an infinite
stream and how much memory it will cost you. Sluice.go therefore groups its
operators into packages by that question, and the package name tells you
the answer before you open the documentation:

| Package | What it retains between two batches |
|---|---|
| `sluice` (the root) | nothing |
| `sluice/join` | one side of the other stream |
| `sluice/window` | time |
| `sluice/parallel` | goroutines |
| `sluice/distinct` | keys |

> **In plain terms.** Everything in the root composes without limit and
> cannot overflow memory. Everything elsewhere retains something, and makes
> you write how much, at most.

## The root retains nothing

`Map`, `Convert`, `Filter`, `FlatMap`, `Peek`, `Scan`, `Take`, `Drop`,
`TakeWhile`, `DropWhile`, `Split`, `Concat`, `Merge`, `Coalesce`, and the
terminals (`Collect`, `Count`, `Reduce`, `First`, `ForEach`). Each costs on
the order of a nanosecond per element per stage, without allocating memory
once running.

A few deserve a word:

- `FlatMap` replaces each element with zero, one or several elements (an
  order becomes its lines).
- `Scan` accumulates a value as it goes (a running total): it keeps that
  value, not elements.
- `Take(n)` and `Drop(n)` keep or skip the first `n`; `TakeWhile` and
  `DropWhile` do the same as long as a condition holds.
- `Coalesce` glues small batches back into full ones; it keeps at most one
  batch while filling it, and it sits in the root because it is the core's
  own tool for repairing its sparse batches.

Two operators do not exist, on purpose. **No `Sort`**: sorting needs to see
every value, so it is written `Collect` then `slices.Sort`, so that loading
everything shows in the code. **No `Buffer`**: one might think a buffer
between two stages decouples them; in synchronous reading it does not — it
only delays. What you are looking for is `parallel.Async`, below.

## `join` retains one side

A **join** pairs the elements of two streams that share a **key**: order
no. 42 with customer no. 7 who placed it. It is SQL's `JOIN`, applied to
streams.

| Operator | What it retains | When to pick it |
|---|---|---|
| `join.Merge(left, right, keyL, keyR, cmp)` | only the current run of equal keys | both streams are **sorted** on the key |
| `join.ZipLongest(left, right)` | nothing beyond the two current batches | pairing by position: the first with the first, and so on |
| `join.Interval(left, right, …, lower, upper, …, limit)` | a bounded time window of both sides | two streams **ordered by time**, possibly infinite |
| `join.StreamTable(probe, build, …, limit)` | **all** of the `build` side in memory | `build` is finite (a reference table), `probe` may not be |

### Two sorted streams: the merge join

If both streams are sorted on the key, you can walk them side by side like
two alphabetical lists, advancing whichever is behind. You never need to
retain more than the elements of the current key. That is `join.Merge`,
and it is the join to prefer whenever you can sort:

```go
// customers and orders are sorted on the customer id.
pairs := join.Merge(customers, orders,
    func(c Customer) int { return c.ID },        // the key on the left side
    func(o Order) int { return o.CustomerID },   // the key on the right side
    cmp.Compare[int])                            // how to compare two keys
inner := sluice.Filter(pairs, func(e join.EitherOrBoth[Customer, Order]) bool {
    return e.Both() // keep only the pairs: an inner join
})
```

The output is an `EitherOrBoth`: a left element, a right one, or both.
Filtering on `Both()` gives an inner join; keeping `HasLeft` gives a left
join; keeping everything, a full outer join. Six SQL semantics, one
operator and one filter.

> **In plain terms.** If a key repeats on both sides (ten orders from the
> same customer, and ten addresses for that customer), the operator must
> produce every combination, and then retains the right-hand run for that
> key. It is the only case where its memory grows. If the streams are not
> actually sorted, it panics with `sluice.ErrUnsorted` rather than silently
> returning a wrong result.

### A reference table against a stream: the table join

When one side is a finite list (the customers) and the other an arbitrary
stream (the orders, arriving in any order), you load the list into memory
and look each order up in it. That is `join.StreamTable`:

```go
enriched := join.StreamTable(orders, customers,
    func(o Order) int { return o.CustomerID },
    func(c Customer) int { return c.ID },
    func(o Order, c Customer) Line { return Line{o, c.Name} }, // how to merge a pair
    join.BuildLimit{MaxEntries: 100_000, OnOverflow: sluice.Fail})
```

The last argument is **required**: how many entries the table may hold,
and what to do beyond. There is no default, because a table without a limit
is a program that will die the day the reference list has grown.

**Why prefer the merge join** when you have the choice: on 65,536 rows a
side, the merge costs 24 ns per element and 25 KB; the table, 99 ns and
8.4 MB. That is not a small difference, it is a difference in kind: the
merge advances a cursor, the table does a lookup per element. Loading a
side into memory is what you do when you *cannot* sort.

### Two streams in time: the interval join

When both streams are ordered by time and never end (clicks and ad
impressions), you can neither load everything nor wait for the end. You
pair what happened "within 15 seconds":

```go
attributed := join.Interval(clicks, impressions,
    func(c Click) string { return c.UserID },         // the key
    func(i Impression) string { return i.UserID },
    func(c Click) time.Time { return c.At },          // the time on each side
    func(i Impression) time.Time { return i.At },
    -15*time.Second, 0,                               // the impression between 15 s before and the click
    func(c Click, i Impression) Attribution { return Attribution{c, i} },
    join.BuildLimit{MaxEntries: 10_000, OnOverflow: sluice.DropNewest,
        Report: func(limit, held int) { log.Printf("interval join truncated at %d", held) }})
```

Time order is what allows forgetting: as soon as one side's clock has
passed the last instant that could match, the element can be evicted. The
limit is there because "what arrives within 15 seconds" is not a number.

## `window` retains time

A **window** groups the elements of one slice of time: all the clicks of
minute 14:05. To produce it, you must accumulate until the minute has
passed.

| Operator | What it retains | When to pick it |
|---|---|---|
| `window.Tumbling(s, ts, size, n, policy)` | the current window, at most `n` elements | grouping a time-ordered stream by slice of time |
| `window.CoalesceWithin(s, size, within)` | a batch being filled, and a clock | rebuilding full batches without making anyone wait more than `within` |

```go
// One window per minute, at most 100,000 elements per window.
perMinute := window.Tumbling(clicks,
    func(c Click) time.Time { return c.At },
    time.Minute, 100_000, sluice.DropNewest)
stats := sluice.Convert(perMinute, func(p window.Pane[Click]) Stat {
    return Stat{At: p.Start, N: len(p.Items)} // a Pane owns its elements
})

// Batches of 64 requests under load, never more than 2 ms of waiting when idle.
requests := window.CoalesceWithin(incoming, 64, 2*time.Millisecond)
```

> **In plain terms: why `CoalesceWithin` exists beside `Coalesce`.**
> `Coalesce` waits until it has `size` elements, however long that takes:
> perfect for reading a file, catastrophic for a server where ten requests
> per second would fill a batch of 1024 in a hundred seconds.
> `CoalesceWithin` adds a deadline. It costs a *goroutine* (one of Go's
> lightweight threads), because a deadline that only expired when data
> arrived would not be a deadline.

Windows are *tumbling*: consecutive, non-overlapping, aligned on the clock
rather than on the first element — the same data gives the same windows
whatever the batch cuts. A `Pane` is a closed window, with its start, its
end and its elements; unlike an ordinary batch, it **owns** its elements:
you may keep it.

## `parallel` retains goroutines

Everything above runs on a single thread of execution: when you read a
batch, the source, `Filter` and `Map` take turns, on your goroutine. That
is simple and fast. But if a stage is slow because it computes a lot, or
because it waits on disk or network, you want several running at once.

| Operator | What it retains | When to pick it |
|---|---|---|
| `parallel.Ordered(s, workers, f)` | `workers` goroutines and a small buffer to put batches back in order | `f` is **genuinely** expensive (decoding, hashing, compression) and order matters |
| `parallel.Unordered(s, workers, f)` | `workers` goroutines, nothing to reorder | `f` is expensive, order does not matter, and its cost varies from batch to batch |
| `parallel.WithState(s, workers, mkState, f)` | a private state per goroutine, closed at the end | `f` needs a cache or a table you do not want to share |
| `parallel.Async(s, depth)` | one goroutine and `depth` batches of lead | decoupling a stage that waits (reading, writing) from what follows |

```go
// Expensive decoding, order preserved, four goroutines.
decoded := parallel.Ordered(rows, 4, func(b sluice.Batch[Row]) sluice.Batch[Entity] {
    out := make([]Entity, len(b.Items))
    for i, r := range b.Items {
        out[i] = decode(r)
    }
    return sluice.Batch[Entity]{Items: out}
})

// One reader per goroutine, opened once, closed at the end.
enriched := parallel.WithState(rows, 4,
    func() (*Reader, error) { return OpenReader(path) },
    func(r *Reader, b sluice.Batch[Row]) sluice.Batch[Entity] { return r.hydrate(b) })

// Reading the file goes on while the rest works: up to four batches of lead.
rows := parallel.Async(readCSV(path), 4)
```

> **In plain terms: parallelism pays less than you think.** Measured: on
> work heavy enough to spread, four ordered goroutines give ×1.29, not ×4 —
> because the operator waits for the oldest batch, and one slow goroutine
> holds the others. On a trivial `f`, four goroutines are 3.5 times
> *slower* than none: the coordination costs more than the work.
> `Unordered` does better when the cost varies (×2.75 on skewed work). And
> `Async`, which does not parallelise the computation but overlaps the
> waiting with the work, halves the wall clock of a three-stage ETL.
> "Adding parallelism" is a claim to check, not a given.

All these operators own the goroutines they start and stop them before
handing back, on every exit path, panic included: no goroutine outlives the
call. The input batch is copied on the way, so that the previous stage may
reuse its buffer while a goroutine reads.

## `distinct` retains keys

**Deduplicating** means letting elements with the same key through only
once. On a finite list, you keep a set of every key seen. On an infinite
stream, that set would grow without end.

`distinct.By(s, n, key, policy)` does a **local** deduplication: it
remembers the last `n` distinct keys, and an element whose key has left
that window passes again.

```go
// The same order reference repeated within the last 1,000 passes once.
unique := distinct.By(orders, 1000,
    func(o Order) string { return o.Ref },
    sluice.DropOldest)
```

> **In plain terms.** If you need a *global* deduplication over a finite
> stream, say so with `Collect` and a `map`: the code then shows that
> everything is loaded. If the stream is infinite, the global one does not
> exist, and `distinct.By` is the only honest form.

## The overflow policy

Every operator that retains elements takes a limit and a policy
(`sluice.Overflow`) that says what to do when the limit is reached:

- `Fail` — panic with `sluice.ErrOverflow` (which `sluice.Try` turns into
  an error in a server). The choice to prefer: an overflow must be seen.
- `DropNewest` — keep what is retained, let the new element through
  without retaining it. The result is incomplete, never wrong, provided you
  know: that is why `join.BuildLimit.Report` exists.
- `DropOldest` — evict the oldest to make room. Table joins refuse it,
  because a table has no "oldest".

There is no default policy. A policy nobody chose is a policy nobody owns.

## Measure before choosing

The figures on this page come from [Measurements](../benchmarks.md), and so
does the method that makes them credible: on a laptop, two runs of the same
program differ by 14%. Before deciding that one operator is worth another
in your pipeline, `compare.sh` runs both versions alternately and counts
the wins.
