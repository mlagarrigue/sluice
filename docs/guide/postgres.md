# PostgreSQL

## What this is about

PostgreSQL is a server: your program talks to it over a network
connection, following a **protocol** — an agreement on the shape of the
messages exchanged. In Go, one usually goes through `database/sql` and a
**driver** (the library that speaks the protocol for you). But
`database/sql` hands rows back **one at a time**, which is exactly the
shape Sluice.go is trying to avoid.

`database/postgres` therefore speaks the protocol directly, on Go's
standard library alone. What that buys:

- querying a thousand identifiers in **one round trip** (a question to the
  server and its answer: one network journey, often the slowest thing in
  your whole program);
- loading millions of rows with **COPY**, PostgreSQL's bulk-load mechanism,
  a batch at a time;
- receiving results as a **stream** you read at your own pace, which the
  server produces at that pace.

## Connecting

The network stays yours: which host, which timeout, encrypted or not are
deployment choices the library cannot make for you. You open the
connection; Sluice.go handles everything after, including
**authentication** (proving to the server who you are):

```go
nc, err := net.DialTimeout("tcp", "db.internal:5432", 5*time.Second)
if err != nil { return err }
nc.SetDeadline(time.Now().Add(30 * time.Second)) // past that, the handshake is abandoned

conn, err := postgres.Startup(nc, postgres.StartupConfig{
    User:     "orders",
    Database: "orders",
    Password: os.Getenv("PGPASSWORD"),
    Params:   map[string]string{"application_name": "orders-etl"},
})
if err != nil { return err }
defer conn.Close()
```

By default, authentication uses SCRAM-SHA-256: the password never travels
as is. Sending it in cleartext is possible but must be asked for explicitly
(`AllowCleartextPassword`), because only you know whether the network you
opened may carry a secret. To encrypt the connection, pass an already
verified `*tls.Conn` instead of the raw socket.

> **In plain terms: no pool.** A *pool* is a reserve of open connections
> shared by everyone. There is none here, because batching calls
> (`gateway`, see [Web](web.md)) does with **one** connection what a pool
> does with sixty-four, and how many connections to open is a decision for
> your application, not for the library.

A wrong password arrives as a server error with code `28P01`; an
authentication method the package refuses (`md5`, deprecated) arrives as
`ErrAuthUnsupported`. Two different errors because they are fixed
differently.

## Reading

A query returns a `Source[Row]`: a stream of batches of rows, and the error
to consult afterwards (see [Getting started](getting-started.md) for
`Source`).

```go
keys := []int64{1, 2, 3 /* … a thousand */}
src := conn.Query(ctx,
    "SELECT id, total FROM orders WHERE id = ANY($1)",
    [][]byte{postgres.AppendInt8Array(nil, keys)},                       // the parameter, encoded
    postgres.QueryConfig{BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8Array}},
)
for b := range src.Stream() {
    for _, row := range b.Items {
        idBytes, isNull := row.Value(0)       // column 0, as bytes, and "is it NULL?"
        id, err := postgres.DecodeInt8(idBytes)
        // …
    }
}
if err := src.Err(); err != nil { return err }
```

Three ideas in this code.

**Values never go into the query text.** The `$1` is a **parameter**: a
slot the server fills with the value sent separately, in binary. That is
what makes **SQL injection** (slipping SQL into a value to hijack the
query) impossible by construction, not by vigilance.

**`= ANY($1)` rather than `IN (…)`.** With `IN`, a thousand values make a
thousand slots and a different query shape for every number of values,
which the server must prepare each time. With `= ANY`, the whole array is
**one** parameter: one shape, one plan, whatever the number of keys. That
is the shape to remember.

**`BatchRows` is the back-pressure.** The server sends that many rows, then
waits for you to ask for more. If you stop reading, it stops sending. Your
program's memory is bounded by the size of the batch, not of the result:
that is what makes a ten-million-row result readable. The price: one round
trip per batch.

> **In plain terms: `AllRows`.** For a small read you will consume whole (a
> row by its identifier), `QueryConfig{AllRows: true}` asks for everything
> at once and saves a round trip — at parity with pgx. What it gives up: if
> you stop reading midway, the server has already been told to send it all.
> An option for small reads, not the default.

> **A round-trip trap.** If you expect one row and ask for `BatchRows: 1`,
> the server sends the row then waits, because it cannot know there is no
> other; your program asks again, and pays a round trip to learn there is
> nothing. Ask for one more than you expect.

## Decoding

A `Row` is a view into its batch, valid for the call (rule 1 of
[Getting started](getting-started.md)). `row.Value(col)` returns a column's
bytes and a boolean "is it NULL?". **NULL is never the empty value**: an
absent string and an empty string are two things, and the protocol tells
them apart.

The `Decode*` functions turn bytes into a Go value: `DecodeInt8` for a
`bigint`, `DecodeText` for a `text`, `DecodeTimestampTZ` for a date with a
time zone, `DecodeNumeric` for an exact amount. The `Scan*` functions do
the same for **a whole column** of a batch in one pass
(`ScanInt8(dst, rows, col)`) — the shape that produces the next array of
keys to send as `= ANY`.

> **In plain terms: `numeric`.** A `numeric(12,2)` column is chosen
> precisely when a floating-point number would lose something (cents).
> Sluice.go keeps those values **exact**, and every lossy conversion is
> named: `n.Float64()` says what it does; `n.String()` renders `10.00`, not
> `10`, because that is what an invoice prints.

An error returned by the server is a `*postgres.Error` with its
**SQLSTATE** (`Code`), a standardised five-character code, the only part
stable across versions and languages, and the constraint concerned:
`e.Code == "23505"` means "duplicate key" whatever the server's language.

## Hydrating: from rows to structs, without reflection

**Hydrating** a struct means filling its fields from a row. Doing it field
by field, with **reflection** (Go's ability to inspect a type at run time),
costs on every field of every row — the cost the batch exists to avoid.
`cmd/sluicegen` therefore generates, for each of your structs, the code
that hydrates a **whole batch** in one loop:

```go
//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type Order -in order.go

type Order struct {
    ID        int64     `db:"id"`
    Total     float64   `db:"total_amount"`
    Label     string    // no tag: the column is named label, by convention
    CreatedAt time.Time `db:"created_at"`
    Note      *string   `db:"note"` // nullable: a pointer is how to say "may be NULL"
    Computed  int64     `db:"-"`    // not a column
}
```

`go generate ./...` produces a file beside it, with two functions:

```go
var plan entity.OrderPlan
var bound bool
for b := range src.Stream() {
    if !bound {
        plan, err = entity.BindOrder(b.Items[0].Fields()) // once per result
        bound = true
    }
    orders, err = entity.HydrateOrder(orders[:0], b.Items[0].Rows(), plan)
    // …
}
```

`BindOrder` matches the result's columns to the fields **by name**, not by
position, and checks the **types**: if a migration changed a column from
`int4` to `bigint`, or a `SELECT` reordered two columns, the error comes
when the result opens, not in the middle of a batch as plausible, wrong
values. `HydrateOrder` then fills a slice from each batch.

> **In plain terms.** The generated file is committed with your code, and a
> test regenerates it and compares: if someone changes the struct without
> rerunning the generator, the build fails instead of leaving a silent
> difference. Measured: 47 ns per row for six columns. A nullable field
> costs one allocation per row (the pointer): keep pointers for columns that
> are genuinely nullable.

## Writing

### In bulk: COPY

To load many rows, PostgreSQL has a dedicated mode, COPY, far faster than
repeated `INSERT`s. Sluice.go uses it in binary, a batch at a time:

```go
cp, err := conn.CopyFrom(ctx,
    "COPY orders (id, label) FROM STDIN WITH (FORMAT BINARY)",
    postgres.CopyConfig{RowsPerTx: 100_000})
if err != nil { return err }

var buf []byte
for _, o := range orders {
    buf = buf[:0]
    buf = postgres.AppendTupleHeader(buf, 2)                             // a row of two columns
    buf = postgres.AppendField(buf, postgres.AppendInt8(nil, o.ID))
    buf = postgres.AppendField(buf, postgres.AppendText(nil, o.Label))
    if err := cp.WriteTuples(buf, 1); err != nil { return err }
}
if err := cp.Close(); err != nil { return err }
```

`RowsPerTx` is **required**. A **transaction** is a group of writes the
database commits or discards as a block; putting a whole eight-hour load in
one transaction is everyone's reflex, and it is a defect, for three
reasons: nothing is saved until the end (one network blip and everything is
lost), restart is all or nothing, and a long transaction stops PostgreSQL
from cleaning up in the **whole** database while it lasts — your other
applications slow down without knowing why. By writing `RowsPerTx:
100_000`, you say every how many rows the database commits.

> **In plain terms: at-least-once.** If the load is interrupted after its
> third transaction, the first three are in the database and nothing says
> where it stopped. It is up to you to make reloading harmless (with a
> unique key, for instance). And index the table **after** the COPY, not
> during: about twice as fast.

### Retail: transactions

```go
tx, err := conn.Begin(ctx, postgres.TxConfig{})
if err != nil { return err }
defer tx.Rollback() // a no-op if Commit succeeded: the safety net

if err := tx.SetLocal(ctx, "app.tenant_id", tenant); err != nil { return err } // visible to row-level security
if err := tx.Exec(ctx, "UPDATE orders SET …"); err != nil { return err }
return tx.Commit(ctx)
```

`Rollback` takes no context, on purpose: it runs from a `defer`, often
after the request's context has expired, and a block left open on a reused
connection is the worst outcome. A stream obtained from `tx.Query` is read
**before** the `Commit`; afterwards it refuses to run rather than running
outside the transaction under nobody's identity.

## What is not there

No connection pool, no automatic resumption of an interrupted load, `md5`
refused, no `money` or `tsvector` types, and passwords with characters
outside printable ASCII may fail cleanly (SASLprep normalisation is not
applied). The detail: [Limits](limits.md).

## Measured

Against pgx, the reference driver, on one connection each: parity on a
point read with `AllRows` (139 µs against 140), 697 µs against 742 for a
thousand keys in `= ANY`, a wide scan 1.5 times slower at decode but 60
allocations against 280,008, and a faster COPY. The rows and the method:
[Measurements](../benchmarks.md).
