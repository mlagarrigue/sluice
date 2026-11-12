# Files: CSV and JSON Lines

A word you do not know is likely in the [glossary](../glossary.md).

## What this is about

A CSV file (comma-separated values) or a JSON Lines file (one JSON value
per line) is the plainest way data is exchanged: a spreadsheet export, a
log of events. Go's standard library already reads both, with
`encoding/csv` and `encoding/json`, one record at a time.
`encoding/csvstream` and `encoding/jsonl` read them **in batches** — a
thousand records at a time — so that they enter a Sluice.go pipeline like
any other source, and add a **bound** the standard library does not have:
a record that is too long is refused instead of being held whole in
memory.

> **In plain words: a bound per record.** `encoding/csv` reads a line to
> its end, however long it is; a gigabyte file with no line break takes a
> gigabyte. `MaxRecordBytes` (1 MiB by default) stops the read as soon as
> one record passes it, and the error says so.

## Reading

`csvstream.Read` and `jsonl.Read` take an `io.Reader` — a file, a request
body — and return a `sluice.Source`: range over its batches, then ask
`Err`.

```go
f, err := os.Open("orders.csv")
if err != nil {
	return err
}
defer f.Close()
src := csvstream.Read(f, csvstream.ReadConfig{})
for b := range src.Stream() {
	for _, rec := range b.Items {
		// rec is a []string: rec[0], rec[1], …
	}
}
if err := src.Err(); err != nil {
	return err // a *csv.ParseError carries the line and the column
}
```

For JSON Lines, name the type of each value: every line is decoded by
`json.Unmarshal` into a fresh value of that type.

```go
type Event struct {
	ID int    `json:"id"`
	At string `json:"at"`
}

src := jsonl.Read[Event](f, jsonl.ReadConfig{})
```

What stops the read: the end of the input, or the first record that cannot
be read. That record is not skipped: the records before it are delivered,
the ones after it are not read, and `Err` says why — for CSV, a
`*csv.ParseError` with its line and column; for JSON Lines, an error that
names the line. An empty line is an error in JSON Lines: it holds no
value. A record can be kept after its batch; the batch's `Items` slice
cannot, as everywhere in the library.

## Writing

`csvstream.NewWriter` and `jsonl.NewWriter` take an `io.Writer`; `Write`
writes one batch and flushes it before returning, so what is in the file is
what the last successful `Write` returned.

```go
w := jsonl.NewWriter[Event](out)
for b := range src.Stream() {
	if err := w.Write(b); err != nil {
		return err
	}
}
```

JSON Lines writes `<`, `>` and `&` as they are, without the escaping
`json.Marshal` applies: a data file is not HTML.

## From a file to PostgreSQL

The whole of an ETL pipeline with nothing between the file and the
database: [`example/csvcopy`](../../example/csvcopy/csvcopy.go) reads a CSV
file with `csvstream.Read`, drops its header with `sluice.Drop`, types each
record with `sluice.Convert`, and sends each batch to PostgreSQL in one
binary COPY call (`Copier.WriteTuples`, see
[PostgreSQL: In bulk, COPY](postgres.md#in-bulk-copy)).

It runs in one transaction. A record that cannot be read, or a field that
is not a number, aborts the copy (`Copier.Abort`), and the batches already
sent are rolled back with it: the table holds the whole file or nothing of
it. A file too large for one transaction is cut by `CopyConfig.RowsPerTx`
instead, and a failed load then resumes after the last committed batch.

## Settings

`csvstream.ReadConfig`, the batch and the bound:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `BatchRecords` | 1024 | ≥ 0 | records per batch |
| `MaxRecordBytes` | 1 MiB | ≥ 0 | bytes one record may take in the input, quotes and line ends included |

and the parsing, `csv.Reader`'s own fields with its meanings:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Comma` | `','` | not `"`, `\r`, `\n`, nor `Comment` | the field delimiter |
| `Comment` | no comment lines | not `"`, `\r`, `\n` | the character that starts a skipped line |

and what the parser tolerates:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `FieldsPerRecord` | the first record's count | negative: no check | fields each record must have |
| `LazyQuotes` | false | | accept a quote inside an unquoted field |
| `TrimLeadingSpace` | false | | drop the white space that starts a field |

`csvstream.WriteConfig`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Comma` | `','` | as above | the field delimiter |
| `UseCRLF` | false | | end each record with `\r\n` rather than `\n` |

`jsonl.ReadConfig`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `BatchRecords` | 1024 | ≥ 0 | values per batch |
| `MaxRecordBytes` | 1 MiB | ≥ 0 | bytes one line may take, its `\n` excluded |

A negative value, or a delimiter `encoding/csv` cannot use, is refused
before anything is read: `Err` names the field.

## What the batch costs

The reading and writing are the standard library's, and the package adds
almost nothing to them: the benchmarks in each package set `Read` against
the standard library's own loop, and against that loop when it also fills
a batch. Holding a batch has a cost of its own — the records stay alive
until the batch is handed on — which is the price of the batch everywhere
in the library, not of these packages.
