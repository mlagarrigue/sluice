# Glossary

The terms that recur throughout the documents, one sentence each, in the
order a newcomer meets them. A term explained where it is used is not
repeated here; a term used in several places is.

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
  [Security section](design/architecture.md#security--verifiable-properties)
  of the architecture, which refers to them by number.
- **Nanoseconds per element (ns/element)** — the unit of the core's
  measurements; the ceiling is 0.310 ns, an operator's budget 1.5 ns per
  stage.

- **Source** — a stream paired with the errors that cannot travel inside it
  (the connection refused, the final flush short), read with `Err` once the
  stream is consumed.
- **Coalesce** — the core operator that regroups small batches into full
  ones; it holds at most one batch.
- **Pane** — what a time window hands over: its elements, with the start and
  end of the slice of time they belong to.
- **Demand** — what a reader tells its source through `pushdown`: how much it
  still wants, and below which key nothing is wanted any more.
- **Gateway** — the package that groups simultaneous calls (one per HTTP
  request, say) into one batch, so that a pipeline answers them together.
- **Principal** — who a request is, as authentication established it.
- **Grant** — what a principal is allowed to do, as authorisation
  established it.
- **Tenant** — one customer of a service shared by several, whose data must
  stay apart from the others'.
- **Row-level security** — PostgreSQL's policies that decide which rows a
  session may see, from that session's settings (`SET LOCAL app.tenant_id`).
- **OID** — the number PostgreSQL gives each type (and every other catalog
  object); a result describes its columns' types by OID.
- **QUIC** — the transport over UDP that HTTP/3 runs on (RFC 9000):
  encrypted, with several independent streams per connection.
- **Terminal** — a function that reads a stream to its end and returns a
  result (`Collect`, `Count`); it is what makes the stream flow.
- **ETL** — a job that extracts data from a source, transforms it and loads
  it elsewhere; the same chain of operators as a web request.
- **CVE** — the public identifier of a known vulnerability
  (CVE-2023-44487, HTTP/2's "rapid reset"); a bound cited with its CVE says
  which attack it stops.
- **ADR** — Architecture Decision Record: one structural decision, its
  problem, the decision and why, in [design/adr](design/adr/).
