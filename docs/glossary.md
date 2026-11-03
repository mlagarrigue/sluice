# Glossary

The terms that recur throughout the documents, one sentence each, grouped by
what they belong to; inside a group, in the order a newcomer meets them. A
term explained where it is used is not repeated here; a term used in several
places is.

## Streams and batches

- **Stream** — a sequence of batches of values, produced only when someone
  reads it.
- **Batch** — a slice of values processed together; the unit of transport
  of the whole library, a thousand by default.
- **Operator** — a function that takes a stream and returns another
  (`Filter`, `Map`, a join).
- **Stage** — one step of a pipeline, written one of two ways: as an
  operator, over the whole stream, or as a function from one batch to the
  next, called once per batch — the web path's shape (`httpstream.Handler`,
  the `web/stream` stages).
- **Pull / push** — in pull, the reader asks for the next batch; in push,
  the source sends it when it has it. Sluice.go is pull-based.
- **Back-pressure** — what slows a source down when the reader cannot keep
  up; in pull, it follows from the very shape of the type.
- **Iterator** — in Go, a function that `for range` can walk; `iter.Seq` is
  its standard type.
- **Terminal** — a function that reads a stream to its end and returns a
  result (`Collect`, `Count`); it is what makes the stream flow.
- **Source, sink, provider, consumer** — two pairs for one picture: what a
  stream comes from is its source (or provider), what reads it to the end
  is its sink (or consumer). The documents use the first pair for a
  pipeline and the second for a connector's two sides. `Source`, the type,
  is a stream paired with the errors that cannot travel inside it (the
  connection refused, the final flush short), read with `Err` once the
  stream is consumed.
- **Coalesce** — the core operator that regroups small batches into full
  ones; it holds at most one batch.
- **Goroutine** — one of Go's lightweight threads; several run at once on
  the available cores.
- **Allocation** — reserving memory for a new value; expensive to repeat
  per element, which is what buffer reuse avoids.

## Bounds, errors and diagnostics

- **Bound** — the limit an operator that retains state must receive as a
  parameter; without it, memory can grow without end.
- **Overflow policy (`Overflow`)** — what an operator does when its bound
  is reached: drop the oldest, the newest, or fail.
- **Sentinel** — an exported error value, always the same one
  (`ErrOverflow`, `postgres.ErrAuth`), that code finds with `errors.Is`
  rather than by reading a message. The core's are also raised by panic,
  for lack of another channel in an iterator, and `Try` converts them back.
- **`defer`** — Go's statement that runs a call when the surrounding
  function returns, whichever way it returns.
- **Diagnostic** — what can be said about a value without stopping the
  stream: an error, a warning, where, for what reason.
- **Affordance** — the remedy attached at the same place as a diagnostic:
  what the client can do to fix it.
- **S1 … S14** — the fourteen verifiable security guarantees listed in the
  [Security section](design/architecture.md#security--verifiable-properties)
  of the architecture, which refers to them by number.

## Joins, windows and demand

- **Join** — pairing the elements of two streams that share a key.
- **Build side** — in a table join, the side loaded entirely into memory;
  the other is the *probe* side.
- **Window** — a group of elements from one slice of time.
- **Pane** — what a time window hands over: its elements, with the start and
  end of the slice of time they belong to.
- **Demand** — what a reader tells its source through `pushdown`: how much it
  still wants, and below which key nothing is wanted any more.
- **Gateway** — the package that groups simultaneous calls (one per HTTP
  request, say) into one batch, so that a pipeline answers them together.

## Time and concurrency

- **Data race** — two goroutines touching the same memory at once, one of
  them writing, with nothing ordering them; Go's `-race` detector finds one
  when a test makes it happen.
- **Coroutine** — a function that can pause and resume where it stopped;
  `iter.Pull` runs a stream as one, so a reader takes its batches one at a
  time.
- **Event time** — the time an element says it happened, carried in the
  element, as opposed to the time it is processed; time windows group by
  it.
- **Watermark** — a mark of how far a stream has got: in event time, the
  point below which no more elements are expected; for a job, the position
  it records to resume from.
- **Idempotence** — the property of an operation that has the same effect
  done twice as done once; what makes a delivery that may repeat safe.

## Databases

- **Connector** — the code that speaks an external system's protocol
  (PostgreSQL) to read it as a stream, or to write a stream into it.
- **Hydration** — filling a Go struct from a result row.
- **Wire protocol** — the exact shape of the bytes exchanged with a server.
- **OID** — the number PostgreSQL gives each type (and every other catalog
  object); a result describes its columns' types by OID.
- **SQLSTATE** — PostgreSQL's standard five-character error code
  (`23505`, a unique violation): what code compares, never the message.
- **Prepared statement** — a query PostgreSQL has parsed and planned once,
  run again with new parameters; the **plan** is how the server decided to
  read the tables.
- **Tenant** — one customer of a service shared by several, whose data must
  stay apart from the others'.
- **Row-level security** — PostgreSQL's policies that decide which rows a
  session may see, from that session's settings (`SET LOCAL app.tenant_id`).
- **WAL** — PostgreSQL's write-ahead log: every change, written before the
  data files; logical replication reads it as a stream of changes.
- **LSN** — log sequence number: a position in the WAL, written like
  `0/16B3748`; logical replication resumes from one.
- **ETL** — a job that extracts data from a source, transforms it and loads
  it elsewhere; the same chain of operators as a web request.
- **TVP** — table-valued parameter: SQL Server's way of passing a whole
  table as one query parameter.

## Web and network

- **Round trip** — a question sent to a server and its answer; one network
  journey, often the dominant cost.
- **RTT** — round-trip time: how long a packet takes to reach the peer and
  its answer to come back.
- **TLS** — the encryption layer HTTPS and QUIC run under; the handshake is
  where the two ends agree on keys and check certificates.
- **ALPN** — the TLS extension by which the two ends agree, during the
  handshake, on the protocol they will speak (`h2`, `http/1.1`, `h3`).
- **QUIC** — the transport over UDP that HTTP/3 runs on (RFC 9000):
  encrypted, with several independent streams per connection.
- **Parser** — the code that reads bytes from the network and makes
  something of them (a request, a packet); where hostile input does its
  damage, so it is written once and bounded everywhere.
- **Frame** — the unit HTTP/2, HTTP/3 and QUIC cut their bytes into, each
  with a type (DATA, HEADERS, ACK). `END_STREAM` is the flag on the last
  frame of a stream; `CONTINUATION` the frame that carries the rest of a
  header block too large for one.
- **MTU** — the largest packet a link carries in one piece; a datagram
  larger than the path's MTU is fragmented or dropped.
- **AQM** — active queue management: a router that marks or drops packets
  before its queue is full, so that senders slow down early.
- **GREASE** — values a protocol reserves to be sent at random, so that a
  peer that chokes on unknown values is found before a real extension
  needs them.
- **File descriptor** — the number the operating system gives an open file
  or socket; a process has a limited supply.
- **CDN** — content delivery network: servers in front of a service that
  answer from a copy near the client.

## Identity and credentials

- **Principal** — who a request is, as authentication established it.
- **Grant** — what a principal is allowed to do, as authorisation
  established it.
- **JWT** — JSON Web Token: a signed token carrying claims, sent as a
  bearer credential; `web` verifies the kind signed with HMAC.
- **Claims** — the statements a token makes about its holder: who (`sub`),
  for which service (`aud`), until when (`exp`), and any the issuer adds.
- **Dimension** — an axis on which a grant permits values (`region`,
  `tenant`), read from a claim; `grant.Allows("region", "eu")` asks one.
- **HMAC** — a signature computed with a secret key both sides hold; a
  verifier with the key checks that a token was signed with it.
- **PBKDF2** — a function that derives a key from a password slowly, by
  hashing it many times over; SCRAM uses it, and the iteration count is
  what makes guessing costly.

## The project's own words

- **Nanoseconds per element (ns/element)** — the unit of the core's
  measurements; the ceiling is 0.219 ns, an operator's budget 1.5 ns per
  stage.
- **0dep** — "zero dependencies": the standard library and the recorded
  `golang.org/x` modules only (ADR 0002), nothing else.
- **ADR** — Architecture Decision Record: one structural decision, its
  problem, the decision and why, in [design/adr](design/adr/).
- **CVE** — the public identifier of a known vulnerability
  (CVE-2023-44487, HTTP/2's "rapid reset"); a bound cited with its CVE says
  which attack it stops.
- **h2spec** — a public test suite that sends an HTTP/2 server the cases of
  RFC 9113 and checks each answer.
