# ADR 0008 — A multiplexed connection is a reader, a responder and a writer

- **Created:** 2026-08-22
- **Revised:** 2026-10-05 (streamed bodies hold the responder; HTTP/3 per-stream writers), 2026-10-07 (renumbered)
- **Theme:** httpstream

## Problem

Both multiplexed servers (HTTP/2, HTTP/3) ran everything on the goroutine
that read the socket: parsing, the handler, response framing, and the
draining of streamed bodies. One goroutine per connection is the shape the
project wants; the same goroutine reading and writing is not. Observed: a
streamed response past the 64 KiB default window killed the whole
connection, because the goroutine that would read the `WINDOW_UPDATE` was
blocked writing; the connection was deaf to `PING`, `GOAWAY` and
`RST_STREAM` while a handler ran; on HTTP/3 the inline drain blocked the
QUIC read loop and the handshake; and every refusal was a `GOAWAY`, so one
bad request among a hundred lost all hundred. A multiplexed protocol exists
to make requests independent.

## Decision

Three goroutines per HTTP/2 connection, each owning one piece of state:
a **reader** (frame parsing, stream states, receive windows, the HPACK
decoder), a **responder** (the `Handler` call, response framing, pulling
streamed bodies), a **writer** (the socket's write side, send windows,
scheduling). HTTP/3 has a responder per request stream since 2026-10-04,
so a slow body holds only its stream. Concurrency is per connection, not
per request: a batch that arrived together is still handed to the handler
together.

Every hand-off is bounded and the bound is stated: reader → responder is a
channel of `MaxConcurrentStreams` batches that cannot block (a request
holds its stream slot until its response is written); reader → writer for
control frames is bounded by `MaxPendingControlBytes` and the reader blocks
when full, which is the prescribed back-pressure; responder → writer for
payloads is not a queue — the responder blocks until the bytes are on the
wire, which keeps borrowed payloads safe (S14) and runs the producer at the
socket's pace. Flow control waits instead of refusing, bounded by
`WriteTimeout`; scheduling is round-robin per byte-budget round, control
frames first; failures are classified (stream error resets one stream,
connection error is reserved for framing and HPACK state); closed streams
are remembered so late `DATA` is tolerated and still counted; the idle
clock does not run while answers are owed; the connection receive window
starts at 1 MiB.

## Rationale

Streamed responses of any size, a connection that stays responsive, one
request's failure costing one request, and cancellation that reaches the
handler. The cost is three goroutines per connection and their
synchronisation — per connection, not per request, which
`TestH2ConnectionsLeaveNoGoroutines` keeps honest.

Deliberately not built: stream priorities (RFC 9113 deprecated the scheme),
server push (`ENABLE_PUSH` 0), and the interleaving of two streamed bodies
on one HTTP/2 connection — while a producer is pulled, the responder runs
no handler, so other requests on that connection wait. Accepted because the
alternative is a goroutine or a chunk copy per streamed response; a client
with an event feed opens a second connection or uses HTTP/3. A consumer
with an SSE workload on HTTP/2 is the signal to revisit, with per-stream
writers as the known fix. A general write queue with its own memory bound
was rejected: blocking hand-offs give exact back-pressure with no copy and
no policy for a full queue.
