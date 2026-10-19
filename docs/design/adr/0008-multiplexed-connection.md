# ADR 0008 — A multiplexed connection is a reader, a responder and a writer

- **Created:** 2026-08-22
- **Revised:** 2026-10-05 (streamed bodies hold the responder; HTTP/3 per-stream writers), 2026-10-07 (renumbered), 2026-10-16 (the HTTP/2 responder no longer waits for the wire), 2026-10-19 (each HTTP/2, then HTTP/3, handler call on a goroutine of its own)
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
decoder), a **responder** (response framing, pulling streamed bodies), a
**writer** (the socket's write side, send windows, scheduling). The reader
hands each batch to a `Handler` call on a goroutine of its own, which it
never waits for, and the responder frames what the calls return: a batch
that arrives while another is being answered does not wait for it. Each
call holds stream slots, so the calls in flight are bounded by
`MaxConcurrentStreams`. HTTP/3 has no responder: its read callback hands
each batch to a call on a goroutine of its own, which writes each answer on
a goroutine per request stream (since 2026-10-04), so a slow call holds only
its batch and a slow body only its stream. Concurrency is per batch, not per request: a batch that arrived
together is still handed to the handler together, in one call.

Every hand-off is bounded and the bound is stated: reader → responder is a
channel of `MaxConcurrentStreams` batches that cannot block (a request
holds its stream slot until its response has left the writer); reader →
writer for control frames is bounded by `MaxPendingControlBytes` and the
reader blocks when full, which is the prescribed back-pressure; responder →
writer for buffered bodies is a queue bounded by the same stream slot — the
responder queues a batch and moves on to the next, so a stream waiting for
its window no longer holds the others, and the writer hands the batch's
buffer back once each of its answers is written, dropped or expired. A
streamed body is still pulled by the responder, at the socket's pace, once
its head is out. Flow control waits instead of refusing, bounded by
`WriteTimeout` read as a time without progress (`WriteProgressBytes`, 4 KiB
by default); scheduling is round-robin per byte-budget round, control
frames first; failures are classified (stream error resets one stream,
connection error is reserved for framing and HPACK state); closed streams
are remembered so late `DATA` is tolerated and still counted; the idle
clock does not run while answers are owed; the connection receive window
starts at 1 MiB.

## Rationale

Streamed responses of any size, a connection that stays responsive, one
request's failure costing one request, and cancellation that reaches the
handler. The cost is three goroutines per connection plus one per call in
flight, and their synchronisation, which
`TestH2ConnectionsLeaveNoGoroutines` keeps honest. The calls were serialised
per connection until 2026-10-19: a handler that waits on something per
call — a gateway's window, a round trip — made a stream that arrived during
a call wait twice, for that call and then for its own, where HTTP/1.1
carries the same request on a connection of its own and waits once.
Measured on the comparison study's unit scenario (a 2 ms gateway window,
8,000 requests per second, 256 and 1,024 connections, three paired
rounds), the HTTP/2 p99 went from 1.35-1.63 times the HTTP/1.1 figure to
0.96-1.06, and the p50 from 1.12-1.22 to 0.96-1.00. Merging the batches
queued behind a busy call, without parallel calls, changed nothing there.
Starting the goroutine costs 2.3% of a request answered alone
(`BenchmarkH2FromNetHTTP/plain` on one core, about half a microsecond);
reusing call goroutines is the known way down if a handler that waits on
nothing ever makes that the larger cost. HTTP/3 had the same serialisation
and lost it the same day: at the same point its p99 went from 1.48-2.32
times the HTTP/1.1 figure to 1.13-1.57, and its p50 from 1.19-1.35 to
1.07-1.18; what remains of the p99 gap no longer comes from the calls. The
goroutine costs 1.7% of a request answered alone
(`BenchmarkH3FromQuicGo/plain`, median of twelve paired rounds, within the
noise of a sign test).

Deliberately not built: stream priorities (RFC 9113 deprecated the scheme),
server push (`ENABLE_PUSH` 0), and the interleaving of two streamed bodies
on one HTTP/2 connection — while a producer is pulled, the responder frames
no other answer, so other requests on that connection wait for theirs. Accepted because the
alternative is a goroutine or a chunk copy per streamed response; a client
with an event feed opens a second connection or uses HTTP/3. A consumer
with an SSE workload on HTTP/2 is the signal to revisit, with per-stream
writers as the known fix. A general write queue with its own memory bound
was rejected: blocking hand-offs give exact back-pressure with no copy and
no policy for a full queue.
