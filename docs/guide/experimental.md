# Experimental

Three packages carry the **experimental** label, in the first sentence of
their documentation: `net/quic` and its helper `net/quic/udp`, and
`web/stream`. This page first says what the word means here, then what
each one does and what not to expect from it, and ends with the two
supported packages this page remains the guide of, and what each is held
to. The deployments they support have [their own page](deployments.md).

## What "experimental" means

**What is promised.** The code exists. It is tested with Go's data race
detector (`-race`). It is measured by the same benchmarks as the rest. And
what it claims holds end to end: `example/vertical` takes a real request
from the received byte all the way to PostgreSQL and back, counting its own
round trips.

**What is not promised.** That names and signatures stay the same from one
version to the next. Production use. For QUIC, **interoperability** beyond
two peers: everything is checked against itself, against the examples
published in the standard (RFC 9001), against a test harness that loses,
shuffles and forges packets, and against two implementations written by
someone else (quic-go and ngtcp2, in the QUIC Interop Runner, run by hand:
the last results are in the run log) — which is serious, but two peers are
not every peer. A
review by security specialists. Fuzzing
(sending masses of random inputs to find flaws) against a public corpus.

> **In plain terms: then why do these packages exist?** A **parser** (the
> code that reads bytes from the network and makes a request of them) is
> the most dangerous place in a server: it is where hostile input can do
> damage. Sluice.go's architecture refuses to write one, and `web` over
> `net/http` remains the supported path. But the library's thesis — "the
> batch is the unit of transport, from the byte to the answer" — can only
> be *verified* if reading the bytes is itself a stream stage. These
> packages exist to prove that it is feasible and to measure it, not to be
> deployed as they are. Put them behind something you trust, or do not
> expose them.

## `net/quic` — a recent transport, reimplemented

**QUIC** is the protocol HTTP/3 runs on. Unlike TCP, it runs over **UDP**
(**datagrams**: independent packets with no prior connection) and does
itself what TCP used to do: establish a connection, encrypt (with TLS 1.3,
built in), acknowledge receipt, retransmit what was lost, regulate the
rate, and carry **several streams** at once without one lost packet
blocking the others. Go does not provide QUIC in its standard library;
`net/quic` is an implementation of it.

Why it is here: a QUIC datagram carries pieces of several streams at once.
One read on the socket is therefore **a batch by construction**, without
asking anything of anyone — the library's thesis, verified at the transport
level.

```go
client, err := quic.Dial(ctx, pc, addr, tlsConfig, quic.DefaultParameters())               // client side
ln, err := quic.NewListener(pc, tlsConfig, quic.DefaultParameters(), quic.ListenerConfig{}) // server side
server, err := ln.Accept()
stats := server.Stats() // counters, RTT, congestion window, bytes in flight, losses
```

The **transport parameters** are the limits each end announces to the
other (how much data, how many streams, what idle timeout). Start from
`DefaultParameters()`; the zero value is refused, because it would announce
a connection with no credit at all, unable to send anything. What a server
must do beyond the handshake — route datagrams by connection ID, validate
addresses against amplification, follow a client whose NAT rebinds — is
[ADR 0015](../design/adr/0015-quic-server.md).

> **In plain terms: the dependency QUIC needs.** QUIC's encryption needs an
> algorithm (ChaCha20-Poly1305) that `crypto/tls` uses but does not expose.
> It is for this that the project depends on `golang.org/x/crypto`, the only
> module outside the standard library that QUIC uses.

`net/quic/udp` is its helper, experimental with it: it opens the UDP socket
with the options a QUIC endpoint needs (no IP fragmentation, one IPv6 flow
label per path) and, on Linux, lets a server on a host with several
addresses answer from the address a datagram reached.

**Key update** (RFC 9001 §6, changing the encryption keys mid-connection)
is handled both ways: a peer's rotation is followed, and this end rotates
its own keys before the cipher's packet budget runs out, so a long
connection no longer dies of its packet count. `Stats().KeyUpdates`
counts the rotations.

### `net/quic` settings

`DefaultParameters()` fills in the "Default" column. The fields marked
*local* never go on the wire: they set what this end does, not what it
announces.

| Field | Default | Unit, bound | What it bounds |
|---|---|---|---|
| `MaxIdleTimeout` | 30 s | duration; zero is no timeout, which this package never announces of its own | a connection nobody is using |
| `MaxUDPPayloadSize` | 1452 | bytes; 1200 to 65527, otherwise refused | the largest datagram read, and what the peer probes for |
| `InitialMaxData` | 1 MiB | bytes | everything the peer may send, all streams together |
| `InitialMaxStreamDataBidiLocal` | 256 KiB | bytes | one bidirectional stream this end opened |
| `InitialMaxStreamDataBidiRemote` | 256 KiB | bytes | one bidirectional stream the peer opened — an HTTP/3 request |
| `InitialMaxStreamDataUni` | 64 KiB | bytes | one unidirectional stream of the peer |
| `InitialMaxStreamsBidi` | 100 | streams | how many bidirectional streams the peer may open (RFC 9114 §6.1 asks for 100) |
| `InitialMaxStreamsUni` | 8 | streams | how many unidirectional streams |
| `AckDelayExponent` | — | exponent; *read from the peer*, never announced (this end uses 3) | the scale of the peer's ACK delays |
| `MaxAckDelay` | — | duration; *read from the peer*, never announced (this end uses 25 ms) | how long the peer may sit on an acknowledgement |
| `ActiveConnIDLimit` | 4 | identifiers; 0 or 1 become 4 | the peer's connection IDs kept active, and the ones this end issues |
| `DisableActiveMigration` | false | boolean | announces that this end does not follow a peer that changes address |
| `Congestion` | NewReno | *local* | the congestion controller this end sends with ([below](#choosing-a-congestion-controller)) |
| `HoldClosing` | false | *local* | whether a connection from `Dial` holds the closing states of RFC 9000 §10.2 |
| `MaxDatagramSize` | 1452 | bytes, *local*; 1200 to 65527, otherwise refused | the largest datagram sent, once the path is probed |

### Choosing a congestion controller

An advanced setting: it asks you to know your network path — round trip,
loss, how deep the routers' buffers are — and to measure with your own
traffic. Without that, leave it at zero.

> **In plain terms.** **Congestion control** decides how fast this end
> sends: too slow and the line stays half empty; too fast and the routers'
> buffers fill, latency climbs and packets are lost. The choice is local:
> it is never announced to the peer, which sends with its own (RFC 9002
> §7).

Three controllers, chosen by `TransportParameters.Congestion`:

```go
p := quic.DefaultParameters()
p.Congestion = quic.CongestionConfig{Algorithm: quic.CongestionCubic} // or quic.CongestionBBR
```

- **NewReno** (RFC 9002 §7), the zero value and the default: the window
  grows by one datagram per round trip and halves on each loss episode.
  Every measurement of this package is made with it, and it stays the
  default until another has been measured against a real client.
- **CUBIC** (RFC 9438, `CongestionCubic`): after a loss the window drops
  to seven tenths rather than half, and grows back along a cubic curve. It
  wins on long paths that lose: 1.7 to 2.3 times faster than NewReno on the
  emulated 80 ms path with a shallow buffer, 1.24 times with 1% loss, 1.59
  times with burst loss; it is within 2.5% of NewReno on the clean and
  bottleneck profiles, and it fills a deep buffer exactly as NewReno does,
  because loss is all it reads. Two fine settings: `SlowStart` (HyStart++
  by default for CUBIC, RFC 9406 — the initial slow start ends on the
  round trip rising, with fewer losses and a somewhat fuller queue on a
  deep buffer) and `DisableFastConvergence`, for a flow alone on its path.
- **BBR** (draft-ietf-ccwg-bbr-06, what Linux ships as BBRv3,
  `CongestionBBR`): a model of the path — the highest delivery rate and the
  lowest round trip seen recently — in which loss is evidence of how much
  the path holds. 16 times NewReno with 1% loss, about 3 times with burst
  loss; on a deep buffer the queue stays at 1.8 times the path's round
  trip, against 5.9 under NewReno, at the same goodput. Against a NewReno
  flow it takes 0.56 of the line. Its startup has its own exit and ignores
  `SlowStart`.

The last two are **experimental**: measured on this repository's emulated
paths only ([QUIC, on an emulated path](../benchmarks.md#quic-on-an-emulated-path),
with the command that replays the comparison). To judge on yours,
`Conn.Stats()` gives the controller in use (`Algorithm`), its window
(`CongestionWindow`), the losses (`LostPackets`) and the round trip
(`SmoothedRTT` against `MinRTT`: the gap is the queue the flow keeps).

## Deployments

Which deployments the network packages are written for — HTTP/1.1 exposed
directly, behind nginx or Envoy, TLS in front, QUIC behind a mobile NAT,
0-RTT — and what to set for each is on a page of its own:
[Deployments](deployments.md).

## `web/stream` — the same middle, for the other transport

`web` provides the middle of the supported path (route, authenticate,
authorize, decode, answer) for `net/http`. `web/stream` provides the **same
middle**, as batch stages, for `net/httpstream`'s requests, with the same
vocabulary (`web.Verifier`, `web.Authorizer`, `web.Report`) — so that the
two paths do not drift apart. What the stages learn (the route, the
principal, a refusal) travels in the element itself, an `Exchange` that
wraps the request. Why these stages ship beside the core rather than in
it is [ADR 0011](../design/adr/0011-web-stream-beside-core.md). It keeps
the label alone: its transport has left, but
no real consumer has yet built its request path on these stages. That
confrontation is what it lacks, not a measurement.

```go
rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
handle := func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    ex := stream.Exchanges(nil, b)              // one envelope per request
    rt.Route(ex)                                // which route for each
    stream.Authenticate(ex, verifier, "orders") // who is calling; a 401 names the realm "orders"
    stream.Authorize(ex, authorizer)            // what they may do
    for i := range ex {
        order, _ := stream.PathInt64(&ex[i], 0, "order") // the route's first wildcard
        // the business stage, one batch at a time
    }
    return stream.Render(ex, func(i int) (int, any) { return 200, results[i] })
}
```

The router is a table compiled once: first match wins in table order,
`{name}` matches one segment, and nothing is percent-decoded
([ADR 0010](../design/adr/0010-routing-table.md)).

## `net/httpstream` — supported

net/httpstream is supported — out of the experimental label since
2026-10-10 and under the compatibility promise — and fuzzed in CI without
a public corpus; it has neither run in production nor had an outside
security review.

What it is held to, in continuous integration: HTTP/1.1 and /2 face curl,
h2load (10,000 requests over ten connections) and h2spec
(`scripts/interop-http.sh`), HTTP/3 quic-go's client (the `interop/`
module), curl, aioquic's client and h3spec (`scripts/interop-http3.sh`,
whose expected h3spec failures are listed with their reasons in
`net/httpstream/testdata/h3spec-exclusions.txt`); every week it also
faces nginx, HAProxy and Envoy in front of it (`scripts/interop-proxy.sh`);
the parsers are fuzzed in CI; the limit on streamed HTTP/2
responses is owned below, and their per-batch cost is measured from a real
client.

HTTP exists in three versions. HTTP/1.1 sends requests one at a time over
a connection (or several in a row, "pipelined"). HTTP/2 **multiplexes**:
several requests travel interleaved over the same connection. HTTP/3 does
the same over QUIC, a newer protocol described above. In `httpstream`, all
three produce the same thing: **batches of requests**, and one handler type
serves them without knowing which carried them.

```go
handle := func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    // ctx is cancelled if the server stops by force: hand it to what you call
    out := make([]httpstream.Response, 0, b.Len())
    for _, r := range b.Items {
        out = append(out, httpstream.Response{Status: 200, Body: append([]byte("you asked for "), r.Target...)})
    }
    return sluice.Batch[httpstream.Response]{Items: out} // one response per request, in order
}

httpstream.Serve(ctx, ln, httpstream.Config{IdleTimeout: time.Second}, handle) // HTTP/1.1 on a net.Listener
httpstream.ServeH2Conn(ctx, conn, httpstream.Config{}, handle)                      // HTTP/2 on a connection
httpstream.ServeH3Conn(ctx, quicConn, httpstream.Config{}, handle)                  // HTTP/3 on a *quic.Conn
```

> **In plain terms.** On an HTTP/1.1 connection where several requests
> follow each other, nothing carries a number: the i-th response answers
> the i-th request, and that is all. Hence "one response per request, in
> order". HTTP/2 and HTTP/3 number their streams, and the handler does not
> need to know.

`Config` bounds everything a client controls — length of the request line,
of the headers, of the body, number of requests per batch and per
connection, timeouts — and each field left at zero takes the default its
documentation states (the table below): `httpstream.Config{}` is a bounded
server. Two fields are the exception, `MaxConns` and `MaxConnsPerIP`, whose zero means "no
bound": they are the deployer's memory budget and one client's share of it,
and a server facing the public sets them
([ADR 0018](../design/adr/0018-zero-values-and-mandatory-bounds.md)).
Request bodies are bounded and read whole before being handed over.
Responses, on the other hand, can be **streamed**: `Response.Stream` lets
your code supply batches of bytes that the transport pulls at the client's
pace; if the client leaves, your producer sees it (`yield` returns `false`)
and stops.

One limit is owned rather than hidden, and it follows from the second
architecture rule (the batch is the unit of transport). Over HTTP/2 the
buffered responses of one batch are framed at once and interleave on the
wire. A **streamed** response is different: its producer is pulled by the
goroutine that answers the batch, one producer at a time. Two streamed
responses that arrived in the same batch therefore go out one after the
other, not interleaved, and the second starts when the first producer has
finished. The client receives both, whole and in request order; what it
loses is simultaneity, and a slow reader of the first delays the second.
Where that matters, give long-lived streamed responses a connection of
their own, or serve them over HTTP/3, where each stream has its own
transport. `scripts/interop-http.sh` checks the two-stream case against
curl; the decision record on the multiplexed connection
([ADR 0008](../design/adr/0008-multiplexed-connection.md)) records the
trade and what would reverse it.

The cost of a streamed batch over HTTP/2 is measured, not promised
(`docs/benchmarks.md`, "HTTP/2 and HTTP/3, from a real client"). The first
batch of a response leaves as soon as the producer yields it; those it
yields during that write are gathered and leave together on the next one,
END_STREAM on the last. Seen from a real client over loopback, three batches
of eight bytes cost about 1.6 times a response yielded whole, and sixteen
batches of 4 KiB exceed 120 MB/s. What remains per batch is the price of not
holding back the first one: a producer that yields tiny batches at a high
rate pays one write per round of the writer and one wake-up of the reader on
the client side per DATA frame. Size your batches accordingly — a bigger
batch divides that cost by as much — and remember that
`Config.MaxWriteBufferBytes` bounds how far ahead of the socket a
producer may be, per stream.

### `net/httpstream` settings

Each field left at zero takes the value of the "Zero" column; a negative
value is refused, with the field's name, by `Config.Validate`, `Serve`, `Server`,
`ServeH2Conn`, `ServeH3Conn` and `Config.TransportParameters`, before
anything is read. The "Protocols" column says where the field acts.

| Field | Zero | Unit, bound | Protocols | What it bounds |
|---|---|---|---|---|
| `MaxRequestLineBytes` | 8 KiB | bytes | 1.1 | method, target and version together |
| `MaxHeaderBytes` | 32 KiB | bytes | all | all the headers of one request; announced on 2 and 3 |
| `MaxHeaders` | 64 | fields | all | how many headers one request carries |
| `MaxBodyBytes` | 1 MiB | bytes | all | a body delivered whole; four times that per HTTP/2 or HTTP/3 connection |
| `BodyDelivery` | `BodyStreamed` | form | all | whether a body streams or is held whole (`BodyWhole`) |
| `MaxStreamedBodyBytes` | `MaxBodyBytes` | bytes | all | a streamed body; the bound alone, never the switch |
| `MaxRequestsPerBatch` | 64 | requests | 1.1 | the pipelined requests of one batch |
| `MaxConcurrentStreams` | 100 | streams; at most 4096 on 3 | 2, 3 | the requests open at once on one connection |
| `MaxRequestsPerConn` | 1024 | requests | 1.1 | the requests of one connection before it closes |
| `IdleTimeout` | 2 min | duration | all | the silence between two requests; on 3, the transport's max_idle_timeout |
| `ReadTimeout` | 10 s | duration | all | the arrival of a whole request, and of the first byte |
| `WriteTimeout` | 10 s | duration | all | a response write without progress |
| `WriteProgressBytes` | 4 KiB | bytes | all | the progress that restarts `WriteTimeout` |
| `MaxConns` | **no bound** | connections | 1.1, 2 | the connections open at once; also the memory bound |
| `MaxConnsPerIP` | **no bound** | connections | 1.1, 2 | the same bound per remote address |
| `IPv4PrefixBits` | 32 | bits; 1 to 32 | 1.1, 2 | the prefix `MaxConnsPerIP` counts IPv4 by |
| `IPv6PrefixBits` | 64 | bits; 1 to 128 | 1.1, 2 | the same for IPv6 |
| `ShutdownGrace` | 5 s | duration | all | the wait for requests in flight at a stop; `Server` ignores it |
| `KeepAlive` | nothing sent | duration | all | the silence before an HTTP/3 PING or a TCP probe |
| `Logger` | `slog.Default()` | — | all | where warnings go |
| `LingerTimeout` | 1 s, or `ReadTimeout` if shorter | duration | 1.1 | the read of a closing connection's tail |
| `MaxFrameSize` | 16384 | bytes; 16384 to 2^24−1 | 2 | one frame received or sent |
| `MaxFramesPerBatch` | 64 | frames | 2 | the frames of one batch |
| `MaxContinuationFrames` | 16 | frames | 2 | the CONTINUATION frames of one header block |
| `InitialWindowSize` | 65535 | bytes; at most 2^31−1 | 2 | the window of one request stream |
| `MaxPendingControlBytes` | 16 KiB | bytes | 2 | the control frames waiting to be written |
| `MaxWriteBufferBytes` | 64 KiB | bytes | 2 | what the writer gathers before one write |
| `QPACKStatic` | RFC 9204's table | — | 3 | for tests and interop only ([ADR 0013](../design/adr/0013-qpack-static-table.md)) |
| `EarlyData` | no request | predicate | all | the requests that may be acted on although they could be a replay ([ADR 0019](../design/adr/0019-quic-early-data.md)) |

Over HTTP/3 the windows are the transport's: `Config.TransportParameters()`
derives them from this table — bodies, headers, streams, idle timeout — for
the QUIC listener.

### A server that stops in two steps

`Serve` is the short form: cancelling its context drains, and
`ShutdownGrace` bounds the drain. `Server` is the production form
([ADR 0017](../design/adr/0017-graceful-shutdown.md)): a TCP listener and
a QUIC listener on the same server, `Shutdown` at the first signal, `Close`
at the second or when the wait runs out.

```go
srv := &httpstream.Server{Config: httpstream.Config{MaxConns: 10_000}, Handler: handle}
go srv.Serve(ln)         // HTTP/1.1 and HTTP/2; an error here is a failed listener
go srv.ServeQUIC(quicLn) // HTTP/3

sig, unsig := signal.NotifyContext(context.Background(), os.Interrupt)
<-sig.Done() // first signal
unsig()      // the second one interrupts the process
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := srv.Shutdown(ctx); err != nil { // nothing new accepted, requests in flight answered
    srv.Close() // the wait ran out: everything is cut, the handlers' context cancelled
}
```

> **In plain terms.** `Shutdown` does not wait for idle connections: it
> closes them at once. It waits for those with a request in flight, and
> each closes once its response is out. The context given to `Shutdown`
> is the only bound on that wait; `ShutdownGrace` is for `Serve`,
> `ServeH2Conn` and `ServeH3Conn` alone.

## `pushdown` — supported

A reader that knows where it is can help its source: "skip to key X", "I
only need a hundred more". That is what database engines call *pushing* a
condition down to the source (*pushdown*). `Demand` carries that
information; the source reads it once per batch (one atomic read, too
little to measure), and downstream operators **tighten** it — never the
reverse, because a source that has already skipped a thousand rows cannot
go back.

A source in the library reads it, which is what the package is held to: the
PostgreSQL connector takes a `Demand` in its `QueryConfig`. The limit
becomes the number of rows the server is asked for next; the lower bound is
bound to the key parameter the query names, and the query resumes above it
without an extra round trip.

```go
var d pushdown.Demand                      // the zero value means "everything"
src := conn.Query(ctx,
    "SELECT id, total FROM orders WHERE id > $1 ORDER BY id",
    []postgres.Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 0)}},
    postgres.QueryConfig{Demand: &d, KeyParam: 1})
rows := pushdown.AdvanceBy(src.Stream(), &d, keyOf) // publishes the key reached
```

Measured on the case it exists for — a reader advancing in key order, a
source skipping whole pages — ×6.9.

> **In plain terms.** What is pushed down is the limit and the key bound;
> the column set is published but not acted on, because the SQL is yours
> and the connector does not rewrite it.

## The end-to-end example

[`example/vertical`](../../example/vertical) runs the whole architecture on
this path: an HTTP request authenticated, authorized, batched by principal,
decided by one business rule, read from PostgreSQL under row-level
security, and answered with its problems and their remedies. Every arrow
of the journey is a stream stage, and the example counts its round trips.
`go test ./example/vertical -run TestVerticalEndToEnd -v` runs it, against the
PostgreSQL the `SLUICE_PG`, `SLUICE_PG_USER`, `SLUICE_PG_DB` and
`SLUICE_PG_PASSWORD` variables name; without them it skips. Its twin on the
supported path is `example/orders`.
