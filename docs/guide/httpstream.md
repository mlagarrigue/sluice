# Native HTTP: `net/httpstream` and `pushdown`

Two packages that left the experimental label, and what each is held to:
`net/httpstream`, the native request path — HTTP/1.1, HTTP/2 and HTTP/3
read as a stream of batches of requests — and `pushdown`, through which a
reader tells its source what it no longer needs. The deployments they
serve have [their own page](deployments.md); the stages that route,
authenticate and decode their requests, `web/stream`, are still
[experimental](experimental.md#webstream--the-same-middle-for-the-other-transport).

## `net/httpstream` — supported

net/httpstream is supported — outside the experimental label and
under the compatibility promise — and fuzzed in CI without
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

> **In plain words.** On an HTTP/1.1 connection where several requests
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
A request body is **streamed** by default (`Config.BodyDelivery`): a body
already whole in the read that brought the headers, under `MaxBodyBytes`,
arrives in `Request.Body`; any other arrives through `Request.Stream`, at
the pace the handler reads it, bounded by `MaxStreamedBodyBytes`.
`BodyWhole` hands every body over whole. Responses can be **streamed**
too: `Response.Stream` lets
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
| `ReadTimeout` | 10 s | duration | all | the arrival of a whole request, and of the first byte; each `ReadProgressBytes` of a streamed body |
| `WriteTimeout` | 10 s | duration | all | a response write without progress |
| `WriteProgressBytes` | 4 KiB | bytes | all | the progress that restarts `WriteTimeout` |
| `ReadProgressBytes` | 4 KiB | bytes | all | the progress of a streamed body that restarts `ReadTimeout` |
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

> **In plain words.** `Shutdown` does not wait for idle connections: it
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

> **In plain words.** What is pushed down is the limit and the key bound;
> the column set is published but not acted on, because the SQL is yours
> and the connector does not rewrite it.

