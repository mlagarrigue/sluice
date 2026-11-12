# Native HTTP: `net/httpstream`, `web/stream` and `pushdown`

Three packages under the compatibility promise, and what each is held to:
`net/httpstream`, the native request path — HTTP/1.1, HTTP/2 and HTTP/3
read as a stream of batches of requests —; `web/stream`, the stages that
route, authenticate and decode its requests; and `pushdown`, through which
a reader tells its source what it no longer needs. The deployments they
serve have [their own page](deployments.md).

## `net/httpstream` — supported

HTTP exists in three versions. HTTP/1.1 sends requests one at a time over
a connection (or several in a row, "pipelined"). HTTP/2 **multiplexes**:
several requests travel interleaved over the same connection. HTTP/3 does
the same over QUIC, a newer protocol the
[experimental guide](experimental.md#netquic--a-recent-transport-reimplemented)
describes. In `httpstream`, all
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

The path of one batch, from the bytes a client sends to the bytes it gets
back:

```
client ── bytes ──▶ reader: parses and bounds each request (Config)
                        │
                        ▼
                    batch of requests   [r1 r2 r3]   (MaxRequestsPerBatch at most, on HTTP/1.1)
                        │
                        ▼
                    handle(ctx, batch) — your code, once per batch
                        │
                        ▼
                    batch of responses  [s1 s2 s3]   (one per request, in order)
                        │
                        ▼
client ◀── bytes ── writer: frames each response for its version
```

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
`BodyWhole` hands every body over whole.

Responses can be **streamed** too: `Response.Stream` lets
your code supply batches of bytes that the transport pulls at the client's
pace; if the client leaves, your producer sees it (`yield` returns `false`)
and stops.

Over HTTP/2 a streamed response holds no other response of its connection.
The buffered responses of one batch are framed at once, and each streamed
one is pulled on a goroutine of its own once its head is out: a producer
that takes its time, or a client that reads it slowly, delays nobody else —
a second streamed response or a buffered one, of the same batch or a later
one. The writer interleaves their frames on the wire. What a client loses is
an order: two streamed responses of one batch no longer arrive one after
the other. `scripts/interop-http.sh` checks the two-stream case against
curl; the decision record on the multiplexed connection
([ADR 0008](../design/adr/0008-multiplexed-connection.md)) records what the
goroutine costs: 59% more on a streamed response's write side measured
alone (no socket, one core, 2026-11-12), and nothing measurable from a
real client
([benchmarks](../benchmarks.md#a-streamed-http2-response-on-a-goroutine-of-its-own)).

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

### What it is held to

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

Two readings of the texts are chosen. In HTTP/2, the values of a SETTINGS
frame are all checked in order, then only the last value of each parameter
is applied (RFC 9113 §6.5.3 asks for them one at a time): the peer sees the
same result, and a frame full of repetitions does not cost one operation
per repetition. In HTTP/3, only `http` and `https` are taken as schemes
with a mandatory authority: a request of another scheme that carries
`:authority` is delivered as it is (RFC 9114 §4.3.1 forbids it to the
sender; the server does not know the other schemes).

### `net/httpstream` settings

Each field of `Config` left at zero takes a default, and a negative value
is refused, with the field's name, before anything is read. The thirty
settings, their zero values, bounds and the protocols they act on:
[reference](../reference/httpstream.md#nethttpstream-settings).

Over HTTP/3 the windows are the transport's: `Config.QUICConfig()`
derives them from these settings — bodies, headers, streams, idle timeout — for
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
go srv.ServeQUIC(quicLn) // HTTP/3, experimental

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

## `web/stream` — supported

`web` provides the middle of the supported path (route, authenticate,
authorize, decode, answer) for `net/http`. `web/stream` provides the **same
middle**, as batch stages, for `net/httpstream`'s requests, with the same
vocabulary (`web.Verifier`, `web.Authorizer`, `web.Report`) — so that the
two paths do not drift apart. What the stages learn (the route, the
principal, a refusal) travels in the element itself, an `Exchange` that
wraps the request. Why these stages ship beside the core rather than in
it is [ADR 0011](../design/adr/0011-web-stream-beside-core.md).

What it is held to: a first real consumer, an application outside this
repository, serves its requests through these stages, and what it needs
is in them: a streamed body reaches `DecodeJSON`, a late element in a
window meets a policy rather than a panic, and the two dead ends it met
are signposted. Its figures against third-party clients — HTTP/1.1,
HTTP/2, HTTP/3 — are in [the benchmarks](../benchmarks.md). It is under
the compatibility promise ([ADR 0020](../design/adr/0020-compatibility.md)):
until v1 a break is
announced with `!` as everywhere; from v1 `scripts/check-api.sh` refuses
it, `!` or not. Three symbols stay outside the promise, each documented
with a paragraph that opens on "Experimental:": the HTTP/3 entry points
`ServeH3Conn` and `Server.ServeQUIC` (experimental, like the `net/quic`
they run on; [ADR 0021](../design/adr/0021-net-quic-stays.md)), and
`Config.QUICConfig`.

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

## The end-to-end example

[`example/vertical`](../../example/vertical) runs the whole architecture on
this path: an HTTP request authenticated, authorized, batched by principal,
decided by one business rule, read from PostgreSQL under row-level
security, and answered with its problems and their remedies. Every arrow
of the journey is a stream stage, and the example counts its round trips.
`go test ./example/vertical -run TestVerticalEndToEnd -v` runs it, against the
PostgreSQL the `SLUICE_PG`, `SLUICE_PG_USER`, `SLUICE_PG_DB` and
`SLUICE_PG_PASSWORD` [variables](postgres.md#running-the-examples-against-a-server)
name; without them it skips. Its twin on the
supported path is `example/orders`.

## You can now

- serve HTTP/1.1, HTTP/2 and HTTP/3 with one handler that receives the
  requests that arrived together as one batch;
- bound everything a client controls through `httpstream.Config`, and
  stream a request body or a response at the client's pace;
- push a limit and a key bound down to the PostgreSQL query that feeds a
  response.

Try it: serve the handler above with `Serve` on `localhost:8080`, then ask
`curl http://localhost:8080/a` and
`curl --http2-prior-knowledge http://localhost:8080/a` — the same handler
answers both versions, the connection's first bytes choosing the protocol.

The examples of the three packages run as tests, their output checked:

```sh
go test -run Example -v ./net/httpstream/ ./web/stream/ ./pushdown/
```
