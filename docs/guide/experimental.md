# Experimental

Three packages carry the **experimental** label, in the first sentence of
their documentation: `net/quic` and its helper `net/quic/udp`, and
`web/stream`. This page first says what the word means here, then what
each one does and what not to expect from it, and ends with the packages
that left the list and what it took to leave.

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
conn, err := quic.Dial(ctx, pc, addr, tlsConfig, quic.DefaultParameters())                // client side
ln, err := quic.NewListener(pc, tlsConfig, quic.DefaultParameters(), quic.ListenerConfig{}) // server side
conn, err := ln.Accept()
stats := conn.Stats() // counters, RTT, congestion window, bytes in flight, losses
```

The **transport parameters** are the limits each end announces to the
other (how much data, how many streams, what idle timeout). Start from
`DefaultParameters()`; the zero value is refused, because it would announce
a connection with no credit at all, unable to send anything.

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

## Supported deployments

The network packages make deployment choices, and each one is carried at the
place in the code that decides it. This section gathers them: what is
supported as it is, what is only supported behind something else, what is
not supported yet.

| Situation | Supported? | Where it is decided |
|---|---|---|
| HTTP/1.1 exposed directly, with no proxy in front | Yes, by construction: parsing refuses anything two hops could read differently (obsolete line folding, a space before the colon, control characters, `Transfer-Encoding` next to `Content-Length`); an absolute-form target is served as the origin-form it holds, its authority taking the Host's place. A proxy that re-splits chunks is not required. | `net/httpstream/request.go`, `splitRequestLine` and `parseHeaders` |
| HTTP/1.1 or HTTP/2 in clear text on a bare TCP socket | Kept for interoperability (terminating proxy, health probe, local tooling), **not for a network boundary**: whatever crosses a network goes behind TLS or over HTTP/3, whose transport encrypts by construction. | `net/httpstream/serve.go`, godoc of `Serve`; `h2conn.go`, godoc of `ServeH2Conn` |
| HTTP/1.1 behind nginx or Envoy, upstream connections reused | Yes. The rule: the proxy's upstream idle timeout must be **shorter** than `IdleTimeout`, so that the proxy closes first instead of reusing a connection this server is closing and reporting a 502. nginx keeps its upstream connections 60 s by default, below the 2 min default of `IdleTimeout`: nothing to set as long as neither is raised or lowered (full configuration below the table). Envoy keeps them an hour: `idle_timeout: 60s` under `common_http_protocol_options` in the cluster's `HttpProtocolOptions`. Every response that keeps the connection does announce `Connection: keep-alive` and `Keep-Alive: timeout=<IdleTimeout in seconds>`, but neither proxy reads that value: nginx ignores the header, Envoy strips it. HTTP/2 announces the end with `GOAWAY` instead. | `net/httpstream/response.go`, `keepAliveField`; `httpstream.go`, `defaultIdleTimeout` |
| HTTP/2 clients with a reduced HPACK table (nghttp2, Envoy) | Yes: the table size requested by `SETTINGS_HEADER_TABLE_SIZE` is honored from the next header block, in the same critical section as the acknowledgement. | `net/httpstream/h2write.go`, `ackSettings` |
| HTTP/2 uploads on a link with latency | Yes, with a window to set: the default stream window of 65535 bytes is one round trip's worth, about 0.5 MB/s at 100 ms of RTT; `Config.InitialWindowSize` raises it, for `Serve`, `Server` and `ServeH2Conn` alike, and 1 MiB measured eleven times the default on a 4 MiB upload through a proxy adding 100 ms. The connection window follows it, so it never queues what the streams alone admit. | `net/httpstream/httpstream.go`, `Config.InitialWindowSize`; `h2rtt_test.go`, `TestH2UploadThroughputAtRTT` |
| QUIC clients behind a mobile NAT (address change in the middle of a connection) | Yes: four active connection IDs per endpoint by default (`TransportParameters.ActiveConnIDLimit`), enough for a NAT rebind and a deliberate rotation to overlap; the new path is validated by `PATH_CHALLENGE` before it is believed. | `net/quic/migration.go`, `issueLocalCIDsLocked` and `migrateLocked` |
| QUIC server on a host with several addresses | **Linux only**: `net/quic/udp` learns which address a datagram arrived on and answers from it. Elsewhere, `EnablePacketInfo` returns `errors.ErrUnsupported` and the kernel picks the source by route: the client sees a reply come from an address it never wrote to, which for QUIC is a different path. Bind the socket to one specific address, or stay on Linux. | `net/quic/udp/udp.go`, `EnablePacketInfo` |

Two behaviors of `ServeH3Conn` complete the table without fitting in it: manual
stream credit only turns on if the announced windows cover the admission
bounds, and otherwise falls back to the transport's automatic credit,
bounded by the ceilings of `Config` (`net/httpstream/h3serve.go`, at the
top of `ServeH3Conn`); `Config.TransportParameters` produces accepted windows.
And everything that "[What "experimental"
means](#what-experimental-means)" says applies to every row: the table
says what the code decides, not what an external review has confirmed.

> **In plain terms.** "Supported" means here: the code makes this choice
> explicitly, a test covers it, and the row says where to read the reason.
> It takes nothing away from the *experimental* label of `net/quic`.

The nginx row, in full: a configuration to copy as it is, where only the
server's address changes. An *upstream* is the group of servers nginx
relays to; `keepalive` there keeps connections open between two requests
instead of opening one per request.

```nginx
upstream app {
    server 10.0.0.1:8080;
    keepalive 16;           # connections kept per worker; on by default since nginx 1.29.7
    keepalive_timeout 60s;  # nginx's default, below IdleTimeout's 2 min
}

server {
    listen 80;

    location / {
        proxy_pass http://app;
        proxy_http_version 1.1;          # needed before nginx 1.29.7
        proxy_set_header Connection "";  # needed before nginx 1.29.7
    }
}
```

Before 1.29.7, nginx speaks HTTP/1.0 to the upstream and sends
`Connection: close` with every request: without the last two lines, every
request opens a connection and `keepalive` does nothing. Since 1.29.7 that
is the default, and the two lines no longer change anything. If you raise
`keepalive_timeout` above 2 min, raise `IdleTimeout` above it.

The "clear text" row points to TLS; this is what goes in front. The package
does not terminate TLS ([ADR 0014](../design/adr/0014-tls-stays-in-front.md)):
`crypto/tls` wraps the listener, and `Serve` reads the decrypted stream as
it would read a bare socket.

```go
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    MinVersion:   tls.VersionTLS12,
    CipherSuites: []uint16{ // TLS 1.2 only: TLS 1.3 picks its own, all AEAD
        tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
        tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
    },
    NextProtos: []string{"h2", "http/1.1"}, // ALPN: a browser opens HTTP/2 this way only
}
err := httpstream.Serve(ctx, tls.NewListener(ln, tlsConfig), httpstream.Config{}, handle)
```

TLS 1.3 alone would do (`MinVersion: tls.VersionTLS13`, no `CipherSuites`):
all of its suites are **AEAD** (encryption and authentication in one pass)
and Go picks them itself. The list only serves clients still on TLS 1.2, to
keep them off the suites RFC 9113 (Appendix A) prohibits for HTTP/2; the
compression and renegotiation it prohibits too, `crypto/tls` never does on
the server side. The package itself checks nothing of what was negotiated:
it reads neither the version, nor the suite, nor the ALPN, and the preface
picks the protocol, as in clear text. `scripts/interop-http.sh` replays
curl and h2spec over `https://` with exactly this configuration.

## `web/stream` — the same middle, for the other transport

`web` provides the middle of the supported path (route, authenticate,
authorize, decode, answer) for `net/http`. `web/stream` provides the **same
middle**, as batch stages, for `net/httpstream`'s requests, with the same
vocabulary (`web.Verifier`, `web.Authorizer`, `web.Report`) — so that the
two paths do not drift apart. What the stages learn (the route, the
principal, a refusal) travels in the element itself, an `Exchange` that
wraps the request. It keeps the label alone: its transport has left, but
no real consumer has yet built its request path on these stages. That
confrontation is what it lacks, not a measurement.

```go
rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
handle := func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    ex := stream.Exchanges(nil, b)       // one envelope per request
    rt.Route(ex)                         // which route for each
    stream.Authenticate(ex, verifier)    // who is calling
    stream.Authorize(ex, authorizer)     // what they may do
    for i := range ex {
        order, _ := stream.PathInt64(&ex[i], 0, "order") // the route's first wildcard
        // the business stage, one batch at a time
    }
    return stream.Render(ex, func(i int) (int, any) { return 200, results[i] })
}
```

## `net/httpstream` — no longer experimental

The package left the list the day its three conditions were met. HTTP/1.1
and /2 have faced curl and h2spec in continuous integration
(`scripts/interop-http.sh`), HTTP/3 quic-go's client (the `interop/`
module); the parsers are fuzzed in CI; the limit on streamed HTTP/2
responses is owned below, and their per-batch cost measured from a real
client, then halved.

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
documentation states: `httpstream.Config{}` is a bounded server. Two fields
are the exception, `MaxConns` and `MaxConnsPerIP`, whose zero means "no
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
curl; the decision record on the multiplexed connection records the trade
and what would reverse it.

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

## `pushdown` — no longer experimental

A reader that knows where it is can help its source: "skip to key X", "I
only need a hundred more". That is what database engines call *pushing* a
condition down to the source (*pushdown*). `Demand` carries that
information; the source reads it once per batch (one atomic read, too
little to measure), and downstream operators **tighten** it — never the
reverse, because a source that has already skipped a thousand rows cannot
go back.

The package left this list the day a source in the library read it: the
PostgreSQL connector takes a `Demand` in its `QueryConfig`. The limit
becomes the number of rows the server is asked for next; the lower bound is
bound to the key parameter the query names, and the query resumes above it
without an extra round trip.

```go
var d pushdown.Demand                      // the zero value means "everything"
src := conn.Query(ctx,
    "SELECT id, total FROM orders WHERE id > $1 ORDER BY id",
    []postgres.Param{{OID: postgres.OIDInt8, Value: postgres.AppendInt8(nil, 0)}},
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
Its twin on the supported path is `example/orders`.
