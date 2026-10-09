# Experimental

Four packages carry the **experimental** label, in the first sentence of
their documentation: `net/httpstream`, `net/quic`, `web/stream` and
`pushdown`. This page first says what the word means here, then what each
one does and what not to expect from it.

## What "experimental" means

**What is promised.** The code exists. It is tested with Go's data race
detector (`-race`). It is measured by the same benchmarks as the rest. And
what it claims holds end to end: `example/vertical` takes a real request
from the received byte all the way to PostgreSQL and back, counting its own
round trips.

**What is not promised.** That names and signatures stay the same from one
version to the next. Production use. For QUIC, **interoperability**:
everything is checked against itself, against the examples published in
the standard (RFC 9001) and against a test harness that loses, shuffles and
forges packets — which is serious, but not the same as having talked to a
server written by someone else. A review by security specialists. Fuzzing
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

## `net/httpstream` — HTTP/1.1, /2 and /3 as stream stages

HTTP exists in three versions. HTTP/1.1 sends requests one at a time over
a connection (or several in a row, "pipelined"). HTTP/2 **multiplexes**:
several requests travel interleaved over the same connection. HTTP/3 does
the same over QUIC, a newer protocol described below. In `httpstream`, all
three produce the same thing: **batches of requests**, and one handler type
serves them without knowing which carried them.

```go
handle := func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    out := make([]httpstream.Response, 0, b.Len())
    for _, r := range b.Items {
        out = append(out, httpstream.Response{Status: 200, Body: append([]byte("you asked for "), r.Target...)})
    }
    return sluice.Batch[httpstream.Response]{Items: out} // one response per request, in order
}

httpstream.Serve(ctx, ln, httpstream.Config{IdleTimeout: time.Second}, handle) // HTTP/1.1 on a net.Listener
httpstream.ServeH2(ctx, conn, httpstream.H2Config{}, handle)                    // HTTP/2 on a connection
httpstream.ServeH3(ctx, quicConn, httpstream.Config{}, handle)                  // HTTP/3 on a *quic.Conn
```

> **In plain terms.** On an HTTP/1.1 connection where several requests
> follow each other, nothing carries a number: the i-th response answers
> the i-th request, and that is all. Hence "one response per request, in
> order". HTTP/2 and HTTP/3 number their streams, and the handler does not
> need to know.

`Config` bounds everything a client controls — length of the request line,
of the headers, of the body, number of requests per batch and per
connection, timeouts — and has no usable zero value: you write the limits.
Request bodies are bounded and read whole before being handed over.
Responses, on the other hand, can be **streamed**: `Response.Stream` lets
your code supply batches of bytes that the transport pulls at the client's
pace; if the client leaves, your producer sees it (`yield` returns `false`)
and stops.

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
conn, err := quic.Dial(pc, addr, tlsConfig, quic.DefaultParameters())                     // client side
ln, err := quic.NewListener(pc, tlsConfig, quic.DefaultParameters(), quic.ListenerConfig{}) // server side
conn, err := ln.Accept()
stats := conn.Stats() // discarded packets, write failures, key-rotation suspects
```

The **transport parameters** are the limits each end announces to the
other (how much data, how many streams, what idle timeout). Start from
`DefaultParameters()`; the zero value is refused, because it would announce
a connection with no credit at all, unable to send anything.

> **In plain terms: the one dependency.** QUIC's encryption needs an
> algorithm (ChaCha20-Poly1305) that `crypto/tls` uses but does not expose.
> It is for this that the project depends on `golang.org/x/crypto`, its only
> module outside the standard library.

What is missing is stated: **key update** (RFC 9001 §6, changing the
encryption keys mid-connection) is not implemented; a peer that does it
shows up in `Stats().KeyPhaseSuspects` and its connection eventually times
out.

## Supported deployments

The two packages make deployment choices, and each one is carried at the
place in the code that decides it. This section gathers them: what is
supported as it is, what is only supported behind something else, what is
not supported yet.

| Situation | Supported? | Where it is decided |
|---|---|---|
| HTTP/1.1 exposed directly, with no proxy in front | Yes, by construction: parsing refuses anything two hops could read differently (obsolete line folding, a space before the colon, control characters, `Transfer-Encoding` next to `Content-Length`, absolute-form targets). A proxy that re-splits chunks is not required. | `net/httpstream/request.go`, `splitRequestLine` and `parseHeaders` |
| HTTP/1.1 or HTTP/2 in clear text on a bare TCP socket | Kept for interoperability (terminating proxy, health probe, local tooling), **not for a network boundary**: whatever crosses a network goes behind TLS or over HTTP/3, whose transport encrypts by construction. | `net/httpstream/serve.go`, godoc of `Serve`; `h2conn.go`, godoc of `ServeH2` |
| HTTP/2 clients with a reduced HPACK table (nghttp2, Envoy) | Yes: the table size requested by `SETTINGS_HEADER_TABLE_SIZE` is honored from the next header block, in the same critical section as the acknowledgement. | `net/httpstream/h2write.go`, `ackSettings` |
| QUIC clients behind a mobile NAT (address change in the middle of a connection) | Yes: four active connection IDs per endpoint, enough for a NAT rebind and a deliberate rotation to overlap; the new path is validated by `PATH_CHALLENGE` before it is believed. | `net/quic/migration.go`, `maxLocalActiveCIDs` and `migrateLocked` |
| QUIC server on a host with several addresses | **Linux only**: `net/quic/udp` learns which address a datagram arrived on and answers from it. Elsewhere, `EnablePacketInfo` returns `errors.ErrUnsupported` and the kernel picks the source by route: the client sees a reply come from an address it never wrote to, which for QUIC is a different path. Bind the socket to one specific address, or stay on Linux. | `net/quic/udp/udp.go`, `EnablePacketInfo` |

Two behaviors of `ServeH3` complete the table without fitting in it: manual
stream credit only turns on if the announced windows cover the admission
bounds, and otherwise falls back to the transport's automatic credit,
bounded by the ceilings of `Config` (`net/httpstream/h3serve.go`, at the
top of `ServeH3`); `Config.TransportParameters` produces accepted windows.
And everything that "[What "experimental"
means](#what-experimental-means)" says applies to every row: the table
says what the code decides, not what an external review has confirmed.

> **In plain terms.** "Supported" means here: the code makes this choice
> explicitly, a test covers it, and the row says where to read the reason.
> It takes nothing away from the *experimental* label of the two packages.

## `web/stream` — the same middle, for the other transport

`web` provides the middle of the supported path (route, authenticate,
authorize, decode, answer) for `net/http`. `web/stream` provides the **same
middle**, as batch stages, for `net/httpstream`'s requests, with the same
vocabulary (`web.Verifier`, `web.Authorizer`, `web.Report`) — so that the
two paths do not drift apart. What the stages learn (the route, the
principal, a refusal) travels in the element itself, an `Exchange` that
wraps the request.

```go
rt, _ := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
handle := func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    ex := stream.Exchanges(nil, b)       // one envelope per request
    rt.Route(ex)                         // which route for each
    stream.Authenticate(verifier, ex)    // who is calling
    stream.Authorize(authorizer, ex)     // what they may do
    for i := range ex {
        order, _ := stream.PathInt64(&ex[i], 0, "order") // the route's first wildcard
        // the business stage, one batch at a time
    }
    return stream.Render(ex, func(i int) (int, any) { return 200, results[i] })
}
```

## `pushdown` — saying better than "stop"

A reader that knows where it is can help its source: "skip to key X", "I
only need a hundred more". That is what database engines call *pushing* a
condition down to the source (*pushdown*). `Demand` carries that
information; the source reads it once per batch (one atomic read, too
little to measure), and downstream operators **tighten** it — never the
reverse, because a source that has already skipped a thousand rows cannot
go back.

```go
var d pushdown.Demand                      // the zero value means "everything"
rows := scan(&d)                           // a source that consults the demand
rows = pushdown.AdvanceBy(rows, &d, keyOf) // publishes the key reached
```

Measured on the case it exists for — a reader advancing in key order, a
source skipping whole pages — ×6.9.

> **In plain terms.** No source in the library listens to a `Demand` yet:
> the PostgreSQL connector does not. It is a ready and measured mechanism,
> not a feature you can use today without writing the source yourself.
> That is why it is experimental.

## The end-to-end example

[`example/vertical`](../../example/vertical) runs the whole architecture on
this path: an HTTP request authenticated, authorized, batched by principal,
decided by one business rule, read from PostgreSQL under row-level
security, and answered with its problems and their remedies. Every arrow
of the journey is a stream stage, and the example counts its round trips.
Its twin on the supported path is `example/orders`.
