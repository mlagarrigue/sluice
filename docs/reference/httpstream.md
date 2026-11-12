# HTTP reference

The settings of `net/httpstream.Config`, each with what its zero value means
and on which protocol it acts; then how a connection is served, what HTTP/2
and HTTP/3 add, and what the HTTP/1.1 parser refuses. The
[HTTP guide](../guide/httpstream.md) says when to reach for them.

## `net/httpstream` settings

Each field left at zero takes the value of the "Zero" column; a negative
value is refused, with the field's name, by `Config.Validate`, `Serve`, `Server`,
`ServeH2Conn`, `ServeH3Conn` and `Config.QUICConfig`, before
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
| `MaxConcurrentStreams` | 100 | streams; at most 4096 | 2, 3 | the requests open at once on one connection |
| `MaxRequestsPerConn` | 1024 | requests | 1.1 | the requests of one connection before it closes |
| `IdleTimeout` | 2 min | duration | all | the time between two requests, pings not counted; on 3, also the transport's max_idle_timeout |
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

## One connection, three goroutines and a call per batch

A multiplexed connection is a reader, a responder and a writer. The reader
parses frames, owns the receive side and hands each batch to one `Handler`
call on a goroutine of its own — one call per batch, not per request, so a
batch that arrived together is still served together, and the calls in
flight are bounded by `MaxConcurrentStreams`; the responder frames what the
calls return; the writer owns the socket's write side, the send windows and
the scheduling. [ADR 0008](../design/adr/0008-multiplexed-connection.md)
records the bounds each hand-off has and what was deliberately left out.

A response larger than the window the peer granted waits for credit rather
than failing the connection, and the connection stays answerable while a
handler runs: PING is answered, SETTINGS acknowledged, and a peer's
RST_STREAM reaches the producer still working for it. A failure is
classified rather than escalated: a malformed request, a per-stream
flow-control violation, a body past `Config.MaxBodyBytes`, a handler that
panics, a `Response.Stream` producer that panics partway through — each
costs its own stream and nothing else. GOAWAY is kept for what cannot be
resynchronised once in doubt: framing and HPACK state.

HTTP/1.1 and HTTP/2 pull a producer on the goroutine that serves the
connection, so `Serve` and `ServeH2Conn` wait for a producer blocked in its
own code, however long it stays there — releasing one would take a context
the producer's signature does not carry. `ServeH3Conn` pulls each on its
own stream's goroutine and returns once the connection has ended. A
producer learns that the connection is gone, or its stream reset, by its
yield returning false on its next batch; that is the only signal.

## HTTP/2 and HTTP/3

`Serve` chooses between HTTP/1.1 and prior-knowledge HTTP/2 by what the
client sends first — the HTTP/2 preface can begin no HTTP/1.1 request, so
one listener serves both — or after ALPN behind a TLS listener
([ADR 0014](../design/adr/0014-tls-stays-in-front.md)). `ServeH2Conn` applies the rules
RFC 9113 §8.3 states — pseudo-headers first and unrepeated, lowercase field
names, no connection-specific fields, TE only "trailers", origin-form
`:path` — which exist because an HTTP/2 request reaching an HTTP/1.1 hop
unchecked is how a smuggled request is written. It answers PING,
acknowledges SETTINGS, grants flow-control windows as it consumes DATA,
bounds concurrent streams, counts RST_STREAM against the rapid-reset bound
(CVE-2023-44487), and tracks a header block across CONTINUATION with the
count and total bounded (CVE-2024-27316). Trailers are delivered in
`Request.Trailers`, whichever way the body is, behind a body that has
ended; on a stream already ended or reset they are decoded then discarded
rather than refused, because HPACK's shared table means an undecoded block
corrupts every one after it.

HPACK and QPACK are transcribed from their RFCs rather than typed, and
pinned by the RFCs' own vectors (RFC 7541 Appendix C, including C.4 for
the Huffman codes; RFC 9204 Appendix A) and by structural checks. The QPACK
static table ships; `Config.QPACKStatic` remains as an interop and testing
override ([ADR 0013](../design/adr/0013-qpack-static-table.md)).

`ServeH3Conn` runs over `net/quic` — a real TLS 1.3 handshake on a UDP
socket, RFC 9001 packet protection, RFC 9002 recovery — not on a
dependency, since the standard library has no QUIC. That package is still
experimental, in a sense this one has left: checked against itself,
against published vectors, against a harness that drops, reorders and
forges datagrams, and against two peers that share no code (quic-go, in the
`interop/` module, where its HTTP/3 client also reads `ServeH3Conn`'s
responses, and ngtcp2, in the QUIC Interop Runner). `ServeH3Conn` and
`Server.ServeQUIC` take its types, and so stay outside the compatibility
promise with it ([ADR 0021](../design/adr/0021-net-quic-stays.md)).

The frame-level codecs under the three Serve functions — HTTP/2 framing and
HPACK, HTTP/3 framing and QPACK — are unexported. The servers above them
are built from them and the benchmark harness measures them one stage at a
time from inside the package; a caller wants one of the three Serve
functions, and `web` over net/http before any of them. `Preface` is the one
piece of the layer a caller may need, to recognise a prior-knowledge HTTP/2
client in front of the server.

## What it refuses

A parser is judged on what it declines. This one implements the smallest
HTTP/1.1 that can serve a JSON API, and refuses the rest rather than
guessing:

- **Transfer-Encoding, except "chunked" alone.** A chunked body with no
  Content-Length beside it is decoded — bounded like any body, by
  `Config.MaxBodyBytes` or, streamed, `Config.MaxStreamedBodyBytes`; chunk
  extensions ignored, trailers delivered in `Request.Trailers` once the
  body has ended. A coding this server does not implement is answered 501;
  Transfer-Encoding beside Content-Length — the request-smuggling pair in
  its entirety — or on an HTTP/1.0 request is answered 400.
- **A repeated or non-numeric Content-Length**, answered 400.
- **Authority-form targets** — origin-form, a target starting with "/", is
  what the handler sees, or the asterisk "*" of an OPTIONS request (RFC 9112
  §3.2.4); an absolute-form target is accepted as RFC 9112 §3.2.2 requires,
  its authority replacing the Host and its path delivered as origin-form.
  The authority-form is CONNECT's, and a CONNECT is answered 501 on every
  protocol: this server opens no tunnel, and the handler never sees the
  request.
- **A missing or repeated Host**, answered 400, as HTTP/1.1 requires.
- **Whitespace between a header name and its colon**, answered 400: RFC
  9112 §5.1 requires rejection precisely because tolerating it is how two
  hops disagree about where a header ends.
- **CR or LF inside a header value**, in both directions — refused on the
  way in, and refused again when a response is written, which is response
  splitting closed at the only place it can be closed.
- **Connection upgrades and WebSocket.** Upgrades are a deployment choice,
  not a protocol feature this package implements. TLS is wrapped outside
  this package: the server takes a `net.Listener`, and `crypto/tls` wraps
  one.

Request bodies stay bounded whether they stream or arrive whole. Every
limit is a number the caller sets in `Config`; none of them has a value
that suits every deployment, so none has a default that pretends otherwise
beyond the modest ones a zero field takes, each named in the field's own
documentation and in the [settings table](#nethttpstream-settings).
