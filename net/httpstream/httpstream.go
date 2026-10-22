// Package httpstream serves HTTP/1.1, /2 and /3 where the transport is a
// stream stage rather than a thing that calls you.
//
// # Read this before using it
//
// This package parses bytes that arrive from whoever dialled the socket.
// docs/design/architecture.md ("What this is — and is not") refuses to ship
// such a parser, in those words — "rewriting a parser exposed to hostile
// input is how CVEs are manufactured" — and that judgement is not revised
// here. [web] over net/http remains the supported path, and nothing in this
// package is on it.
//
// What is claimed for this code is narrower: that the architecture's own
// table can be honoured end to end, and that doing so can be measured. It has
// faced curl, h2spec and quic-go's HTTP/3 client in continuous integration,
// and its parsers are fuzzed there; it has not run in production, nor been
// reviewed by anyone who does this for a living. Put it behind something you trust, or do not
// expose it at all. Which deployments it is written for — HTTP/1.1 exposed
// directly, cleartext only behind a terminating proxy, clients with a
// reduced HPACK table — is gathered in docs/guide/experimental.md
// ("Supported deployments"); each row names where the code decides it.
//
// # The thesis
//
// The library's table promises this for the web vertical:
//
//	byte → Frame → Request → … → Response → byte
//
// Over net/http the first two arrows are not the library's. The standard
// server owns the read loop, parses one request, and calls a handler — so the
// pipeline begins at Request, one element at a time, and the all-batch model
// meets its worst case at the boundary it was supposed to own. Measured, that
// costs ×133 against the same six steps written by hand
// (BenchmarkWebBatchOne), which is 0.14% of what the transport itself costs
// and therefore affordable — but the model does not apply there.
//
// Here the arrows are all stream stages. A connection is a [sluice.Source]
// of requests, a pipeline is a Stream-to-Stream function, and the responses
// go back out a batch at a time. Both halves are streams: [Response.Stream]
// lets the transport pull the batches of an answer at the socket's pace —
// chunks over HTTP/1.1, DATA frames over /2 and /3, no total needed in
// advance — and a producer learns that the connection is gone, or its stream
// reset, by its yield returning false on its next batch. That is the only
// signal: no transport interrupts a producer blocked in its own code.
//
// # Where the batch comes from
//
// Not from waiting. Over HTTP/1.1, keep-alive makes one connection a
// *sequence* of requests and pipelining (RFC 9112 §6.3.2) lets a client put
// several on the wire before reading any answer; one read then yields
// several complete requests, handed over as one [sluice.Batch] — the one
// thing net/http cannot express, since it serves a pipelined connection
// strictly one request at a time. Over HTTP/2 the connection multiplexes by
// design, so one read routinely carries frames for several streams and the
// batch is the ordinary case (measured in
// TestH2MultiplexedRequestsArriveAsOneBatch: six requests, one batch). Over
// HTTP/3 a QUIC datagram has always carried frames for several streams
// (TestH3RequestsFromOneDatagramAreOneBatch: four requests from one
// datagram, one batch). Three protocols, one [Handler], and the pipeline
// never learns which carried it.
//
// So the batch is opportunistic: it is whatever arrived together. The
// measurement that matters is therefore not "is this faster than net/http"
// but "what does a batch of k requests buy at k > 1". Measured by
// BenchmarkReadPipelined, reading only — no socket, so the syscall a real
// connection would also amortize is not in these figures:
//
//	depth   ns/request   buffer
//	    1        139       4.7 KB
//	    2        107       5.1 KB
//	    8         84.5     7.9 KB
//	   64         81       37 KB
//
// −42% per request at depth 64, and the curve is flat past 8: the read, the
// refill and the hand-off are paid once per batch while the parse is paid
// once per request, so what batching removes runs out. Depth 8 is the number
// to set [Config.MaxRequestsPerBatch] from rather than a reason to raise it.
//
// # What it is worth against a real backend, and where it is worth nothing
//
// The transport's nanoseconds are noise beside a database round trip, so the
// unit that decides is **round trips**. TestTransportBatchingMatrix runs the
// same clients against the same backend — one call per batch, 500 µs each,
// which is what `= ANY($1)` buys — over three servers:
//
//	server                   pipelined client      concurrent clients
//	net/http + gateway       32 trips, 108 ms       2 trips,  27 ms
//	httpstream                1 trip,    1 ms      32 trips,   3 ms
//	httpstream + gateway      1 trip,    4 ms       1 trip,    5 ms
//
// Three things are in that table, and only one of them flatters this package.
//
// **It does nothing for separate connections.** One request per connection is
// one batch of one however good the transport is, and the middle row says so.
// The mechanism that serves that shape is [gateway], which batches by waiting.
//
// **The pipelined client is where net/http costs something real.** Not the
// 0.14% of transport overhead, but 32 round trips against one — because the
// standard server serves a connection's requests strictly in sequence, so
// requests that arrived together reach the gateway one at a time and each
// waits out its Within alone.
//
// **The two mechanisms compose.** The bottom row costs one round trip in both
// shapes: the connection's batch is submitted to the gateway together —
// [gateway.Gateway.DoBatch], one submission, no goroutine per element — and
// callers on other connections still join the same batch. Neither mechanism
// subsumes the other.
//
// # One connection, three goroutines
//
// A connection is a reader, a responder and a writer — never one goroutine
// per request, so a batch that arrived together is still served together.
// The reader parses frames and owns the receive side; the responder runs the
// [Handler]; the writer owns the socket's write side, the send windows and
// the scheduling. ADR 0008 records the bounds each hand-off has and what was
// deliberately left out.
//
// A response larger than the window the peer granted waits for credit rather
// than failing the connection, and the connection stays answerable while a
// handler runs: PING is answered, SETTINGS acknowledged, and a peer's
// RST_STREAM reaches the producer still working for it. A failure is
// classified rather than escalated: a malformed request, a per-stream
// flow-control violation, a body past [Config.MaxBodyBytes], a handler that
// panics, a [Response.Stream] producer that panics partway through — each
// costs its own stream and nothing else. GOAWAY is kept for what cannot be
// resynchronised once in doubt: framing and HPACK state.
//
// HTTP/1.1 and HTTP/2 pull a producer on the goroutine that serves the
// connection, so [Serve] and [ServeH2] wait for a producer blocked in its
// own code, however long it stays there — releasing one would take a context
// the producer's signature does not carry. [ServeH3] pulls each on its own
// stream's goroutine and returns once the connection has ended.
//
// # HTTP/2 and HTTP/3
//
// [Serve] chooses between HTTP/1.1 and prior-knowledge HTTP/2 by what the
// client sends first — the HTTP/2 preface can begin no HTTP/1.1 request, so
// one listener serves both — or after ALPN behind a TLS listener (ADR 0014).
// [ServeH2] applies the rules RFC 9113 §8.3 states — pseudo-headers first and
// unrepeated, lowercase field names, no connection-specific fields, TE only
// "trailers", origin-form :path — which exist because an HTTP/2 request
// reaching an HTTP/1.1 hop unchecked is how a smuggled request is written.
// It answers PING, acknowledges SETTINGS, grants flow-control windows as it
// consumes DATA, bounds concurrent streams, counts RST_STREAM against the
// rapid-reset bound (CVE-2023-44487), and tracks a header block across
// CONTINUATION with the count and total bounded (CVE-2024-27316). Trailers
// are delivered in [Request.Trailers]; on a stream already ended or reset
// they are decoded then discarded rather than refused, because HPACK's
// shared table means an undecoded block corrupts every one after it.
//
// HPACK and QPACK are transcribed from their RFCs rather than typed, and
// pinned by the RFCs' own vectors (RFC 7541 Appendix C, including C.4 for
// the Huffman codes; RFC 9204 Appendix A) and by structural checks. The
// QPACK static table ships; [Config.QPACKStatic] remains as an interop and
// testing override (ADR 0013).
//
// [ServeH3] runs over [github.com/mlagarrigue/sluice/net/quic] — a real
// TLS 1.3 handshake on a UDP socket, RFC 9001 packet protection, RFC 9002
// recovery — not on a dependency, since the standard library has no QUIC.
// That package is still experimental, in a sense this one has left: checked
// against itself, against published vectors, against a harness that drops,
// reorders and forges datagrams, and against two peers that share no code
// (quic-go, in the interop/ module, where its HTTP/3 client also reads
// [ServeH3]'s responses, and ngtcp2, in the QUIC Interop Runner).
// Keep [ServeH3] off the supported path for the same reason as the rest of
// this package.
//
// # The codec layer is not exported
//
// The frame-level codecs under [Serve], [ServeH2] and [ServeH3] — HTTP/2
// framing and HPACK, HTTP/3 framing and QPACK — are unexported. The servers
// above them are built from them and the benchmark harness measures them one
// stage at a time from inside the package; a caller wants one of the three
// Serve functions, and [web] over net/http before any of them. [Preface] is
// the one piece of the layer a caller may need, to recognise a
// prior-knowledge HTTP/2 client in front of the server.
//
// # What it refuses
//
// A parser is judged on what it declines. This one implements the smallest
// HTTP/1.1 that can serve a JSON API, and refuses the rest rather than
// guessing:
//
//   - **Transfer-Encoding, except "chunked" alone.** A chunked body with no
//     Content-Length beside it is decoded — bounded by [Config.MaxBodyBytes],
//     chunk extensions ignored, trailers delivered in [Request.Trailers]. A
//     coding this server does not implement is answered 501;
//     Transfer-Encoding beside Content-Length — the request-smuggling pair
//     in its entirety — or on an HTTP/1.0 request is answered 400.
//   - **A repeated or non-numeric Content-Length**, answered 400.
//   - **Authority-form targets** — origin-form, a target starting with "/",
//     is what the handler sees; an absolute-form target is accepted as RFC
//     9112 §3.2.2 requires, its authority replacing the Host and its path
//     delivered as origin-form. The authority-form is CONNECT's, and a
//     CONNECT is answered 501 on every protocol: this server opens no
//     tunnel, and the handler never sees the request.
//   - **A missing or repeated Host**, answered 400, as HTTP/1.1 requires.
//   - **Whitespace between a header name and its colon**, answered 400: RFC
//     9112 §5.1 requires rejection precisely because tolerating it is how two
//     hops disagree about where a header ends.
//   - **CR or LF inside a header value**, in both directions — refused on the
//     way in, and refused again when a response is written, which is response
//     splitting closed at the only place it can be closed.
//   - **Connection upgrades and WebSocket.** Upgrades are a deployment choice,
//     not a protocol feature this package implements. TLS is wrapped outside
//     this package: the server takes a [net.Listener], and [crypto/tls]
//     wraps one.
//
// Request bodies stay bounded and are not streamed, deliberately: the handler
// takes a batch of complete requests, and an element that is not complete is
// not an element. Every limit is a number the caller sets in [Config]; none
// of them has a value that suits every deployment, so none has a default
// that pretends otherwise beyond the modest ones a zero field takes, each
// named in the field's own documentation.
package httpstream

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mlagarrigue/sluice/internal/ipkey"
)

// ErrMalformed reports a request this package will not parse. The connection
// is answered with a status and closed: a stream whose framing is in doubt
// cannot be resynchronised, exactly as [postgres.ErrProtocol] argues for the
// other wire protocol in this repository.
var ErrMalformed = errors.New("httpstream: malformed request")

// ErrTooLarge reports a request past one of [Config]'s bounds. The server
// answers it itself, before any handler runs — 413, or 414 for the request
// line and 431 for the header fields, where the protocol has a status for
// them — so a handler never sees it. When legitimate requests reach it,
// raise the [Config] bound the refused part runs into: the body, the
// request line or the header fields.
var ErrTooLarge = errors.New("httpstream: request exceeds a configured bound")

// errRequestLineTooLong and errHeadersTooLarge are internal: the two bounds
// an HTTP/1.1 refusal has its own status for. Both wrap [ErrTooLarge], so a
// caller matching on it sees no change; they exist because the status a
// client is told differs — RFC 9112 §3 puts a request-target the server will
// not parse at 414, RFC 6585 §5 puts header fields it will not hold at 431 —
// and a client told 413 for either goes looking at a body that was never the
// problem. A body over MaxBodyBytes stays the plain ErrTooLarge, which is 413.
var (
	errRequestLineTooLong = fmt.Errorf("%w: the request line", ErrTooLarge)
	errHeadersTooLarge    = fmt.Errorf("%w: the header fields", ErrTooLarge)
)

// errVersionUnsupported is internal: the request line carries an HTTP-version
// the grammar accepts and this server does not speak. It wraps [ErrMalformed]
// for callers and is answered 505 rather than 400 (RFC 9110 §15.6.6): the
// line is well-formed, the version is the disagreement, and 505 is the status
// that tells a client to try another. A version the grammar refuses — no
// digits, no slash — is a malformed line and stays 400.
var errVersionUnsupported = fmt.Errorf("%w: HTTP version", ErrMalformed)

// errUnsupportedCoding is internal: a request declared a transfer coding this
// server does not implement. Distinct from [ErrMalformed] because the honest
// answer is 501, not 400 (RFC 9112 §6.1: a coding not understood SHOULD be
// answered 501 Not Implemented) — the request may be perfectly well-formed
// for a server that has the decoder. The connection still closes: the body's
// framing was never established, so there is no reading past it.
var errUnsupportedCoding = errors.New("httpstream: unsupported transfer coding")

// errConnect is internal: a well-formed CONNECT request, which asks for a
// tunnel (RFC 9110 §9.3.6). This server is an origin, not a proxy, and opens
// none, so the request is answered 501 on all three protocols (§15.6.2) —
// the status the web/stream router gives a CONNECT as well — and never
// reaches the handler: a [Request] has no shape for it, its target being an
// authority rather than a path. Distinct from [ErrMalformed] because the
// request is well formed; a reset or a 400 would tell a conforming client
// its request was wrong when the server is the one without the feature.
var errConnect = errors.New("httpstream: CONNECT is not implemented")

// errIncomplete is internal: the buffer holds the start of a request and not
// yet all of it. It is not a failure, it is a read that has not finished.
var errIncomplete = errors.New("httpstream: incomplete request")

// errIdleTimeout is internal: the read timeout fired while the connection
// held no part of a request, between one pipelined batch and the next client
// that may never come. Distinct from a timeout mid-request (conn.go's
// reader.fill, [Config.ReadTimeout]) so the two are not answered the same
// way — see statusFor in serve.go. The HTTP/2 frame reader returns it too,
// for the same silence between requests, which ServeH2 ends with
// GOAWAY(NO_ERROR) rather than as a failure.
var errIdleTimeout = errors.New("httpstream: idle timeout")

// errExpectContinue is errIncomplete's other case: the headers are whole, the
// body is not, and the client said Expect: 100-continue, so it is waiting for
// an interim response before it sends that body rather than sending it
// unprompted. It wraps errIncomplete so the ordinary "keep buffering" callers
// still recognise it; the reader distinguishes it to answer the 100 once.
var errExpectContinue = fmt.Errorf("httpstream: awaiting a 100-continue body: %w", errIncomplete)

// Config bounds everything a client controls, for every protocol this package
// serves: [Serve] reads it for HTTP/1.1 and HTTP/2, [ServeH3] for HTTP/3.
// Every bound is mandatory and none is a required parameter: the zero value
// of each field is the default named in its comment, chosen modest rather
// than generous, so that an empty Config is a bounded server and a
// deployment that needs more says so. Zero is the only value that means
// something else: a negative bound is refused, with an error naming the
// field, by every function that takes a Config. A field that one protocol has no use
// for says which protocols read it, and a bound the protocols share is set
// once, here, whichever function serves the connection. See
// docs/design/adr/0018.
type Config struct {
	// MaxRequestLineBytes bounds the method, target and version together.
	// Zero means 8 KiB. HTTP/1.1 only: HTTP/2 and HTTP/3 carry them as
	// pseudo-header fields, under MaxHeaderBytes.
	MaxRequestLineBytes int

	// MaxHeaderBytes bounds all headers of one request together, including
	// their names, colons and line endings. Zero means 32 KiB. Every
	// protocol: over HTTP/2 it is the header block, CONTINUATION frames
	// included, over HTTP/3 the field section, and both announce it to the
	// peer as their MAX_HEADER_LIST_SIZE or MAX_FIELD_SECTION_SIZE setting.
	MaxHeaderBytes int

	// MaxHeaders bounds how many header fields one request may carry. Zero
	// means 64. It exists beside MaxHeaderBytes because a thousand one-byte
	// headers cost lookups rather than bytes. Every protocol.
	MaxHeaders int

	// MaxBodyBytes bounds one request body. Zero means 1 MiB. Every
	// protocol. A body is delivered whole, never streamed
	// (docs/design/adr/0009, article 4), so this is also what one connection
	// may hold in its read buffer: the buffer grows from 4 KiB toward
	// MaxRequestLineBytes + MaxHeaderBytes + MaxBodyBytes as a request
	// arrives, and a request that has not arrived in full after ReadTimeout
	// is refused and the buffer freed. The most the request buffers of a
	// server hold at once is therefore that sum times MaxConns, and a server
	// without MaxConns has no such bound. Over HTTP/2 and HTTP/3, where
	// several bodies assemble at once, four times this value bounds what one
	// connection holds across all of them. The per-stream bound alone is not
	// a connection bound: flow-control windows are replenished as DATA is
	// buffered, so without the aggregate cap a peer could park
	// MaxConcurrentStreams × MaxBodyBytes — 100 MiB at the defaults — by
	// never finishing any of its uploads. The factor of four keeps several
	// full-size concurrent uploads legal while bounding the parked memory to
	// the same order as one window's worth of honest traffic.
	MaxBodyBytes int

	// MaxRequestsPerBatch bounds how many pipelined requests are handed over
	// at once. Zero means 64. HTTP/1.1 only: an HTTP/2 batch is bounded in
	// frames (MaxFramesPerBatch) and an HTTP/3 batch is what one
	// datagram carried, each already bounded by MaxConcurrentStreams. It is
	// what keeps a client that writes ten
	// thousand requests in one burst from turning them into one batch and one
	// allocation the size of the burst. For a client that sends one request
	// at a time the bound never binds — a batch is whatever one read
	// returned; for one that pipelines 256 deep, 8 costs +83% per request
	// against 64 and 256 saves 16%, one response write per batch instead of
	// four (docs/benchmarks.md, "The connection budget and the batch
	// bound"). 64 is the allocation bound, not the measured optimum.
	MaxRequestsPerBatch int

	// MaxConcurrentStreams bounds how many request streams one HTTP/2 or
	// HTTP/3 connection may have open and unfinished at once; the stream over
	// it is refused (REFUSED_STREAM, H3_REQUEST_REJECTED) rather than the
	// connection lost (RFC 9113 §5.1.2, RFC 9114 §5.2). Zero means 100 for
	// both: RFC 9114 §6.1 has an HTTP/3 server grant at least 100 request
	// streams, [quic.DefaultParameters] grants exactly that as its
	// InitialMaxStreamsBidi, so a zero config admits what the transport
	// invites, and HTTP/2 advertises the same number as
	// SETTINGS_MAX_CONCURRENT_STREAMS. Over HTTP/2 a stream holds its slot
	// until its response has been written, so the queue between the read
	// goroutine and the handler never holds more than this many requests —
	// which is what lets the reader hand a batch over without blocking on it.
	// Over HTTP/3 the count is this layer's rather than the transport's: a
	// request is in flight from its first frame until its response has been
	// written, and a client that cancels a request (STOP_SENDING, RFC 9114
	// §4.1.1) frees its QUIC stream at once but not its place here, so the
	// handler calls on one connection never hold more than this many
	// requests either. On both, a connection whose cancellations and
	// refusals outrun a flat allowance plus two per request answered — four
	// batches of frames over HTTP/2, four times this bound over HTTP/3 — is
	// closed (ENHANCE_YOUR_CALM, H3_EXCESSIVE_LOAD): the rapid-reset bound,
	// CVE-2023-44487.
	// It exists beside MaxRequestsPerBatch because the two bound different
	// things — one is how many requests ride one hand-off, the other how
	// much assembly state a peer may park — and tuning the batch size must
	// not change who is admitted. HTTP/2 and HTTP/3 only: HTTP/1.1, being
	// sequential, has no use for it.
	MaxConcurrentStreams int

	// MaxRequestsPerConn bounds how many requests one HTTP/1.1 connection
	// may make before it is closed. Zero means 1024, which is keep-alive
	// with an end. HTTP/1.1 only: an HTTP/2 or HTTP/3 connection is bounded
	// by MaxConcurrentStreams and IdleTimeout, not by a request count. The end
	// costs a client that does not pipeline a TCP and a TLS handshake every
	// 1024 requests: +3.7% per request over loopback against no end at all,
	// the handshake and nothing else (docs/benchmarks.md, "The connection
	// budget and the batch bound"). Over a real network it is two extra
	// round trips on one request in 1024 — the tail, which loopback does
	// not see. There is no unbounded setting; math.MaxInt is one.
	MaxRequestsPerConn int

	// IdleTimeout is how long a connection may hold the socket open between
	// requests — after the first; a connection that has not sent one yet
	// has ReadTimeout to do so. Zero means two minutes, as in package web: two minutes
	// outlasts the idle timeout of the common proxies in front (60 seconds on
	// most load balancers and on nginx's upstream pool), so the server is not
	// the side that closes a connection the proxy is about to reuse; a proxy
	// that keeps its connections longer — Envoy, one hour by default — wants
	// its upstream idle timeout set below this one. [Serve] announces it on
	// every HTTP/1.1 response that does not close, as Keep-Alive:
	// timeout=<seconds>, for a proxy in front that reads it; nginx and Envoy
	// do not. HTTP/2 and HTTP/3 have no such field; their peers learn the
	// end from GOAWAY. Every protocol, each in its own way: over HTTP/2 the
	// clock does not run while this end owes the peer answers — a streamed
	// response may take minutes during which a well-behaved client has
	// nothing to send — and once the last answer is out the connection ends
	// with GOAWAY(NO_ERROR) after between one and two IdleTimeouts of
	// silence, where silence means no stream activity: a PING, SETTINGS or
	// WINDOW_UPDATE does not restart the clock, so a peer cannot hold a
	// connection it never uses; over HTTP/3 the transport keeps it, as the
	// max_idle_timeout that [Config.TransportParameters] derives from this
	// field, so a connection ServeH3 serves on other parameters has the
	// idle timeout those parameters say, not this one.
	IdleTimeout time.Duration

	// ReadTimeout is how long one request has to arrive in full once its
	// first byte has, and how long a new connection has to send its first
	// byte at all (the TLS handshake included). Zero means 10s. Every
	// protocol. This is the bound that answers slowloris: a client that
	// sends a header byte a minute is disconnected by the clock rather than
	// tolerated; over HTTP/2 and HTTP/3 it is the stream still assembling
	// that is reset, not the connection.
	ReadTimeout time.Duration

	// WriteTimeout is how long a response may go without progress: a write
	// that has not put [Config.WriteProgressBytes] on the wire by the time it
	// passes is given up on, and one that has gets the same time again, so a
	// slow client that keeps reading is served to the end and one that stops
	// is not waited on. Over HTTP/1.1 the progress is the socket's and giving
	// up ends the connection. Over HTTP/2 it is the stream's: the wait for the
	// flow-control credit one stream needs, after which that stream alone is
	// reset with CANCEL while the others carry on, and each write to the
	// socket, which ends the connection. Over HTTP/3 it is each write of a
	// response — its head, then each batch of a streamed body — on the
	// stream's flow control, and the stream is reset with
	// H3_REQUEST_CANCELLED. A peer that drips one byte of credit per timeout
	// is therefore cut at the second timeout rather than kept (data dribble,
	// CVE-2019-9511). Zero means 10s. Every protocol.
	//
	// Over HTTP/1.1 on TLS the first timeout ends the connection whatever
	// the progress: crypto/tls makes a write that timed out fatal to the
	// connection, so there is no second wait to grant.
	WriteTimeout time.Duration

	// WriteProgressBytes is the progress that moves [Config.WriteTimeout]:
	// how much of a response must go out for the wait to start again. Zero
	// means 4 KiB — a quarter of the smallest frame an HTTP/2 peer may ask
	// for, 400 bytes a second at the default timeout, which no client that is
	// reading falls under. It is a number of bytes rather than a share of
	// anything the peer announces, so that the peer cannot move it. Every
	// protocol.
	WriteProgressBytes int

	// MaxConns bounds how many connections [Serve] holds open at once,
	// HTTP/1.1 and HTTP/2 together. Zero means no bound. The connection
	// over it is closed as it is accepted, with nothing written: answering
	// 503 would mean reading its request, which is the cost the bound
	// exists to refuse — a connection is a goroutine and a read buffer
	// before its first byte says which protocol it speaks. This is what
	// net/http does under netutil.LimitListener. There is no queue: a
	// client that was refused retries, as it would after a reset. [ServeH3]
	// is not bound by it; QUIC bounds its connections in the transport.
	//
	// It is also the memory bound: each connection may hold one request at
	// its ceiling, MaxRequestLineBytes + MaxHeaderBytes + MaxBodyBytes (about
	// 1 MiB at the defaults), for ReadTimeout at most, so MaxConns times
	// that sum is the most the request buffers occupy. A public socket sets
	// it; a server without it is bounded by the peers' goodwill.
	MaxConns int

	// MaxConnsPerIP is the same bound applied per remote address, so that
	// one peer cannot hold the whole MaxConns on its own; the connection
	// over it is closed as it is accepted, the same way. The port is ignored
	// and an address is counted by its prefix, IPv4PrefixBits or
	// IPv6PrefixBits long; an IPv4-mapped IPv6 address counts as IPv4. A
	// connection whose remote address is not IP, a Unix socket's, is bound
	// by MaxConns alone: its peers share one empty address, and a per-peer
	// bound would be a second total. Zero means no bound: a figure that is
	// fair to a home connection starves a carrier-grade NAT, so the operator
	// chooses. [ServeH3] is not bound by it: the QUIC listener has its own,
	// [github.com/mlagarrigue/sluice/net/quic.ListenerConfig].MaxConnsPerIP.
	MaxConnsPerIP int

	// IPv4PrefixBits and IPv6PrefixBits are how much of a remote address
	// MaxConnsPerIP counts by: two addresses that share that many leading
	// bits share one count. Zero means 32 and 64. An IPv6 host is handed a
	// whole /64 (RFC 6177), so a count by the full 128 bits is one it steps
	// around by changing address; an IPv4 host rarely holds more than one.
	// A value outside 1..32 or 1..128 is a configuration error. The QUIC
	// listener's fields of the same names count the same way.
	IPv4PrefixBits int
	IPv6PrefixBits int

	// ShutdownGrace is how long [Serve], [ServeH2] or [ServeH3], once its
	// context is cancelled, waits for the requests already in flight to be
	// answered before it closes the connections still open, cancels their
	// handlers' context and returns [ErrShutdownForced]. Zero means 5s.
	// Every protocol. What it bounds is the handler and the response's writes
	// together, not either alone: WriteTimeout governs one write, this
	// governs how long a stopping server stays up for a response that was
	// owed. Idle connections are closed at once and do not spend it. A
	// [Server] does not read it: the context given to [Server.Shutdown] is
	// its bound. See docs/design/adr/0017.
	ShutdownGrace time.Duration

	// KeepAlive is how long a connection may stay silent before this end
	// sends something to keep it, and the paths between, alive: a PING on
	// HTTP/3 (see [quic.Conn.SetKeepAlive], which holds it to half the idle
	// timeout the two ends negotiated), a TCP keep-alive probe on HTTP/1.1
	// and HTTP/2, over TLS too. What it is for is a client that keeps a
	// connection between two requests across a NAT or a load balancer that
	// forgets a silent flow, or a QUIC idle timeout shorter than the gaps
	// between requests. [Serve] and [Server] apply it to every connection
	// they accept; [ServeH2] and [ServeH3] to the one they are handed. Zero
	// sends nothing of this end's own: an HTTP/3 connection stays silent,
	// and a TCP connection keeps what its listener set — one from
	// [net.Listen] probes every 15s. Every protocol.
	//
	// Over HTTP/1.1 and HTTP/2 it does not hold a connection open against
	// IdleTimeout, which counts requests, not packets. Over HTTP/3 it does:
	// IdleTimeout is there the transport's, which every PING restarts, so a
	// connection stays open until the client closes it or the server
	// stops. That is the price: a client that opened a connection and went
	// silent holds its state for as long as it stays, where the idle
	// timeout would otherwise have reclaimed it.
	KeepAlive time.Duration

	// Logger receives what the server reports besides the errors it
	// returns — today, a warning with the number of connections a forced
	// stop cut with requests in flight. Nil means [slog.Default]. Every
	// protocol.
	Logger *slog.Logger

	// LingerTimeout is how long a closing HTTP/1.1 connection, its write side
	// already shut, reads and drops what the client still sends before the
	// socket is closed — so that the close does not turn the response still
	// in flight into a reset. Zero means a second, or ReadTimeout if that is
	// shorter: this is a tail being drained, not a request being waited for,
	// and a client that neither closes nor stops is not owed more. HTTP/1.1
	// only.
	LingerTimeout time.Duration

	// MaxFrameSize is the largest frame payload accepted. Zero means 16384,
	// the protocol's own initial value. Any other value outside
	// [16384, 1<<24-1] is refused: the value is advertised as
	// SETTINGS_MAX_FRAME_SIZE and RFC 9113 §6.5.2 bounds that to
	// [2^14, 2^24-1] — a smaller value on the wire is an illegal setting a
	// conformant client must treat as a connection error, §4.2 obliges this
	// end to receive 16384-byte frames regardless, and 1<<24-1 is what the
	// length field can express. HTTP/2 only.
	//
	// It also caps the frames this end sends: a peer may advertise up to
	// 16 MiB, and a frame that large is a buffer that large on this side.
	// It is a transport bound, not a body bound: bodies are bounded by
	// MaxBodyBytes across frames, however large each frame may be.
	MaxFrameSize int

	// MaxFramesPerBatch bounds how many HTTP/2 frames are handed over at
	// once. Zero means 64. HTTP/2 only.
	MaxFramesPerBatch int

	// MaxContinuationFrames bounds how many CONTINUATION frames may follow
	// one HEADERS before END_HEADERS arrives. Zero means 16. HTTP/2 only.
	//
	// This is not a tuning knob. A peer may legally split a header block
	// across CONTINUATION frames, and a server that accepts an unbounded
	// number of them can be made to read and buffer forever without ever
	// completing a request — CVE-2024-27316's shape, and the reason the bound
	// exists rather than the count being trusted.
	MaxContinuationFrames int

	// InitialWindowSize is the flow-control window this server grants each
	// HTTP/2 request stream, advertised as SETTINGS_INITIAL_WINDOW_SIZE and
	// topped back up as the body is consumed. Zero means 65535, the
	// protocol's own initial value; a value over 2^31-1 — the largest a
	// window can be (RFC 9113 §6.5.2) — is refused rather than advertised as
	// a setting the peer must refuse. HTTP/2 only: HTTP/3's windows are the
	// QUIC transport's, in [Config.TransportParameters].
	//
	// The window is what a client may have in flight on one stream before it
	// hears back, so on a link with latency it is the ceiling on an upload's
	// throughput: 65535 bytes per round trip is about 0.65 MB/s at 100 ms of
	// RTT, whatever the link carries. The test behind this field moves a
	// 4 MiB body over a proxy that adds 100 ms of round trip; a window of
	// 1 MiB carried it eleven times faster than the default on that bench
	// (0.48 MB/s against 5.5 MB/s, h2rtt_test.go).
	//
	// What it costs: a client may send the whole window before this end has
	// read any of it, so the window is memory the peer can make this side
	// hold, per stream. It is bounded above by MaxBodyBytes — a body over
	// that bound is refused whatever the window granted — and across the
	// connection by the aggregate 4×MaxBodyBytes bound, so a window larger
	// than MaxBodyBytes only lets the peer hit the 413 sooner. The
	// connection-level window follows this one: it is raised to
	// MaxConcurrentStreams times the stream window, capped at 2^31-1, so that
	// the connection never queues what the streams alone would admit.
	InitialWindowSize int

	// MaxPendingControlBytes bounds the HTTP/2 control frames — SETTINGS and
	// PING acknowledgements, WINDOW_UPDATE, RST_STREAM — waiting to be
	// written. Zero means 16 KiB. HTTP/2 only.
	//
	// It is the S1 bound on the one queue a peer fills directly: a client can
	// send PING faster than this end can answer, and the answer to that is to
	// stop reading until the queue drains, not to buffer without limit. The
	// pressure then sits in the peer's send buffer, which is what TCP flow
	// control is for.
	//
	// It is also the only bound on the frames RFC 9113 §10.5 asks an
	// implementation to track — PING, SETTINGS, PRIORITY, WINDOW_UPDATE,
	// empty DATA — and deliberately so: none of them earns an answer larger
	// than itself or costs this end an allocation (measured by
	// TestH2ControlFramesAnswerNoMoreThanTheyCost), so a flood of them is
	// paid in the peer's own bandwidth, and none restarts the idle timeout.
	// The frames that do amplify have bounds of their own: RST_STREAM the
	// rapid-reset allowance, CONTINUATION and header blocks MaxHeaderBytes.
	MaxPendingControlBytes int

	// MaxWriteBufferBytes is how much HTTP/2 response payload the writer
	// gathers before one write to the socket. Zero means 64 KiB. A round
	// always holds at least one whole frame, so below the frame size in use a
	// round is one frame: the value is honoured as a bound on gathering, not
	// as a reason to split a frame. HTTP/2 only.
	//
	// It is the round of the writer's round-robin: one stream on its own gets
	// the whole of it and pays a single syscall for it, and several streams
	// share it a frame at a time so that a large response does not hold a
	// small one behind it.
	MaxWriteBufferBytes int

	// QPACKStatic overrides RFC 9204 Appendix A's static table, which this
	// package ships. Nil means the shipped table, which
	// is what every real client assumes; an empty non-nil table refuses
	// indexed field lines outright. HTTP/3 only.
	//
	// It is an interop and testing override, kept by ADR 0013 so a
	// decoder can be pinned against a table it did not transcribe itself.
	// Nothing outside this package's tests sets it, and a deployment has no
	// reason to: a real client speaks the RFC's table and no other.
	QPACKStatic []Header
}

// The defaults a bound takes when its field is zero, named because more than
// Config.resolve reads them.
const (
	defaultMaxHeaderBytes = 32 << 10
	defaultMaxHeaders     = 64
	defaultMaxBodyBytes   = 1 << 20
	defaultIdleTimeout    = 2 * time.Minute
	defaultReadTimeout    = 10 * time.Second
	defaultWriteTimeout   = 10 * time.Second
)

// resolve returns c with every zero bound replaced by its default, or an
// error naming each field whose value is out of range. Zero is the one value
// that stands for another: a negative bound is the caller's mistake, said at
// start rather than quietly turned into the default (docs/design/adr/0018).
func (c Config) resolve() (Config, error) {
	err := errors.Join(
		orDefault("Config.MaxRequestLineBytes", &c.MaxRequestLineBytes, 8<<10),
		orDefault("Config.MaxHeaderBytes", &c.MaxHeaderBytes, defaultMaxHeaderBytes),
		orDefault("Config.MaxHeaders", &c.MaxHeaders, defaultMaxHeaders),
		orDefault("Config.MaxBodyBytes", &c.MaxBodyBytes, defaultMaxBodyBytes),
		orDefault("Config.MaxRequestsPerBatch", &c.MaxRequestsPerBatch, 64),
		orDefault("Config.MaxConcurrentStreams", &c.MaxConcurrentStreams, defaultMaxConcurrency),
		orDefault("Config.MaxRequestsPerConn", &c.MaxRequestsPerConn, 1024),
		orDefault("Config.IdleTimeout", &c.IdleTimeout, defaultIdleTimeout),
		orDefault("Config.ReadTimeout", &c.ReadTimeout, defaultReadTimeout),
		orDefault("Config.WriteTimeout", &c.WriteTimeout, defaultWriteTimeout),
		orDefault("Config.WriteProgressBytes", &c.WriteProgressBytes, defaultMaxFrameSize/4),
		orDefault("Config.ShutdownGrace", &c.ShutdownGrace, 5*time.Second),
		orDefault("Config.MaxFramesPerBatch", &c.MaxFramesPerBatch, 64),
		orDefault("Config.MaxContinuationFrames", &c.MaxContinuationFrames, 16),
		orDefault("Config.MaxPendingControlBytes", &c.MaxPendingControlBytes, 16<<10),
		orDefault("Config.MaxWriteBufferBytes", &c.MaxWriteBufferBytes, 64<<10),
	)
	// RFC 9113 §6.5.2 bounds both settings; a value outside would be
	// advertised as a setting a conformant peer must answer with a connection
	// error, so every connection would end at its first SETTINGS.
	switch {
	case c.MaxFrameSize == 0:
		c.MaxFrameSize = defaultMaxFrameSize
	case c.MaxFrameSize < defaultMaxFrameSize || c.MaxFrameSize > maxFrameSize:
		err = errors.Join(err, fmt.Errorf("httpstream: Config.MaxFrameSize %d is outside %d..%d (RFC 9113 §6.5.2) — zero means %d",
			c.MaxFrameSize, defaultMaxFrameSize, maxFrameSize, defaultMaxFrameSize))
	}
	switch {
	case c.InitialWindowSize == 0:
		c.InitialWindowSize = defaultInitialWindow
	case c.InitialWindowSize < 0 || c.InitialWindowSize > maxWindow:
		err = errors.Join(err, fmt.Errorf("httpstream: Config.InitialWindowSize %d is outside 1..%d (RFC 9113 §6.5.2) — zero means %d",
			c.InitialWindowSize, maxWindow, defaultInitialWindow))
	}
	if c.MaxConns < 0 {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.MaxConns %d is negative — zero means no bound", c.MaxConns))
	}
	if c.MaxConnsPerIP < 0 {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.MaxConnsPerIP %d is negative — zero means no bound", c.MaxConnsPerIP))
	}
	err = errors.Join(err, ipkey.Check("httpstream: Config.", &c.IPv4PrefixBits, &c.IPv6PrefixBits))
	if c.KeepAlive < 0 {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.KeepAlive %v is negative — zero means none", c.KeepAlive))
	}
	if c.LingerTimeout < 0 {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.LingerTimeout %v is negative — zero means a second, or ReadTimeout if shorter", c.LingerTimeout))
	}
	if err != nil {
		return Config{}, err
	}
	if c.LingerTimeout == 0 {
		c.LingerTimeout = min(c.ReadTimeout, time.Second)
	}
	return c, nil
}

// logger is c.Logger, or the default one.
func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// orDefault sets *v to def when it is zero and refuses it when it is
// negative, naming the field, its value and what zero would have meant.
func orDefault[T int | time.Duration](field string, v *T, def T) error {
	switch {
	case *v < 0:
		return fmt.Errorf("httpstream: %s %v is negative — zero means the default, %v", field, *v, def)
	case *v == 0:
		*v = def
	}
	return nil
}
