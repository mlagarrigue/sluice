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
// are decoded then discarded rather than refused, because HPACK's shared
// table means an undecoded block corrupts every one after it.
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
// reorders and forges datagrams, and against one peer that shares no code
// (quic-go, in the interop/ module, where its HTTP/3 client also reads
// [ServeH3]'s responses) — not yet against the public QUIC Interop Runner.
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
//     chunk extensions ignored, trailers parsed and discarded. A coding this
//     server does not implement is answered 501; Transfer-Encoding beside
//     Content-Length — the request-smuggling pair in its entirety — or on an
//     HTTP/1.0 request is answered 400.
//   - **A repeated or non-numeric Content-Length**, answered 400.
//   - **Absolute-form and authority-form targets** — only origin-form, a
//     target starting with "/", is accepted. The others exist for proxies and
//     are a routing hazard in a server that is not one.
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
// that pretends otherwise beyond the modest ones [Config.withDefaults] fills
// in.
package httpstream

import (
	"errors"
	"fmt"
	"time"
)

// ErrMalformed reports a request this package will not parse. The connection
// is answered with a status and closed: a stream whose framing is in doubt
// cannot be resynchronised, exactly as [postgres.ErrProtocol] argues for the
// other wire protocol in this repository.
var ErrMalformed = errors.New("httpstream: malformed request")

// ErrTooLarge reports a request past one of [Config]'s bounds.
var ErrTooLarge = errors.New("httpstream: request exceeds a configured bound")

// errUnsupportedCoding is internal: a request declared a transfer coding this
// server does not implement. Distinct from [ErrMalformed] because the honest
// answer is 501, not 400 (RFC 9112 §6.1: a coding not understood SHOULD be
// answered 501 Not Implemented) — the request may be perfectly well-formed
// for a server that has the decoder. The connection still closes: the body's
// framing was never established, so there is no reading past it.
var errUnsupportedCoding = errors.New("httpstream: unsupported transfer coding")

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

// Config bounds everything a client controls. There is no zero value that is
// safe on a public socket, and the defaults below are modest rather than
// generous — a deployment that needs more says so.
type Config struct {
	// MaxRequestLineBytes bounds the method, target and version together.
	// Zero means 8 KiB.
	MaxRequestLineBytes int

	// MaxHeaderBytes bounds all headers of one request together, including
	// their names, colons and line endings. Zero means 32 KiB.
	MaxHeaderBytes int

	// MaxHeaders bounds how many header fields one request may carry. Zero
	// means 64. It exists beside MaxHeaderBytes because a thousand one-byte
	// headers cost lookups rather than bytes.
	MaxHeaders int

	// MaxBodyBytes bounds one request body. Zero means 1 MiB.
	MaxBodyBytes int

	// MaxRequestsPerBatch bounds how many pipelined requests are handed over
	// at once. Zero means 64. It is what keeps a client that writes ten
	// thousand requests in one burst from turning them into one batch and one
	// allocation the size of the burst.
	MaxRequestsPerBatch int

	// MaxConcurrentStreams bounds how many request streams one HTTP/2 or
	// HTTP/3 connection may have open and unfinished at once; the stream over
	// it is refused (REFUSED_STREAM, H3_REQUEST_REJECTED) rather than the
	// connection lost (RFC 9113 §5.1.2, RFC 9114 §5.2). For HTTP/3, zero
	// means 64, the transport's own MAX_STREAMS grant
	// ([quic.DefaultParameters]'s InitialMaxStreamsBidi), so a zero config
	// admits exactly what the transport invites; for an HTTP/2 connection
	// [Serve] accepts, zero keeps [H2Config.MaxConcurrentStreams]'s own
	// default. It exists beside MaxRequestsPerBatch because the two bound
	// different things — one is how many requests ride one hand-off, the
	// other how much assembly state a peer may park — and tuning the batch
	// size must not change who is admitted. HTTP/1.1, being sequential, has
	// no use for it.
	MaxConcurrentStreams int

	// MaxRequestsPerConn bounds how many requests one connection may make
	// before it is closed. Zero means 1024, which is keep-alive with an end.
	MaxRequestsPerConn int

	// IdleTimeout is how long a connection may hold the socket open between
	// requests. Zero means 30s.
	IdleTimeout time.Duration

	// ReadTimeout is how long one request has to arrive in full once its
	// first byte has. Zero means 10s. This is the bound that answers
	// slowloris: a client that sends a header byte a minute is disconnected
	// by the clock rather than tolerated.
	ReadTimeout time.Duration

	// WriteTimeout is how long one batch of responses has to be written.
	// Zero means 10s.
	WriteTimeout time.Duration

	// QPACKStatic overrides RFC 9204 Appendix A's static table, which this
	// package ships — see [QPACKDecoder]. Nil means the shipped table, which
	// is what every real client assumes; an empty non-nil table refuses
	// indexed field lines outright.
	//
	// It is an interop and testing override, kept by ADR 0013 so a
	// decoder can be pinned against a table it did not transcribe itself.
	// Nothing outside this package's tests sets it, and a deployment has no
	// reason to: a real client speaks the RFC's table and no other.
	QPACKStatic []Header
}

func (c Config) withDefaults() Config {
	if c.MaxRequestLineBytes <= 0 {
		c.MaxRequestLineBytes = 8 << 10
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = 32 << 10
	}
	if c.MaxHeaders <= 0 {
		c.MaxHeaders = 64
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxRequestsPerBatch <= 0 {
		c.MaxRequestsPerBatch = 64
	}
	if c.MaxConcurrentStreams <= 0 {
		c.MaxConcurrentStreams = 64
	}
	if c.MaxRequestsPerConn <= 0 {
		c.MaxRequestsPerConn = 1024
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 10 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	return c
}
