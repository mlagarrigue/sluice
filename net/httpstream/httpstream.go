// Package httpstream serves HTTP/1.1, /2 and /3 where the transport is a
// stream stage rather than a thing that calls you. A connection keeps, from
// one batch of requests to the next, only what its protocol carries across
// them — header tables and stream states — within the bounds of Config.
//
// # Read this before using it
//
// This package parses bytes that arrive from whoever dialled the socket.
// docs/design/architecture.md ("What this is — and is not") refuses to ship
// such a parser — "rewriting a parser exposed to hostile input is how CVEs
// are manufactured" — and that judgement is not revised here: [web] over
// net/http leaves that parsing to the standard library.
//
// net/httpstream is supported — outside the experimental label and
// under the compatibility promise — and fuzzed in CI without
// a public corpus; it has neither run in production nor had an outside
// security review. It faces curl, h2spec, h2load, h3spec, aioquic,
// quic-go's HTTP/3 client, and nginx, HAProxy and Envoy in front of it, in
// continuous integration. Put it behind something you trust, or do not
// expose it at all.
//
// # The thesis
//
// Over net/http the server owns the read loop and the pipeline begins at
// Request, one element at a time. Here a connection is a [sluice.Source] of
// requests: one read that brings several — pipelined HTTP/1.1, multiplexed
// HTTP/2 and HTTP/3 — hands them to one [Handler] call as one batch, and
// the responses go back out a batch at a time ([Handler] says where the
// batch comes from and what it is worth).
//
//	handle := func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
//		out := make([]httpstream.Response, 0, b.Len())
//		for _, r := range b.Items {
//			out = append(out, httpstream.Response{Status: 200, Body: r.Target})
//		}
//		return sluice.Batch[httpstream.Response]{Items: out} // one per request, in order
//	}
//	err := httpstream.Serve(ctx, ln, httpstream.Config{}, handle) // HTTP/1.1 and HTTP/2
//
// Where to read on, past the guide:
//   - docs/reference/httpstream.md — every Config setting, the three
//     goroutines of a connection, HTTP/2 and HTTP/3, what the parser
//     refuses;
//   - docs/guide/deployments.md — HTTP/1.1 exposed directly, cleartext only
//     behind a terminating proxy, clients with a reduced HPACK table.
//
// The guide: docs/guide/httpstream.md — serving, bodies, streamed responses,
// stopping in two steps.
package httpstream

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/mlagarrigue/sluice/internal/ipkey"
)

// ErrMalformed reports a request this package will not parse. The connection
// is answered with a status and closed: a stream whose framing is in doubt
// cannot be resynchronised, exactly as [github.com/mlagarrigue/sluice/database/postgres.ErrProtocol] argues for the
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
// h1Conn.fill, [Config.ReadTimeout]) so the two are not answered the same
// way — see statusFor in serve.go. The HTTP/2 frame reader returns it too,
// for the same silence between requests, which ServeH2Conn ends with
// GOAWAY(NO_ERROR) rather than as a failure.
var errIdleTimeout = errors.New("httpstream: idle timeout")

// errExpectContinue is errIncomplete's other case: the headers are whole, the
// body is not, and the client said Expect: 100-continue, so it is waiting for
// an interim response before it sends that body rather than sending it
// unprompted. It wraps errIncomplete so the ordinary "keep buffering" callers
// still recognise it; the reader distinguishes it to answer the 100 once.
var errExpectContinue = fmt.Errorf("httpstream: awaiting a 100-continue body: %w", errIncomplete)

// BodyDelivery says how a request body reaches the handler
// (docs/design/adr/0009, article 4). The zero value streams it: a body is
// handed over as it arrives, so a server holds a stream window rather than
// a whole upload, and a handler can answer before the last byte. A server
// that wants every body complete and in memory before its handler runs asks
// for [BodyWhole].
//
// It is what a body arrives as, and what bounds it: [Config.MaxBodyBytes]
// for a body held whole, raised by [Config.MaxStreamedBodyBytes] for one
// that streams.
type BodyDelivery int

const (
	// BodyStreamed hands a body to the handler as it arrives, through
	// [Request.Stream], except one already whole in the read that brought
	// the request's headers and under [Config.MaxBodyBytes], which still
	// arrives in [Request.Body] and batches with its neighbours.
	BodyStreamed BodyDelivery = iota

	// BodyWhole holds a body until it is complete, so every request handed
	// to a handler has its body in [Request.Body] and [Request.Stream] is
	// nil. What one connection may hold is then bounded by
	// [Config.MaxBodyBytes], and four times it across the bodies an HTTP/2
	// or HTTP/3 connection assembles at once.
	BodyWhole
)

// Config bounds everything a client controls, for every protocol this package
// serves: [Serve] reads it for HTTP/1.1 and HTTP/2, [ServeH3Conn] for HTTP/3.
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
	// protocol. A body is delivered whole when BodyDelivery asks for it
	// (docs/design/adr/0009, article 4), and this is then what one connection
	// may hold in its read buffer: the buffer grows from 4 KiB toward
	// MaxRequestLineBytes + MaxHeaderBytes + MaxBodyBytes, plus four bytes
	// of line endings, as a request arrives, and a request that has not
	// arrived in full after ReadTimeout is refused and the buffer freed. The
	// most the request buffers of a server hold at once is therefore that sum times MaxConns, and a server
	// without MaxConns has no such bound. Over HTTP/2 and HTTP/3, where
	// several bodies assemble at once, four times this value bounds what one
	// connection holds across the bodies still assembling. The per-stream
	// bound alone is not a connection bound: flow-control windows are
	// replenished as DATA is buffered, so without the aggregate cap a peer
	// could park MaxConcurrentStreams × MaxBodyBytes — 100 MiB at the
	// defaults — by never finishing any of its uploads. The factor of four
	// keeps several full-size concurrent uploads legal while bounding the
	// parked memory to the same order as one window's worth of honest
	// traffic. A body complete and waiting for its handler, or held while the
	// handler runs, is out of that count: it is a request in flight, and
	// those are bounded by the concurrency limit, MaxConcurrentStreams (or
	// the transport's stream limit over HTTP/3) times MaxBodyBytes at most.
	// Counted until its response, it would have a fifth full-size upload
	// refused on a connection whose handlers are merely slow.
	MaxBodyBytes int

	// BodyDelivery says whether a request body is streamed or held whole.
	// The zero value, [BodyStreamed], streams it on every protocol
	// (docs/design/adr/0009, article 4) — on HTTP/3 when the QUIC
	// connection's windows cover a request's head, MaxHeaderBytes and a few
	// kilobytes of framing, which [Config.QUICConfig] gives:
	// streaming needs the transport's manual credit, and on smaller windows
	// bodies arrive whole. A value that names neither form is refused.
	BodyDelivery BodyDelivery

	// MaxStreamedBodyBytes bounds a streamed body. Zero means MaxBodyBytes;
	// a negative value is refused. It is read when BodyDelivery streams, and
	// raises what is accepted above MaxBodyBytes for a body that arrives as
	// a stream; under [BodyWhole] it takes MaxBodyBytes's value, whatever it
	// was set to.
	//
	// When bodies stream, a body already whole in the read that brought
	// the request's headers — over HTTP/2, in the frames read with them;
	// over HTTP/3, in the packet — and within MaxBodyBytes, is still delivered in [Request.Body] and
	// batched with its neighbours; any other body arrives through
	// [Request.Stream], taken from the connection only as the handler pulls
	// it. A body is accepted up to the larger of this bound and MaxBodyBytes,
	// whichever form it takes. A content length past that is refused with
	// 413 before any of the body is read, and before the 100 Continue a
	// client may be waiting for, which goes out when the handler first waits
	// for the body; a body of unannounced length that grows past it ends its
	// stream with ErrTooLarge. The body must move by ReadProgressBytes within
	// each ReadTimeout of waiting; there is no deadline on the whole body, so
	// a large upload that keeps moving is not cut, and a client that stops,
	// or drips, is.
	//
	// Over HTTP/2 and HTTP/3 a streamed body's window is granted back as the
	// handler consumes it, so what it holds is bounded by the stream window —
	// InitialWindowSize, or the QUIC transport's — and by MaxConcurrentStreams
	// times that across a connection, rather than by the aggregate bound
	// MaxBodyBytes states for the bodies assembled whole. A body left unread
	// when its handler returns is stopped after the response: the HTTP/1.1
	// connection closes, the HTTP/2 stream is reset with NO_ERROR, and the
	// HTTP/3 client is sent STOP_SENDING with H3_NO_ERROR.
	MaxStreamedBodyBytes int

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
	// streams, [quic.DefaultConfig] grants exactly that as its
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
	// sequential, has no use for it. At most 4096: an HTTP/2 connection
	// allocates its request queue at this size when it is accepted, and an
	// HTTP/3 connection runs up to a handler call and a writer per stream
	// it admits; a larger value is refused by [Config.Validate].
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
	// has ReadTimeout to do so over HTTP/1.1 and HTTP/2, and over HTTP/3 is
	// closed by this clock, one to two IdleTimeouts after it opened. Zero means two minutes, as in package web: two minutes
	// outlasts the idle timeout of the common proxies in front (60 seconds on
	// most load balancers and on nginx's upstream pool), so the server is not
	// the side that closes a connection the proxy is about to reuse; a proxy
	// that keeps its connections longer — Envoy, one hour by default — wants
	// its upstream idle timeout set below this one. [Serve] announces it on
	// every HTTP/1.1 response that does not close, as Keep-Alive:
	// timeout=<seconds>, for a proxy in front that reads it; nginx and Envoy
	// do not. HTTP/2 and HTTP/3 have no such field; their peers learn the
	// end from GOAWAY. Every protocol, HTTP/2 and HTTP/3 alike: the clock
	// does not run while this end owes the peer answers — a streamed
	// response may take minutes during which a well-behaved client has
	// nothing to send — and once the last answer is out the connection ends
	// with GOAWAY then NO_ERROR (H3_NO_ERROR, or its grease, over HTTP/3)
	// after between one and two IdleTimeouts without a request. A PING,
	// SETTINGS or WINDOW_UPDATE does not restart the clock, nor does any
	// other packet over HTTP/3, so a peer cannot hold a connection it never
	// uses. QUIC keeps an idle timer of its own besides, which every packet
	// from the peer restarts (RFC 9000 §10.1): [Config.QUICConfig] sets it
	// to this field too, so a client that goes silent altogether is
	// reclaimed at the same bound.
	IdleTimeout time.Duration

	// ReadTimeout is how long one request has to arrive in full once its
	// first byte has, and how long a new connection has to send its first
	// byte at all (the TLS handshake included). Zero means 10s. Every
	// protocol; over HTTP/3 a connection's first bytes are its QUIC
	// handshake's, and one that then opens no request is the transport's
	// idle timeout's to end ([Config.IdleTimeout]), not this one's. This is the bound that answers slowloris: a client that
	// sends a header byte a minute is disconnected by the clock rather than
	// tolerated; over HTTP/2 and HTTP/3 it is the stream still assembling
	// that is reset, not the connection. A streamed body ([Request.Stream])
	// has it as WriteTimeout has its writes: ReadTimeout of waiting to
	// deliver [Config.ReadProgressBytes], given back whole each time it
	// has, so an upload that keeps moving is read to the end and one that
	// drips a byte at a time is cut.
	ReadTimeout time.Duration

	// ReadProgressBytes is the progress that moves [Config.ReadTimeout] on a
	// streamed body: how much of it must arrive for the wait to start
	// again. Only waiting for the peer counts, not the handler's own time
	// between two reads; a body shorter than this has ReadTimeout to arrive
	// whole. Zero means 4 KiB — 400 bytes a second at the default timeout,
	// as for [Config.WriteProgressBytes]. Every protocol.
	ReadProgressBytes int

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
	// H3_REQUEST_CANCELLED; it is also each write on the control stream
	// (SETTINGS, the GOAWAYs of a drain) and the wait for the acknowledgement
	// of the GOAWAY an idle close sends, and giving up there ends the
	// connection. A peer that drips one byte of credit per timeout
	// is therefore cut at the second timeout rather than kept (data dribble,
	// CVE-2019-9511). Zero means 10s. Every protocol.
	//
	// Over TLS, HTTP/1.1 and HTTP/2 alike, the first timeout ends the
	// connection whatever the progress: crypto/tls makes a write that timed
	// out fatal to the connection, so there is no second wait to grant.
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
	// client that was refused retries, as it would after a reset. [ServeH3Conn]
	// is not bound by it; QUIC bounds its connections in the transport.
	//
	// It is also the memory bound. A request buffer holds one request at its
	// ceiling: MaxRequestLineBytes + MaxHeaderBytes + MaxBodyBytes plus four
	// bytes of line endings, about 1 MiB at the defaults. An HTTP/1.1
	// connection holds two at most: the one its requests are read from, for
	// ReadTimeout at most, and what a pipelining client sent ahead while a
	// response streamed or a handler watched its context's Done, kept for
	// the requests that follow. So MaxConns times two request buffers is the
	// most the request buffers occupy. A public socket sets it; a server
	// without it is bounded by the peers' goodwill.
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
	// chooses. [ServeH3Conn] is not bound by it: the QUIC listener has its own,
	// [github.com/mlagarrigue/sluice/net/quic.ListenerConfig].MaxConnsPerIP.
	MaxConnsPerIP int

	// IPv4PrefixBits and IPv6PrefixBits are how much of a remote address
	// MaxConnsPerIP counts by: two addresses that share that many leading
	// bits share one count. Zero means 32 and 64. Every IPv6 host has at
	// least a /64 (RFC 4291 §2.5.4), so a count by the full 128 bits is one
	// it steps around by changing address; a site usually holds more (RFC
	// 6177), so the /64 bounds a host and 56 or 48 bounds the site. An IPv4
	// host rarely holds more than one address.
	// A value outside 1..32 or 1..128 is a configuration error. The QUIC
	// listener's fields of the same names count the same way.
	IPv4PrefixBits int
	IPv6PrefixBits int

	// ShutdownGrace is how long [Serve], [ServeH2Conn] or [ServeH3Conn], once its
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
	// they accept; [ServeH2Conn] and [ServeH3Conn] to the one they are handed. Zero
	// sends nothing of this end's own: an HTTP/3 connection stays silent,
	// and a TCP connection keeps what its listener set — one from
	// [net.Listen] probes every 15s. Every protocol.
	//
	// It does not hold a connection open against IdleTimeout, which counts
	// requests, not packets, over every protocol: over HTTP/3 it keeps the
	// transport's idle timer from firing, and the connection still ends
	// after IdleTimeout without a request.
	KeepAlive time.Duration

	// Logger receives what the server reports besides the errors it
	// returns. At the error level, a panic in the handler, in a streamed
	// response's producer or in a Trailers function — its value and stack,
	// which never reach the client's 500 or reset — and a handler that
	// answered the wrong number of requests. At the warn level, the number
	// of connections a forced stop cut with requests in flight, and the
	// error that ended a connection [Serve] or [Server] accepted when it is
	// not an ordinary end (the peer's close between requests, a deadline, a
	// reset): a peer that broke the protocol, a transport that failed — over
	// HTTP/1.1 a request answered 400, not one refused by a bound or for a
	// feature this end lacks, whose status says it to the client. A close in
	// the middle of a request or a frame is one of these: answered 400 over
	// HTTP/1.1 and logged, though RFC 9112 §8 says it is "usually due to a
	// canceled request". That last line is one
	// per connection and within any client's reach, and its text may quote,
	// escaped, bytes the peer sent: a client that opens connections only to
	// break them sets its rate. A Logger that must hold a
	// budget rate-limits what it receives. Nil means [slog.Default]. It
	// serves every protocol.
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
	// QUIC transport's, in [Config.QUICConfig].
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
	// It is the bounded-memory guarantee (S1 of the architecture's security
	// properties) on the one queue a peer fills directly: a client can
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

	// EarlyData says which requests may be acted on although they could be
	// a replay: an HTTP/3 request that arrived in 0-RTT, on a connection
	// whose handshake has not completed yet ([quic.ListenerConfig.EarlyData]
	// accepts it), and on any protocol a request carrying Early-Data, which
	// an intermediary adds to a request it received in early data itself
	// (RFC 8470 §5.1). Nil allows none, which is RFC 8470 §2's rule when
	// the resource has said nothing. ADR 0019 has the design.
	//
	// A 0-RTT request it does not allow waits for the handshake to complete
	// and is then handled like any other: a replay never completes it, so
	// it is never handled, and the client pays nothing more than without
	// 0-RTT. A request carrying Early-Data it does not allow is answered
	// 425 Too Early without reaching the handler, since waiting does not
	// make it safe (§5.2): the client retries it once its own handshake is
	// through.
	//
	// The method alone does not decide — a resource may have side effects
	// on GET (§2) — so the predicate sees the whole request. It is called
	// on the connection's goroutine, concurrently across connections, and
	// must not keep r.
	EarlyData func(r *Request) bool
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

// Validate reports what [Serve], [ServeH2Conn], [ServeH3Conn] and the
// other entry points would refuse in c, naming each field out of range, and
// nil when they would accept it. A configuration read at deployment — from
// the environment, flags, a file — is checked with it before any socket is
// opened, so a bad value stops the program at start instead of after it
// has taken its port.
func (c Config) Validate() error {
	_, err := c.resolve()
	return err
}

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
		orDefault("Config.ReadProgressBytes", &c.ReadProgressBytes, defaultMaxFrameSize/4),
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
	err = errors.Join(err, ceilings(c))
	if c.MaxConcurrentStreams > maxConcurrentStreams {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.MaxConcurrentStreams %d is over %d: an HTTP/2 connection allocates its request queue at this size when it is accepted, and ServeH3Conn serves no more streams on one connection",
			c.MaxConcurrentStreams, maxConcurrentStreams))
	}
	if c.MaxStreamedBodyBytes < 0 {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.MaxStreamedBodyBytes %d is negative — zero means Config.MaxBodyBytes", c.MaxStreamedBodyBytes))
	}
	if c.BodyDelivery != BodyStreamed && c.BodyDelivery != BodyWhole {
		err = errors.Join(err, fmt.Errorf("httpstream: Config.BodyDelivery %d names neither form — %d streams a body, %d holds it whole",
			c.BodyDelivery, BodyStreamed, BodyWhole))
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
	// After MaxBodyBytes has its own default: the two are read together,
	// and a streamed body's bound is the whole one unless it is raised.
	// Under BodyWhole nothing streams and the field is not read: every
	// protocol holds a declared length to the larger of the two, so left
	// raised it would let a request declaring between them pass its head
	// and get the 100 Continue it asked for, only to be refused 413 once its
	// body crossed MaxBodyBytes.
	if c.MaxStreamedBodyBytes == 0 || c.BodyDelivery == BodyWhole {
		c.MaxStreamedBodyBytes = c.MaxBodyBytes
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

// ceilings refuses the bounds the request buffer or the wire cannot hold.
// The buffer holds a request line, its headers and a body at their
// bounds together (maxBuffer), so each is kept under a quarter of the
// machine word, past which the sum overflows; MaxStreamedBodyBytes, which
// no buffer holds, may be math.MaxInt. HTTP/2 announces MaxHeaderBytes and
// MaxConcurrentStreams in SETTINGS values of 32 bits (RFC 9113 §6.5.1), so
// neither may be larger.
func ceilings(c Config) error {
	var err error
	for _, f := range [...]struct {
		name string
		v    int
	}{
		{"Config.MaxRequestLineBytes", c.MaxRequestLineBytes},
		{"Config.MaxHeaderBytes", c.MaxHeaderBytes},
		{"Config.MaxBodyBytes", c.MaxBodyBytes},
	} {
		if f.v > math.MaxInt/4 {
			err = errors.Join(err, fmt.Errorf("httpstream: %s %d is over %d: the request buffer, which holds a request line, its headers and a body together, would overflow",
				f.name, f.v, math.MaxInt/4))
		}
	}
	for _, f := range [...]struct {
		name string
		v    int
	}{
		{"Config.MaxHeaderBytes", c.MaxHeaderBytes},
	} {
		if f.v > math.MaxUint32 {
			err = errors.Join(err, fmt.Errorf("httpstream: %s %d is over %d: HTTP/2 announces it in a 32-bit SETTINGS value (RFC 9113 §6.5.1)",
				f.name, f.v, uint32(math.MaxUint32)))
		}
	}
	return err
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
