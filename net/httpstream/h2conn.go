package httpstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
)

// The HTTP/2 server: frames assembled into requests, a batch handed to the
// same [Handler] the HTTP/1.1 side takes, and responses framed back.
//
// The batch is the reason this exists. Over HTTP/1.1 it needs a client that
// pipelines; here it needs nothing — a multiplexing client has several
// requests in flight by design, so one read completes several of them and
// they go to the handler together.
//
// # Three goroutines, and what each one owns
//
// A connection runs a reader, a responder and a writer, plus one goroutine
// per handler call in flight. A batch that arrived together is one call, and
// is served together.
//
//   - The **reader** is this goroutine. It parses frames, owns the receive
//     side — stream states, the receive windows, the HPACK decoder — and
//     hands completed requests over. It never runs a handler and never waits
//     on one, because it is the goroutine that must stay free to answer PING,
//     ACK SETTINGS, and read the WINDOW_UPDATE a stalled response is waiting
//     for.
//   - Each **call** runs the handler over one batch, on a goroutine the
//     reader starts and never waits for. See [h2Conn.call].
//   - The **responder** frames what the calls return. See [h2Conn.answer].
//   - The **writer** owns the socket's write side, the send windows and the
//     scheduling. See [h2Writer].
//
// Everything between them is bounded, and the bounds are stated in
// [Config]. The one that is not a setting is the batch queue: it holds at
// most [Config.MaxConcurrentStreams] answered batches, which it can never
// exceed because every batch carries at least one request and a request
// occupies a stream slot from the moment it is admitted until its response is
// written. The same count bounds the calls in flight. That is why the reader
// can start a call without ever blocking, and the call hand its batch over
// likewise — and the reader must never block, since it is the goroutine
// carrying the credit the writer is waiting for.

// SETTINGS identifiers this server reads and sends (RFC 9113 §6.5.2).
const (
	settingHeaderTableSize   = 0x1
	settingEnablePush        = 0x2
	settingMaxConcurrent     = 0x3
	settingInitialWindowSize = 0x4
	settingMaxFrameSize      = 0x5
	settingMaxHeaderListSize = 0x6
)

// Error codes (RFC 9113 §7). Only the ones this server sends are named.
const (
	errNoError           = 0x0
	errProtocolError     = 0x1
	errInternalError     = 0x2
	errFlowControlError  = 0x3
	errStreamClosed      = 0x5
	errFrameSizeError    = 0x6
	errRefusedStream     = 0x7
	errCancel            = 0x8
	errCompressionError  = 0x9
	errEnhanceYourCalm   = 0xb
	defaultInitialWindow = 65535
)

// connRecvWindow is the floor of the connection-level receive window this
// server grants; connWindow computes the window itself.
//
// The protocol starts it at 65535, which is also where every stream starts —
// so a connection with two uploads on it runs out of connection credit before
// either stream runs out of its own, and the pacing that was meant to be per
// stream becomes a queue shared by all of them. Raising it once, at the start
// of the connection, puts the pacing back where it belongs: a stream's own
// window bounds that stream, and the connection's bounds the sum.
const connRecvWindow = 1 << 20

// connWindow is the connection-level receive window for cfg: enough for every
// admitted stream to spend its whole window at once, so that the connection's
// credit never binds before the streams' own does, and never below the floor
// the default configuration always granted.
func connWindow(cfg Config) int32 {
	sum := min(int64(cfg.MaxConcurrentStreams)*int64(cfg.InitialWindowSize), maxWindow)
	return int32(max(sum, connRecvWindow)) //nolint:gosec // G115: capped at maxWindow
}

// streamError is a failure RFC 9113 §5.4.2 ends one stream for rather than the
// connection.
//
// It is the distinction a multiplexed protocol exists to make. A client that
// sends one malformed request among a hundred healthy ones loses that one:
// before this type, it lost all hundred and the connection with them, because
// every refusal this server could express was a GOAWAY. Connection errors are
// now reserved for what they are for — framing and HPACK state, which cannot
// be resynchronised once in doubt.
type streamError struct {
	id   uint32
	code uint32
	err  error
	// status, when set, is answered on the stream before it is reset: a
	// request over one of the configured bounds gets 431 or 413 and then
	// RST_STREAM NO_ERROR, which RFC 9113 §8.1 lets a server send after a
	// complete response to stop a request it will not read; a malformed
	// request gets the 400 §8.2.1 says a server SHOULD generate, then the
	// RST_STREAM PROTOCOL_ERROR §8.1.1 requires (the response before it is
	// §8.1.1's MAY). A client behind a proxy acts on a status; a bare reset
	// tells it nothing (§10.5.1). The HTTP/1.1 reader answers 400 too.
	status int
	// head marks a refused request read as a HEAD: its status carries the
	// body's length and type and no bytes (RFC 9110 §9.3.2). Unset when
	// the refusal came before the method was read.
	head bool
}

func (e streamError) Error() string { return e.err.Error() }
func (e streamError) Unwrap() error { return e.err }

// streamFail builds a stream error whose text wraps [ErrH2Protocol] like every
// other refusal here, so a caller keeps matching on the one error it knows.
func streamFail(id, code uint32, format string, args ...any) error {
	return streamError{id: id, code: code, err: fmt.Errorf(format, args...)}
}

// statusFail builds a stream error answered with status before the reset —
// for the bounds [Config] documents a status for, nothing else. method is
// the request's when it was read before the refusal, nil otherwise.
func statusFail(id uint32, status int, method []byte, format string, args ...any) error {
	return streamError{
		id: id, code: errNoError, status: status, head: bytes.Equal(method, headMethod),
		err: fmt.Errorf(format, args...),
	}
}

// malformedFail is a malformed request's stream error (RFC 9113 §8.1.1,
// PROTOCOL_ERROR) with the 400 of §8.2.1 answered ahead of the reset. The
// reset keeps PROTOCOL_ERROR rather than §8.1's NO_ERROR: §8.1.1 names the
// code, and a client whose own END_STREAM closed the stream ignores a reset
// on it (§5.1) — Go's does, so does h2spec's verifier, which reads past the
// response to the RST_STREAM. The one §8.2.1 exception, a fault in a
// trailer section, keeps the bare reset: the response has been written or
// is being written by then.
func malformedFail(id uint32, method []byte, err error) error {
	return streamError{id: id, code: errProtocolError, status: 400, head: bytes.Equal(method, headMethod), err: err}
}

// h2State is where a stream is in RFC 9113 §5.1's lifecycle, kept to the three
// states this server can be in.
type h2State uint8

const (
	// stateOpen: the peer may still send on it.
	stateOpen h2State = iota
	// stateHalfClosed: the peer sent END_STREAM. Nothing more may arrive, and
	// this is the state that makes "DATA after the request ended" a refusal
	// rather than a body that keeps growing.
	stateHalfClosed
	// stateClosed: reset, or answered. The state is kept for a grace period
	// rather than deleted — see retire.
	stateClosed
)

// h2Stream is one request being assembled.
type h2Stream struct {
	id      uint32
	state   h2State
	req     Request
	body    []byte
	headers []Header

	// block accumulates a header block's fragments. HPACK decodes a whole
	// block or nothing: a peer may split one at any byte, including inside an
	// integer or a Huffman string, so decoding per frame refuses a client
	// that is doing nothing wrong. The byte bound is already enforced a layer
	// down by MaxHeaderBytes, so holding the fragments adds nothing
	// unbounded.
	block []byte
	// endStream remembers END_STREAM from the HEADERS frame, because the
	// CONTINUATION that closes the block carries no such flag.
	endStream bool

	gotHead bool
	// gotTrailers marks a trailer section, which ends the message whether or
	// not the peer flagged END_STREAM on it. DATA after it is malformed
	// (§8.1): a body that resumes after its own trailers is a body with no
	// stated end, bounded only by MaxBodyBytes.
	gotTrailers bool

	recvWind int32 // what we have granted the peer on this stream
	// windowGrace is the overshoot tolerated above recvWind on a stream
	// opened before the peer acknowledged this end's SETTINGS: the
	// protocol's default window less the smaller one advertised (RFC 9113
	// §6.9.3). Zero on every other stream.
	windowGrace int32

	// declared is the content-length the head stated, or -1 when it stated
	// none. §8.1.1 makes a body disagreeing with it malformed, and checking
	// is not pedantry: the field is the one statement of length an HTTP/1.1
	// hop will believe, and a hop trusting it while this end trusted the
	// DATA sum is two hops reading two different bodies out of one request.
	declared int

	// stream is the body once it streams (docs/design/adr/0009, article 4):
	// set when the head is complete, streaming is on and the body has not
	// ended by the end of the frame batch that brought the head — or has
	// outgrown MaxBodyBytes within it. handed marks it given to the handler
	// with its request: from there the stream is in flight rather than open,
	// though the peer still sends on it, and DATA goes to stream.
	stream *bodyQueue
	handed bool

	// deadline is when assembly of this request gives up — the h2 twin of
	// the HTTP/1.1 side's fixed per-request ReadTimeout. The frame reader's
	// own clock restarts on every frame it consumes, so without this a peer
	// could park a nearly-complete body forever for the price of one PING
	// per interval.
	deadline time.Time
}

// h2Batch is what the reader hands the responder: the requests one read
// completed, and the identifier each arrived on.
//
// The two slices are positional to each other, and that is the whole point of
// carrying both: a response is framed onto the stream its request came in on.
// Pairing the handler's answers with a separately ordered list of finished
// streams is how one caller receives another caller's response — completion
// order and identifier order are not the same order the moment a client
// interleaves, which is the ordinary case on a multiplexed connection.
type h2Batch struct {
	reqs []Request
	ids  []uint32
	ctx  *batchCtx
}

// h2Answer is one response on its way out: where its framed head sits in the
// responder's buffer, and what body follows.
type h2Answer struct {
	id   uint32
	from int
	// mid ends the head in s.out and starts the trailer section, which runs
	// to to; they meet when there is none.
	mid    int
	to     int
	body   []byte
	stream sluice.Stream[[]byte]
	// length and trailers are a streamed response's: the ContentLength its
	// producer is held to, and the function called once the stream is.
	length   int64
	trailers func() []Header
	// skip marks a response nothing could be framed for; its stream was reset
	// instead. job is its index in the jobs built from these answers, or -1.
	skip bool
	job  int
	// unread marks a request whose streamed body had not ended when its
	// handler returned.
	unread bool
}

// h2Conn is one HTTP/2 connection's state, read and written by the three
// goroutines the top of this file names; [Server] is the whole server, which
// holds many of them.
type h2Conn struct {
	cfg     Config
	handler Handler
	// ctx is the handler's context: cancelled when the connection is cut
	// rather than drained (see [Handler]).
	ctx context.Context
	// peer is written into every request before the handler sees it.
	peer peer
	// calls maps each stream handed to a handler to its batch's context,
	// until the batch is answered: a RST_STREAM takes the stream's request
	// out of it. The reader adds and removes, the responder removes.
	calls streamCalls[uint32]
	dec   *hpackDecoder
	w     *h2Writer

	// streams is the open streams, the ones still being assembled.
	streams map[uint32]*h2Stream
	// open counts them: written by the reader, read by the responder to know
	// whether a drained connection is finished, hence the atomic rather than
	// len(streams).
	open atomic.Int32
	// inFlight counts requests handed to the responder whose answer has not
	// yet left the writer's hands — written, dropped or given up on. open +
	// inFlight is what SETTINGS_MAX_CONCURRENT_STREAMS bounds, and the sum is
	// invariant across the hand-off — which is what makes the batch queue
	// provably big enough for the reader never to block on it, and what
	// bounds the answers the writer holds once deliver no longer waits for
	// them. Decremented by answered, on the responder or the writer.
	inFlight atomic.Int32
	// settled wakes the responder when the writer has finished an answer, so
	// that a drained connection whose last answer the writer, not the
	// responder, finished is still closed.
	settled chan struct{}
	// lastID is the highest stream the client has opened. Stream identifiers
	// must increase, and one that goes backwards is a client reusing a
	// closed stream rather than opening a new one.
	lastID uint32
	// goAwayLast is the last stream identifier the connection has announced
	// in a GOAWAY so far, 2^31-1 until one is sent: RFC 9113 §6.8 forbids
	// raising it, and a stream refused after the drain's second GOAWAY
	// still advances lastID. Atomic because beginDrain runs on the drain's
	// own goroutine while the reader may be sending an error's GOAWAY.
	goAwayLast atomic.Uint32
	// settingsAcked is set once the peer has acknowledged this end's
	// SETTINGS. Before that, a stream the peer opens is sized by the
	// protocol's default window, whatever InitialWindowSize says (§6.9.2),
	// so its first flight may legitimately exceed a smaller one (§6.9.3).
	settingsAcked bool
	// opened is one bit per odd identifier, for the h2OpenedWindow most
	// recent ones: set for a stream the client opened, clear for one it
	// skipped. It is what tells a frame on a stream this end reset and has
	// since forgotten (tolerated, §5.1 "closed") from a frame on a stream
	// the client never opened (a connection error, §5.1.1), once the
	// closed-stream ring no longer knows. See stream.
	opened [h2OpenedWords]uint64
	// peerMaxHeaderList is the peer's SETTINGS_MAX_HEADER_LIST_SIZE, zero
	// until it sends one (the initial value is unlimited). Written by the
	// reader, read by the responder.
	peerMaxHeaderList atomic.Uint32
	// closed is the streams retired from streams and still remembered,
	// half-closed or closed; closedRing is their identifiers in the order
	// they were retired, oldest first. See retire.
	closed     map[uint32]h2State
	closedRing []uint32
	// late is the stream a header block on a retired stream is assembled
	// in. One is enough: a header block is never interleaved with another
	// (§6.10), so only one is ever arriving.
	late h2Stream

	// connWind is the connection window this end grants, connWindow(cfg);
	// recvWind is how much of it the peer has not yet spent.
	connWind int32
	recvWind int32
	// assembling is the sum of the bodies still being assembled across all
	// open streams — what the aggregate 4×MaxBodyBytes bound (see
	// [Config.MaxBodyBytes]) is measured against. Windows are replenished
	// as DATA is buffered, so this, not flow control, is what bounds the
	// memory a peer can park by never finishing its uploads.
	assembling int
	// pending is the streams whose head completed in this frame batch with
	// their body still to come, when bodies stream: at the end of the batch
	// each one still open is handed over with a streamed body. handed counts
	// the streams handed over and still in streams.
	pending []*h2Stream
	handed  int
	// expired is the deadline sweep's scratch, reused across batches.
	expired []uint32
	// resets and served are the rapid-reset bound; the RST_STREAM branch
	// states what the bound means.
	resets int
	served int
	// discard is where a header block nobody will read is decoded to and
	// left — one for a stream already ended or reset, which must still go
	// through the decoder (see headers). It is reused rather than allocated
	// per block, and it is never a request's.
	discard []Header

	goingAway bool

	// drainStep is where the graceful shutdown stands (see serveH2Conn):
	// drainNone, then drainBegun once the first GOAWAY and the PING are
	// queued, then drainAnnounced once the PING's acknowledgement has had
	// the second GOAWAY sent. Written by the drain's function and the
	// reader, read by the responder.
	drainStep atomic.Int32
	// greeted settles the race between the peer's first frame and a drain
	// that begins before it: greetPending until one of them takes it, then
	// greetTaken when the reader has a SETTINGS to apply, greetRefused when
	// the drain gave up waiting for one (see beginDrain). Whichever swaps
	// first decides; the other steps back. greetWait is the drain's timer
	// for that wait, stopped when the reader ends.
	greeted   atomic.Int32
	greetWait atomic.Pointer[time.Timer]
	// drained is set just before the socket of a drained connection is
	// closed — by the responder after the last answer, by beginDrain for a
	// peer that never got past its preface — so the reader knows the error
	// that unparks it is the close and not a failure. Swapped, never stored:
	// the two must not both close.
	drained atomic.Bool

	// readEnded is set by the reader once it has stopped reading — the peer
	// closed, the connection was cancelled, or a connection error ended it.
	// A streamed producer is still being pulled then, and this is how it
	// learns there is nobody left to read its answer.
	readEnded atomic.Bool

	// The responder's own scratch, reused across batches. Nothing here is
	// touched by the reader: the responder is the only goroutine in answer.
	// The framed heads are not among it: they go out after answer returns,
	// so each batch frames into an h2Sent the writer hands back when done.
	block   []byte
	names   []byte // lowercased copies of the response field names a handler spelled in HTTP/1.1 case
	answers []h2Answer
	// date is the batch's Date value, read from the clock once in answer
	// (time.Now is 35 ns here, a fifth of a head) and from here by
	// appendHead, whose signature the tests and the benchmark call.
	date []byte
	// streamBody's state, kept here rather than in locals its pull closure
	// would move to the heap on every call: two jobs, alternating — one may
	// be queued or in flight while the other, done, is ready to be reused
	// with its buffer (see h2Writer.extend for why two and never more) — the
	// one the last batch went to, and the scratch a multi-chunk batch is
	// flattened into.
	// heads is the head job of each streamed answer of the batch answer is
	// on, read once deliver has taken the batch.
	heads      []*h2Job
	streamJobs [2]h2Job
	streamCur  *h2Job
	streamBuf  []byte
	// streamTail is the streamed response's framed trailer section, held
	// until finish has seen it written.
	streamTail []byte
}

// maxWindow is the largest a flow-control window may be (RFC 9113 §6.9.1).
const maxWindow = 1<<31 - 1

// ServeH2Conn runs one HTTP/2 connection whose client preface has already been
// read, or is about to be.
//
// It is prior-knowledge HTTP/2: the client has been told to speak HTTP/2
// straight away, by ALPN or by configuration, and there is no upgrade dance.
// [Serve] hands it both the connections a TLS listener negotiated to h2 and
// the cleartext ones whose preface it sniffed. The cleartext form — h2c on a
// bare socket — is kept for interoperability, mirroring [Serve]'s own label:
// a terminating proxy or local tooling, not a network boundary. Terminating
// TLS is still not this function's job; a [net.Conn] from a [crypto/tls]
// listener arrives here already decrypted.
//
// The cancellation of ctx stops the connection normally, as [Serve] stops
// the connections it accepted (ADR 0017): a GOAWAY
// and a PING, the streams already open answered whole, then the close. When
// [Config.ShutdownGrace] passes before the drain ends, the connection is
// closed under its handler, whose context is cancelled, the [Config.Logger]
// is told, and ServeH2Conn returns [ErrShutdownForced]. The handler's context
// carries ctx's values but not its cancellation, which is the normal stop
// and not the handler's to see.
//
// ServeH2Conn owns c once it has accepted cfg: c is closed before ServeH2Conn
// returns, whatever ended the connection — the peer's own close, the idle
// timeout, a drain, the grace, or a connection error, after whose GOAWAY
// RFC 9113 §5.4.1 says the endpoint MUST close the TCP connection. A caller
// has nothing to close and nothing to reuse, as under [Serve].
//
// cfg is the [Config] [Serve] takes: ServeH2Conn reads the fields that say
// they bound HTTP/2 and ignores the others. A configuration it refuses is
// returned as an error before a byte is read, and the connection is left to
// the caller, open.
func ServeH2Conn(ctx context.Context, c net.Conn, cfg Config, handle Handler) error {
	cfg, err := cfg.resolve()
	if err != nil {
		return err
	}
	// Runs last: the grace timer's close, when it fired, has gone first and
	// this one is a no-op on a closed socket. A connection error has already
	// half-closed and drained the socket (linger) by the time it gets here.
	defer func() { _ = c.Close() }()
	setTCPKeepAlive(c, cfg.KeepAlive)
	// The grace is a timer armed at the cancellation, as in ServeH3Conn. When it
	// passes it closes the socket, which unwinds the connection's goroutines
	// through their ordinary error paths, and only then cancels the
	// handler's context: the other order lets a handler that returns at its
	// cancellation have its answer written before the close. cut is set
	// before either, since serveH2Conn may return between the two.
	forced, force := context.WithCancel(context.WithoutCancel(ctx))
	defer force()
	var graceTimer atomic.Pointer[time.Timer]
	var cut atomic.Bool
	stop := context.AfterFunc(ctx, func() {
		graceTimer.Store(time.AfterFunc(cfg.ShutdownGrace, func() {
			cut.Store(true)
			_ = c.Close()
			force()
		}))
	})
	defer func() {
		stop()
		// Stopped once ServeH2Conn has returned, when nobody reads it.
		if t := graceTimer.Load(); t != nil {
			t.Stop()
		}
	}()
	err = serveH2Conn(forced, c, &connState{drain: ctx}, cfg, handle)
	if cut.Load() {
		cfg.logger().Warn("httpstream: the shutdown grace passed with requests in flight",
			"connections", 1, "grace", cfg.ShutdownGrace)
		return ErrShutdownForced
	}
	return err
}

// serveH2Conn is [ServeH2Conn] once its two stops are set: ctx is the forced
// stop, which closes the socket through its owner's bookkeeping — ServeH2Conn's
// grace timer, or a [Server]'s — and reaches the handler through its
// context, and st.drain is the normal stop, which takes the connection
// through RFC 9113 §6.8's graceful close rather than the socket's —
//
//   - first a GOAWAY with the last stream identifier at 2^31−1 and NO_ERROR,
//     which tells the peer to open nothing more, and a PING, which measures
//     the round trip during which its streams already on the wire arrive;
//   - at the PING's acknowledgement, a second GOAWAY naming the last stream
//     actually taken, after which a new stream is refused (REFUSED_STREAM,
//     the code that says it may be retried elsewhere);
//   - when no stream is open or in flight, the close — by the reader if that
//     is already true at the second GOAWAY, by the responder when it has
//     written the last answer.
//
// A peer still short of the SETTINGS that ends its preface is told the same
// way and given LingerTimeout to send it; without it, a GOAWAY naming stream
// 0 and the close (see beginDrain).
//
// The reader also keeps the state's phase: busy while a stream is open or in
// flight, idle otherwise, which is what a forced stop counts as cut. The
// drain tells every HTTP/2 connection rather than closing an idle one (ADR
// 0017). The phase is the reader's view at
// its last batch, which runs late when the last answer went out and the peer
// has since said nothing: that connection is drained like a busy one and
// closes one round trip after the cancellation rather than at it. Writing the
// phase from the responder too would make it exact, and would also let an
// idle verdict land after a stream the reader has just opened; late is the
// safe side. A peer that never acknowledges the PING holds the connection
// until the grace closes it, which is the bound every drain has. A nil state
// is a test driving the connection directly, which nothing drains.
//
// serveH2Conn runs in this order: build the connection's state and start the
// responder goroutine; read the preface and send SETTINGS (hello); register
// with the drain; start the goroutine that runs one handler call per applied
// request; read the frames until the connection ends — each batch consumed
// (consume), the phase marked before a handler starts, the ready requests
// handed to the calls, the drain's GOAWAY and PING sent when idle; then wake
// the handlers waiting on a streamed body, leave the drain's count, wait for
// the calls and the responder, send the last GOAWAY with the code the end
// deserves, and linger on an error so that GOAWAY is read.
func serveH2Conn(ctx context.Context, c net.Conn, st *connState, cfg Config, handler Handler) error {
	s := &h2Conn{
		cfg:      cfg,
		handler:  handler,
		ctx:      ctx,
		dec:      newHPACKDecoder(4096, cfg.MaxHeaderBytes),
		streams:  make(map[uint32]*h2Stream),
		closed:   make(map[uint32]h2State),
		connWind: connWindow(cfg),
		recvWind: connWindow(cfg),
		w:        newH2Writer(c, cfg),
		settled:  make(chan struct{}, 1),
	}
	s.goAwayLast.Store(maxStreamID)
	s.w.answered = s.answered

	go s.w.run()
	if err := s.hello(); err != nil {
		s.w.stop()
		<-s.w.done
		return err
	}
	// After the preface, so that a TLS handshake is done.
	s.peer = peerOf(c)
	if st != nil {
		// Registered after hello has queued the SETTINGS: a drain already
		// under way runs beginDrain at once, on its own goroutine, and its
		// GOAWAY must not reach the writer first (RFC 9113 §3.4: SETTINGS
		// is the first frame the server sends).
		stop := context.AfterFunc(st.drain, s.beginDrain)
		defer stop()
	}

	// Capacity MaxConcurrentStreams: see the package note above for why a
	// send on this channel can never block, and why it must not. Pointers,
	// because the buffer is allocated whole with every connection and an
	// h2Applied is 88 bytes: 8.8 KB at the default, most of what an idle
	// HTTP/2 connection held beyond an HTTP/1.1 one.
	in := make(chan *h2Applied, cfg.MaxConcurrentStreams)
	var calls sync.WaitGroup
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		for {
			select {
			case a, ok := <-in:
				if !ok {
					return
				}
				s.answer(a)
			case <-s.settled:
			}
			// Idle after a burst: the answers kept for the largest batch and
			// the writer's batches let go (see h2Writer.shed), read in the
			// same order as the drained check below.
			if cap(s.answers) > h2KeptJobs && s.open.Load() == 0 && s.inFlight.Load() == 0 {
				s.answers = nil
				s.w.shed()
			}
			// The last answer of a drained connection closes it, whether the
			// responder or the writer finished it. open is read before
			// inFlight because finish moves a stream the other way round,
			// inFlight up then open down: read in this order, a stream
			// mid-hand-off is counted at least once. Nothing opens after the
			// second GOAWAY, so both at zero is final. The writer goes first
			// so the answer is on the wire before the socket closes under the
			// reader.
			if s.drainStep.Load() == drainAnnounced && s.open.Load() == 0 && s.inFlight.Load() == 0 && s.drained.CompareAndSwap(false, true) {
				s.w.stop()
				<-s.w.done
				_ = c.Close()
			}
		}
	}()

	// The idle clock does not end the connection while this end owes the
	// peer answers: a streamed response may take minutes during which a
	// well-behaved client has nothing to send, and killing it for silence
	// would be this server refusing what it just built. A half-finished
	// request is a different matter and stays on ReadTimeout, which is the
	// slowloris bound.
	src := framesWith(c, cfg, func() bool { return s.inFlight.Load() > 0 })
	var fatal error
	greeted := false
	src.Stream()(func(b sluice.Batch[h2Frame]) bool {
		if !greeted && len(b.Items) > 0 {
			if !s.greeted.CompareAndSwap(greetPending, greetTaken) {
				// The drain gave up on this peer before its SETTINGS and told
				// it no stream was taken: none may be now.
				return false
			}
			greeted = true
		}
		ready, ids, err := s.consume(b.Items)
		if err != nil {
			fatal = err
			return false
		}
		// The phase is set before the handler starts: marked after, a
		// handler could be running while a forced stop read the connection
		// as idle and reported no cut.
		live := s.open.Load() > 0 || s.inFlight.Load() > 0
		if live {
			st.set(connBusy)
		} else {
			st.set(connIdle)
		}
		if len(ready) > 0 {
			b := h2Batch{reqs: ready, ids: ids, ctx: s.calls.register(s.ctx, ids)}
			calls.Go(func() { s.call(b, in) })
		}
		if !live && s.drainStep.Load() == drainAnnounced {
			// Drained with nothing left to answer: the second GOAWAY is out
			// and this end owes nothing more, so this is the clean close.
			return false
		}
		// A peer's GOAWAY does not stop the reader. While answers are still
		// owed, the WINDOW_UPDATE and PING an in-flight response depends on
		// arrive on this goroutine and nowhere else, so stopping at the
		// GOAWAY stalled every response larger than its remaining send
		// window until WriteTimeout reset it — a client that said "finish
		// what you have and close" got its streams killed instead. Once
		// nothing is owed it keeps reading too, until the peer hangs up or
		// IdleTimeout does, and the GOAWAY this end owes (§6.8) goes out at
		// that close as on every other. Closing here instead fails h2spec
		// (generic/3.8/1): a peer whose PING follows its GOAWAY on the wire
		// has the PING sitting unread in the socket when the close happens,
		// which TCP answers with a reset — the peer sees ECONNRESET on a
		// connection it ended cleanly, or a clean close, depending on which
		// arrives first. New streams are refused either way (see
		// stream).
		return true
	})
	s.readEnded.Store(true)
	if t := s.greetWait.Load(); t != nil {
		t.Stop()
	}
	// A handler waiting for a streamed body's bytes learns that none are
	// coming before it is waited for.
	for _, st := range s.streams {
		if st.stream != nil {
			st.stream.fail(errBodyConnClosed)
		}
	}

	told := false
	if fatal != nil {
		// RFC 9113 §5.4.1: a connection error is told first, with GOAWAY,
		// then the connection closes. Queued now, before the handlers in
		// flight are waited for: a peer whose connection is condemned
		// learns it at once, not when the slowest handler returns.
		s.goAway(codeFor(fatal))
		told = true
	}
	// The client is gone, or the connection failed: no request in flight
	// has anyone left to answer to, and their handlers learn it before they
	// are waited for.
	s.calls.leaveAll()
	// The calls still running are answered before the responder goes, so
	// that every request is answered once, as ever.
	calls.Wait()
	close(in)
	<-answered

	err := errors.Join(fatal, ignoreEOF(src.Err()))
	if s.drained.Load() {
		// The responder closed the socket under this reader once the last
		// answer was written; the read error that unparked it is that close.
		err = nil
	}
	code := uint32(errNoError)
	if err != nil {
		code = codeFor(err)
	}
	if !told && (err != nil || s.drainStep.Load() < drainAnnounced) {
		// A drained connection has already said its last GOAWAY, and a
		// third one would only repeat it; a failure after it is still told.
		s.goAway(code)
	}
	s.w.stop()
	<-s.w.done
	if err != nil {
		// The peer may still be writing — the frames behind the one that
		// failed, or a flood. Closing a socket with unread bytes in it makes
		// TCP answer the next segment with a reset, which can erase the
		// GOAWAY the peer had not read yet: the two-stage close HTTP/1.1
		// uses (RFC 9112 §9.6, tearDown) applies here too.
		linger(c, cfg)
	}
	return errors.Join(err, s.w.failure())
}

// ignoreEOF drops the two clean ends of a connection: the peer closing it,
// and the idle timeout closing it for the peer. Neither is a failure, and an
// idle close is GOAWAY(NO_ERROR) — RFC 9113 §6.8's graceful shutdown — not
// the INTERNAL_ERROR an unexplained error maps to.
func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, errIdleTimeout) {
		return nil
	}
	return err
}

// errFlowControl marks the failures RFC 9113 §6.9 answers with
// FLOW_CONTROL_ERROR rather than PROTOCOL_ERROR. It is joined to
// [ErrH2Protocol] rather than exported: a caller keeps handling the one
// protocol error it already knows, and only the code on the wire changes.
var errFlowControl = errors.New("flow control")

// errFrameSize marks the failures RFC 9113 §4.2 and §6 answer with
// FRAME_SIZE_ERROR rather than PROTOCOL_ERROR: a frame over the advertised
// maximum, and a fixed-length frame type of the wrong length. Like
// [errFlowControl] it is joined to [ErrH2Protocol] rather than exported —
// callers keep matching the one error they know; only the code on the wire
// changes.
var errFrameSize = errors.New("frame size")

// errCalm marks the rapid-reset bound tripping, which RFC 9113 §5.4.1 and
// every CVE-2023-44487 mitigation answer with ENHANCE_YOUR_CALM: the peer did
// nothing malformed, it did too much of something legal.
var errCalm = errors.New("enhance your calm")

func codeFor(err error) uint32 {
	switch {
	case errors.Is(err, errFlowControl):
		return errFlowControlError
	case errors.Is(err, errFrameSize):
		return errFrameSizeError
	case errors.Is(err, errCalm):
		return errEnhanceYourCalm
	case errors.Is(err, ErrHPACK):
		return errCompressionError
	case errors.Is(err, ErrTooLarge):
		return errFrameSizeError
	case errors.Is(err, ErrH2Protocol):
		return errProtocolError
	}
	return errInternalError
}

// hello sends the settings this end imposes. They are sent before anything is
// read, because a peer may start sending on the defaults immediately and the
// only way to bound it is to have said so first.
func (s *h2Conn) hello() error {
	var payload []byte
	settings := [][2]uint32{
		{settingEnablePush, 0}, // a server does not push here
		{settingMaxConcurrent, uint32(s.cfg.MaxConcurrentStreams)},  //nolint:gosec // G115: bounded by Config.resolve
		{settingInitialWindowSize, uint32(s.cfg.InitialWindowSize)}, //nolint:gosec // G115: same
		{settingMaxFrameSize, uint32(s.cfg.MaxFrameSize)},           //nolint:gosec // G115: same
		{settingMaxHeaderListSize, uint32(s.cfg.MaxHeaderBytes)},    //nolint:gosec // G115: same
	}
	for _, kv := range settings {
		payload = binary.BigEndian.AppendUint16(payload, uint16(kv[0])) //nolint:gosec // G115: setting identifiers are 16 bits
		payload = binary.BigEndian.AppendUint32(payload, kv[1])
	}
	if err := s.w.control(h2Frame{Type: frameSettings, StreamID: 0, Payload: payload}); err != nil {
		return err
	}
	// And the connection window this end grants, which SETTINGS cannot carry:
	// SETTINGS_INITIAL_WINDOW_SIZE is per stream, and the connection's is only
	// ever moved by WINDOW_UPDATE.
	return s.w.control32(frameWindowUpdate, 0, uint32(s.connWind-defaultInitialWindow)) //nolint:gosec // G115: connWind is at least connRecvWindow
}

// goAway ends the connection with the code the failure maps to, and with no
// debug data.
//
// GOAWAY's trailing field is free-form and goes to whoever dialled the socket.
// What was sent there was the wrapped error text, and one of those carries a
// handler's panic value (see apply) — an internal detail handed to a stranger
// who caused the failure. The code is what a peer acts on; the text stays on
// this side, where the caller reads it as ServeH2Conn's return value.
func (s *h2Conn) goAway(code uint32) {
	s.goAwayWith(s.lastID, code)
}

// goAwayWith is goAway with the last stream identifier chosen by the caller:
// the graceful shutdown's first GOAWAY names 2^31−1, a stream the peer can
// never have reached, before the reader knows the real one (RFC 9113 §6.8).
// Whatever the caller names, the identifier sent never rises above one
// already announced (§6.8 MUST NOT): the peer may have retried the streams
// past it elsewhere.
func (s *h2Conn) goAwayWith(last, code uint32) {
	for {
		announced := s.goAwayLast.Load()
		if last >= announced {
			last = announced
			break
		}
		if s.goAwayLast.CompareAndSwap(announced, last) {
			break
		}
	}
	payload := make([]byte, 8)
	be32put(payload[:4], last)
	be32put(payload[4:8], code)
	_ = s.w.control(h2Frame{Type: frameGoAway, Payload: payload})
}

// The steps of a graceful shutdown, in drainStep; see serveH2Conn.
const (
	drainNone int32 = iota
	drainBegun
	drainAnnounced
)

// The states of h2Conn.greeted.
const (
	greetPending int32 = iota
	greetTaken
	greetRefused
)

// drainPing is the opaque data of the PING the shutdown sends after its
// first GOAWAY, so its acknowledgement is told apart from any other.
const drainPing = "shutdown"

// maxStreamID is the highest stream identifier the protocol has (RFC 9113
// §5.1.1), and what the first GOAWAY of a graceful shutdown names.
const maxStreamID = 1<<31 - 1

// beginDrain is the shutdown's first step, run by the drain's
// [context.AfterFunc] on a goroutine of its own: the GOAWAY that forbids new
// streams without yet naming the last one taken, and the PING whose
// acknowledgement will. Both are queued before the step is recorded, so the
// second GOAWAY the reader sends at the acknowledgement cannot be queued
// ahead of the first (§6.8: the identifier sent must never increase).
//
// A peer that has sent the preface and not yet the SETTINGS §3.4 makes follow
// it has opened nothing, and its PING acknowledgement would come, if ever,
// behind that SETTINGS: waiting for it held the shutdown to its grace for 24
// silent bytes, where ADR 0017 closes an accepted and silent connection at
// once. Deciding at once would be wrong the other way: a client sends its
// preface and SETTINGS together, and a drain that lands between the two reads
// would cut a connection whose SETTINGS, and perhaps a request, are already
// in the socket — unread bytes a close answers with a reset that can erase
// the GOAWAY. So the peer is told as any other is and given LingerTimeout,
// the time this package gives a closing connection to say its last bytes,
// for its first frame; with none by then, refuseSilent takes the connection.
func (s *h2Conn) beginDrain() {
	s.goAwayWith(maxStreamID, errNoError)
	_ = s.w.control(h2Frame{Type: framePing, Payload: []byte(drainPing)})
	s.drainStep.Store(drainBegun)
	if s.greeted.Load() == greetPending {
		s.greetWait.Store(time.AfterFunc(s.cfg.LingerTimeout, s.refuseSilent))
	}
}

// refuseSilent closes a draining connection whose peer never followed its
// preface: a second GOAWAY names stream 0 — nothing was processed, so a
// request crossing the close may be retried as is — and the socket closes
// once the writer has sent it. The reader's first batch races this on
// greeted, so a stream is either taken before the decision or never.
func (s *h2Conn) refuseSilent() {
	if !s.greeted.CompareAndSwap(greetPending, greetRefused) {
		return
	}
	s.goAwayWith(0, errNoError)
	s.drainStep.Store(drainAnnounced)
	if s.drained.CompareAndSwap(false, true) {
		s.w.stop()
		<-s.w.done
		_ = s.w.conn.Close()
	}
}

func be32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func be32put(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v) //nolint:gosec // G115: big-endian serialisation keeps the low octet of each shift
}
