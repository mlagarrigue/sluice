package httpstream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
// A connection runs a reader, a responder and a writer, and no more: the
// concurrency is per connection, not per request, so a batch that arrived
// together is still served together.
//
//   - The **reader** is this goroutine. It parses frames, owns the receive
//     side — stream states, the receive windows, the HPACK decoder — and
//     hands completed requests over. It never runs a handler and never waits
//     on one, because it is the goroutine that must stay free to answer PING,
//     ACK SETTINGS, and read the WINDOW_UPDATE a stalled response is waiting
//     for.
//   - The **responder** runs the handler and frames what it returns. See
//     [h2Server.answer].
//   - The **writer** owns the socket's write side, the send windows and the
//     scheduling. See [h2Writer].
//
// Everything between them is bounded, and the bounds are stated in
// [H2Config]. The one that is not a setting is the batch queue: it holds at
// most [H2Config.MaxConcurrentStreams] batches, which it can never exceed
// because every batch carries at least one request and a request occupies a
// stream slot from the moment it is admitted until its response is written.
// That is why the reader can hand a batch over without ever blocking — and it
// must never block, since it is the goroutine carrying the credit the writer
// is waiting for.

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

// connRecvWindow is the connection-level receive window this server grants.
//
// The protocol starts it at 65535, which is also where every stream starts —
// so a connection with two uploads on it runs out of connection credit before
// either stream runs out of its own, and the pacing that was meant to be per
// stream becomes a queue shared by all of them. Raising it once, at the start
// of the connection, puts the pacing back where it belongs: a stream's own
// window bounds that stream, and the connection's bounds the sum.
const connRecvWindow = 1 << 20

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
}

func (e streamError) Error() string { return e.err.Error() }
func (e streamError) Unwrap() error { return e.err }

// streamFail builds a stream error whose text wraps [ErrH2Protocol] like every
// other refusal here, so a caller keeps matching on the one error it knows.
func streamFail(id, code uint32, format string, args ...any) error {
	return streamError{id: id, code: code, err: fmt.Errorf(format, args...)}
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
	// rather than deleted — see closedRing.
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
	// down by MaxHeaderBlockBytes, so holding the fragments adds nothing
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

	// declared is the content-length the head stated, or -1 when it stated
	// none. §8.1.1 makes a body disagreeing with it malformed, and checking
	// is not pedantry: the field is the one statement of length an HTTP/1.1
	// hop will believe, and a hop trusting it while this end trusted the
	// DATA sum is two hops reading two different bodies out of one request.
	declared int

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
}

// h2Answer is one response on its way out: where its framed head sits in the
// responder's buffer, and what body follows.
type h2Answer struct {
	id     uint32
	from   int
	to     int
	body   []byte
	stream sluice.Stream[[]byte]
	// skip marks a response nothing could be framed for; its stream was reset
	// instead. job is its index in the jobs built from these answers, or -1.
	skip bool
	job  int
}

type h2Server struct {
	cfg     H2Config
	handler Handler
	dec     *hpackDecoder
	w       *h2Writer

	streams map[uint32]*h2Stream
	// open counts the streams the peer may still send on. It is not
	// len(streams), which also holds the ones kept for the closed-stream
	// grace period.
	open int
	// inFlight counts requests handed to the responder and not yet answered.
	// open + inFlight is what SETTINGS_MAX_CONCURRENT_STREAMS bounds, and the
	// sum is invariant across the hand-off — which is what makes the batch
	// queue provably big enough for the reader never to block on it.
	inFlight atomic.Int32
	// lastID is the highest stream the client has opened. Stream identifiers
	// must increase, and one that goes backwards is a client reusing a
	// closed stream rather than opening a new one.
	lastID uint32
	// peerMaxHeaderList is the peer's SETTINGS_MAX_HEADER_LIST_SIZE, zero
	// until it sends one (the initial value is unlimited). Written by the
	// reader, read by the responder.
	peerMaxHeaderList atomic.Uint32
	// closedRing is the closed streams in the order they closed, oldest
	// first. See closeStream.
	closedRing []uint32

	recvWind int32 // the connection window we have granted and not topped up
	// assembling is the sum of the bodies still being assembled across all
	// open streams — what the aggregate 4×MaxBodyBytes bound (see
	// [H2Config.MaxBodyBytes]) is measured against. Windows are replenished
	// as DATA is buffered, so this, not flow control, is what bounds the
	// memory a peer can park by never finishing its uploads.
	assembling int
	// expired is the deadline sweep's scratch, reused across batches.
	expired []uint32
	// resets and served are the rapid-reset bound; the RST_STREAM branch
	// states what the bound means.
	resets int
	served int
	// trailers is where a trailing header block is decoded to and left. It is
	// reused rather than allocated per block, and it is never a request's.
	trailers []Header

	goingAway bool

	// readEnded is set by the reader once it has stopped reading — the peer
	// closed, the connection was cancelled, or a connection error ended it.
	// A streamed producer is still being pulled then, and this is how it
	// learns there is nobody left to read its answer.
	readEnded atomic.Bool

	// The responder's own scratch, reused across batches. Nothing here is
	// touched by the reader: the responder is the only goroutine in answer.
	out       []byte
	block     []byte
	answers   []h2Answer
	jobs      []h2Job
	ptrs      []*h2Job
	one       []*h2Job
	chunk     h2Job
	streamBuf []byte // streamBody's scratch: one producer batch, framed as one job
}

// maxWindow is the largest a flow-control window may be (RFC 9113 §6.9.1).
const maxWindow = 1<<31 - 1

// ServeH2 runs one HTTP/2 connection whose preface has already been read by
// [h2Frames], or is about to be.
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
// ctx cancels the connection. It reaches all three of its goroutines through
// the socket: closing it unblocks the read the reader is parked in and the
// write the writer is parked in, and both then unwind through their ordinary
// error paths. That is [context.AfterFunc] rather than a fourth goroutine
// parked on ctx.Done() for the life of every connection.
func ServeH2(ctx context.Context, c net.Conn, cfg H2Config, handler Handler) error {
	cfg = cfg.withDefaults()
	s := &h2Server{
		cfg:      cfg,
		handler:  handler,
		dec:      newHPACKDecoder(4096, cfg.MaxHeaderBlockBytes),
		streams:  make(map[uint32]*h2Stream),
		recvWind: connRecvWindow,
		w:        newH2Writer(c, cfg),
		one:      make([]*h2Job, 1),
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()

	go s.w.run()
	if err := s.hello(); err != nil {
		s.w.stop()
		<-s.w.done
		return err
	}

	// Capacity MaxConcurrentStreams: see the package note above for why a
	// send on this channel can never block, and why it must not.
	in := make(chan h2Batch, cfg.MaxConcurrentStreams)
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		for b := range in {
			s.answer(b)
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
	src.Stream()(func(b sluice.Batch[h2Frame]) bool {
		ready, ids, err := s.consume(b.Items)
		if err != nil {
			fatal = err
			return false
		}
		if len(ready) > 0 {
			in <- h2Batch{reqs: ready, ids: ids}
		}
		// A peer's GOAWAY does not stop the reader while answers are still
		// owed: the WINDOW_UPDATE and PING an in-flight response depends on
		// arrive on this goroutine and nowhere else, so stopping at the
		// GOAWAY stalled every response larger than its remaining send
		// window until WriteTimeout reset it — a client that said "finish
		// what you have and close" got its streams killed instead. New
		// streams are refused (see stream); the reader stops once nothing is
		// owed.
		return !s.goingAway || s.open > 0 || s.inFlight.Load() > 0
	})
	s.readEnded.Store(true)

	close(in)
	<-answered

	err := errors.Join(fatal, ignoreEOF(src.Err()))
	code := uint32(errNoError)
	if err != nil {
		code = codeFor(err)
	}
	s.goAway(code)
	s.w.stop()
	<-s.w.done
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
func (s *h2Server) hello() error {
	var payload []byte
	settings := [][2]uint32{
		{settingEnablePush, 0}, // a server does not push here
		{settingMaxConcurrent, uint32(s.cfg.MaxConcurrentStreams)},    //nolint:gosec // G115: bounded by withDefaults
		{settingMaxFrameSize, uint32(s.cfg.MaxFrameSize)},             //nolint:gosec // G115: same
		{settingMaxHeaderListSize, uint32(s.cfg.MaxHeaderBlockBytes)}, //nolint:gosec // G115: same
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
	return s.w.control32(frameWindowUpdate, 0, connRecvWindow-defaultInitialWindow)
}

// consume folds a batch of frames into stream state and returns the requests
// that became complete, with the identifier each arrived on.
//
// A stream error does not end the loop: the offending stream is reset and the
// frames behind it — which belong to other requests — are served. That is the
// difference the split made, and the reason the loop looks like this rather
// than returning at the first refusal.
func (s *h2Server) consume(frames []h2Frame) ([]Request, []uint32, error) {
	var (
		ready []Request
		ids   []uint32
	)
	for _, f := range frames {
		st, err := s.frame(f)
		if err != nil {
			if se, ok := errors.AsType[streamError](err); ok {
				if err := s.reset(se); err != nil {
					return nil, nil, err
				}
				continue
			}
			return nil, nil, err
		}
		if st != nil {
			ready = append(ready, st.req)
			ids = append(ids, st.id)
			// The request is the responder's now. The stream outlives it in
			// the closed-stream ring, and a copy left here would pin its
			// body and headers there for MaxConcurrentStreams more closes.
			st.req = Request{}
		}
	}
	if err := s.expireAssembly(); err != nil {
		return nil, nil, err
	}
	return ready, ids, nil
}

// expireAssembly resets every stream whose request has been assembling for
// longer than ReadTimeout.
//
// It runs once per frame batch, which is exactly often enough: the only way a
// stalled stream's memory is held is by the peer sending *something* — a PING,
// a WINDOW_UPDATE, a byte of another stream — to keep the frame-level clock
// alive, and each of those somethings arrives through consume. A peer that
// sends nothing at all is the frame reader's own timeout's to end.
func (s *h2Server) expireAssembly() error {
	if s.open == 0 {
		return nil
	}
	now := time.Now()
	s.expired = s.expired[:0]
	for id, st := range s.streams {
		if st.state == stateOpen && now.After(st.deadline) {
			s.expired = append(s.expired, id)
		}
	}
	for _, id := range s.expired {
		err := s.reset(streamError{id: id, code: errCancel, err: fmt.Errorf(
			"%w: stream %d assembled nothing complete within the read timeout", ErrH2Protocol, id)})
		if err != nil {
			return err
		}
	}
	return nil
}

// reset ends one stream: RST_STREAM to the peer, the state closed on this
// side, and whatever the responder had queued for it dropped.
//
// It counts against the same allowance a peer's own RST_STREAM does. A peer
// that only ever sends malformed requests would otherwise hold a connection
// open indefinitely for the cost of one refusal each, which is the rapid-reset
// shape with the roles swapped.
func (s *h2Server) reset(se streamError) error {
	if err := s.countReset(); err != nil {
		return fmt.Errorf("%w: %w", err, se.err)
	}
	s.retire(se.id, stateClosed)
	s.w.forget(se.id)
	return s.w.control32(frameRSTStream, se.id, se.code)
}

// countReset charges one stream reset, from either end, against the
// rapid-reset allowance: four batches' worth of frames, plus two for every
// request this connection has completed. The RST_STREAM branch of frame
// states what "completed" has to mean for the bound to hold.
func (s *h2Server) countReset() error {
	s.resets++
	if s.resets > s.cfg.MaxFramesPerBatch*4+2*s.served {
		return fmt.Errorf("%w: %w: %d stream resets against %d completed requests",
			ErrH2Protocol, errCalm, s.resets, s.served)
	}
	return nil
}

// idle reports a stream the client has not opened (RFC 9113 §5.1): an even
// identifier, which only a server opens and this one opens none, or an odd
// one past the highest the client has used. A stream that has fallen out of
// the closed-stream ring is not idle — its identifier is at or below lastID.
func (s *h2Server) idle(id uint32) bool {
	return id%2 == 0 || id > s.lastID
}

// retire moves a stream out of the set the peer may still send on, and
// remembers it for a while.
//
// The two destinations are not the same news. A request that arrived whole is
// half-closed: nothing more may be sent on it, and DATA that arrives anyway is
// the refusal RFC 9113 §5.1 asks for — a body that resumes after its own
// END_STREAM grows past the bound it was already measured against. A stream
// that was reset, by either end, is closed: frames for it may already be on
// the wire and are tolerated rather than refused, which is what §5.1 asks for
// in the other direction.
//
// Kept rather than deleted, for two reasons beyond politeness. A trailing
// header block still has to reach the HPACK decoder — a block that is not
// decoded mis-decodes every block after it, on every stream, for the rest of
// the connection. And a trailing DATA still has to be counted against the
// connection window and replenished, because the peer already debited it
// there; a server that quietly drops it stalls the connection a few resets
// later with nothing in the logs to say why.
//
// The memory is bounded to MaxConcurrentStreams entries, oldest evicted first.
func (s *h2Server) retire(id uint32, state h2State) {
	st := s.streams[id]
	if st == nil || st.state == stateClosed {
		return
	}
	if st.state == stateOpen {
		s.open--
		s.closedRing = append(s.closedRing, id)
		for len(s.closedRing) > s.cfg.MaxConcurrentStreams {
			delete(s.streams, s.closedRing[0])
			s.closedRing = s.closedRing[1:]
		}
	}
	st.state = state
	// The body stops counting against the aggregate assembly bound here,
	// whichever way the stream left the open state: completed, it is the
	// handler's and bounded by the concurrency limit; reset, it is freed.
	s.assembling -= len(st.body)
	// Dropping these does not free a completed request's memory — st.req
	// holds the same backing arrays until consume hands it over and clears
	// it — but it is the last reference a reset stream had. block goes too,
	// unless it holds fragments of a block still arriving: those must reach
	// the HPACK decoder whole, or every later block mis-decodes.
	st.body, st.headers = nil, nil
	if len(st.block) == 0 {
		st.block = nil
	}
	if state == stateClosed {
		st.req = Request{}
	}
}

func (s *h2Server) frame(f h2Frame) (*h2Stream, error) {
	switch f.Type {
	case frameSettings:
		if f.Flags&flagAck != 0 {
			return nil, nil
		}
		table, haveTable, err := s.applySettings(f.Payload)
		if err != nil {
			return nil, err
		}
		return nil, s.w.ackSettings(table, haveTable)

	case framePing:
		if f.Flags&flagAck != 0 {
			return nil, nil
		}
		return nil, s.w.control(h2Frame{Type: framePing, Flags: flagAck, Payload: f.Payload})

	case frameWindowUpdate:
		inc := int32(be32(f.Payload) & 0x7fffffff) //nolint:gosec // G115: masked to 31 bits
		if f.StreamID != 0 && s.idle(f.StreamID) {
			// §5.1: on an idle stream only HEADERS and PRIORITY may arrive,
			// anything else is a connection error. A stream error is not an
			// option either, since RST_STREAM must never name an idle stream.
			return nil, fmt.Errorf("%w: WINDOW_UPDATE on stream %d, which is idle", ErrH2Protocol, f.StreamID)
		}
		if inc == 0 {
			// §6.9 splits the refusal: a zero increment on the connection is
			// a connection error, on a stream it costs only that stream.
			if f.StreamID == 0 {
				return nil, fmt.Errorf("%w: a WINDOW_UPDATE of zero", ErrH2Protocol)
			}
			return nil, streamFail(f.StreamID, errProtocolError,
				"%w: a WINDOW_UPDATE of zero on stream %d", ErrH2Protocol, f.StreamID)
		}
		if f.StreamID == 0 {
			return nil, s.w.creditConn(inc)
		}
		// Forwarded whatever this side's stream state says: a stream whose
		// request is complete is still one this end is writing a response on,
		// and the credit for it is exactly what that response is waiting for.
		return nil, s.w.credit(f.StreamID, inc)

	case frameRSTStream:
		// Rapid reset: a client may legally cancel, and a client that cancels
		// endlessly makes a server allocate stream state endlessly for free.
		// CVE-2023-44487 is that, and the bound is the answer.
		//
		// The bound is a rate rather than a lifetime count: an allowance of
		// four batches' worth of frames, plus two cancellations for every
		// request this connection has actually completed. A long-lived
		// connection that serves requests may go on cancelling; one that only
		// cancels runs out. It is expressed against work done rather than
		// against a clock, so it holds while a connection is stalled and it
		// can be tested without one.
		//
		// "Completed" means answered, not merely assembled. The
		// CVE-2023-44487 pattern is HEADERS with END_STREAM followed at once
		// by RST_STREAM: the HEADERS completes a request — served goes up —
		// and the reset cancels it. Counted as served, each pair earned the
		// allowance for two more resets and the bound never tripped; the
		// handler ran for every cancelled request. A reset of a request that
		// is still in the responder's hands takes its credit back, so the
		// pattern spends the flat allowance and nothing else.
		if s.idle(f.StreamID) {
			return nil, fmt.Errorf("%w: RST_STREAM on stream %d, which is idle", ErrH2Protocol, f.StreamID)
		}
		if st := s.streams[f.StreamID]; st != nil && st.state == stateHalfClosed {
			s.served--
		}
		if err := s.countReset(); err != nil {
			return nil, err
		}
		s.retire(f.StreamID, stateClosed)
		// The cancellation reaches the response being written for it, which
		// is what stops a producer working for a caller who walked away.
		s.w.forget(f.StreamID)
		return nil, nil

	case frameGoAway:
		s.goingAway = true
		return nil, nil

	case framePriority:
		if len(f.Payload) != 5 {
			// §6.3 makes the wrong length a stream error, FRAME_SIZE_ERROR —
			// not the connection error the other fixed-length frames rate —
			// which is why the shape check leaves it to this handler. On an
			// idle stream there is nothing to reset (§5.1: RST_STREAM never
			// names one), so it is the connection's, as nghttp2 and x/net
			// also make it.
			if s.idle(f.StreamID) {
				return nil, fmt.Errorf("%w: %w: PRIORITY is %d bytes on idle stream %d, want 5",
					ErrH2Protocol, errFrameSize, len(f.Payload), f.StreamID)
			}
			return nil, streamFail(f.StreamID, errFrameSizeError,
				"%w: %w: PRIORITY is %d bytes, want 5", ErrH2Protocol, errFrameSize, len(f.Payload))
		}
		return nil, nil // deprecated by RFC 9113 and ignored, as it allows

	case framePushPromise:
		// §8.4: only a server pushes, and this one has said ENABLE_PUSH is 0.
		return nil, fmt.Errorf("%w: a client sent PUSH_PROMISE", ErrH2Protocol)

	case frameHeaders, frameContinuation:
		return s.headers(f)

	case frameData:
		return s.data(f)
	}
	return nil, nil
}

func (s *h2Server) headers(f h2Frame) (*h2Stream, error) {
	st, err := s.stream(f.StreamID)
	if err != nil {
		return nil, err
	}
	block := f.Payload
	// Pad stripping is HEADERS-only, by both conditions below being exact:
	// CONTINUATION defines no flag besides END_HEADERS (§6.10), so a
	// CONTINUATION carrying the PADDED bit has the bit ignored — as §4.1
	// requires for undefined flags — and its *whole* payload is header block
	// fragment. Reading a pad length out of it instead would let the peer
	// carve bytes out of the HPACK stream that this end's decoder sees and a
	// conformant intermediary's does not, which is a dynamic-table desync.
	if f.Type == frameHeaders && f.Flags&flagPadded != 0 {
		if len(block) == 0 {
			return nil, fmt.Errorf("%w: a padded HEADERS with no pad length", ErrH2Protocol)
		}
		pad := int(block[0])
		block = block[1:]
		if pad > len(block) {
			return nil, fmt.Errorf("%w: HEADERS padding of %d exceeds the block", ErrH2Protocol, pad)
		}
		block = block[:len(block)-pad]
	}
	if f.Type == frameHeaders && f.Flags&flagPriority != 0 {
		if len(block) < 5 {
			return nil, fmt.Errorf("%w: HEADERS with priority but no priority fields", ErrH2Protocol)
		}
		block = block[5:]
	}

	// [Frames] has already refused an interleaved block, so appending here is
	// appending to this stream's own block. END_STREAM rides on the HEADERS
	// frame and has to be remembered: the CONTINUATION that closes the block
	// cannot carry it, so reading the flag off the closing frame loses it.
	st.block = append(st.block, block...)
	if f.Type == frameHeaders && f.Flags&flagEndStream != 0 {
		st.endStream = true
	}
	if !f.EndsHeaders() {
		return nil, nil
	}

	// The block is decoded whatever happens to the stream, and that ordering
	// is not an accident: HPACK carries a dynamic table across a connection,
	// so a block left undecoded mis-decodes every block after it — on every
	// other stream. A closed stream's block is decoded and dropped; a
	// refusal after this point is a stream error, which by then can be one
	// because the shared decoder is already consistent.
	dst := st.headers
	if st.gotHead || st.state != stateOpen {
		dst = s.trailers[:0]
	}
	fields, err := s.dec.Decode(dst, st.block)
	st.block = st.block[:0]
	if st.state != stateOpen {
		st.block = nil // a retired stream keeps no scratch warm
	}
	if err != nil {
		return nil, err
	}
	if st.state == stateClosed {
		// Tolerated, and before the field-count bound below on purpose: the
		// stream has already been reset, and refusing an oversized block on
		// it would send a second RST_STREAM — and charge a second reset —
		// for a stream that is gone.
		s.trailers = fields
		return nil, nil
	}
	// The field-count bound the h1 parser applies as it reads, applied here
	// at the first moment the count exists. The byte bound the decoder
	// enforces is not this bound: a block of near-empty fields fits the
	// bytes while making every [Request.Get] walk hundreds of entries. A
	// stream error, not a connection error — the table is already consistent.
	if len(fields) > s.cfg.MaxHeaders {
		return nil, streamFail(st.id, errEnhanceYourCalm,
			"%w: over %d header fields", ErrTooLarge, s.cfg.MaxHeaders)
	}

	switch {
	case st.state == stateHalfClosed:
		return nil, streamFail(st.id, errStreamClosed,
			"%w: HEADERS on stream %d after it ended", ErrH2Protocol, st.id)
	case st.gotHead:
		// A trailer section. Not served here, as on the HTTP/3 side — but it
		// ends the message, and §8.1 makes a pseudo-header in it malformed:
		// a trailer that carries :status or :path is a second request's head
		// pretending to be the end of this one.
		s.trailers = fields
		st.gotTrailers = true
		for _, h := range fields {
			if len(h.Name) > 0 && h.Name[0] == ':' {
				return nil, streamFail(st.id, errProtocolError,
					"%w: the pseudo-header %s in a trailer section", ErrH2Protocol, h.Name)
			}
			// The same octet rules the head's fields passed (§8.2.1): a
			// trailer reaches whatever reads the request last, so it is just
			// as good a smuggling carrier as a header.
			if !isToken(h.Name) || !lowerASCII(h.Name) {
				return nil, streamFail(st.id, errProtocolError,
					"%w: %q is not a trailer field name", ErrH2Protocol, h.Name)
			}
			if !validFieldValue(h.Value) {
				return nil, streamFail(st.id, errProtocolError,
					"%w: the trailer %q holds a control character", ErrH2Protocol, h.Name)
			}
		}
		if st.endStream {
			return s.finish(st)
		}
		return nil, nil
	}

	st.headers = fields
	st.gotHead = true
	if err := s.buildRequest(st); err != nil {
		// §8.1.1: a malformed request is a stream error. It is the whole
		// point of the per-stream path — one bad request among a hundred
		// costs that one.
		return nil, streamError{id: st.id, code: errProtocolError, err: err}
	}
	if st.endStream {
		return s.finish(st)
	}
	return nil, nil
}

func (s *h2Server) data(f h2Frame) (*h2Stream, error) {
	st := s.streams[f.StreamID]
	if st == nil {
		// An even identifier can never have been opened: those are the
		// server's to open (§5.1.1) and this server opens none, so DATA on
		// one is not a late frame for a forgotten stream — it is a frame on a
		// stream that never existed, and tolerating it would window-account
		// traffic the peer can send forever without ever owing a request.
		if f.StreamID == 0 || f.StreamID%2 == 0 || f.StreamID > s.lastID {
			return nil, fmt.Errorf("%w: DATA on stream %d, which was never opened",
				ErrH2Protocol, f.StreamID)
		}
		// A stream this end closed long enough ago to have forgotten. The
		// peer still debited the connection window for it, so it is counted
		// and nothing else.
		return nil, s.consumeConnWindow(f)
	}
	if !st.gotHead && st.state == stateOpen {
		return nil, fmt.Errorf("%w: DATA on stream %d before its headers", ErrH2Protocol, f.StreamID)
	}
	// Whatever the stream's state, the payload was debited from the
	// connection window by the peer and has to be accounted here.
	if err := s.consumeConnWindow(f); err != nil {
		return nil, err
	}
	switch {
	case st.state == stateClosed:
		return nil, nil // tolerated within the grace period, and now counted
	case st.state == stateHalfClosed:
		return nil, streamFail(st.id, errStreamClosed,
			"%w: DATA on stream %d after END_STREAM", ErrH2Protocol, st.id)
	case st.gotTrailers:
		return nil, streamFail(st.id, errProtocolError,
			"%w: DATA on stream %d after its trailers", ErrH2Protocol, st.id)
	}

	body := f.Payload
	if f.Flags&flagPadded != 0 {
		if len(body) == 0 {
			return nil, fmt.Errorf("%w: padded DATA with no pad length", ErrH2Protocol)
		}
		pad := int(body[0])
		body = body[1:]
		if pad > len(body) {
			return nil, fmt.Errorf("%w: DATA padding of %d exceeds the frame", ErrH2Protocol, pad)
		}
		body = body[:len(body)-pad]
	}
	if len(st.body)+len(body) > s.cfg.MaxBodyBytes {
		return nil, streamFail(st.id, errEnhanceYourCalm,
			"%w: a body over %d bytes", ErrTooLarge, s.cfg.MaxBodyBytes)
	}
	// The aggregate bound, beside the per-stream one: windows are granted
	// back as DATA is buffered here, so flow control does not bound what a
	// connection's unfinished uploads hold together — this does. See
	// [H2Config.MaxBodyBytes] for the factor.
	if s.assembling+len(body) > 4*s.cfg.MaxBodyBytes {
		return nil, streamFail(st.id, errEnhanceYourCalm,
			"%w: over %d bytes buffered across this connection's unfinished requests",
			ErrTooLarge, 4*s.cfg.MaxBodyBytes)
	}
	st.body = append(st.body, body...)
	s.assembling += len(body)

	// The stream's own window, granted back as its body is consumed.
	// Replenishing only the connection's leaves the peer stalled at the 65535
	// bytes its stream started with, so an upload between 64 KiB and
	// MaxBodyBytes — a size the configuration says is allowed — hangs until
	// the read timeout kills the connection.
	//
	// The whole frame payload counts, padding included: that is what the peer
	// debited (RFC 9113 §6.9.1).
	used := int32(len(f.Payload)) //nolint:gosec // G115: bounded by MaxFrameSize
	if used > st.recvWind {
		// §6.9.1 wants FLOW_CONTROL_ERROR for a peer that sends more than it
		// was granted. Harmless here — the body is bounded by MaxBodyBytes
		// whatever the window says — so it is conformance rather than safety,
		// and it is a stream error rather than a connection one.
		return nil, streamFail(st.id, errFlowControlError,
			"%w: %w: %d bytes on stream %d against a window of %d",
			ErrH2Protocol, errFlowControl, used, st.id, st.recvWind)
	}
	st.recvWind -= used

	last := f.Flags&flagEndStream != 0
	// Not on a stream the peer has just ended: nothing more will arrive on
	// it, and the grant would name a stream that is closing.
	if !last && st.recvWind < defaultInitialWindow/2 {
		inc := defaultInitialWindow - st.recvWind
		st.recvWind = defaultInitialWindow
		if err := s.w.control32(frameWindowUpdate, st.id, uint32(inc)); err != nil { //nolint:gosec // G115: positive by construction
			return nil, err
		}
	}
	if last {
		return s.finish(st)
	}
	return nil, nil
}

// consumeConnWindow debits a DATA frame from the connection's receive window
// and tops it back up, refusing a peer that spent more than it was granted.
//
// It runs for every DATA frame including the ones on a closed stream, which is
// the trap in tolerating those: the peer debited them at the connection level
// whatever this end thinks of the stream, and skipping the accounting stalls
// the connection a few resets later.
func (s *h2Server) consumeConnWindow(f h2Frame) error {
	used := int32(len(f.Payload)) //nolint:gosec // G115: bounded by MaxFrameSize
	if used > s.recvWind {
		return fmt.Errorf("%w: %w: %d bytes against a connection window of %d",
			ErrH2Protocol, errFlowControl, used, s.recvWind)
	}
	s.recvWind -= used
	if s.recvWind >= connRecvWindow/2 {
		return nil
	}
	inc := connRecvWindow - s.recvWind
	s.recvWind = connRecvWindow
	return s.w.control32(frameWindowUpdate, 0, uint32(inc)) //nolint:gosec // G115: positive by construction
}

// buildRequest turns decoded fields into a [Request], applying the rules
// RFC 9113 §8.3 states — which exist because an HTTP/2 request that reaches an
// HTTP/1.1 hop unchecked is how a smuggled request is written.
func (s *h2Server) buildRequest(st *h2Stream) error {
	var method, path, scheme, authority []byte
	regular := st.headers[:0]
	seenRegular := false
	for _, h := range st.headers {
		// h.Name stays []byte throughout: binding string(h.Name) to a
		// variable allocates per field (verified with -gcflags=-m), while a
		// conversion feeding only a switch or comparison does not.
		if len(h.Name) == 0 {
			return fmt.Errorf("%w: an empty header name", ErrH2Protocol)
		}
		if h.Name[0] == ':' {
			if seenRegular {
				return fmt.Errorf("%w: pseudo-header %s after a regular field", ErrH2Protocol, h.Name)
			}
			var dst *[]byte
			switch string(h.Name) {
			case ":method":
				dst = &method
			case ":path":
				dst = &path
			case ":scheme":
				dst = &scheme
			case ":authority":
				dst = &authority
			default:
				return fmt.Errorf("%w: unknown pseudo-header %s", ErrH2Protocol, h.Name)
			}
			if *dst != nil {
				return fmt.Errorf("%w: %s appears twice", ErrH2Protocol, h.Name)
			}
			*dst = h.Value
			continue
		}
		seenRegular = true
		if !lowerASCII(h.Name) {
			return fmt.Errorf("%w: %q is not lowercase", ErrH2Protocol, h.Name)
		}
		// The h1 parser's octet rules, applied to what HPACK decoded
		// (§8.2.1). HPACK can carry any byte, so without these a name with
		// an embedded colon or a value with CR LF reaches the handler — and
		// the first hop that re-serialises the request toward HTTP/1.1 turns
		// that value into a second request.
		if !isToken(h.Name) {
			return fmt.Errorf("%w: %q is not a header name", ErrH2Protocol, h.Name)
		}
		if !validFieldValue(h.Value) {
			return fmt.Errorf("%w: the value of %q holds a control character", ErrH2Protocol, h.Name)
		}
		// Connection-specific fields have no meaning here and are how an
		// HTTP/2 request becomes two HTTP/1.1 requests at the next hop.
		switch string(h.Name) {
		case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
			return fmt.Errorf("%w: the connection-specific field %q", ErrH2Protocol, h.Name)
		case "te":
			if string(h.Value) != "trailers" {
				return fmt.Errorf("%w: TE may only be \"trailers\"", ErrH2Protocol)
			}
		case "content-length":
			// Remembered for finish, where the DATA sum is known. A repeat
			// is refused as the h1 parser refuses it: two lengths on one
			// request is the smuggling family's opening move.
			if st.declared >= 0 {
				return fmt.Errorf("%w: content-length appears twice", ErrH2Protocol)
			}
			n, err := parseContentLength(h.Value)
			if err != nil {
				return err
			}
			st.declared = n
		}
		regular = append(regular, h)
	}
	if method == nil || path == nil || scheme == nil {
		return fmt.Errorf("%w: a request without :method, :path and :scheme", ErrH2Protocol)
	}
	// Pseudo-header values compose the request line of any downstream
	// HTTP/1.1 hop, which is why §10.3 asks for octet-level checks on them
	// specifically: these are the same refusals the h1 request line makes.
	if !isToken(method) {
		return fmt.Errorf("%w: :method is not a token", ErrH2Protocol)
	}
	if !isToken(scheme) {
		return fmt.Errorf("%w: :scheme is not a token", ErrH2Protocol)
	}
	if len(path) == 0 || path[0] != '/' {
		return fmt.Errorf("%w: :path is not origin-form", ErrH2Protocol)
	}
	for _, c := range path {
		if c <= 0x20 || c == 0x7f {
			return fmt.Errorf("%w: :path holds a control character", ErrH2Protocol)
		}
	}
	for _, c := range authority {
		if c <= 0x20 || c == 0x7f {
			return fmt.Errorf("%w: :authority holds a control character", ErrH2Protocol)
		}
	}
	regular, err := withAuthority(regular, authority, ErrH2Protocol)
	if err != nil {
		return err
	}
	st.headers = regular
	st.req = Request{Method: method, Target: path, Headers: regular, KeepAlive: true}
	return nil
}

// finish completes a request and hands its stream over to the responder.
//
// The stream moves to closed here rather than when its answer is written:
// nothing more may arrive on it, and its slot in the concurrency bound moves
// from open to inFlight in the same step, which is what keeps the sum — and
// therefore the batch queue — bounded.
func (s *h2Server) finish(st *h2Stream) (*h2Stream, error) {
	if st.state != stateOpen {
		return nil, streamFail(st.id, errStreamClosed,
			"%w: stream %d ended twice", ErrH2Protocol, st.id)
	}
	// §8.1.1: a content-length that disagrees with the DATA sum is a
	// malformed request, checked here because END_STREAM is the first moment
	// the sum is final — it catches the short body, the long one, and the
	// "content-length: 10" with END_STREAM on the HEADERS alike.
	if st.declared >= 0 && st.declared != len(st.body) {
		return nil, streamFail(st.id, errProtocolError,
			"%w: content-length %d against a body of %d bytes", ErrH2Protocol, st.declared, len(st.body))
	}
	st.req.Body = st.body
	s.served++ // what the rapid-reset bound is measured against
	s.inFlight.Add(1)
	s.retire(st.id, stateHalfClosed)
	return st, nil
}

// stream returns the state for an identifier, opening it if the client may.
func (s *h2Server) stream(id uint32) (*h2Stream, error) {
	if id == 0 || id%2 == 0 {
		// Even identifiers are the server's to open, and zero is the
		// connection. A client using either is not addressing a request.
		return nil, fmt.Errorf("%w: a client opened stream %d", ErrH2Protocol, id)
	}
	if st := s.streams[id]; st != nil {
		return st, nil
	}
	if id <= s.lastID {
		// This also catches a trailer block for a stream that has fallen out
		// of the closed-stream ring, which is a connection error rather than
		// the tolerance §5.1 asks for. Left that way deliberately: telling the
		// two apart needs state this end has by then forgotten, and decoding a
		// header block for a stream it knows nothing about would put the HPACK
		// table at the mercy of a peer naming any old identifier. The ring
		// holds MaxConcurrentStreams entries, so reaching here means the peer
		// opened that many more streams before its trailers arrived.
		return nil, fmt.Errorf("%w: stream %d reopens one already used", ErrH2Protocol, id)
	}
	if s.goingAway || s.open+int(s.inFlight.Load()) >= s.cfg.MaxConcurrentStreams {
		// §5.1.2 makes this a stream error, and REFUSED_STREAM is the code
		// that tells the client nothing was processed — so it may retry the
		// request elsewhere rather than assume it half-happened. The same
		// refusal answers a stream opened after the peer's own GOAWAY.
		//
		// Refused is not the same as unseen. The stream is registered, in
		// the closed state, because two things about it still matter to the
		// connection that survives it: its header block must reach the
		// shared HPACK decoder (§4.3 — an undecoded block desynchronises the
		// dynamic table, and every later block on every stream decodes
		// wrong), and its identifier must advance lastID (§5.1 — a client
		// that POSTed at this server's own advertised limit already has DATA
		// on the wire behind the HEADERS, and a lastID left behind makes
		// that DATA read as a frame on an idle stream, a connection error).
		// Returning the closed stream gets both for free: headers decodes
		// its blocks and drops them, data counts its frames and tolerates
		// them, and exactly one RST_STREAM — this one — answers the lot.
		if err := s.countReset(); err != nil {
			return nil, err
		}
		s.lastID = id
		st := &h2Stream{id: id, state: stateClosed, declared: -1, recvWind: defaultInitialWindow}
		s.streams[id] = st
		s.closedRing = append(s.closedRing, id)
		for len(s.closedRing) > s.cfg.MaxConcurrentStreams {
			delete(s.streams, s.closedRing[0])
			s.closedRing = s.closedRing[1:]
		}
		if err := s.w.control32(frameRSTStream, id, errRefusedStream); err != nil {
			return nil, err
		}
		return st, nil
	}
	s.lastID = id
	st := &h2Stream{
		id: id, declared: -1, recvWind: defaultInitialWindow,
		deadline: time.Now().Add(s.cfg.ReadTimeout),
	}
	s.streams[id] = st
	s.open++
	s.w.open(id)
	return st, nil
}

// validFieldValue reports a field value free of the bytes RFC 9110 §5.5
// keeps out of field content: CR and LF, which let one field smuggle a
// second field or a second request into whatever re-serialises this one
// toward HTTP/1.1, plus the other C0 controls (HTAB excepted) and DEL — the
// same octet rules the h1 parser enforces in parseHeaderLine and the writers
// enforce outbound via hasFieldControl.
func validFieldValue(v []byte) bool {
	for _, c := range v {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// parseContentLength parses a decimal content-length the way the h1 parser
// does: digits only, overflow refused per digit rather than detected after
// the wrap — a wrapped length is a small "valid" number, which is exactly the
// disagreement the check exists to prevent.
func parseContentLength(v []byte) (int, error) {
	if len(v) == 0 {
		return 0, fmt.Errorf("%w: an empty content-length", ErrH2Protocol)
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: content-length %q is not a number", ErrH2Protocol, v)
		}
		if n > maxWindow/10 {
			return 0, fmt.Errorf("%w: content-length %q overflows", ErrH2Protocol, v)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// applySettings validates every entry in order, then applies the last value
// each identifier carried.
//
// Validate-all, apply-last matters for the one setting whose application is
// not O(1): SETTINGS_INITIAL_WINDOW_SIZE walks every live stream window under
// the writer's lock, and nothing in the protocol stops a frame from carrying
// the same identifier thousands of times — a frame full of repeats was a way
// to buy millions of locked map operations for a few kilobytes of wire. The
// result is the one the in-order rule produces anyway: settings are
// overwrites, so the last value is the state either way, and the window delta
// is computed against the previously *applied* value in both readings.
//
// SETTINGS_HEADER_TABLE_SIZE is returned rather than applied: it changes what
// the next header block this end sends must begin with, and that is decided
// where the ACK is queued (see [h2Writer.ackSettings]).
func (s *h2Server) applySettings(payload []byte) (table uint32, haveTable bool, err error) {
	var (
		haveWindow, haveFrame bool
		window, frame         uint32
	)
	for len(payload) >= 6 {
		id := uint32(payload[0])<<8 | uint32(payload[1])
		v := be32(payload[2:6])
		payload = payload[6:]
		switch id {
		case settingHeaderTableSize:
			table, haveTable = v, true
		case settingInitialWindowSize:
			if v > maxWindow {
				return 0, false, fmt.Errorf("%w: %w: an initial window of %d", ErrH2Protocol, errFlowControl, v)
			}
			window, haveWindow = v, true
		case settingMaxFrameSize:
			if v < defaultMaxFrameSize || v > 1<<24-1 {
				return 0, false, fmt.Errorf("%w: a maximum frame size of %d", ErrH2Protocol, v)
			}
			frame, haveFrame = v, true
		case settingMaxHeaderListSize:
			// Read by the responder, which frames the heads; zero, which no
			// peer can usefully mean, stays "unlimited" with the default.
			s.peerMaxHeaderList.Store(v)
		case settingEnablePush:
			if v > 1 {
				return 0, false, fmt.Errorf("%w: ENABLE_PUSH of %d", ErrH2Protocol, v)
			}
		}
	}
	if haveWindow {
		if err := s.w.setInitialWindow(int32(window)); err != nil { //nolint:gosec // G115: bounded above
			return 0, false, err
		}
	}
	if haveFrame {
		s.w.setMaxFrame(int(frame))
	}
	return table, haveTable, nil
}

// answer runs the handler over one batch and hands what it returned to the
// writer. It is the responder goroutine, and the only one that calls the
// caller's code.
//
// The heads of a batch are framed into one buffer and queued together, so the
// writer can interleave their bodies rather than serve them in order — a
// large response no longer holds a small one behind it. The buffer is reused
// across batches, which is safe because deliver does not return until the
// writer has finished with every byte of it.
func (s *h2Server) answer(b h2Batch) {
	defer s.inFlight.Add(-int32(len(b.reqs))) //nolint:gosec // G115: a batch is bounded by MaxConcurrentStreams

	res, err := apply(s.handler, sluice.Batch[Request]{Items: b.reqs})
	if err != nil {
		// The handler panicked, or answered a number of requests that does
		// not match what it was given. Neither is the peer's doing and
		// neither is a reason to lose the other streams on this connection:
		// each request of the batch is answered 500 and the connection
		// carries on. This was a GOAWAY before the write side was split off,
		// so one handler bug cost every request in flight.
		res = make([]Response, len(b.reqs))
		for i := range res {
			res[i] = Response{Status: 500}
		}
	}

	// Header blocks are framed at the protocol's floor, not at the peer's
	// SETTINGS_MAX_FRAME_SIZE. The heads are framed here, on the responder,
	// and written later by the writer; a peer that lowers the setting in
	// between — it may, down to this floor — would receive HEADERS over its
	// new limit and end the connection with FRAME_SIZE_ERROR. DATA reads the
	// live value at write time, so only headers needed this. A block over
	// 16384 bytes is rare, and costs one CONTINUATION more when it happens.
	const maxFrame = defaultMaxFrameSize
	s.out, s.answers = s.out[:0], s.answers[:0]
	for i, r := range res {
		method := b.reqs[i].Method
		status := r.Status
		if status == 0 {
			status = 200
		}
		_, noBody := noLengthAndBody(status, method)
		body, stream := r.Body, r.Stream
		if noBody {
			// A HEAD or 204/304 response states nothing it shouldn't, and
			// sends nothing it shouldn't either: the producer is never
			// pulled, the same way the HTTP/1.1 path never writes res.Body
			// for these.
			body, stream = nil, nil
		}
		a := h2Answer{id: b.ids[i], from: len(s.out), body: body, stream: stream, job: -1}
		out, err := s.appendHead(s.out, a.id, method, r, maxFrame)
		if err != nil {
			// A response this package will not put on the wire — an injected
			// header, a status no version of HTTP carries, a body and a
			// stream at once. The caller's bug, answered as one, on its own
			// stream and nobody else's.
			s.out = s.out[:a.from]
			a.body, a.stream = nil, nil
			if out, err = s.appendHead(s.out, a.id, method, Response{Status: 500}, maxFrame); err != nil {
				_ = s.w.control32(frameRSTStream, a.id, errInternalError)
				s.w.forget(a.id)
				a.skip = true
				s.answers = append(s.answers, a)
				continue
			}
		}
		s.out = out
		a.to = len(s.out)
		s.answers = append(s.answers, a)
	}

	// Two passes, because appending to s.out may move it: the heads are
	// sliced only once the buffer has stopped growing.
	s.jobs, s.ptrs = s.jobs[:0], s.ptrs[:0]
	for i := range s.answers {
		a := &s.answers[i]
		if a.skip {
			continue
		}
		a.job = len(s.jobs)
		s.jobs = append(s.jobs, h2Job{
			id:   a.id,
			head: s.out[a.from:a.to],
			data: a.body,
			end:  len(a.body) > 0,
			// A response with neither a body nor a stream carries END_STREAM
			// on its head, so it closes the stream without a DATA frame.
			closes: a.stream == nil,
		})
	}
	for i := range s.jobs {
		s.ptrs = append(s.ptrs, &s.jobs[i])
	}
	if err := s.w.deliver(s.ptrs); err != nil {
		return // the write side is gone; the reader will see it too
	}

	// The streamed ones follow, one at a time. Each is pulled here, on this
	// goroutine, so a producer never runs ahead of the socket and what is
	// held is one batch rather than the whole answer — and, since the writer
	// waits for credit instead of refusing, a streamed response may be any
	// size the peer is willing to receive.
	for i := range s.answers {
		a := &s.answers[i]
		if a.stream == nil || a.job < 0 || s.jobs[a.job].err != nil {
			continue
		}
		s.streamBody(a.id, a.stream)
	}
}

// appendHead frames a response's HEADERS, and its CONTINUATION frames if the
// block does not fit one.
func (s *h2Server) appendHead(dst []byte, id uint32, method []byte, r Response, maxFrame int) ([]byte, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	status := r.Status
	if status == 0 {
		status = 200
	}
	if err := checkStatus(status); err != nil {
		return nil, err
	}
	noLength, noBody := noLengthAndBody(status, method)
	block := appendHPACKStatus(s.block[:0], status)
	// listSize is RFC 9113 §6.5.2's measure — names and values uncompressed,
	// plus 32 per field — of what this block decodes to at the peer.
	listSize := uint64(len(":status") + 3 + 32)
	for _, h := range r.Headers {
		// h.Name and h.Value stay []byte into appendHPACK: a string binding
		// here allocates twice per response field on the encode hot path.
		if !lowerASCII(h.Name) {
			return nil, fmt.Errorf("%w: response header %q is not lowercase", ErrH2Protocol, h.Name)
		}
		// The h1 writer's octet rule: HPACK carries any byte, and a name
		// with a space or a colon is a second field at the first hop that
		// re-serialises this response toward HTTP/1.1.
		if !isToken(h.Name) {
			return nil, fmt.Errorf("%w: response header %q is not a token", ErrH2Protocol, h.Name)
		}
		if hasCRLF(h.Value) {
			return nil, ErrHeaderInjection
		}
		if hasFieldControl(h.Value) {
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrH2Protocol, h.Name)
		}
		// The output half of §8.2.2's rule, mirrored on the input half
		// above: a response generated with a connection-specific field is
		// malformed, and a conformant client MUST reject it — so a handler's
		// HTTP/1.1 habit ("connection: close") must fail here, as the
		// caller's bug it is, rather than there, as the whole response.
		// Content-length is the h1 writer's refusal for the h1 writer's
		// reason: framing is this package's to state, and a second opinion
		// on it is the smuggling class arriving from the inside.
		switch string(h.Name) {
		case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
			return nil, fmt.Errorf("%w: the connection-specific response field %q", ErrH2Protocol, h.Name)
		case "content-length":
			return nil, fmt.Errorf("%w: content-length is set by the writer, not the caller", ErrH2Protocol)
		}
		block = appendHPACK(block, h.Name, h.Value)
		listSize += uint64(len(h.Name)+len(h.Value)) + 32
	}
	if !r.streamed() && !noLength {
		// A streamed response has no length to state, which is what the
		// DATA frame's END_STREAM says instead. A 204/304 never states one
		// either (RFC 9110 §8.6).
		n := itoa(len(r.Body))
		block = appendHPACK(block, "content-length", n)
		listSize += uint64(len("content-length")+len(n)) + 32
	}
	s.block = block
	// The peer's SETTINGS_MAX_HEADER_LIST_SIZE is advisory (§6.5.2), but a
	// peer that set it will likely refuse a longer block, and a refused
	// response is one the handler never hears about. Refused here instead,
	// it becomes the 500 a handler bug gets — the same treatment HTTP/3
	// gives the peer's MAX_FIELD_SECTION_SIZE.
	if limit := s.peerMaxHeaderList.Load(); limit > 0 && listSize > uint64(limit) {
		return nil, fmt.Errorf("%w: a header list of %d bytes, over the peer's %d limit",
			ErrH2Protocol, listSize, limit)
	}

	// A block larger than one frame goes out as HEADERS followed by
	// CONTINUATION, which is what those frames are for. END_HEADERS marks the
	// last piece and nothing else may be interleaved between them — the same
	// rule this server enforces on the way in.
	flags := byte(0)
	if noBody || (len(r.Body) == 0 && !r.streamed()) {
		flags |= flagEndStream
	}
	return appendHeaderFrames(dst, id, flags, block, maxFrame)
}

// appendHeaderFrames frames one header block as HEADERS and as many
// CONTINUATION frames as maxFrame makes it need; flags are the HEADERS
// frame's, END_HEADERS aside, which lands on whichever frame is last.
func appendHeaderFrames(dst []byte, id uint32, flags byte, block []byte, maxFrame int) ([]byte, error) {
	var err error
	first, rest := block, []byte(nil)
	if len(block) > maxFrame {
		first, rest = block[:maxFrame], block[maxFrame:]
	} else {
		flags |= flagEndHeaders
	}
	if dst, err = appendH2Frame(dst, h2Frame{Type: frameHeaders, Flags: flags, StreamID: id, Payload: first}); err != nil {
		return nil, err
	}
	for len(rest) > 0 {
		n := min(len(rest), maxFrame)
		var f byte
		if n == len(rest) {
			f = flagEndHeaders
		}
		if dst, err = appendH2Frame(dst, h2Frame{Type: frameContinuation, Flags: f, StreamID: id, Payload: rest[:n]}); err != nil {
			return nil, err
		}
		rest = rest[n:]
	}
	return dst, nil
}

// streamBody pulls a streamed response and hands each producer batch to the
// writer as one job, which frames it as however many DATA frames it takes
// and waits for the credit it needs.
//
// The hand-off blocks until the bytes are on the wire, which is what makes
// the batch safe to borrow into streamBuf — the batch contract forbids
// retaining it — and what makes the producer run at the socket's pace rather
// than ahead of it. A chunk the writer refuses because the stream is gone
// stops the pull: that is a peer's RST_STREAM reaching a handler that is
// still working for it. Every batch, empty or not, is also checked against the
// stream's and the connection's state first, since an empty one is never
// handed to the writer and so could not be refused by it.
//
// One job per producer batch rather than one per chunk is the same "one
// write per batch" thesis h1 and h3 already honour: [h2Writer.fill] frames
// everything a job's window and the round's budget allow into one buffer, so
// a job spanning several chunks costs the syscalls its size needs and no
// more — instead of one write per chunk, blocking in between for no reason
// the wire required.
func (s *h2Server) streamBody(id uint32, stream sluice.Stream[[]byte]) {
	stopped := false
	perr := pull(stream, func(b sluice.Batch[[]byte]) bool {
		// Asked on every batch, empty ones included: an empty batch is never
		// handed to the writer, so neither a reset stream nor a dead
		// connection would otherwise reach a producer of cadence ticks.
		if s.readEnded.Load() || !s.w.alive(id) {
			stopped = true // cut short: no END_STREAM may claim the body whole
			return false
		}
		s.streamBuf = s.streamBuf[:0]
		for _, chunk := range b.Items {
			if len(chunk) == 0 {
				continue // an empty batch is cadence, not an end
			}
			s.streamBuf = append(s.streamBuf, chunk...)
		}
		if len(s.streamBuf) == 0 {
			return true
		}
		s.chunk = h2Job{id: id, head: nil, headSent: true, data: s.streamBuf}
		s.one[0] = &s.chunk
		if err := s.w.deliver(s.one); err != nil {
			stopped = true
			return false
		}
		return s.chunk.err == nil
	})
	if stopped {
		return
	}
	if perr != nil {
		// The producer panicked partway through its answer. Only its own
		// stream pays: it is reset, as the response that cannot be framed
		// is, and the connection carries on serving the others.
		_ = s.w.control32(frameRSTStream, id, errInternalError)
		s.w.forget(id)
		return
	}
	// The end: an empty DATA carrying END_STREAM, which costs no credit and
	// therefore always goes out.
	s.chunk = h2Job{id: id, headSent: true, end: true, closes: true}
	s.one[0] = &s.chunk
	_ = s.w.deliver(s.one)
}

// addWindow moves a flow-control window, refusing the overflow RFC 9113
// §6.9.1 makes an error rather than letting int32 wrap — a wrapped window
// reads as a peer that granted nothing, or as room it never granted.
//
// The window may legitimately go negative: a SETTINGS_INITIAL_WINDOW_SIZE
// that shrinks applies to streams already written to (§6.9.2).
func addWindow(cur, delta int32) (int32, error) {
	next := int64(cur) + int64(delta)
	if next > maxWindow || next < -maxWindow {
		return 0, fmt.Errorf("%w: %w: a window of %d falls outside ±%d",
			ErrH2Protocol, errFlowControl, next, maxWindow)
	}
	return int32(next), nil
}

// goAway ends the connection with the code the failure maps to, and with no
// debug data.
//
// GOAWAY's trailing field is free-form and goes to whoever dialled the socket.
// What was sent there was the wrapped error text, and one of those carries a
// handler's panic value (see apply) — an internal detail handed to a stranger
// who caused the failure. The code is what a peer acts on; the text stays on
// this side, where the caller reads it as ServeH2's return value.
func (s *h2Server) goAway(code uint32) {
	payload := make([]byte, 8)
	be32put(payload[:4], s.lastID)
	be32put(payload[4:8], code)
	_ = s.w.control(h2Frame{Type: frameGoAway, Payload: payload})
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
