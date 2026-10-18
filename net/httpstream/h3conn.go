package httpstream

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// h3Assembler turns QUIC stream frames into HTTP/3 requests.
//
// It is the request half of an HTTP/3 server, and deliberately not the whole
// of one: it takes frames that a QUIC connection has already decrypted and
// ordered, and gives back the requests they completed. What is missing under
// it is the connection — see the [quic] package for what that means and what
// it would still need.
//
// The point it makes is the one this library keeps making, now at the
// transport where it costs nothing: a datagram carries frames for several
// streams, so the requests it completes are a batch before this code sees
// them. Nothing waits, and the peer does nothing unusual.
//
// It is not safe for concurrent use — the shutdown's state under mu is the
// one exception, and says so — and it is per connection: a stream identifier
// means nothing outside one.
type h3Assembler struct {
	cfg     Config
	qpack   *qpackDecoder
	streams map[uint64]*h3Stream

	// The graceful shutdown's state (RFC 9114 §5.2; ServeH3 says how it is
	// used). mu is shared with the goroutine that begins the shutdown and
	// is taken on the admission of a new request stream and at the GOAWAY,
	// never per frame: it is what makes the identifier the GOAWAY names and
	// the streams this assembler admitted agree — a stream is either
	// admitted before the GOAWAY, and below its identifier, or rejected.
	mu       sync.Mutex
	accepted bool   // whether a request stream was ever admitted
	last     uint64 // the highest request stream admitted, when accepted
	draining bool   // the GOAWAY is decided: streams from rejectFrom on are rejected
	// rejectFrom is the identifier the GOAWAY carries: the first request
	// stream this connection rejects (§5.2: "the indicated identifier or
	// greater are rejected"), zero when none was ever admitted.
	rejectFrom uint64
	// inFlight counts the request streams admitted and not yet retired:
	// still assembling, queued for the handler, being handled or being
	// answered. The assembler retires a stream that leaves without a
	// request (failed, forgotten); the response writer retires the rest.
	inFlight atomic.Int64
	// Finish, when set, is called by retire when the last in-flight stream
	// of a draining connection retires: nothing more is owed, the
	// connection may close. It must not block — it runs on the read
	// goroutine or a writer's.
	Finish func()

	// refused remembers streams this assembler already rejected, so frames
	// still arriving on them are dropped instead of re-opening assembly for
	// a request nobody will answer. ServeH3 tells the peer to stop with
	// STOP_SENDING, but telling is not enforcing: bytes already in flight —
	// or a peer that ignores the frame — keep coming, and they must land on
	// a tombstone, not on a fresh entry. Bounded like every memory here.
	refused      map[uint64]struct{}
	refusedOrder []uint64

	// assembling sums what every unfinished request stream still retains —
	// its carry buffer and its drained body together. Under a transport
	// that re-credits flow-control windows as soon as a datagram is
	// delivered here, its windows do not bound what unfinished uploads hold
	// across the connection; this does, at 4×MaxBodyBytes, the same factor
	// the h2 side uses for the same reason. ServeH3 additionally moves the
	// transport to manual credit (Credit below), which closes the gap at
	// the window itself; the cap stays as the standalone assembler's bound
	// and as defense in depth.
	assembling int

	// Credit, when set, reports released bytes back to a transport running
	// manual flow-control credit ([quic.Conn.SetManualCredit]): every
	// delivered byte the assembler does not retain — consumed by header
	// decoding, dropped with a failed or forgotten stream, carried on a
	// unidirectional stream past its parse — is released here, the moment
	// it stops being retained. Request bodies handed to the handler are the
	// one exception: the responder releases those when the response
	// retires. Nil means the transport credits on delivery, as before.
	Credit func(streamID, n uint64)

	// The peer's unidirectional streams (RFC 9114 §6.2): the control stream
	// and the QPACK pair are critical — their closure, duplication or
	// misframing is a connection error, not a stream one — and the control
	// stream is where the client's SETTINGS arrive.
	unis            map[uint64]*h3Uni
	hasControl      bool
	hasEncoder      bool
	hasDecoder      bool
	controlSettings bool
	// peerFieldSection is the client's SETTINGS_MAX_FIELD_SECTION_SIZE, zero
	// until its control stream delivers one. It is the one setting a server
	// consults — it bounds the field sections it SHOULD send (RFC 9114
	// §4.2.2); ServeH3 snapshots it per batch and the response encoder
	// enforces it. The rest are ignored as §7.2.4 requires, and not kept:
	// a SETTINGS frame of MaxHeaderBytes holds thousands of identifiers, and
	// a map of them all lived as long as the connection.
	peerFieldSection uint64

	batch  []Request
	ids    []uint64
	failed []h3StreamError

	// frames is drain's scratch for ParseH3Frames, reused across streams and
	// calls: nothing drain does retains the slice past its own use, so there
	// is no reason to allocate it fresh every time a stream's buffer drains.
	frames []h3Frame
}

// maxRefusedRemembered bounds the tombstones. A peer still sending on a
// stream refused this long ago is not confused but hostile, and re-refusing
// it costs one map hit.
const maxRefusedRemembered = 256

type h3Stream struct {
	buf     []byte
	headers []Header
	req     Request
	gotHead bool
	// gotTrailers remembers a trailing HEADERS: a frame after it is a second
	// request trying to ride the stream (RFC 9114 §4.1).
	gotTrailers bool
	// declared is the content-length the request stated, or -1 when it
	// stated none — compared with the assembled body at FIN, the same §4.1.2
	// check the h2 side makes at END_STREAM.
	declared int
	// deadline is when assembly of this request gives up — the h3 twin of
	// the h2 reader's per-stream deadline, measured from the stream's first
	// byte. Without it a trickling peer parks buffers for as long as it
	// keeps the connection's idle timer alive.
	deadline time.Time
}

// h3Uni is one client unidirectional stream: untyped until its first varint
// arrives, then the control stream (whose frames buf carries between
// datagrams), a QPACK stream, or something to discard.
type h3Uni struct {
	// kind is -1 until the stream's type varint has arrived, then one of the
	// H3Stream* types, or uniKindUnknown for a type this server discards.
	kind int
	buf  []byte
}

// uniKindUnknown marks a typed unidirectional stream this server does not
// read — reserved and extension types, which §6.2 says to discard without
// erroring.
const uniKindUnknown = -2

// maxUniStreams bounds how many unidirectional streams the assembler tracks.
// The transport's own stream admission sits far below this; the bound is for
// the assembler driven without one.
const maxUniStreams = 32

// newH3Assembler returns an assembler bounded by cfg, which is resolved.
func newH3Assembler(cfg Config) *h3Assembler {
	return &h3Assembler{
		cfg:     cfg,
		qpack:   newQPACKDecoder(cfg.QPACKStatic, cfg.MaxHeaderBytes),
		streams: make(map[uint64]*h3Stream),
		refused: make(map[uint64]struct{}),
		unis:    make(map[uint64]*h3Uni),
	}
}

// h3StreamError reports a request stream this assembler will not serve.
//
// It is the per-stream error channel RFC 9114 gives a server and a sequential
// protocol does not: the stream is reset with Code and the connection carries
// on, so one malformed request among the several a datagram carried costs that
// one. Every refusal below is one of these except the ones about the
// connection's own machinery — its control and QPACK streams — which come
// back as an [h3ConnectionError] instead, because a frame layer in doubt
// cannot be resynchronised.
type h3StreamError struct {
	StreamID uint64
	Code     uint64
	Err      error
	// Status, when set, is answered on the stream before it is closed —
	// for the bounds [Config] documents a status for, nothing else. Code is
	// then H3_NO_ERROR: the response is complete and the stream ends with
	// a FIN, and STOP_SENDING carries the code (RFC 9114 §4.1).
	Status int
}

// Error returns the underlying error's message; the stream and code are
// fields, not prose.
func (e h3StreamError) Error() string { return e.Err.Error() }

// Unwrap exposes the underlying error to [errors.Is] and [errors.As].
func (e h3StreamError) Unwrap() error { return e.Err }

// h3StatusError is drain's refusal of a request over one of the bounds
// [Config] documents a status for: Frames turns it into an [h3StreamError]
// with that Status, answered on the stream rather than reset without a word
// — RFC 9114 §4.2.2 permits the 431, §4.1 a complete response ahead of the
// request's end. It wraps err so a caller keeps matching on [ErrTooLarge].
type h3StatusError struct {
	status int
	err    error
}

func (e h3StatusError) Error() string { return e.err.Error() }
func (e h3StatusError) Unwrap() error { return e.err }

// h3StatusFail builds an h3StatusError, as statusFail builds the h2 kind.
func h3StatusFail(status int, format string, args ...any) error {
	return h3StatusError{status: status, err: fmt.Errorf(format, args...)}
}

// HTTP/3 error codes (RFC 9114 §8.1). Only the ones this server sends are
// named.
const (
	h3NoError              = 0x0100
	h3GeneralProtocolError = 0x0101
	h3InternalError        = 0x0102
	h3StreamCreationError  = 0x0103
	h3ClosedCriticalStream = 0x0104
	h3FrameUnexpected      = 0x0105
	h3FrameError           = 0x0106
	h3ExcessiveLoad        = 0x0107
	h3SettingsError        = 0x0109
	h3MissingSettings      = 0x010a
	h3RequestRejected      = 0x010b
	h3RequestCancelled     = 0x010c
	h3RequestIncomplete    = 0x010d
	h3MessageError         = 0x010e
)

// qpackEncoderStreamError is RFC 9204 §8.3's connection error for an encoder
// stream that violates what the decoder's SETTINGS allowed — here, any
// instruction that would use the dynamic table a capacity of zero refused.
const qpackEncoderStreamError = 0x0201

// qpackDecoderStreamError is RFC 9204 §8.3's connection error for a decoder
// stream instruction the encoder cannot accept — here, any acknowledgment of
// dynamic-table state, since this server's encoder never creates any.
const qpackDecoderStreamError = 0x0202

// errH3FrameUnexpected marks a frame that is well-formed but arrived where
// RFC 9114 forbids it: DATA before HEADERS or anything after the trailers
// (§4.1), a control-stream frame on a request stream (§7.2.3–§7.2.7), a
// PUSH_PROMISE from a client (§7.2.5), an HTTP/2 frame type with no HTTP/3
// meaning (§7.2.8). Every one of those sections makes it a *connection*
// error of type H3_FRAME_UNEXPECTED, and Frames returns it as one — unlike a
// malformed message, which is the stream's fault alone (H3_MESSAGE_ERROR).
// It used to be answered on the stream; a peer that frames a stream wrongly
// has a broken framer, not one bad request, and §4.1's MUST says so.
var errH3FrameUnexpected = fmt.Errorf("%w: a frame out of sequence", ErrH3Protocol)

// qpackDecompressionFailed is RFC 9204 §6's code for a field section the
// decoder cannot interpret. §2.2.3, §3.1 and §4.5.1.1 make the shapes this
// decoder can meet — a dynamic-table reference against a capacity of zero, a
// static index past the table, a Required Insert Count no conformant encoder
// produces — connection errors; §7.4 keeps one case on the stream, a value
// wider than the decoder's limit. Before, every one of them was
// H3_MESSAGE_ERROR on the stream, as if the request were malformed rather
// than the compression state broken.
const qpackDecompressionFailed = 0x0200

// h3ConnectionError reports a violation of the connection's own critical
// machinery — a duplicated or closed control or QPACK stream, a control
// stream that did not open with SETTINGS — which RFC 9114 §8 makes connection
// errors rather than stream ones: there is no stream to refuse when the
// stream at fault is the one the conversation depends on. [ServeH3] answers
// one by closing the connection with Code.
type h3ConnectionError struct {
	Code uint64
	Err  error
}

// Error returns the underlying error's message; the code is a field, not
// prose.
func (e h3ConnectionError) Error() string { return e.Err.Error() }

// Unwrap exposes the underlying error to [errors.Is] and [errors.As].
func (e h3ConnectionError) Unwrap() error { return e.Err }

// Frames folds a datagram's QUIC frames into stream state and returns the
// requests that became complete, the stream each arrived on, and the streams
// whose own framing this will not serve.
//
// A stream error does not end the loop. The frames behind the offending one
// belong to other requests — that is what multiplexing means — and serving
// them is the whole reason a datagram is handed over as a batch. Only a
// failure of the connection's own framing comes back as the error return.
//
// Only client-initiated bidirectional streams carry requests — identifiers
// with their two low bits clear (RFC 9000 §2.1). The client's unidirectional
// streams carry its control stream and QPACK pair, which are read just far
// enough to hold RFC 9114 §6.2.1 and RFC 9204 §4.2's critical-stream rules
// and to take the client's SETTINGS; their violations come back as the error
// return, an [h3ConnectionError], because a connection whose own machinery
// is broken has no stream to refuse instead.
//
// A RESET_STREAM is folded in right here, in packet order, rather than in a
// separate pass: STREAM deltas that preceded the reset in the same datagram
// must not recreate state for a stream the transport has already abandoned —
// an entry reborn that way could never complete and never leave, and each
// one would burn an admission slot for the connection's lifetime.
//
// Overlapping or duplicated STREAM bytes are not this method's concern:
// [quic.Frame.Data] already is the ordered, deduplicated delta by the time
// it reaches here — quic/reassembly.go's addThenTake/add did the merging —
// and this does nothing but st.buf = append(st.buf, f.Data...), with no
// offset in sight to detect an overlap from even if it wanted to.
// Retransmission-safety for a stream's bytes belongs in
// quic/reassembly_test.go, not duplicated at this layer.
func (a *h3Assembler) Frames(frames []quic.Frame) (sluice.Batch[Request], []uint64, []h3StreamError, error) {
	a.batch, a.ids, a.failed = a.batch[:0], a.ids[:0], a.failed[:0]
	now := time.Now() // one clock read per delivery, not per new stream
	a.expire(now)
	for _, f := range frames {
		if f.Type == quic.FrameResetStream {
			if err := a.reset(f.StreamID); err != nil {
				return sluice.Batch[Request]{}, nil, a.failed, err
			}
			continue
		}
		if !f.IsStream() {
			continue
		}
		switch f.StreamID & 0x03 {
		case 0: // client-initiated bidirectional: a request stream
		case 2: // client-initiated unidirectional: control, QPACK or push
			// Released around the call: whatever the parse did not leave
			// in the stream's carry buffer stopped being retained here.
			rb := a.uniRetained(f.StreamID)
			err := a.uni(f)
			a.release(f.StreamID, len(f.Data)+rb-a.uniRetained(f.StreamID))
			if err != nil {
				return sluice.Batch[Request]{}, nil, a.failed, err
			}
			continue
		default: // server-initiated: nothing of the peer's arrives here
			a.release(f.StreamID, len(f.Data))
			continue
		}
		if _, gone := a.refused[f.StreamID]; gone {
			a.release(f.StreamID, len(f.Data))
			continue // a refused stream's stragglers; the refusal stands
		}
		st := a.streams[f.StreamID]
		if st == nil {
			if !a.admit(f.StreamID) {
				// Past the GOAWAY's identifier: §5.2 has the server reject
				// what it said it would, and REQUEST_REJECTED is the code
				// that says nothing was processed, so the client retries
				// elsewhere.
				a.fail(f.StreamID, h3RequestRejected, fmt.Errorf("%w: stream %d opened after GOAWAY(%d)",
					ErrH3Protocol, f.StreamID, a.rejectFrom))
				a.release(f.StreamID, len(f.Data))
				continue
			}
			if len(a.streams) >= a.cfg.MaxConcurrentStreams {
				// §5.2 wants a request refused rather than a connection lost
				// when a server is at its limit, and REQUEST_REJECTED tells
				// the client nothing was processed so it may retry elsewhere.
				a.fail(f.StreamID, h3RequestRejected, fmt.Errorf("%w: over %d open request streams",
					ErrH3Protocol, a.cfg.MaxConcurrentStreams))
				a.release(f.StreamID, len(f.Data))
				continue
			}
			st = &h3Stream{declared: -1, deadline: now.Add(a.cfg.ReadTimeout)}
			a.streams[f.StreamID] = st
			a.inFlight.Add(1)
		}
		// The unparsed bytes are framing, not body: a request whose body is
		// exactly MaxBodyBytes arrives as its HEADERS frame plus a DATA
		// frame of that payload plus two frame headers, and a bound of
		// MaxBodyBytes on the raw buffer refused it whenever it came in one
		// delivery. The body bound itself is drain's, on the payload.
		if bound := a.rawStreamBound(); len(st.buf)+len(f.Data) > bound {
			a.fail(f.StreamID, h3ExcessiveLoad, fmt.Errorf("%w: a stream over %d bytes",
				ErrTooLarge, bound))
			a.release(f.StreamID, len(f.Data)) // never appended; fail released the rest
			continue
		}
		// The connection-wide bound, beside the per-stream one: under a
		// transport that re-credits every delivered byte while buf and the
		// drained body are still held here, flow control alone would let a
		// peer park per-stream bounds times the stream limit. ServeH3's
		// manual credit closes that at the window; this cap stays as the
		// standalone bound and as defense in depth.
		if a.assembling+len(f.Data) > 4*a.cfg.MaxBodyBytes {
			a.fail(f.StreamID, h3ExcessiveLoad, fmt.Errorf(
				"%w: over %d bytes buffered across this connection's unfinished requests",
				ErrTooLarge, 4*a.cfg.MaxBodyBytes))
			a.release(f.StreamID, len(f.Data))
			continue
		}
		before := len(st.buf) + len(st.req.Body)
		st.buf = append(st.buf, f.Data...)

		err := a.drain(st)
		a.assembling += len(st.buf) + len(st.req.Body) - before
		// What drain consumed outright — header blocks decoded, frame
		// headers parsed — stopped being retained now; the body bytes it
		// kept are released by fail, Forget, or the responder, whichever
		// ends the request. (The credit arithmetic: delivered = released
		// here + Δretained, every path, so the windows always balance.)
		a.release(f.StreamID, len(f.Data)+before-len(st.buf)-len(st.req.Body))
		if err != nil {
			if se, ok := errors.AsType[h3StatusError](err); ok {
				a.answer(f.StreamID, se.status, err)
				continue
			}
			// The connection's fault, not the stream's: an invalid frame
			// sequence (§4.1), a frame that belongs on the control stream
			// (§7.2.3–§7.2.7) or has no HTTP/3 meaning (§7.2.8), or a field
			// section the QPACK state cannot decode (RFC 9204 §2.2.3, §3.1).
			// Each of those is a MUST for a connection error, and a
			// connection whose framer is wrong has nothing left to serve.
			if errors.Is(err, errH3FrameUnexpected) {
				err = h3ConnectionError{Code: h3FrameUnexpected, Err: err}
			}
			if ce, ok := errors.AsType[h3ConnectionError](err); ok {
				return sluice.Batch[Request]{}, nil, a.failed, ce
			}
			code := uint64(h3MessageError)
			switch {
			case errors.Is(err, ErrTooLarge):
				code = h3ExcessiveLoad
			case errors.Is(err, errIntegerWide):
				code = qpackDecompressionFailed // RFC 9204 §7.4, on the stream
			}
			a.fail(f.StreamID, code, err)
			continue
		}
		if f.Fin {
			if !st.gotHead {
				a.fail(f.StreamID, h3MessageError, fmt.Errorf("%w: a stream ended before its HEADERS", ErrH3Protocol))
				continue
			}
			if len(st.buf) > 0 {
				// A clean FIN inside a frame is H3_FRAME_ERROR (RFC 9114
				// §7.1): the tail the peer declared never arrived, and
				// serving the request without it would deliver a truncated
				// body as an intact one.
				a.fail(f.StreamID, h3FrameError, fmt.Errorf(
					"%w: the stream ended inside a frame, %d bytes short", ErrH3Protocol, len(st.buf)))
				continue
			}
			if st.declared >= 0 && st.declared != len(st.req.Body) {
				// §4.1.2: a content-length that disagrees with the DATA sum
				// is a malformed request, checked here because FIN is the
				// first moment the sum is final.
				a.fail(f.StreamID, h3MessageError, fmt.Errorf(
					"%w: content-length %d against a body of %d bytes", ErrH3Protocol, st.declared, len(st.req.Body)))
				continue
			}
			st.req.Body = st.req.Body[:len(st.req.Body):len(st.req.Body)]
			a.batch = append(a.batch, st.req)
			a.ids = append(a.ids, f.StreamID)
			// The body stops counting against the aggregate bound here: it
			// is the handler's now, bounded by the admission limit.
			a.assembling -= len(st.req.Body)
			delete(a.streams, f.StreamID)
		}
	}
	return sluice.Batch[Request]{Items: a.batch}, a.ids, a.failed, nil
}

// expire fails every request stream that has been assembling for longer than
// ReadTimeout — the h3 twin of the h2 reader's assembly sweep, run on the
// same occasion: a datagram arriving is the only clock this layer has, and
// the only way a stalled stream's memory stays parked is the peer sending
// *something* to keep the connection's own idle timer alive. A peer that
// sends nothing at all is the transport's idle timeout's to end.
func (a *h3Assembler) expire(now time.Time) {
	for id, st := range a.streams {
		if now.After(st.deadline) {
			a.fail(id, h3RequestIncomplete, fmt.Errorf(
				"%w: stream %d assembled nothing complete within the read timeout", ErrH3Protocol, id))
		}
	}
}

// reset folds one RESET_STREAM into assembly state, in packet order. For a
// request stream that means Forget; for a critical unidirectional stream it
// is the connection error RFC 9114 §6.2.1 and RFC 9204 §4.2 require.
func (a *h3Assembler) reset(id uint64) error {
	switch id & 0x03 {
	case 0:
		a.Forget(id)
	case 2:
		if u := a.unis[id]; u != nil {
			a.release(id, len(u.buf))
			delete(a.unis, id)
			return a.criticalGone(u.kind)
		}
	}
	return nil
}

// release reports n released bytes of one stream to the manual-credit
// transport, when one is wired (Credit non-nil; no-op otherwise). n may
// legitimately be zero — every delivery path calls this with "delivered
// minus newly retained", and a frame fully absorbed into the carry buffer
// releases nothing yet.
func (a *h3Assembler) release(id uint64, n int) {
	if a.Credit != nil && n > 0 {
		a.Credit(id, uint64(n))
	}
}

// uniRetained is how many bytes stream id's unidirectional parse currently
// carries, zero for a stream it does not know.
func (a *h3Assembler) uniRetained(id uint64) int {
	if u := a.unis[id]; u != nil {
		return len(u.buf)
	}
	return 0
}

// fail records a stream error, drops whatever was assembled for it, and
// leaves a tombstone so later frames on the stream are dropped rather than
// re-opening assembly. The peer is *told* to stop with STOP_SENDING — ServeH3
// sends it alongside the reset — but frames already in flight arrive anyway,
// and a tombstone is what keeps them from re-accumulating state for a
// request nobody will answer.
func (a *h3Assembler) fail(id, code uint64, err error) {
	if a.drop(id) {
		a.retire()
	}
	a.failed = append(a.failed, h3StreamError{StreamID: id, Code: code, Err: err})
}

// answer records a stream refused with a status: dropped and tombstoned as
// fail does, but not retired — ServeH3 retires it once the status has left,
// as it does a handler's response, so a draining connection waits for the
// refusal to be written and acknowledged like any other answer.
func (a *h3Assembler) answer(id uint64, status int, err error) {
	a.drop(id)
	a.failed = append(a.failed, h3StreamError{StreamID: id, Code: h3NoError, Err: err, Status: status})
}

// drop discards a stream's assembly state, releasing what it retained, and
// leaves a tombstone. It reports whether the stream was being assembled —
// admitted, so owed a retire.
func (a *h3Assembler) drop(id uint64) bool {
	st := a.streams[id]
	if st != nil {
		a.assembling -= len(st.buf) + len(st.req.Body)
		a.release(id, len(st.buf)+len(st.req.Body))
		delete(a.streams, id)
	}
	a.tombstone(id)
	return st != nil
}

// tombstone remembers a stream as dead so later frames on it are dropped
// rather than re-opening assembly, FIFO-bounded like every memory here.
func (a *h3Assembler) tombstone(id uint64) {
	if _, ok := a.refused[id]; ok {
		return
	}
	a.refused[id] = struct{}{}
	a.refusedOrder = append(a.refusedOrder, id)
	for len(a.refusedOrder) > maxRefusedRemembered {
		delete(a.refused, a.refusedOrder[0])
		a.refusedOrder = a.refusedOrder[1:]
	}
}

// Forget drops a stream's assembly state, which is what a RESET_STREAM from
// the peer means: the request is cancelled and what arrived for it is not a
// request. It leaves a tombstone, exactly as a refusal does, because the
// transport delivers a datagram's frames in packet order: STREAM deltas that
// preceded the reset in the same batch would otherwise recreate an entry no
// future frame can ever complete — each one a permanently burnt admission
// slot.
func (a *h3Assembler) Forget(id uint64) {
	if a.drop(id) {
		a.retire()
	}
}

// admit decides, under mu, whether a new request stream may be served: not
// once the GOAWAY is decided and the stream is at or past its identifier.
// An admitted stream raises last, which is what the GOAWAY will be computed
// from — a stream refused later for the concurrency limit raises it too,
// which §5.2 allows ("servers MAY reject individual requests on streams
// below the indicated ID").
func (a *h3Assembler) admit(id uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.draining && id >= a.rejectFrom {
		return false
	}
	if !a.accepted || id > a.last {
		a.accepted, a.last = true, id
	}
	return true
}

// beginDrain decides the GOAWAY, under mu, and returns the identifier it
// carries: the request stream after the highest one admitted, or zero when
// none was. From here on admit rejects every stream from that identifier.
func (a *h3Assembler) beginDrain() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.draining = true
	if a.accepted {
		a.rejectFrom = a.last + 4
	}
	return a.rejectFrom
}

// retire takes one request stream out of the in-flight count and, when it
// was the last one of a draining connection, calls Finish. The count is
// read after the decrement and the flag after that, under mu; beginDrain
// sets the flag under mu before it reads the count: whichever of the two
// runs second sees the other's write, so a connection that empties as the
// GOAWAY is decided is finished exactly once, by one of them.
func (a *h3Assembler) retire() {
	if a.inFlight.Add(-1) != 0 {
		return
	}
	a.mu.Lock()
	draining := a.draining
	a.mu.Unlock()
	if draining && a.Finish != nil {
		a.Finish()
	}
}

// uni folds one frame of a client unidirectional stream (RFC 9114 §6.2): the
// type varint first, then the stream's own content: the control stream's
// frames, and the QPACK pair's instructions, checked against the dynamic
// table neither side uses. Push streams are a client opening what only
// servers may, and unknown types are discarded, as §6.2 permits.
func (a *h3Assembler) uni(f quic.Frame) error {
	u := a.unis[f.StreamID]
	if u == nil {
		if len(a.unis) >= maxUniStreams {
			return nil // far past the transport's own stream admission
		}
		u = &h3Uni{kind: -1}
		a.unis[f.StreamID] = u
	}
	switch u.kind {
	case -1:
		// The whole delivery is appended, but only an incomplete type varint
		// — under 8 bytes — stays parked: once it parses, the buffer keeps
		// just what follows it.
		u.buf = append(u.buf, f.Data...)
		typ, rest, err := quic.Varint(u.buf)
		if err != nil {
			// Not yet a whole varint. A stream closed before its type byte
			// is tolerated (§6.2), not an error.
			if f.Fin {
				delete(a.unis, f.StreamID)
			}
			return nil
		}
		u.buf = append(u.buf[:0], rest...)
		switch typ {
		case h3StreamControl:
			if a.hasControl {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second control stream", ErrH3Protocol)}
			}
			a.hasControl = true
			u.kind = h3StreamControl
		case h3StreamPush:
			// §6.2.2: push streams are server-initiated by definition.
			return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
				"%w: a client opened a push stream", ErrH3Protocol)}
		case h3StreamQPACKEncode:
			if a.hasEncoder {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second QPACK encoder stream", ErrH3Protocol)}
			}
			a.hasEncoder = true
			u.kind = h3StreamQPACKEncode
			// The bytes after the type varint are instructions, kept: a
			// capacity of zero was advertised, so almost any of them is the
			// peer misusing a table it was told does not exist.
		case h3StreamQPACKDecode:
			if a.hasDecoder {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second QPACK decoder stream", ErrH3Protocol)}
			}
			a.hasDecoder = true
			u.kind = h3StreamQPACKDecode
			// Like the encoder stream's, the bytes after the type are
			// instructions and are read (decoderInstructions).
		default:
			u.kind = uniKindUnknown
			u.buf = nil
		}
	case h3StreamControl:
		// Control frames are small; a frame that cannot fit under the header
		// bound is not one this server reads, so the carry buffer is bounded
		// by what ParseH3Frames below will accept plus its header.
		if len(u.buf)+len(f.Data) > a.cfg.MaxHeaderBytes+16 {
			return h3ConnectionError{Code: h3ExcessiveLoad, Err: fmt.Errorf(
				"%w: a control-stream frame over %d bytes", ErrTooLarge, a.cfg.MaxHeaderBytes)}
		}
		u.buf = append(u.buf, f.Data...)
	case h3StreamQPACKEncode, h3StreamQPACKDecode:
		// Each instruction is consumed as soon as it is whole, so what parks
		// here is at most one integer's few bytes.
		u.buf = append(u.buf, f.Data...)
	}
	switch u.kind {
	case h3StreamControl:
		if err := a.controlFrames(u); err != nil {
			return err
		}
	case h3StreamQPACKEncode:
		if err := a.encoderInstructions(u); err != nil {
			return err
		}
	case h3StreamQPACKDecode:
		if err := a.decoderInstructions(u); err != nil {
			return err
		}
	}
	if f.Fin {
		kind := u.kind
		delete(a.unis, f.StreamID)
		return a.criticalGone(kind)
	}
	return nil
}

// controlFrames consumes whole frames off the control stream's carry buffer:
// SETTINGS first and once (§6.2.1, §7.2.4), nothing that belongs on a
// request stream, and the frames a non-pushing server has no action for
// skipped.
func (a *h3Assembler) controlFrames(u *h3Uni) error {
	if !a.controlSettings && len(u.buf) > 0 {
		// §6.2.1: SETTINGS must be the control stream's very first frame.
		// ParseH3Frames below skips GREASE frame types, as §7.2.8 wants
		// everywhere else — so the first frame's type is read here, before
		// the skip can hide a GREASE frame standing where SETTINGS has to be.
		if typ, _, err := quic.Varint(u.buf); err == nil && typ != h3Settings {
			return h3ConnectionError{Code: h3MissingSettings, Err: fmt.Errorf(
				"%w: the control stream opened with frame %#x, not SETTINGS", ErrH3Protocol, typ)}
		}
	}
	frames, rest, err := parseH3Frames(a.frames[:0], u.buf, a.cfg.MaxHeaderBytes)
	a.frames = frames
	if err != nil {
		// A frame too long is a layout fault (H3_FRAME_ERROR); an HTTP/2
		// frame type is §7.2.8's H3_FRAME_UNEXPECTED.
		code := uint64(h3FrameError)
		if errors.Is(err, errH3FrameUnexpected) {
			code = h3FrameUnexpected
		}
		return h3ConnectionError{Code: code, Err: err}
	}
	for _, fr := range frames {
		if !a.controlSettings {
			if fr.Type != h3Settings {
				return h3ConnectionError{Code: h3MissingSettings, Err: fmt.Errorf(
					"%w: the control stream opened with frame %#x, not SETTINGS", ErrH3Protocol, fr.Type)}
			}
			if err := a.settings(fr.Payload); err != nil {
				// A payload that ends inside a pair fails the frame's layout,
				// which §7.1 makes H3_FRAME_ERROR; what the pairs say is
				// §7.2.4's H3_SETTINGS_ERROR.
				code := uint64(h3SettingsError)
				if errors.Is(err, errH3FrameLayout) {
					code = h3FrameError
				}
				return h3ConnectionError{Code: code, Err: err}
			}
			a.controlSettings = true
			continue
		}
		switch fr.Type {
		case h3Settings:
			return h3ConnectionError{Code: h3FrameUnexpected, Err: fmt.Errorf(
				"%w: a second SETTINGS on the control stream", ErrH3Protocol)}
		case h3Data, h3Headers, h3PushPromise:
			return h3ConnectionError{Code: h3FrameUnexpected, Err: fmt.Errorf(
				"%w: frame type %#x on the control stream", ErrH3Protocol, fr.Type)}
		case h3GoAway, h3MaxPushID, h3CancelPush:
			// Legal here, and inert: this server never pushes and announces
			// its own shutdown by closing, so there is nothing to act on.
		}
	}
	u.buf = append(u.buf[:0], rest...)
	return nil
}

// encoderInstructions consumes QPACK encoder-stream instructions against the
// dynamic-table capacity of zero this server advertised. RFC 9204 §3.2.3
// binds the peer that accepted it to send no instruction that uses the table,
// so there is almost nothing legal to arrive here: the one shape tolerated is
// Set Dynamic Table Capacity (§4.3.1) carrying zero, which changes nothing
// and which §4.3.1 gives no ground to refuse. An insert, a duplicate, or a
// capacity above the advertised zero is the connection error
// QPACK_ENCODER_STREAM_ERROR (§4.2, §8.3).
func (a *h3Assembler) encoderInstructions(u *h3Uni) error {
	for len(u.buf) > 0 {
		if u.buf[0]&0xe0 != 0x20 {
			// Insert With Name Reference (1xxxxxxx), Insert With Literal
			// Name (01xxxxxx) and Duplicate (000xxxxx) all grow or read a
			// table whose capacity cannot leave zero.
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: encoder instruction %#x against a dynamic-table capacity of zero", ErrH3Protocol, u.buf[0])}
		}
		capacity, rest, err := hpackInt(u.buf, 5)
		if err != nil {
			// An integer that runs off the end has not all arrived yet; one
			// wider than 62 bits (RFC 9204 §4.1.1) never will be legal.
			if errors.Is(err, errIntegerShort) {
				return nil
			}
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: a dynamic-table capacity wider than 62 bits", ErrH3Protocol)}
		}
		if capacity != 0 {
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: a dynamic-table capacity of %d, but zero was advertised", ErrH3Protocol, capacity)}
		}
		u.buf = append(u.buf[:0], rest...)
	}
	return nil
}

// decoderInstructions consumes the client's QPACK decoder stream (RFC 9204
// §4.4), which reports on this server's encoder's dynamic table. That table
// is never used: every field section goes out with a Required Insert Count of
// zero and nothing is ever inserted. So Stream Cancellation (§4.4.2), which a
// decoder may send for any reset stream, is the one instruction that can
// legitimately arrive; a Section Acknowledgment (§4.4.1) names a section
// with nothing to acknowledge and an Insert Count Increment (§4.4.3) counts
// inserts never sent, and both are QPACK_DECODER_STREAM_ERROR. Discarding the
// stream unread, as before, accepted either without a word.
func (a *h3Assembler) decoderInstructions(u *h3Uni) error {
	for len(u.buf) > 0 {
		switch b := u.buf[0]; {
		case b&0x80 != 0:
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: a Section Acknowledgment, but no field section used the dynamic table", ErrH3Protocol)}
		case b&0x40 == 0:
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: an Insert Count Increment, but nothing was ever inserted", ErrH3Protocol)}
		}
		// Stream Cancellation: a stream identifier with a 6-bit prefix, and
		// nothing for an encoder without a table to do about it. The
		// identifier is a QUIC stream ID, 62 bits wide (RFC 9000 §2.1), and
		// hpackInt admits the whole width: a decoder that stopped at 28 bits
		// used to close the connection over the cancellation of stream
		// 2^28 — a legal instruction a long-lived client reaches, since
		// HTTP/3 caps no request count.
		_, rest, err := hpackInt(u.buf, 6)
		if err != nil {
			// As in encoderInstructions: an integer that runs off the end
			// has not all arrived yet.
			if errors.Is(err, errIntegerShort) {
				return nil
			}
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: a Stream Cancellation naming a stream wider than 62 bits", ErrH3Protocol)}
		}
		u.buf = append(u.buf[:0], rest...)
	}
	return nil
}

// peerMaxFieldSection is the MAX_FIELD_SECTION_SIZE the client advertised, or
// zero when none has arrived — zero meaning unbounded, since RFC 9114 §4.2.2
// binds a sender only once the parameter has been received.
func (a *h3Assembler) peerMaxFieldSection() uint64 {
	return a.peerFieldSection
}

// settings takes the client's SETTINGS, keeping what this server acts on.
//
// A repeated identifier is refused among the low ones, where every setting
// RFC 9114 and RFC 9204 define lives; above them a repeat is two values of a
// setting this server ignores either way, which §7.2.4 lets it pass over
// (refusing is a MAY) without remembering every identifier to find out.
func (a *h3Assembler) settings(payload []byte) error {
	var seen uint64 // a bit per identifier below 64
	return walkH3Settings(payload, func(id, v uint64) error {
		if id < 64 {
			if seen&(1<<id) != 0 {
				return fmt.Errorf("%w: setting %#x appears twice", ErrH3Protocol, id)
			}
			seen |= 1 << id
		}
		if id == h3SettingMaxFieldSection {
			a.peerFieldSection = v
		}
		return nil
	})
}

// criticalGone is the connection error a closed critical stream is
// (RFC 9114 §6.2.1, RFC 9204 §4.2), and nil for every stream whose closure
// is the peer's own business.
func (a *h3Assembler) criticalGone(kind int) error {
	switch kind {
	case h3StreamControl:
		return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
			"%w: the control stream closed", ErrH3Protocol)}
	case h3StreamQPACKEncode, h3StreamQPACKDecode:
		return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
			"%w: a QPACK stream closed", ErrH3Protocol)}
	}
	return nil
}

// h3FrameSlack is the room a request's frame headers take beyond its header
// and body bounds — a few varints per frame, generously rounded. ServeH3's
// stream window (Config.TransportParameters) carries the same headroom.
const h3FrameSlack = 4096

// rawStreamBound is what one request stream's unparsed bytes may reach: a
// whole request, head and body at their bounds, with their framing.
func (a *h3Assembler) rawStreamBound() int {
	return a.cfg.MaxBodyBytes + a.cfg.MaxHeaderBytes + h3FrameSlack
}

// drain consumes whole HTTP/3 frames out of a stream's buffer, leaving what
// has not arrived. A QUIC stream is a byte stream: a frame split across two
// datagrams is ordinary rather than an error.
func (a *h3Assembler) drain(st *h3Stream) error {
	frames, rest, err := parseH3Frames(a.frames[:0], st.buf, a.cfg.MaxBodyBytes)
	// A frame over the bound leaves rest at its start. A DATA frame there is
	// judged below, after the frames ahead of it, as a body over
	// MaxBodyBytes: h1 and h2 answer that with a 413, and the declared length
	// says so before any of the payload is buffered. Any other type keeps the
	// refusal parseH3Frames gave it.
	if typ, _, e := quic.Varint(rest); err != nil && (e != nil || typ != h3Data) {
		return err
	}
	a.frames = frames
	for _, f := range frames {
		switch f.Type {
		case h3Headers:
			// A HEADERS frame answers to the header bound, not the body one
			// ParseH3Frames enforced: MaxBodyBytes is 32× the advertised
			// MAX_FIELD_SECTION_SIZE, and a frame that large bought a QPACK
			// decode far past the budget the server announced.
			if len(f.Payload) > a.cfg.MaxHeaderBytes {
				return fmt.Errorf("%w: a HEADERS frame of %d bytes, over the %d header bound",
					ErrTooLarge, len(f.Payload), a.cfg.MaxHeaderBytes)
			}
			if st.gotTrailers {
				return fmt.Errorf("%w: a HEADERS frame after the trailers", errH3FrameUnexpected)
			}
			if st.gotHead {
				// Trailers: held to the head's bounds and delivered in
				// Request.Trailers — and their bytes are held to the same
				// octet rules as everything else that could be
				// re-serialised toward an HTTP/1.1 hop (§4.1.2, §10.3).
				// Decoded behind the head's fields in the same storage:
				// Request.Headers is capped at its own length, so neither
				// overwrites the other.
				trailers, err := a.qpack.Decode(st.headers[len(st.headers):], f.Payload)
				if err != nil {
					return sectionRefused(err)
				}
				// Same field-count bound as the request block below.
				if len(trailers) > a.cfg.MaxHeaders {
					return h3StatusFail(431, "%w: over %d trailer fields",
						ErrTooLarge, a.cfg.MaxHeaders)
				}
				for _, h := range trailers {
					if len(h.Name) == 0 {
						return fmt.Errorf("%w: an empty trailer name", ErrH3Protocol)
					}
					if h.Name[0] == ':' {
						return fmt.Errorf("%w: pseudo-header %q in the trailers", ErrH3Protocol, h.Name)
					}
					if !lowerASCII(h.Name) || !isToken(h.Name) {
						return fmt.Errorf("%w: trailer name %q is not a token", ErrH3Protocol, h.Name)
					}
					if !validFieldValue(h.Value) {
						return fmt.Errorf("%w: the value of trailer %q holds a control character", ErrH3Protocol, h.Name)
					}
				}
				if len(trailers) > 0 {
					st.req.Trailers = trailers
				}
				st.gotTrailers = true
				continue
			}
			if st.headers, err = a.qpack.Decode(st.headers[:0], f.Payload); err != nil {
				return sectionRefused(err)
			}
			// The field-count bound the h1 parser applies as it reads and h2
			// applies after its HPACK decode, applied here at the first
			// moment the count exists: the byte bound is not this bound — a
			// block of near-empty fields fits the bytes while making every
			// [Request.Get] walk hundreds of entries.
			if fieldCount(st.headers) > a.cfg.MaxHeaders {
				return h3StatusFail(431, "%w: over %d header fields",
					ErrTooLarge, a.cfg.MaxHeaders)
			}
			req, declared, err := buildRequest(st.headers, ErrH3Protocol)
			if err != nil {
				return err
			}
			st.headers, st.declared, st.req = req.Headers, declared, req
			st.req.Headers = req.Headers[:len(req.Headers):len(req.Headers)]
			st.gotHead = true
		case h3Data:
			if !st.gotHead {
				return fmt.Errorf("%w: DATA before HEADERS", errH3FrameUnexpected)
			}
			if st.gotTrailers {
				return fmt.Errorf("%w: DATA after the trailers", errH3FrameUnexpected)
			}
			// The unparsed buffer is bounded on arrival, but DATA moves out
			// of it as it drains — without this check a client streaming
			// DATA frames would grow the body without limit, bounded buffer
			// notwithstanding.
			if len(st.req.Body)+len(f.Payload) > a.cfg.MaxBodyBytes {
				return h3StatusFail(413, "%w: a body over %d bytes", ErrTooLarge, a.cfg.MaxBodyBytes)
			}
			st.req.Body = append(st.req.Body, f.Payload...)
		case h3Settings, h3GoAway, h3MaxPushID, h3CancelPush:
			// §7.2.3, §7.2.4, §7.2.6, §7.2.7: each names a connection error
			// of type H3_FRAME_UNEXPECTED for its frame off the control stream.
			return fmt.Errorf("%w: frame type %#x belongs on the control stream", errH3FrameUnexpected, f.Type)
		case h3PushPromise:
			// §7.2.5: "A server MUST treat the receipt of a PUSH_PROMISE
			// frame as a connection error of type H3_FRAME_UNEXPECTED."
			return fmt.Errorf("%w: a client sent PUSH_PROMISE", errH3FrameUnexpected)
		}
	}
	// What is left is the start of a frame that has not all arrived, kept at
	// the front for the next datagram — unless its declaration already
	// refuses it: a HEADERS frame over the header bound, or a DATA frame that
	// would take the body past MaxBodyBytes, refused before a buffer near
	// that size has been parked waiting for the payload. RFC 9114 §4.1 lets
	// the server answer before the request has all arrived.
	if typ, after, e := quic.Varint(rest); e == nil {
		if length, _, e2 := quic.Varint(after); e2 == nil {
			switch {
			case typ == h3Headers && length > uint64(a.cfg.MaxHeaderBytes): //nolint:gosec // G115: positive config, set by withDefaults
				return fmt.Errorf("%w: a HEADERS frame of %d bytes, over the %d header bound",
					ErrTooLarge, length, a.cfg.MaxHeaderBytes)
			case typ == h3Data && !st.gotHead:
				return fmt.Errorf("%w: DATA before HEADERS", errH3FrameUnexpected)
			case typ == h3Data && st.gotTrailers:
				return fmt.Errorf("%w: DATA after the trailers", errH3FrameUnexpected)
			case typ == h3Data && uint64(len(st.req.Body))+length > uint64(a.cfg.MaxBodyBytes): //nolint:gosec // G115: positive config, set by withDefaults
				return h3StatusFail(413, "%w: a body over %d bytes", ErrTooLarge, a.cfg.MaxBodyBytes)
			}
		}
	}
	if err != nil {
		return err // unreachable: a DATA frame over the bound is refused above
	}
	st.buf = append(st.buf[:0], rest...)
	return nil
}

// sectionRefused turns the decoder's error for a field section into the
// refusal it earns. A section past the list bound is a 431 on the stream
// (RFC 9114 §4.2.2), as the h2 reader's blockRefused has it. Anything else —
// a dynamic reference, an index past the static table, a truncated string —
// is a connection error of type QPACK_DECOMPRESSION_FAILED (RFC 9204 §2.2.3,
// §3.1): the decoder "is not able to continue decoding that field section",
// and the section's stream is not the fault. It used to be served as a
// malformed request on the stream, which left a client whose encoder is
// broken retrying the same bytes on the next one.
func sectionRefused(err error) error {
	switch {
	case errors.Is(err, errHeadersTooLarge):
		return h3StatusError{status: 431, err: err}
	case errors.Is(err, errIntegerWide):
		// RFC 9204 §7.4: "a value larger than it is able to decode" is a
		// *stream* error of type QPACK_DECOMPRESSION_FAILED on a request
		// stream. Frames reads this through the sentinel.
		return err
	}
	return h3ConnectionError{Code: qpackDecompressionFailed, Err: err}
}

// appendH3StreamedBody writes one batch of a streamed response as DATA
// frames. The stream's FIN is what ends the body, so there is no terminator
// to write here.
func appendH3StreamedBody(dst []byte, chunks [][]byte) []byte {
	for _, c := range chunks {
		if len(c) == 0 {
			continue // an empty batch is cadence, not an end
		}
		dst = appendH3Frame(dst, h3Data, c)
	}
	return dst
}

// appendH3ResponseBasic writes one response's HEADERS frame, and its DATA frame if
// it has a body to send in full now — RFC 9110 §8.6's 204/304 never do,
// no matter who asks. It has no request to check against, so a HEAD
// response's body is this function's business only through [appendH3Response]
// — the internal responder path, which knows the method and calls that
// instead.
func appendH3ResponseBasic(dst []byte, r Response) ([]byte, error) {
	return appendH3Response(dst, r, nil, 0, &h3Scratch{}, dateFor(time.Now()))
}

// h3Scratch is one responder's reusable per-response state: the field list
// and QPACK block appendH3Response builds, and the digits of the two numeric
// values it writes. One of these per responder goroutine is what makes the
// response path allocation-free — safe because [quic.Stream.Write] copies into its own frame before returning.
type h3Scratch struct {
	head   []byte
	fields []Header
	qpack  []byte
	names  []byte   // lowercased copies of the field names a handler spelled in HTTP/1.1 case
	status [3]byte  // checkStatus bounds a final status to 200..599
	clen   [20]byte // enough digits for any int
}

// appendH3Response is AppendH3Response's real body, plus two things the
// exported wrapper cannot know: the request's method, needed to withhold a
// HEAD response's bytes the same way the HTTP/1.1 and HTTP/2 paths do, and
// the peer's SETTINGS_MAX_FIELD_SECTION_SIZE — zero for unbounded — which
// RFC 9114 §4.2.2 says a sender SHOULD NOT exceed. A field section that
// would exceed it is refused here rather than sent to a client that told
// us it will not accept it. date is [dateFor]'s value for the batch.
func appendH3Response(dst []byte, r Response, method []byte, peerFieldSection uint64, sc *h3Scratch, date []byte) ([]byte, error) {
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
	fields := append(sc.fields[:0], Header{Name: h3StatusName, Value: appendDigits(sc.status[:0], status)})
	if !datedBy(r.Headers) {
		// RFC 9110 §6.6.1's Date, as the other two writers add it and
		// where: on every final response the handler did not date itself,
		// first after :status. The value is the batch's, formatted once and
		// never written after.
		fields = append(fields, Header{Name: h3DateName, Value: date})
	}
	sc.names = sc.names[:0]
	for _, h := range r.Headers {
		// A name the handler spelled in HTTP/1.1 case goes on the wire
		// lowercase (RFC 9114 §4.2), from a copy in sc.names: the handler's
		// bytes are borrowed, never written. The copies outlive the loop,
		// since fields holds them until appendQPACK reads them.
		name := lowerName(h.Name, &sc.names)
		// The octet rule the HTTP/1.1 writer applies (response.go): QPACK
		// carries any byte, and a name holding a space or a colon becomes a
		// second field, or a malformed one, at the first hop that
		// re-serialises this response toward HTTP/1.1.
		if !isToken(name) {
			return nil, fmt.Errorf("%w: response field %q is not a token", ErrH3Protocol, h.Name)
		}
		if hasCRLF(h.Value) {
			return nil, ErrHeaderInjection
		}
		if hasFieldControl(h.Value) {
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrH3Protocol, h.Name)
		}
		// The HTTP/2 writer's refusals, for the same reasons: a
		// connection-specific field makes the response malformed (RFC 9114
		// §4.2), and framing is this package's to state.
		switch string(name) {
		case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
			return nil, fmt.Errorf("%w: the connection-specific response field %q", ErrH3Protocol, h.Name)
		case "content-length":
			return nil, fmt.Errorf("%w: content-length is set by the writer, not the caller", ErrH3Protocol)
		}
		fields = append(fields, Header{Name: name, Value: h.Value})
	}
	if !noLength && (!r.streamed() || r.ContentLength > 0) {
		// A streamed response states the length it declared, or none, which
		// is what the stream's FIN says instead. A 204/304 never states one
		// either.
		var v []byte
		if r.streamed() {
			v = strconv.AppendInt(sc.clen[:0], r.ContentLength, 10)
		} else {
			v = appendDigits(sc.clen[:0], len(r.Body))
		}
		fields = append(fields, Header{Name: h3ContentLengthName, Value: v})
	}
	if err := checkH3FieldSection(fields, peerFieldSection); err != nil {
		return nil, err
	}

	sc.fields = fields
	sc.qpack = appendQPACK(sc.qpack[:0], fields)
	dst = appendH3Frame(dst, h3Headers, sc.qpack)
	if !noBody && len(r.Body) > 0 {
		dst = appendH3Frame(dst, h3Data, r.Body)
	}
	if !noBody && !r.streamed() && r.Trailers != nil {
		// A body in hand has its trailers in the same write, so a refusal
		// is still answered with a 500.
		return appendH3Trailers(dst, r, peerFieldSection, sc)
	}
	return dst, nil
}

// checkH3FieldSection refuses a field section over the peer's
// SETTINGS_MAX_FIELD_SECTION_SIZE, zero for unbounded, measured as §4.2.2
// does: the uncompressed name and value, plus 32 per field.
func checkH3FieldSection(fields []Header, peerFieldSection uint64) error {
	if peerFieldSection == 0 {
		return nil
	}
	var size uint64
	for _, h := range fields {
		size += uint64(len(h.Name)+len(h.Value)) + 32
	}
	if size > peerFieldSection {
		return fmt.Errorf("%w: a field section of %d bytes, over the peer's %d limit",
			ErrH3Protocol, size, peerFieldSection)
	}
	return nil
}

// appendH3Trailers writes a response's trailer section as the HEADERS frame
// that follows its DATA (RFC 9114 §4.1), held to [checkTrailer] and to the
// peer's field section limit as the head is. Its names go on the wire
// lowercase, from copies, as the head's do.
func appendH3Trailers(dst []byte, r Response, peerFieldSection uint64, sc *h3Scratch) ([]byte, error) {
	trailers, err := r.trailers()
	if err != nil {
		return nil, err
	}
	fields := sc.fields[:0]
	sc.names = sc.names[:0]
	for _, h := range trailers {
		fields = append(fields, Header{Name: lowerName(h.Name, &sc.names), Value: h.Value})
	}
	sc.fields = fields
	if err := checkH3FieldSection(fields, peerFieldSection); err != nil {
		return nil, err
	}
	sc.qpack = appendQPACK(sc.qpack[:0], fields)
	return appendH3Frame(dst, h3Headers, sc.qpack), nil
}

// h3StatusName, h3ContentLengthName and h3DateName are the three field names
// the responder writes itself, kept as package bytes so no response
// re-allocates them.
var (
	h3StatusName        = []byte(":status")
	h3ContentLengthName = []byte("content-length")
	h3DateName          = []byte("date")
)

// appendDigits writes v in decimal, the no-string itoa the response scratch
// wants: itoa's string return would allocate once per use here.
func appendDigits(dst []byte, v int) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return append(dst, b[i:]...)
}
