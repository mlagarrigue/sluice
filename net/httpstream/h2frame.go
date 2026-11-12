package httpstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mlagarrigue/sluice"
)

// HTTP/2's binary framing, read as a stream of batches.
//
// This is the framing layer and stops there: it reads and validates frames,
// and does not decode header blocks. See the package documentation for why
// that boundary is where it is, and what a server would still need.
//
// The reason the layer is worth having on its own is that it settles the
// architectural question HTTP/1.1 could only answer for a rare client. Over
// HTTP/1.1 a batch requires pipelining, which almost nobody does. Over HTTP/2
// a connection multiplexes many streams by design, so one read routinely
// carries frames belonging to different requests — the batch is the normal
// case rather than the exotic one.

// Preface is the connection preface a client sends before anything else
// (RFC 9113 §3.4). It is a fixed 24-byte string chosen to be invalid HTTP/1.1,
// so a server that speaks both cannot mistake one for the other.
const Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// Frame types (RFC 9113 §6). Named because a switch on 0x1 is unreadable.
const (
	frameData         = 0x0
	frameHeaders      = 0x1
	framePriority     = 0x2
	frameRSTStream    = 0x3
	frameSettings     = 0x4
	framePushPromise  = 0x5
	framePing         = 0x6
	frameGoAway       = 0x7
	frameWindowUpdate = 0x8
	frameContinuation = 0x9
)

// Frame flags, per type.
const (
	flagEndStream  = 0x1 // DATA, HEADERS
	flagAck        = 0x1 // SETTINGS, PING
	flagEndHeaders = 0x4 // HEADERS, CONTINUATION, PUSH_PROMISE
	flagPadded     = 0x8 // DATA, HEADERS, PUSH_PROMISE
	flagPriority   = 0x20
)

// frameHeaderSize is the fixed prefix: length(24) type(8) flags(8)
// reserved(1)+streamID(31).
const frameHeaderSize = 9

// defaultMaxFrameSize is SETTINGS_MAX_FRAME_SIZE's initial value (RFC 9113
// §6.5.2). A peer may negotiate more; until it does, this is the ceiling, and
// a frame declaring more is refused before a byte of it is read.
const defaultMaxFrameSize = 1 << 14 // 16384

// maxFrameSize is the largest SETTINGS_MAX_FRAME_SIZE RFC 9113 §6.5.2 allows,
// and the most the frame header's 24-bit length can express.
const maxFrameSize = 1<<24 - 1

// h2Frame is one HTTP/2 frame. Payload borrows the connection's read buffer,
// like every other batch element in this library.
type h2Frame struct {
	Type     byte
	Flags    byte
	StreamID uint32
	Payload  []byte
}

// EndsStream reports the END_STREAM flag, which only DATA and HEADERS carry.
func (f h2Frame) EndsStream() bool {
	return (f.Type == frameData || f.Type == frameHeaders) && f.Flags&flagEndStream != 0
}

// EndsHeaders reports the END_HEADERS flag, which is what closes a header
// block that CONTINUATION frames may have extended.
func (f h2Frame) EndsHeaders() bool {
	switch f.Type {
	case frameHeaders, frameContinuation, framePushPromise:
		return f.Flags&flagEndHeaders != 0
	}
	return false
}

// defaultMaxConcurrency is SETTINGS_MAX_CONCURRENT_STREAMS' default here, and
// [Config.MaxConcurrentStreams]'s for HTTP/3 as well. The HTTP/2 protocol
// has no initial value for it — a peer that is told nothing may open as many
// as it likes — so saying a number is the bound, not a preference; 100 is
// the floor RFC 9114 §6.1 recommends an HTTP/3 server grant, and what
// [quic.DefaultConfig] grants, so one number serves both protocols.
const defaultMaxConcurrency = 100

// ErrH2Protocol reports a frame sequence this package will not accept. Like
// every framing failure here it ends the connection: a stream whose boundaries
// are in doubt cannot be resynchronised. Named as [ErrHPACK] explains.
var ErrH2Protocol = errors.New("httpstream: HTTP/2 protocol error")

// h2Frames reads a connection as a stream of frame batches, after checking the
// preface.
//
// A batch is what one read returned, parsed into whole frames. Because HTTP/2
// multiplexes, those frames routinely belong to different streams — which is
// the batch this library wants, arriving without anybody waiting for it and
// without the client having to do anything unusual.
//
// Payloads borrow the read buffer and are valid for the duration of the call
// that received the batch.
func h2Frames(c net.Conn, cfg Config) sluice.Source[h2Frame] {
	return framesWith(c, cfg, nil, nil, nil)
}

// framesWith is Frames with the server's view of whether the connection is
// idle. busy reports that this end owes the peer answers, in which case a
// silent round does not end the connection — see [Config.IdleTimeout]. A nil busy is a
// connection with nothing above it, which is what h2Frames gives a caller
// composing operators directly, and it is idle whenever it is silent.
//
// live reports whether a stream is still open at the server, so that only
// a DATA frame that advances one counts as use of the connection; a nil
// live counts every DATA frame.
//
// due is the earliest moment a request still assembling runs out of
// ReadTimeout, zero when none is: a read that reaches it with nothing to
// show ends in an empty batch rather than waiting on, so that the consumer
// resets the stream whose peer went silent (h2Conn.expireAssembly). A nil
// due is a reader with no assembly above it.
//
// cfg is resolved: every caller has been through Config.resolve.
func framesWith(c net.Conn, cfg Config, busy func() bool, live func(id uint32) bool, due func() time.Time) sluice.Source[h2Frame] {
	r := &frameReader{conn: c, cfg: cfg, busy: busy, live: live, due: due}
	return sluice.NewSource(sluice.Stream[h2Frame](r.stream), func() error { return r.err })
}

type frameReader struct {
	conn net.Conn
	cfg  Config
	busy func() bool
	buf  []byte
	n    int
	// base is the array buf is a window of, as for h1Conn: consume moves
	// the window, fill moves the tail down when there is no room after it.
	base []byte
	err  error

	prefaced bool
	// settled is set once the first frame after the preface has been
	// parsed: RFC 9113 §3.4 makes that frame a SETTINGS (ACK flag clear),
	// and anything else is an invalid preface, a connection error.
	settled bool
	batch   []h2Frame

	// pending is what the last batch consumed. Compacted at the start of the
	// next round, not before the batch was yielded: the frames borrow the
	// buffer.
	pending int

	// deadline is the current frame's absolute read deadline, held fixed
	// once set rather than re-armed on every read — see the equivalent field
	// on httpstream's HTTP/1.1 reader for why a sliding deadline is not a
	// timeout at all.
	deadline time.Time
	// idleAt is the absolute deadline of the idle clock, armed when a silent
	// round begins with nothing owed and held until stream activity — a
	// HEADERS, CONTINUATION or DATA frame — re-arms it. A PING, SETTINGS or
	// WINDOW_UPDATE does not: a peer that sends nothing but those is holding
	// the connection, not using it (see Config.IdleTimeout). streamSeen says
	// whether any stream activity ever arrived; until it has, the clock is
	// ReadTimeout's, the bound a connection that has not spoken yet gets.
	//
	// A frame of a stream counts only on a stream live says is open or not
	// yet opened: an empty DATA or HEADERS on a stream closed long ago is
	// tolerated without an answer (h2Conn.data, h2Conn.stream), and counted
	// as use, nine bytes per IdleTimeout held a connection that carried no
	// request — RFC 9113 §10.5's idle connection, kept by frames the clock
	// did not tell apart.
	idleAt         time.Time
	streamActivity bool
	streamSeen     bool
	live           func(id uint32) bool
	// due is framesWith's: the earliest assembly deadline, which bounds
	// every read along with the frame's and the idle clock's.
	due func() time.Time

	// continuations and headerBlock track an open header block across
	// CONTINUATION frames, which is where the flood defence lives.
	openStream    uint32
	inHeaderBlock bool
	continuations int
	headerBlock   int
}

func (r *frameReader) stream(yield func(sluice.Batch[h2Frame]) bool) {
	for {
		batch, ok := r.next()
		if !ok {
			return
		}
		if !yield(sluice.Batch[h2Frame]{Items: batch}) {
			return
		}
	}
}

func (r *frameReader) next() ([]h2Frame, bool) {
	for {
		if r.pending > 0 {
			r.consume(r.pending)
			r.pending = 0
			r.deadline = time.Time{}
		}
		batch, used, err := r.parseBuffered()
		if err != nil {
			r.err = err
			return nil, false
		}
		if len(batch) > 0 {
			r.pending = used
			for i := range batch {
				if r.usesStream(batch[i]) {
					r.streamActivity, r.streamSeen = true, true
					break
				}
			}
			return batch, true
		}
		// The preface consumes bytes without producing a frame, so what it
		// took has to be dropped here or the next round parses it again — as
		// a frame, which it is not. Its deadline goes with it, as a consumed
		// frame's does above.
		if used > 0 {
			r.consume(used)
			r.deadline = time.Time{}
			continue
		}
		if err := r.fill(); err != nil {
			if errors.Is(err, errAssemblyDue) {
				// Nothing arrived by a request's assembly deadline: an empty
				// batch, for the consumer to reset it, then reading on.
				return r.batch[:0], true
			}
			r.err = err
			return nil, false
		}
	}
}

// errAssemblyDue ends a read that reached the earliest assembly deadline
// with nothing to show (see framesWith's due). It never leaves the reader.
var errAssemblyDue = errors.New("httpstream: a request's assembly deadline passed")

// usesStream reports whether f is use of the connection for the idle clock:
// a HEADERS, CONTINUATION or DATA on a stream that is open or not yet opened
// (live).
func (r *frameReader) usesStream(f h2Frame) bool {
	switch f.Type {
	case frameHeaders, frameContinuation, frameData:
		return r.live == nil || r.live(f.StreamID)
	}
	return false
}

// consume drops what was parsed by moving the window past it, not the tail
// down: see h1Conn.consume.
func (r *frameReader) consume(used int) {
	if r.base == nil {
		r.base = r.buf
	}
	if used == r.n {
		r.buf = r.base
	} else {
		r.buf = r.buf[used:]
	}
	r.n -= used
}

func (r *frameReader) parseBuffered() (batch []h2Frame, used int, err error) {
	if !r.prefaced {
		if r.n < len(Preface) {
			return nil, 0, nil // not enough to judge yet
		}
		if string(r.buf[:len(Preface)]) != Preface {
			return nil, 0, fmt.Errorf("%w: the connection preface is not HTTP/2's", ErrH2Protocol)
		}
		r.prefaced = true
		used = len(Preface)
	}

	r.batch = r.batch[:0]
	for len(r.batch) < r.cfg.MaxFramesPerBatch {
		f, n, err := parseFrame(r.buf[used:r.n], r.cfg.MaxFrameSize)
		if errors.Is(err, errIncomplete) {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		if !r.settled {
			r.settled = true
			if f.Type != frameSettings || f.Flags&flagAck != 0 {
				// §3.4: the preface "MUST be followed by a SETTINGS frame".
				// A PING or a HEADERS first is a peer that skipped its
				// preface, and the invalid preface is PROTOCOL_ERROR.
				return nil, 0, fmt.Errorf("%w: the first frame after the preface is type %#x, not SETTINGS (RFC 9113 §3.4)", ErrH2Protocol, f.Type)
			}
		}
		if err := r.trackHeaderBlock(f); err != nil {
			return nil, 0, err
		}
		r.batch = append(r.batch, f)
		used += n
	}
	// The preface alone is progress worth reporting, so the caller's stream
	// starts even before the first frame arrives.
	if len(r.batch) == 0 && used == 0 {
		return nil, 0, nil
	}
	return r.batch, used, nil
}

// trackHeaderBlock enforces what RFC 9113 §6.10 requires and what the
// CONTINUATION flood taught: a header block is one HEADERS followed only by
// CONTINUATION frames on the same stream, bounded in count and in bytes.
func (r *frameReader) trackHeaderBlock(f h2Frame) error {
	if r.inHeaderBlock {
		if f.Type != frameContinuation || f.StreamID != r.openStream {
			return fmt.Errorf("%w: %#x on stream %d interrupts an open header block on stream %d",
				ErrH2Protocol, f.Type, f.StreamID, r.openStream)
		}
		r.continuations++
		r.headerBlock += len(f.Payload)
		if r.continuations > r.cfg.MaxContinuationFrames {
			return fmt.Errorf("%w: over %d CONTINUATION frames in one header block (Config.MaxContinuationFrames)",
				ErrH2Protocol, r.cfg.MaxContinuationFrames)
		}
		if r.headerBlock > r.cfg.MaxHeaderBytes {
			return fmt.Errorf("%w: a header block over %d bytes (Config.MaxHeaderBytes)", ErrH2Protocol, r.cfg.MaxHeaderBytes)
		}
		if f.EndsHeaders() {
			r.inHeaderBlock = false
		}
		return nil
	}

	switch f.Type {
	case frameContinuation:
		return fmt.Errorf("%w: CONTINUATION with no open header block", ErrH2Protocol)
	case frameHeaders:
		r.headerBlock = len(f.Payload)
		if r.headerBlock > r.cfg.MaxHeaderBytes {
			return fmt.Errorf("%w: a header block over %d bytes (Config.MaxHeaderBytes)", ErrH2Protocol, r.cfg.MaxHeaderBytes)
		}
		if !f.EndsHeaders() {
			r.inHeaderBlock, r.openStream, r.continuations = true, f.StreamID, 0
		}
	}
	return nil
}

func (r *frameReader) fill() error {
	limit := len(Preface) + frameHeaderSize + r.cfg.MaxFrameSize
	if r.n >= limit {
		return fmt.Errorf("%w: a frame fills the %d-byte buffer without completing", ErrTooLarge, limit)
	}
	if len(r.buf) == r.n && len(r.buf) < len(r.base) {
		copy(r.base, r.buf[:r.n])
		r.buf = r.base
	}
	if len(r.buf) == r.n {
		grown := min(max(2*len(r.buf), 4<<10), limit)
		next := make([]byte, grown)
		copy(next, r.buf[:r.n])
		r.buf, r.base = next, next
	}
	// A silent round that began while answers were owed never ends the
	// connection; it is followed by a fresh round instead, so the idle close
	// always follows a full IdleTimeout armed with nothing owed. The deadline
	// is armed even while owed — only this goroutine touches it, which keeps
	// re-arming race-free, where a responder re-arming it when its last
	// answer goes out would race this goroutine's own SetReadDeadline — so a
	// connection whose last answer just went out is reaped within two
	// IdleTimeouts of it, rather than never. Once armed with nothing owed,
	// the idle deadline holds across rounds that bring no stream activity:
	// were a PING to restart it, a connection with no stream ever opened
	// could be held for nothing at the price of 17 bytes per IdleTimeout.
	//
	// A frame begun does not stop the idle clock either: a peer that sent
	// each whole frame with the first byte of the next behind it kept the
	// buffer from ever being empty, and the clock was never read — a
	// connection held with no request for seventeen bytes per ReadTimeout.
	// With nothing owed, the deadline is the nearer of the frame's and the
	// idle clock's, and the idle clock's ending the read is the idle close.
	for {
		var deadline time.Time
		owed, idleCut := false, false
		switch {
		case r.busy != nil && r.busy():
			owed = true
			r.idleAt = time.Time{}
			deadline = time.Now().Add(r.cfg.IdleTimeout)
			if r.n > 0 {
				deadline = r.frameDeadline()
			}
		case r.n > 0:
			deadline = r.frameDeadline()
			if idle := r.idleClock(); idle.Before(deadline) {
				deadline, idleCut = idle, true
			}
		default:
			deadline = r.idleClock()
		}
		// A request assembling whose deadline comes first: the read ends
		// there, so that a peer that went silent mid-request has the stream
		// reset at ReadTimeout rather than when it next sends something.
		dueCut := false
		if r.due != nil {
			if due := r.due(); !due.IsZero() && due.Before(deadline) {
				deadline, dueCut, idleCut = due, true, false
			}
		}
		if err := r.conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		n, err := r.conn.Read(r.buf[r.n:])
		r.n += n
		if err == nil {
			return nil
		}
		if errors.Is(err, io.EOF) && r.n == 0 {
			return io.EOF
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: the connection closed mid-frame", ErrH2Protocol)
		}
		if dueCut && isTimeout(err) {
			return errAssemblyDue
		}
		if idleCut && isTimeout(err) {
			return fmt.Errorf("%w: %w", errIdleTimeout, err)
		}
		if r.n == 0 && isTimeout(err) {
			if owed {
				// Silence while answers are owed is expected: a streamed
				// response may run for minutes with nothing to hear back.
				continue
			}
			return fmt.Errorf("%w: %w", errIdleTimeout, err)
		}
		return err
	}
}

// frameDeadline is the bound on the frame begun in the buffer: ReadTimeout
// from the moment its first byte waited, kept until the frame is consumed
// (next).
func (r *frameReader) frameDeadline() time.Time {
	if r.deadline.IsZero() {
		r.deadline = time.Now().Add(r.cfg.ReadTimeout)
	}
	return r.deadline
}

// idleClock is the idle clock with nothing owed, re-armed after stream
// activity: IdleTimeout, or ReadTimeout while nothing has been asked of the
// connection yet — the bound a request has to arrive in, not the one a
// connection may rest between two of them.
func (r *frameReader) idleClock() time.Time {
	if r.idleAt.IsZero() || r.streamActivity {
		wait := r.cfg.IdleTimeout
		if !r.streamSeen {
			wait = r.cfg.ReadTimeout
		}
		r.idleAt = time.Now().Add(wait)
		r.streamActivity = false
	}
	return r.idleAt
}

// parseFrame reads one frame header and its payload.
//
// The length is checked against the negotiated maximum *before* the payload is
// waited for, so a peer cannot make this buffer sixteen megabytes by writing
// nine bytes.
func parseFrame(buf []byte, maxFrameSize int) (h2Frame, int, error) {
	var f h2Frame
	if len(buf) < frameHeaderSize {
		return f, 0, errIncomplete
	}
	length := int(buf[0])<<16 | int(buf[1])<<8 | int(buf[2])
	if length > maxFrameSize {
		// §4.2 mandates FRAME_SIZE_ERROR for a frame over the advertised
		// maximum, which errFrameSize puts on the GOAWAY.
		return f, 0, fmt.Errorf("%w: %w: a frame declares %d bytes, over the %d-byte maximum (Config.MaxFrameSize)",
			ErrH2Protocol, errFrameSize, length, maxFrameSize)
	}
	f.Type, f.Flags = buf[3], buf[4]
	// The top bit is reserved and must be ignored rather than rejected
	// (RFC 9113 §4.1), which is one of the few places the RFC asks for
	// tolerance instead of refusal.
	f.StreamID = binary.BigEndian.Uint32(buf[5:9]) & 0x7fffffff

	if len(buf) < frameHeaderSize+length {
		return f, 0, errIncomplete
	}
	f.Payload = buf[frameHeaderSize : frameHeaderSize+length]
	if err := checkFrameShape(f, length); err != nil {
		return f, 0, err
	}
	return f, frameHeaderSize + length, nil
}

// checkFrameShape applies the per-type rules RFC 9113 §6 states, which are
// mostly about which frames belong on the connection stream and which on a
// request stream. Getting these wrong is how a frame is acted on in the wrong
// context.
func checkFrameShape(f h2Frame, length int) error {
	connectionOnly := func() error {
		if f.StreamID != 0 {
			return fmt.Errorf("%w: frame %#x must be on stream 0, got %d", ErrH2Protocol, f.Type, f.StreamID)
		}
		return nil
	}
	streamOnly := func() error {
		if f.StreamID == 0 {
			return fmt.Errorf("%w: frame %#x must be on a request stream, got 0", ErrH2Protocol, f.Type)
		}
		return nil
	}

	switch f.Type {
	case frameData, frameHeaders, frameContinuation:
		return streamOnly()
	case frameRSTStream:
		if err := streamOnly(); err != nil {
			return err
		}
		if length != 4 {
			return fmt.Errorf("%w: %w: RST_STREAM is %d bytes, want 4", ErrH2Protocol, errFrameSize, length)
		}
	case framePriority:
		if err := streamOnly(); err != nil {
			return err
		}
		// A PRIORITY of the wrong length is deliberately not refused here:
		// §6.3 makes it a *stream* error (FRAME_SIZE_ERROR), and this
		// function's refusals end the connection. The server's frame handler
		// resets the one stream instead.
	case frameSettings:
		if err := connectionOnly(); err != nil {
			return err
		}
		if f.Flags&flagAck != 0 && length != 0 {
			return fmt.Errorf("%w: %w: a SETTINGS acknowledgement carries %d bytes", ErrH2Protocol, errFrameSize, length)
		}
		if f.Flags&flagAck == 0 && length%6 != 0 {
			return fmt.Errorf("%w: %w: SETTINGS is %d bytes, not a multiple of 6", ErrH2Protocol, errFrameSize, length)
		}
	case framePing:
		if err := connectionOnly(); err != nil {
			return err
		}
		if length != 8 {
			return fmt.Errorf("%w: %w: PING is %d bytes, want 8", ErrH2Protocol, errFrameSize, length)
		}
	case frameGoAway:
		if err := connectionOnly(); err != nil {
			return err
		}
		if length < 8 {
			return fmt.Errorf("%w: %w: GOAWAY is %d bytes, want at least 8", ErrH2Protocol, errFrameSize, length)
		}
	case frameWindowUpdate:
		if length != 4 {
			return fmt.Errorf("%w: %w: WINDOW_UPDATE is %d bytes, want 4", ErrH2Protocol, errFrameSize, length)
		}
	case framePushPromise:
		// A client may not promise. Refused rather than ignored: a server that
		// tolerates it is acting on a frame it has no state machine for.
		return fmt.Errorf("%w: a client sent PUSH_PROMISE", ErrH2Protocol)
	}
	return nil
}

// appendH2Frame writes a frame header and payload.
func appendH2Frame(dst []byte, f h2Frame) ([]byte, error) {
	if len(f.Payload) > 1<<24-1 {
		return nil, fmt.Errorf("%w: a frame payload of %d bytes cannot be framed", ErrH2Protocol, len(f.Payload))
	}
	n := len(f.Payload)
	dst = append(dst, byte(n>>16), byte(n>>8), byte(n), f.Type, f.Flags) //nolint:gosec // G115: a 24-bit length, checked just above
	dst = binary.BigEndian.AppendUint32(dst, f.StreamID&0x7fffffff)
	return append(dst, f.Payload...), nil
}

// maxConcurrentStreams is the most Config.MaxConcurrentStreams may say. An
// HTTP/2 connection allocates its request queue at that size when it is
// accepted (serveH2Conn), one pointer a slot, so the bound is memory per
// connection the operator sets once; an HTTP/3 connection runs up to a
// handler call and a writer goroutine per stream it admits, and serves no
// more on one connection (h3MaxServeStreams).
const maxConcurrentStreams = 4096
