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

// H2Config bounds an HTTP/2 connection. As everywhere here, a zero value takes
// the modest defaults rather than the generous ones.
type H2Config struct {
	// MaxFrameSize is the largest frame payload accepted. Zero means 16384,
	// the protocol's own initial value. It is capped at 1<<24-1, which is
	// what the length field can express — and floored at 16384, because the
	// value is advertised as SETTINGS_MAX_FRAME_SIZE and RFC 9113 §6.5.2
	// bounds that to [2^14, 2^24-1]: a smaller value on the wire is an
	// illegal setting a conformant client must treat as a connection error,
	// and §4.2 obliges this end to receive 16384-byte frames regardless.
	//
	// It also caps the frames this end sends: a peer may advertise up to
	// 16 MiB, and a frame that large is a buffer that large on this side.
	MaxFrameSize int

	// MaxFramesPerBatch bounds how many frames are handed over at once. Zero
	// means 64.
	MaxFramesPerBatch int

	// MaxContinuationFrames bounds how many CONTINUATION frames may follow
	// one HEADERS before END_HEADERS arrives. Zero means 16.
	//
	// This is not a tuning knob. A peer may legally split a header block
	// across CONTINUATION frames, and a server that accepts an unbounded
	// number of them can be made to read and buffer forever without ever
	// completing a request — CVE-2024-27316's shape, and the reason the bound
	// exists rather than the count being trusted.
	MaxContinuationFrames int

	// MaxHeaderBlockBytes bounds one header block, CONTINUATION frames
	// included. Zero means 64 KiB.
	MaxHeaderBlockBytes int

	// MaxHeaders bounds how many fields one header block may decode to,
	// pseudo-headers and trailers included. Zero means 64, as [Config]'s
	// MaxHeaders does, and it exists for the same reason: the byte bound
	// alone admits a block of hundreds of near-empty fields, which cost
	// lookups — every [Request.Get] is a linear scan — rather than bytes.
	MaxHeaders int

	// MaxBodyBytes bounds one request body, assembled across DATA frames.
	// Zero means 1 MiB, as [Config]'s does.
	//
	// Four times this value additionally bounds what one connection may hold
	// across *all* bodies still assembling. The per-stream bound alone is not
	// a connection bound: flow-control windows are replenished as DATA is
	// buffered, so without the aggregate cap a peer could park
	// MaxConcurrentStreams × MaxBodyBytes — 100 MiB at the defaults — by
	// never finishing any of its uploads. The factor of four keeps several
	// full-size concurrent uploads legal while bounding the parked memory to
	// the same order as one window's worth of honest traffic.
	MaxBodyBytes int

	// MaxConcurrentStreams bounds how many requests one connection may have
	// open or unanswered at once, and is what this server advertises as
	// SETTINGS_MAX_CONCURRENT_STREAMS. Zero means 100.
	//
	// It bounds two things at once, which is why it is one number: the stream
	// state the peer can make this end allocate, and the requests waiting for
	// the handler. A stream occupies its slot from the moment it is admitted
	// until its response has been written, so the queue between the read
	// goroutine and the handler can never hold more than this many requests —
	// which is what lets the read goroutine hand a batch over without ever
	// blocking on it.
	MaxConcurrentStreams int

	// MaxPendingControlBytes bounds the control frames — SETTINGS and PING
	// acknowledgements, WINDOW_UPDATE, RST_STREAM — waiting to be written.
	// Zero means 16 KiB.
	//
	// It is the S1 bound on the one queue a peer fills directly: a client can
	// send PING faster than this end can answer, and the answer to that is to
	// stop reading until the queue drains, not to buffer without limit. The
	// pressure then sits in the peer's send buffer, which is what TCP flow
	// control is for.
	MaxPendingControlBytes int

	// MaxWriteBufferBytes is how much response payload the writer gathers
	// before one write to the socket. Zero means 64 KiB, and it is raised to
	// MaxFrameSize if that is larger.
	//
	// It is the round of the writer's round-robin: one stream on its own gets
	// the whole of it and pays a single syscall for it, and several streams
	// share it a frame at a time so that a large response does not hold a
	// small one behind it.
	MaxWriteBufferBytes int

	// IdleTimeout, ReadTimeout and WriteTimeout are as [Config]'s, and answer
	// the same slow-peer problem.
	//
	// IdleTimeout bounds a connection with nothing in flight. It does not run
	// while this end owes the peer answers: a streamed response may take
	// minutes during which a well-behaved client has nothing at all to send,
	// and disconnecting it for silence would be this server refusing what it
	// has just agreed to produce. Once the last answer is out the clock runs
	// again, and the connection ends with GOAWAY(NO_ERROR) after between one
	// and two IdleTimeouts of silence. A request that has begun and not
	// finished is a different matter and stays on ReadTimeout, which is the
	// bound that answers slowloris.
	//
	// WriteTimeout bounds two things: one write to the socket, and the wait
	// for the flow-control credit one stream needs. A peer that grants
	// nothing for that long has its stream reset rather than held forever.
	//
	// ReadTimeout also bounds one request's whole assembly, from its HEADERS
	// to its END_STREAM, as it does on the HTTP/1.1 side. The frame-level
	// clock alone is not that bound: it restarts on every frame consumed, so
	// a peer that buffered most of a body and then sends one PING per
	// interval would hold the memory forever. A stream still assembling when
	// its deadline passes is reset, not the connection.
	IdleTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c H2Config) withDefaults() H2Config {
	if c.MaxFrameSize < defaultMaxFrameSize {
		// The floor, not only the zero default: see the field's comment. A
		// configured value below 16384 would be advertised as an illegal
		// SETTINGS_MAX_FRAME_SIZE, so it is raised rather than honoured.
		c.MaxFrameSize = defaultMaxFrameSize
	}
	if c.MaxFrameSize > 1<<24-1 {
		c.MaxFrameSize = 1<<24 - 1
	}
	if c.MaxFramesPerBatch <= 0 {
		c.MaxFramesPerBatch = 64
	}
	if c.MaxContinuationFrames <= 0 {
		c.MaxContinuationFrames = 16
	}
	if c.MaxHeaderBlockBytes <= 0 {
		c.MaxHeaderBlockBytes = 64 << 10
	}
	if c.MaxHeaders <= 0 {
		c.MaxHeaders = 64
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
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxConcurrentStreams <= 0 {
		c.MaxConcurrentStreams = defaultMaxConcurrency
	}
	if c.MaxPendingControlBytes <= 0 {
		c.MaxPendingControlBytes = 16 << 10
	}
	if c.MaxWriteBufferBytes <= 0 {
		c.MaxWriteBufferBytes = 64 << 10
	}
	return c
}

// defaultMaxConcurrency is SETTINGS_MAX_CONCURRENT_STREAMS' default here. The
// protocol has no initial value for it — a peer that is told nothing may open
// as many as it likes — so saying a number is the bound, not a preference.
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
func h2Frames(c net.Conn, cfg H2Config) sluice.Source[h2Frame] {
	return framesWith(c, cfg, nil)
}

// framesWith is Frames with the server's view of whether the connection is
// idle. busy reports that this end owes the peer answers, in which case a
// silent round does not end the connection — see [H2Config.IdleTimeout]. A nil busy is a
// connection with nothing above it, which is what [h2Frames] gives a caller
// composing operators directly, and it is idle whenever it is silent.
func framesWith(c net.Conn, cfg H2Config, busy func() bool) sluice.Source[h2Frame] {
	cfg = cfg.withDefaults()
	r := &frameReader{conn: c, cfg: cfg, busy: busy}
	return sluice.NewSource(sluice.Stream[h2Frame](r.stream), func() error { return r.err })
}

type frameReader struct {
	conn net.Conn
	cfg  H2Config
	busy func() bool
	buf  []byte
	n    int
	err  error

	prefaced bool
	batch    []h2Frame

	// pending is what the last batch consumed. Compacted at the start of the
	// next round, not before the batch was yielded: the frames borrow the
	// buffer.
	pending int

	// deadline is the current frame's absolute read deadline, held fixed
	// once set rather than re-armed on every read — see the equivalent field
	// on httpstream's HTTP/1.1 reader for why a sliding deadline is not a
	// timeout at all.
	deadline time.Time

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
			return batch, true
		}
		// The preface consumes bytes without producing a frame, so what it
		// took has to be dropped here or the next round parses it again — as
		// a frame, which it is not.
		if used > 0 {
			r.consume(used)
			continue
		}
		if err := r.fill(); err != nil {
			r.err = err
			return nil, false
		}
	}
}

func (r *frameReader) consume(used int) {
	copy(r.buf, r.buf[used:r.n])
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
		f, n, err := parseFrame(r.buf[used:r.n], r.cfg)
		if errors.Is(err, errIncomplete) {
			break
		}
		if err != nil {
			return nil, 0, err
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
			return fmt.Errorf("%w: over %d CONTINUATION frames in one header block",
				ErrH2Protocol, r.cfg.MaxContinuationFrames)
		}
		if r.headerBlock > r.cfg.MaxHeaderBlockBytes {
			return fmt.Errorf("%w: a header block over %d bytes", ErrH2Protocol, r.cfg.MaxHeaderBlockBytes)
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
		if r.headerBlock > r.cfg.MaxHeaderBlockBytes {
			return fmt.Errorf("%w: a header block over %d bytes", ErrH2Protocol, r.cfg.MaxHeaderBlockBytes)
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
	if len(r.buf) == r.n {
		grown := min(max(2*len(r.buf), 4<<10), limit)
		next := make([]byte, grown)
		copy(next, r.buf[:r.n])
		r.buf = next
	}
	// A silent round that began while answers were owed never ends the
	// connection; it is followed by a fresh round instead, so the idle close
	// always follows a full IdleTimeout armed with nothing owed. The deadline
	// is armed even while owed — only this goroutine touches it, which keeps
	// re-arming race-free, where a responder re-arming it when its last
	// answer goes out would race this goroutine's own SetReadDeadline — so a
	// connection whose last answer just went out is reaped within two
	// IdleTimeouts of it, rather than never.
	for {
		deadline := time.Now().Add(r.cfg.IdleTimeout)
		owed := false
		switch {
		case r.n > 0:
			if r.deadline.IsZero() {
				r.deadline = time.Now().Add(r.cfg.ReadTimeout)
			}
			deadline = r.deadline
		case r.busy != nil && r.busy():
			owed = true
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

// parseFrame reads one frame header and its payload.
//
// The length is checked against the negotiated maximum *before* the payload is
// waited for, so a peer cannot make this buffer sixteen megabytes by writing
// nine bytes.
func parseFrame(buf []byte, cfg H2Config) (h2Frame, int, error) {
	var f h2Frame
	if len(buf) < frameHeaderSize {
		return f, 0, errIncomplete
	}
	length := int(buf[0])<<16 | int(buf[1])<<8 | int(buf[2])
	if length > cfg.MaxFrameSize {
		// §4.2 mandates FRAME_SIZE_ERROR for a frame over the advertised
		// maximum, which errFrameSize puts on the GOAWAY.
		return f, 0, fmt.Errorf("%w: %w: a frame declares %d bytes, over the %d-byte maximum",
			ErrH2Protocol, errFrameSize, length, cfg.MaxFrameSize)
	}
	f.Type, f.Flags = buf[3], buf[4]
	// The top bit is reserved and must be ignored rather than rejected
	// (RFC 9113 §4.1), which is one of the few places the spec asks for
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
