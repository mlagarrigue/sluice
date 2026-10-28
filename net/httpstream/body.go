package httpstream

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
)

// bodyStream reads an HTTP/1.1 request body from the connection as the
// handler pulls it (docs/design/adr/0009, article 4).
//
// The reader's buffer holds the request's head in front, which the request's
// fields borrow and which is therefore never moved while the handler runs,
// then the body region: start..r.n, read from pos. Body bytes are handed out
// as views of that region and overwritten by the next read, so nothing is
// read from the socket before the handler asks — the backpressure is the
// socket's own.
//
// A chunked body is decoded by a state machine that calls the functions
// walkChunks calls — cutCRLF, parseChunkSize, parseHeaderLine — under the
// same bounds, so a streamed body is held to exactly the grammar a whole one
// is, by the same code.
//
// An HTTP/2 or HTTP/3 body has none of that: q holds it, and the bodyStream
// is only what Request.Stream reads it through. One pointer type for all
// three keeps Request one word over what it was before bodies streamed.
type bodyStream struct {
	r *h1Conn
	q *bodyQueue

	chunked bool
	left    int // a Content-Length body's bytes still to come
	limit   int // MaxStreamedBodyBytes
	total   int // decoded bytes handed out

	start, pos int // the body region in r.buf, and its first unread byte

	expect bool // the client waits for 100 Continue before it sends the body
	done   bool // the body ended, and what follows it is the next request's
	// closed is set by the connection's goroutine once the handler returned:
	// the stream is over whatever its state. Atomic, and read before any other
	// field, because the reader it stops is a handler that kept the stream past
	// its return on a goroutine of its own — the misuse errBodyAfterReturn names
	// — and that reader must not race the connection on the way to the error.
	closed atomic.Bool
	err    error

	// progress is the clock its reads wait under (readProgress).
	progress readProgress

	// onDone is called once when the body ends: the client watch, deferred
	// while the body was still on the wire, may then start.
	onDone func()

	// The chunked framing's state.
	state         chunkState
	chunkLeft     int
	trailerBytes  int
	trailerFields int
	// trailers is the trailer section, copied out of r.buf as each field
	// is read: the request pipelined behind the body moves over it.
	trailers []Header

	item [1][]byte
}

type chunkState uint8

const (
	chunkSize chunkState = iota
	chunkData
	chunkDataEnd
	chunkTrailer
)

// errBodyAfterReturn reports a streamed body read after its handler returned.
var errBodyAfterReturn = errors.New("httpstream: a request body read after its handler returned — the stream is valid during the call")

func newBodyStream(r *h1Conn, req Request, frame streamBody, start int) *bodyStream {
	b := &bodyStream{
		r: r, chunked: frame.chunked, left: frame.length,
		limit: max(r.cfg.MaxBodyBytes, r.cfg.MaxStreamedBodyBytes), start: start, pos: start,
		expect: expectsContinue(req),
	}
	if !b.chunked && b.left == 0 {
		b.finish()
	}
	return b
}

// source is the body as a Source, as Request.Stream holds it.
func (b *bodyStream) source() sluice.Source[[]byte] {
	stream := func(yield func(sluice.Batch[[]byte]) bool) {
		for {
			data, err := b.next()
			if err != nil || data == nil {
				return
			}
			b.item[0] = data
			if !yield(sluice.Batch[[]byte]{Items: b.item[:]}) {
				return
			}
		}
	}
	return sluice.NewSource(sluice.Stream[[]byte](stream), func() error {
		// Past the handler's return, err belongs to the connection's
		// goroutine, which may be reading the next request through it.
		if b.closed.Load() {
			return errBodyAfterReturn
		}
		return b.err
	})
}

// next returns the next piece of the body, nil once it has ended, or the
// error that ended it.
func (b *bodyStream) next() ([]byte, error) {
	if b.q != nil {
		if b.err != nil {
			return nil, b.err
		}
		data, err := b.q.next()
		if err != nil {
			return nil, b.fail(err)
		}
		return data, nil
	}
	for {
		if b.closed.Load() {
			// Nothing written: the connection's goroutine owns every other
			// field once the handler has returned.
			return nil, errBodyAfterReturn
		}
		if b.err != nil {
			return nil, b.err
		}
		if b.done {
			return nil, nil
		}
		data, more, err := b.step()
		if err != nil {
			return nil, b.fail(err)
		}
		if len(data) > 0 {
			return data, nil
		}
		if b.done {
			return nil, nil
		}
		if more {
			continue
		}
		if err := b.read(); err != nil {
			return nil, b.fail(err)
		}
	}
}

func (b *bodyStream) fail(err error) error {
	if b.err == nil {
		b.err = err
	}
	return b.err
}

// step takes what the buffered region holds. more reports that it advanced
// without data — a framing line — and may advance again before reading.
func (b *bodyStream) step() (data []byte, more bool, err error) {
	buf := b.r.buf[:b.r.n]
	if !b.chunked {
		if b.left == 0 {
			// Finished at the pull after the last piece, not with it:
			// finish moves the next request down over the body's region,
			// where that piece still lies.
			b.finish()
			return nil, false, nil
		}
		k := min(len(buf)-b.pos, b.left)
		data = buf[b.pos : b.pos+k : b.pos+k]
		b.pos += k
		b.left -= k
		b.total += k
		return data, b.left == 0, nil
	}
	switch b.state {
	case chunkSize:
		line, rest, ok := cutCRLF(buf[b.pos:], maxChunkLineBytes)
		if !ok {
			if len(buf)-b.pos > maxChunkLineBytes+1 {
				return nil, false, fmt.Errorf("%w: a chunk-size line over %d bytes", ErrMalformed, maxChunkLineBytes)
			}
			return nil, false, nil
		}
		size, err := parseChunkSize(line, b.limit-b.total)
		if err != nil {
			return nil, false, err
		}
		b.pos = len(buf) - len(rest)
		if size == 0 {
			b.state = chunkTrailer
		} else {
			b.chunkLeft, b.state = size, chunkData
		}
		return nil, true, nil
	case chunkData:
		k := min(len(buf)-b.pos, b.chunkLeft)
		data = buf[b.pos : b.pos+k : b.pos+k]
		b.pos += k
		b.chunkLeft -= k
		b.total += k
		if b.chunkLeft == 0 {
			b.state = chunkDataEnd
		}
		return data, k > 0 && b.chunkLeft == 0, nil
	case chunkDataEnd:
		if len(buf)-b.pos < 2 {
			return nil, false, nil
		}
		if buf[b.pos] != '\r' || buf[b.pos+1] != '\n' {
			return nil, false, fmt.Errorf("%w: chunk data is not ended by CRLF", ErrMalformed)
		}
		b.pos += 2
		b.state = chunkSize
		return nil, true, nil
	default: // chunkTrailer
		limit := b.r.cfg.MaxHeaderBytes - b.trailerBytes - 2
		line, rest, ok := cutCRLF(buf[b.pos:], limit)
		if !ok {
			if len(buf)-b.pos > limit+1 {
				return nil, false, fmt.Errorf("%w: trailers over %d bytes", errHeadersTooLarge, b.r.cfg.MaxHeaderBytes)
			}
			return nil, false, nil
		}
		b.trailerBytes += len(buf) - len(rest) - b.pos
		b.pos = len(buf) - len(rest)
		if len(line) == 0 {
			b.finish()
			return nil, false, nil
		}
		if b.trailerFields >= b.r.cfg.MaxHeaders {
			return nil, false, fmt.Errorf("%w: over %d trailer fields", errHeadersTooLarge, b.r.cfg.MaxHeaders)
		}
		b.trailerFields++
		h, err := parseHeaderLine(line)
		if err != nil {
			return nil, false, err
		}
		b.trailers = appendHeaderCopies(b.trailers, h)
		return nil, true, nil
	}
}

// finish ends the body: what the region holds past it is the next request's,
// moved down to where the body began so that the reader, consuming the head,
// finds it at the front.
func (b *bodyStream) finish() {
	r := b.r
	r.n = b.start + copy(r.buf[b.start:], r.buf[b.pos:r.n])
	b.pos = b.start
	b.done = true
	if b.onDone != nil {
		b.onDone()
	}
}

// minBodyRead is the least free space a body read is given: the buffer grows
// to it, within the connection's bound, rather than read a body a few bytes
// at a time behind a large head.
const minBodyRead = 16 << 10

// read reads once from the connection into the body region, after dropping
// what has been handed out.
func (b *bodyStream) read() error {
	r := b.r
	// What was handed out is done with: the next read overwrites it. A
	// framing line cut by the last read moves down with it.
	r.n = b.start + copy(r.buf[b.start:], r.buf[b.pos:r.n])
	b.pos = b.start
	if free := len(r.buf) - r.n; free < minBodyRead && len(r.buf) < r.cfg.maxBuffer() {
		// A new array rather than a resize in place: the request's fields
		// borrow the head in the old one, which stays as it is.
		grown := make([]byte, min(max(2*len(r.buf), r.n+minBodyRead), r.cfg.maxBuffer()))
		copy(grown, r.buf[:r.n])
		r.buf = grown
	}
	if r.n == len(r.buf) {
		return fmt.Errorf("%w: a body framing line fills the %d-byte buffer", ErrTooLarge, len(r.buf))
	}
	if b.expect && !r.sent100 {
		// The client holds its body until it is invited; the invitation goes
		// out when the handler first wants the body, so a handler that
		// answers without reading it never asks for it.
		if err := r.write100Continue(); err != nil {
			return err
		}
		r.sent100 = true
	}
	left := b.progress.left(r.cfg.ReadTimeout)
	if left <= 0 {
		return fmt.Errorf("%w: the body did not move by %d bytes within the read timeout %v",
			os.ErrDeadlineExceeded, r.cfg.ReadProgressBytes, r.cfg.ReadTimeout)
	}
	start := time.Now()
	if err := r.conn.SetReadDeadline(start.Add(left)); err != nil {
		return err
	}
	n, err := r.conn.Read(r.buf[r.n:])
	b.progress.wait(time.Since(start))
	b.progress.moved(n, r.cfg.ReadProgressBytes)
	r.n += n
	if n > 0 {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: the connection closed mid-body", ErrMalformed)
	}
	return err
}

// ReadAll returns the body whole: Body when it arrived whole, otherwise what
// Stream yields appended to dst. A body past limit bytes is refused with
// ErrTooLarge, and so is a streamed one, which ReadAll stops reading there.
// It is the step that turns a streamed body back into the whole-body
// contract, for a handler that wants that and states how much it accepts.
func (r Request) ReadAll(dst []byte, limit int) ([]byte, error) {
	if r.body == nil {
		if len(r.Body) > limit {
			return dst, fmt.Errorf("%w: a body of %d bytes, over %d", ErrTooLarge, len(r.Body), limit)
		}
		return append(dst, r.Body...), nil
	}
	n := 0
	src := r.Stream()
	for b := range src.Stream() {
		for _, p := range b.Items {
			if n += len(p); n > limit {
				return dst, fmt.Errorf("%w: a streamed body over %d bytes", ErrTooLarge, limit)
			}
			dst = append(dst, p...)
		}
	}
	return dst, src.Err()
}

// streamedBody is the streamed body of a batch, which only its last request
// can have: a streamed body ends its batch.
func streamedBody(batch []Request) *bodyStream {
	if len(batch) == 0 {
		return nil
	}
	return batch[len(batch)-1].body
}

// Stream yields the body as it arrives, when [Config.BodyDelivery] streams
// it — the default — and the body was not whole in the read that brought the
// request's headers (docs/design/adr/0009, article 4). For a body whole in
// [Request.Body] it yields nothing, so reading it is always safe;
// [Request.ReadAll] returns either form whole.
//
// It is a Source rather than a Stream because a body can fail midway — the
// client leaves, its chunked framing breaks, a read waits past ReadTimeout —
// and Err says so once the stream ends. It is valid while the handler runs:
// a body left unread when the handler returns closes the HTTP/1.1 connection
// after the response, since nothing can be read past it, resets the HTTP/2
// stream with NO_ERROR after it, and asks the HTTP/3 client to stop sending
// (STOP_SENDING, H3_NO_ERROR) after it. Each element borrows the connection's
// buffer until the next is pulled. The trailers of a streamed body arrive
// after it: [Request.Trailers] reads them once it has ended.
func (r Request) Stream() sluice.Source[[]byte] {
	if r.body == nil {
		return sluice.Source[[]byte]{}
	}
	return r.body.source()
}

// Trailers is the request's trailer section, in the order the client sent
// it: the fields after the last chunk of an HTTP/1.1 body, or the trailing
// HEADERS on HTTP/2 and HTTP/3; nil when it sent none. They arrive after the
// body, so they are nil until the body has ended without error, whichever
// way it is delivered: under [BodyWhole] that is before the handler is
// called, for a streamed body once [Request.Stream] has ended. One call
// reads both, so a handler need not know which mode it was given. They are
// valid while the handler runs.
//
// They are held to the bounds the headers are held to, separately: a
// trailer section of its own up to [Config.MaxHeaders] fields and
// [Config.MaxHeaderBytes] bytes. They are kept apart from Headers on
// purpose and nothing in them is interpreted. RFC 9110 §6.5 has a trailer
// stored separately and never merged into the header section unless the
// field's own definition allows it: a Content-Length or Authorization
// arriving after the body came too late to frame, route or authorise this
// request, and a handler that reads one from here takes that on knowingly.
func (r Request) Trailers() []Header {
	if r.body == nil {
		return r.trailers
	}
	if r.body.q != nil {
		return r.body.q.trailerSection()
	}
	if !r.body.done || r.body.err != nil {
		return nil
	}
	return r.body.trailers
}

// appendHeaderCopies appends copies of hs to dst, their names and values in
// one allocation of their own, apart from the buffer they were read from.
func appendHeaderCopies(dst []Header, hs ...Header) []Header {
	n := 0
	for _, h := range hs {
		n += len(h.Name) + len(h.Value)
	}
	arena := make([]byte, 0, n)
	for _, h := range hs {
		at := len(arena)
		arena = append(arena, h.Name...)
		name := arena[at:len(arena):len(arena)]
		at = len(arena)
		arena = append(arena, h.Value...)
		dst = append(dst, Header{Name: name, Value: arena[at:len(arena):len(arena)]})
	}
	return dst
}

// streamBody is parseRequest's answer for a request whose head is whole and
// accepted and whose body streams: the head is handed over, the body follows
// through Request.Stream. It carries the framing, so the requests that do
// not stream pay nothing for it.
type streamBody struct {
	chunked bool
	length  int // Content-Length, when not chunked
}

func (streamBody) Error() string { return "httpstream: the body streams" }

// readProgress is the clock a streamed body reads under: ReadTimeout of
// waiting for the peer to deliver [Config.ReadProgressBytes], refilled each
// time it has — WriteTimeout's rule on the reading side. Only time spent
// waiting for the peer counts; the handler's own between two reads does
// not. A timeout re-armed at every read is no bound at all: a peer that
// sends a byte per fraction of it holds the request for as long as it
// likes.
type readProgress struct {
	waited time.Duration // waiting since the last step of progress
	got    int           // bytes since the last step of progress
}

// left is how much of timeout the body has to make its next step.
func (p *readProgress) left(timeout time.Duration) time.Duration { return timeout - p.waited }

// wait counts d of waiting for the peer.
func (p *readProgress) wait(d time.Duration) { p.waited += d }

// moved counts n bytes delivered; need of them since the last step is the
// next step, which gives the whole timeout back.
func (p *readProgress) moved(n, need int) {
	if p.got += n; p.got >= need {
		p.got, p.waited = 0, 0
	}
}
