package httpstream

import (
	"errors"
	"fmt"
	"io"
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
// An HTTP/2 body has none of that: h2 holds it, and the bodyStream is only
// what Request.Stream reads it through. One pointer type for both keeps
// Request one word over what it was before bodies streamed.
type bodyStream struct {
	r  *reader
	h2 *h2Body

	chunked bool
	left    int // a Content-Length body's bytes still to come
	limit   int // MaxStreamedBodyBytes
	total   int // decoded bytes handed out

	start, pos int // the body region in r.buf, and its first unread byte

	expect bool // the client waits for 100 Continue before it sends the body
	done   bool // the body ended, and what follows it is the next request's
	closed bool // the handler returned: the stream is over whatever its state
	err    error

	// onDone is called once when the body ends: the client watch, deferred
	// while the body was still on the wire, may then start.
	onDone func()

	// The chunked framing's state.
	state         chunkState
	chunkLeft     int
	trailerBytes  int
	trailerFields int

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

func newBodyStream(r *reader, req Request, frame streamBody, start int) *bodyStream {
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
	return sluice.NewSource(sluice.Stream[[]byte](stream), func() error { return b.err })
}

// next returns the next piece of the body, nil once it has ended, or the
// error that ended it.
func (b *bodyStream) next() ([]byte, error) {
	if b.h2 != nil {
		if b.err != nil {
			return nil, b.err
		}
		data, err := b.h2.next()
		if err != nil {
			return nil, b.fail(err)
		}
		return data, nil
	}
	for {
		if b.err != nil {
			return nil, b.err
		}
		if b.done {
			return nil, nil
		}
		if b.closed {
			return nil, b.fail(errBodyAfterReturn)
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
		k := min(len(buf)-b.pos, b.left)
		data = buf[b.pos : b.pos+k : b.pos+k]
		b.pos += k
		b.left -= k
		b.total += k
		if b.left == 0 {
			b.finish()
		}
		return data, false, nil
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
		if _, err := parseHeaderLine(line); err != nil {
			return nil, false, err
		}
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
	if err := r.conn.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return err
	}
	n, err := r.conn.Read(r.buf[r.n:])
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

// Stream yields the body as it arrives, when [Config.MaxStreamedBodyBytes]
// turned streaming on and the body was not whole in the read that brought the
// request's headers (docs/design/adr/0009, article 4). For a body whole in
// [Request.Body] it yields nothing, so reading it is always safe;
// [Request.ReadAll] returns either form whole.
//
// It is a Source rather than a Stream because a body can fail midway — the
// client leaves, its chunked framing breaks, a read waits past ReadTimeout —
// and Err says so once the stream ends. It is valid while the handler runs:
// a body left unread when the handler returns closes the HTTP/1.1 connection
// after the response, since nothing can be read past it, and resets the
// HTTP/2 stream with NO_ERROR after it. Each element borrows the connection's
// buffer until the next is pulled. The trailers of a streamed body are
// checked against the header bounds and not kept: Trailers was handed over
// before they arrived.
func (r Request) Stream() sluice.Source[[]byte] {
	if r.body == nil {
		return sluice.Source[[]byte]{}
	}
	return r.body.source()
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
