// This file is the framing layer, and it has one job: turn a byte stream into
// messages and back without ever trusting what it reads. Everything above it —
// message types, the extended query protocol, COPY — is built on these two
// types, [Reader] and [Writer].

package pgwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrMessageTooLarge reports a message whose declared length exceeds the
// reader's limit. It is returned before anything is allocated for that
// message, which is the whole point of the limit (guarantee S8): a length
// prefix is attacker-controlled, and a reader that allocates first and checks
// second can be made to allocate two gigabytes by four bytes of input.
var ErrMessageTooLarge = errors.New("postgres: message longer than the reader's limit")

// ErrMalformedMessage reports a length prefix that cannot describe a message —
// shorter than the four bytes it counts itself. There is no recovery: the
// stream position is no longer known, so the connection is finished.
var ErrMalformedMessage = errors.New("postgres: malformed message length")

// DefaultMaxMessage is the message size a [Reader] accepts when none is
// stated: large enough for the rows and COPY chunks a real workload sends,
// small enough that a hostile or corrupted length prefix cannot exhaust the
// process. Guarantee S3 asks for a restrictive default rather than none;
// raise it deliberately through [Reader.SetMaxMessage] for a workload that
// genuinely moves larger values.
const DefaultMaxMessage = 64 << 20 // 64 MiB

// LengthSize is the width of the length prefix, which counts itself.
const LengthSize = 4

// readerRetainBytes is the buffer a [Reader] keeps indefinitely, and
// readerShrinkAfter is how many consecutive messages that fit in it make a
// larger buffer go. One large row otherwise pins up to the message limit for
// the rest of the connection's life — per connection, so a pool of a hundred
// that each met one 64 MiB value holds 6 GiB to read rows of a hundred bytes.
const (
	readerRetainBytes = 1 << 20
	readerShrinkAfter = 256
)

// Message is one protocol message.
//
// Body borrows the reader's buffer and is valid only until the next call to
// [Reader.Next] — the same rule as [sluice.Batch], for the same reason: a
// connection that allocates per message allocates per row, and per row is the
// cost this library exists to remove. Retaining a Body requires copying it.
type Message struct {
	// Type is the message type byte — 'D' for DataRow, 'C' for Command
	// Complete, and so on. It is zero for a startup message, which is the one
	// message with no type byte.
	Type byte

	// Body is the payload, without the type byte and without the length
	// prefix. It is valid until the next read.
	Body []byte
}

// Reader reads protocol messages from a stream.
//
// It is not safe for concurrent use: a connection is a conversation, and two
// goroutines reading one is a protocol violation before it is a data race.
type Reader struct {
	r   io.Reader
	buf []byte
	max int

	// small counts consecutive messages that would have fit in
	// [readerRetainBytes] while the buffer is larger than that. See
	// readBody for why the buffer gives memory back.
	small int

	// hdr is read into before anything is allocated for the body, so that the
	// length can be judged before it is believed.
	hdr [1 + LengthSize]byte
}

// NewReader returns a Reader over r, accepting messages up to
// [DefaultMaxMessage].
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, max: DefaultMaxMessage}
}

// SetMaxMessage sets the largest message the reader will accept. A value of
// zero or less restores [DefaultMaxMessage] rather than removing the limit:
// there is no legitimate way to ask this package for an unbounded read.
//
// The limit bounds the buffer too, which is reused across messages and grows
// to fit the largest one met. It does not stay there: after a run of
// messages that fit in 1 MiB, a larger buffer is released, so one oversized
// row costs its memory for a while rather than for the connection's life.
// MaxMessage reports the limit [Reader.SetMaxMessage] set.
func (rd *Reader) MaxMessage() int { return rd.max }

// Underlying returns the reader messages are read from — a *bufio.Reader
// when the transport is a socket, the transport itself otherwise. Tests use
// it to check which; nothing else should.
func (rd *Reader) Underlying() io.Reader { return rd.r }

func (rd *Reader) SetMaxMessage(n int) {
	if n <= 0 {
		n = DefaultMaxMessage
	}
	rd.max = n
}

// Next reads the next message.
//
// The returned Message borrows the reader's buffer: it is valid until the
// following call. At the clean end of the stream Next returns [io.EOF]; a
// stream that ends part-way through a message returns
// [io.ErrUnexpectedEOF], because those are different facts about a connection
// and collapsing them hides a truncated response.
func (rd *Reader) Next() (Message, error) {
	if _, err := io.ReadFull(rd.r, rd.hdr[:]); err != nil {
		return Message{}, err
	}
	typ := rd.hdr[0]
	// The prefix is a signed int32 in the protocol, and reading it as one is
	// what lets a negative length be reported as the malformed thing it is
	// rather than as an implausibly large message.
	length := int(int32(binary.BigEndian.Uint32(rd.hdr[1:]))) //nolint:gosec // G115: the protocol field is int32

	body, err := rd.readBody(length)
	if err != nil {
		return Message{}, err
	}
	return Message{Type: typ, Body: body}, nil
}

// NextStartup reads a startup-phase message, which carries no type byte: the
// startup packet itself, and the SSL and GSSAPI requests. Every later message
// has one, so this is called once per connection at most.
func (rd *Reader) NextStartup() (Message, error) {
	if _, err := io.ReadFull(rd.r, rd.hdr[:LengthSize]); err != nil {
		return Message{}, err
	}
	length := int(int32(binary.BigEndian.Uint32(rd.hdr[:LengthSize]))) //nolint:gosec // G115: the protocol field is int32

	body, err := rd.readBody(length)
	if err != nil {
		return Message{}, err
	}
	return Message{Body: body}, nil
}

// readBody validates a declared length and reads the payload it announces.
//
// The order is the guarantee: length is compared to the limit **before** the
// buffer is grown, so a four-byte prefix claiming two gigabytes costs a
// comparison and an error rather than two gigabytes.
func (rd *Reader) readBody(length int) ([]byte, error) {
	switch {
	case length < LengthSize:
		// The prefix counts itself, so anything below four — including the
		// negative values a signed int32 can carry — describes nothing.
		return nil, fmt.Errorf("%w: %d", ErrMalformedMessage, length)
	case length > rd.max:
		return nil, fmt.Errorf("%w: %d bytes, limit %d", ErrMessageTooLarge, length, rd.max)
	}

	n := length - LengthSize
	if cap(rd.buf) > readerRetainBytes {
		// Only a reader that once met a large message pays this branch's
		// body; the steady state is one comparison. A run of messages that
		// all fit in the retained size releases the large buffer — the next
		// large one grows it again, which costs an allocation per run rather
		// than per message.
		if n <= readerRetainBytes {
			rd.small++
			if rd.small >= readerShrinkAfter {
				rd.buf, rd.small = make([]byte, readerRetainBytes), 0
			}
		} else {
			rd.small = 0
		}
	}
	if cap(rd.buf) < n {
		// Grown with headroom rather than to exactly n: a stream of steadily
		// larger messages would otherwise reallocate on every new high-water
		// mark. Doubling costs log(max) allocations over the connection's
		// life; the cap keeps a single large message from reserving more than
		// the limit already allows.
		rd.buf = make([]byte, min(max(n, 2*cap(rd.buf)), rd.max-LengthSize))
	}
	rd.buf = rd.buf[:n]
	if n == 0 {
		return rd.buf, nil // a body-less message: Sync, Terminate, and friends
	}
	if _, err := io.ReadFull(rd.r, rd.buf); err != nil {
		if errors.Is(err, io.EOF) {
			// The header promised a body the stream did not deliver. That is a
			// truncated message, not a clean end.
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return rd.buf, nil
}

// Writer builds protocol messages into a reused buffer and writes them out.
//
// Messages accumulate until [Writer.Flush], which is what makes the extended
// query protocol worth its name: Parse, Bind, Execute and Sync for a whole
// batch leave in one write, and §8.5's eleven round trips become two. Like
// [Reader] it is single-conversation, not concurrent.
type Writer struct {
	w   io.Writer
	buf []byte
}

// NewWriter returns a Writer over w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Buffered reports how many bytes are waiting for a flush. It is what a caller
// watches to decide when a batch has grown enough to send.
func (wr *Writer) Buffered() int { return len(wr.buf) }

// Message appends a complete message of the given type.
//
// The length prefix is computed here rather than by the caller, because a
// hand-written length is a class of bug this package should not be able to
// have.
func (wr *Writer) Message(typ byte, body []byte) {
	wr.buf = append(wr.buf, typ)
	wr.buf = binary.BigEndian.AppendUint32(wr.buf, uint32(LengthSize+len(body))) //nolint:gosec // G115: bounded by the caller's body
	wr.buf = append(wr.buf, body...)
}

// Startup appends a message with no type byte, for the startup phase.
func (wr *Writer) Startup(body []byte) {
	wr.buf = binary.BigEndian.AppendUint32(wr.buf, uint32(LengthSize+len(body))) //nolint:gosec // G115: bounded by the caller's body
	wr.buf = append(wr.buf, body...)
}

// Begin starts a message written in place, returning the offset [Writer.End]
// needs. It is for the messages whose length is not known until the fields are
// encoded — Bind with a batch of parameters, CopyData with a batch of rows —
// where building the body in a second buffer just to measure it would be a
// copy per batch.
//
//	at := w.Begin('B')
//	w.AppendString(portal)
//	w.AppendString(stmt)
//	w.End(at)
func (wr *Writer) Begin(typ byte) int {
	wr.buf = append(wr.buf, typ)
	at := len(wr.buf)
	wr.buf = append(wr.buf, 0, 0, 0, 0) // patched by End
	return at
}

// End writes the length of a message begun at the offset Begin returned.
func (wr *Writer) End(at int) {
	binary.BigEndian.PutUint32(wr.buf[at:at+LengthSize], uint32(len(wr.buf)-at)) //nolint:gosec // G115: bounded by the buffer
}

// AppendBytes appends raw bytes to the message being built.
func (wr *Writer) AppendBytes(b []byte) { wr.buf = append(wr.buf, b...) }

// AppendString appends a C string — the protocol's null-terminated form.
func (wr *Writer) AppendString(s string) {
	wr.buf = append(wr.buf, s...)
	wr.buf = append(wr.buf, 0)
}

// AppendUint16 appends a 16-bit field in the protocol's byte order.
func (wr *Writer) AppendUint16(v uint16) { wr.buf = binary.BigEndian.AppendUint16(wr.buf, v) }

// AppendUint32 appends a 32-bit field in the protocol's byte order.
func (wr *Writer) AppendUint32(v uint32) { wr.buf = binary.BigEndian.AppendUint32(wr.buf, v) }

// Flush writes everything buffered and empties the buffer, keeping its
// capacity for the next batch.
//
// A failed flush leaves the buffer cleared: the connection's position is no
// longer known after a partial write, so retrying the same bytes would
// corrupt the stream rather than repair it. The error is the end of that
// connection.
func (wr *Writer) Flush() error {
	if len(wr.buf) == 0 {
		return nil
	}
	_, err := wr.w.Write(wr.buf)
	wr.buf = wr.buf[:0]
	return err
}
