package pgwire

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// frame builds the wire form of a typed message by hand, so the tests do not
// verify the Writer against itself.
func frame(typ byte, body string) []byte {
	out := []byte{typ, 0, 0, 0, 0}
	out[1] = byte((len(body) + 4) >> 24)
	out[2] = byte((len(body) + 4) >> 16)
	out[3] = byte((len(body) + 4) >> 8)
	out[4] = byte(len(body) + 4)
	return append(out, body...)
}

func TestReaderNext(t *testing.T) {
	tests := []struct {
		name string
		typ  byte
		body string
	}{
		{"ordinary message", 'D', "row data"},
		{"empty body", 'S', ""},
		{"body with NUL bytes", 'B', "a\x00b\x00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := NewReader(bytes.NewReader(frame(tt.typ, tt.body)))
			m, err := rd.Next()
			if err != nil {
				t.Fatalf("Next returned %v", err)
			}
			if m.Type != tt.typ {
				t.Errorf("Type = %q, want %q", m.Type, tt.typ)
			}
			if string(m.Body) != tt.body {
				t.Errorf("Body = %q, want %q", m.Body, tt.body)
			}
		})
	}
}

// Several messages in one stream, then a clean end.
func TestReaderSequence(t *testing.T) {
	var in []byte
	in = append(in, frame('1', "")...)      // ParseComplete
	in = append(in, frame('D', "first")...) // DataRow
	in = append(in, frame('D', "second")...)
	in = append(in, frame('C', "SELECT 2")...) // CommandComplete

	rd := NewReader(bytes.NewReader(in))
	var got []string
	for {
		m, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next returned %v", err)
		}
		got = append(got, string(m.Type)+":"+string(m.Body))
	}
	want := []string{"1:", "D:first", "D:second", "C:SELECT 2"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// The body borrows the reader's buffer, so it is only valid until the next
// read. The test states the contract by observing it rather than by trusting
// the comment.
func TestReaderBodyIsBorrowed(t *testing.T) {
	var in []byte
	// The two bodies are the same size, so the second read is guaranteed to
	// reuse the buffer rather than regrow it — the overwrite below is a
	// certainty, not a coincidence of allocator behaviour.
	in = append(in, frame('D', "BORROW")...)
	in = append(in, frame('D', "SECOND")...)

	rd := NewReader(bytes.NewReader(in))
	first, err := rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	kept := first.Body // retained without copying: the mistake being documented
	if _, err := rd.Next(); err != nil {
		t.Fatal(err)
	}
	if string(kept) != "SECOND" {
		t.Errorf("retained body reads %q, want %q: the buffer was not reused, so the "+
			"borrowed-body contract this test pins is no longer observable", kept, "SECOND")
	}
}

// The limit is the S8 guarantee: a length prefix is attacker-controlled, so it
// must be refused before it is believed. A four-byte header claiming a
// gigabyte must cost a comparison, not a gigabyte.
func TestReaderRefusesOversizedBeforeAllocating(t *testing.T) {
	huge := []byte{'D', 0x3F, 0xFF, 0xFF, 0xFF} // ~1 GiB declared, nothing sent
	rd := NewReader(bytes.NewReader(huge))
	rd.SetMaxMessage(1024)

	_, err := rd.Next()
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
	// Nothing was read past the header, and nothing was buffered for it.
	if cap(rd.buf) > 1024 {
		t.Errorf("the reader allocated %d bytes for a message it refused", cap(rd.buf))
	}
}

func TestReaderRejectsMalformedLength(t *testing.T) {
	tests := []struct {
		name   string
		length []byte
	}{
		{"zero", []byte{0, 0, 0, 0}},
		{"below the prefix itself", []byte{0, 0, 0, 3}},
		{"negative as int32", []byte{0xFF, 0xFF, 0xFF, 0xFF}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := append([]byte{'D'}, tt.length...)
			rd := NewReader(bytes.NewReader(in))
			if _, err := rd.Next(); !errors.Is(err, ErrMalformedMessage) {
				t.Errorf("got %v, want ErrMalformedMessage", err)
			}
		})
	}
}

// A clean end and a truncated message are different facts about a connection,
// and collapsing them would hide a short response.
func TestReaderDistinguishesCleanEndFromTruncation(t *testing.T) {
	t.Run("clean end", func(t *testing.T) {
		rd := NewReader(bytes.NewReader(nil))
		if _, err := rd.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("got %v, want io.EOF", err)
		}
	})
	t.Run("header cut short", func(t *testing.T) {
		rd := NewReader(bytes.NewReader([]byte{'D', 0, 0}))
		if _, err := rd.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("body cut short", func(t *testing.T) {
		in := frame('D', "twelve chars")[:10] // header promises more than follows
		rd := NewReader(bytes.NewReader(in))
		if _, err := rd.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

// The startup message is the one with no type byte.
func TestReaderNextStartup(t *testing.T) {
	body := "user\x00alice\x00"
	in := []byte{0, 0, 0, byte(len(body) + 4)}
	in = append(in, body...)

	rd := NewReader(bytes.NewReader(in))
	m, err := rd.NextStartup()
	if err != nil {
		t.Fatalf("NextStartup returned %v", err)
	}
	if m.Type != 0 {
		t.Errorf("Type = %q, want 0 — a startup message has no type byte", m.Type)
	}
	if string(m.Body) != body {
		t.Errorf("Body = %q, want %q", m.Body, body)
	}
}

// What the Writer produces, the Reader must read back — including the messages
// whose length is only known once their fields are encoded.
func TestWriterReaderRoundTrip(t *testing.T) {
	var out bytes.Buffer
	wr := NewWriter(&out)

	wr.Message('Q', []byte("SELECT 1"))
	wr.Message('S', nil)

	at := wr.Begin('B')
	wr.AppendString("portal")
	wr.AppendString("stmt")
	wr.AppendUint16(2)
	wr.AppendUint32(0xDEADBEEF)
	wr.End(at)

	if wr.Buffered() == 0 {
		t.Error("Buffered reported nothing after three messages")
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush returned %v", err)
	}
	if wr.Buffered() != 0 {
		t.Error("Buffered is non-zero after a flush")
	}

	rd := NewReader(bytes.NewReader(out.Bytes()))

	m, err := rd.Next()
	if err != nil || m.Type != 'Q' || string(m.Body) != "SELECT 1" {
		t.Fatalf("first message = %q/%q, %v", m.Type, m.Body, err)
	}
	m, err = rd.Next()
	if err != nil || m.Type != 'S' || len(m.Body) != 0 {
		t.Fatalf("second message = %q/%q, %v", m.Type, m.Body, err)
	}
	m, err = rd.Next()
	if err != nil || m.Type != 'B' {
		t.Fatalf("third message = %q, %v", m.Type, err)
	}
	want := "portal\x00stmt\x00\x00\x02\xde\xad\xbe\xef"
	if string(m.Body) != want {
		t.Errorf("in-place body = %q, want %q", m.Body, want)
	}
	if _, err := rd.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("got %v after the last message, want io.EOF", err)
	}
}

func TestWriterStartupRoundTrip(t *testing.T) {
	var out bytes.Buffer
	wr := NewWriter(&out)
	wr.Startup([]byte("user\x00bob\x00"))
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}

	rd := NewReader(bytes.NewReader(out.Bytes()))
	m, err := rd.NextStartup()
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Body) != "user\x00bob\x00" {
		t.Errorf("Body = %q", m.Body)
	}
}

// A failed write must not leave bytes behind to be sent again: after a partial
// write the connection's position is unknown, so resending would corrupt the
// stream rather than repair it.
func TestWriterFlushErrorClearsBuffer(t *testing.T) {
	wr := NewWriter(failingWriter{})
	wr.Message('Q', []byte("SELECT 1"))
	if err := wr.Flush(); err == nil {
		t.Fatal("Flush returned nil over a failing writer")
	}
	if wr.Buffered() != 0 {
		t.Errorf("%d bytes left buffered after a failed flush", wr.Buffered())
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("connection reset") }

// SetMaxMessage refuses to remove the limit: there is no way to ask this
// package for an unbounded read.
func TestReaderMaxMessageCannotBeDisabled(t *testing.T) {
	rd := NewReader(strings.NewReader(""))
	for _, n := range []int{0, -1} {
		rd.SetMaxMessage(n)
		if rd.max != DefaultMaxMessage {
			t.Errorf("SetMaxMessage(%d) left the limit at %d, want the default", n, rd.max)
		}
	}
}

// The read buffer grows with headroom rather than to exactly the message in
// hand: a stream of steadily larger messages — a scan whose rows widen, a COPY
// whose chunks grow — would otherwise reallocate and copy on every single one.
//
// What is asserted is the shape of the growth, not a capacity: reallocations
// must be logarithmic in the sizes seen, where an exact fit makes them linear.
func TestReaderGrowsItsBufferWithHeadroom(t *testing.T) {
	const messages = 16
	var stream bytes.Buffer
	w := NewWriter(&stream)
	for i := range messages {
		w.Message(BackendDataRow, make([]byte, 1000+i)) // one byte larger each time
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	rd := NewReader(&stream)
	reallocations, last := 0, 0
	for range messages {
		if _, err := rd.Next(); err != nil {
			t.Fatal(err)
		}
		if cap(rd.buf) != last {
			reallocations++
			last = cap(rd.buf)
		}
	}
	// Two: the first message claims a buffer, the second doubles it, and the
	// remaining fourteen fit in the headroom that bought.
	if reallocations > 2 {
		t.Errorf("%d reallocations over %d messages growing by a byte each: the buffer is being fitted exactly rather than grown",
			reallocations, messages)
	}
}

// A parameter count the protocol carries as a 16-bit field is refused before
// a byte is buffered. Writing it truncated would leave a Bind whose declared
// and actual parameter counts disagree, which the server reads as a corrupt
// stream rather than as an error.
func TestParameterCountsPastTheProtocolsFieldAreRefused(t *testing.T) {
	if err := CheckParamCount(maxUint16 + 1); err == nil {
		t.Error("a parameter count past uint16 was accepted")
	}
	// The boundary itself fits: the field holds maxUint16 exactly.
	if err := CheckParamCount(maxUint16); err != nil {
		t.Errorf("the largest count the protocol can carry was refused: %v", err)
	}
}

// The per-message cost of the framing layer on a stream of small rows, the
// steady state of every query.
func BenchmarkReaderNext(b *testing.B) {
	var stream []byte
	for range 1024 {
		stream = append(stream, frame(BackendDataRow, "\x00\x01\x00\x00\x00\x08abcdefgh")...)
	}
	src := bytes.NewReader(stream)
	rd := NewReader(src)
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))
	for b.Loop() {
		src.Reset(stream)
		for range 1024 {
			if _, err := rd.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// One large message must not pin its buffer for the connection's life: after
// a run of messages that fit in the retained size, the large buffer goes —
// and not before, so alternating sizes do not reallocate per message.
func TestReaderReleasesALargeBufferAfterSmallMessages(t *testing.T) {
	big := strings.Repeat("x", 4*readerRetainBytes)
	var stream []byte
	stream = append(stream, frame(BackendDataRow, big)...)
	for range readerShrinkAfter {
		stream = append(stream, frame(BackendDataRow, "small")...)
	}
	rd := NewReader(bytes.NewReader(stream))
	if _, err := rd.Next(); err != nil {
		t.Fatal(err)
	}
	if c := cap(rd.buf); c < len(big) {
		t.Fatalf("cap = %d after a %d-byte message", c, len(big))
	}
	for i := range readerShrinkAfter {
		m, err := rd.Next()
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Body) != "small" {
			t.Fatalf("message %d = %q", i, m.Body)
		}
		if i < readerShrinkAfter-1 && cap(rd.buf) <= readerRetainBytes {
			t.Fatalf("released after %d small messages, want %d", i+1, readerShrinkAfter)
		}
	}
	if c := cap(rd.buf); c > readerRetainBytes {
		t.Errorf("cap = %d after %d small messages, want at most %d", c, readerShrinkAfter, readerRetainBytes)
	}
}

// oneByteReader hands out its stream a byte per read, the worst split a
// transport can make, and fails with err once it is empty.
type oneByteReader struct {
	data []byte
	err  error
}

func (o *oneByteReader) Read(p []byte) (int, error) {
	if len(o.data) == 0 {
		return 0, o.err
	}
	p[0], o.data = o.data[0], o.data[1:]
	return 1, nil
}

// A window must not change what is read, only how often the transport is
// asked: messages split across fills, larger than the window, or delivered a
// byte at a time come back whole and in order, and the end of the stream is
// told apart from a truncation exactly as without one.
func TestWindowedReaderReadsWhatTheDirectOneReads(t *testing.T) {
	big := strings.Repeat("x", 100)
	var stream []byte
	for i := range 20 {
		stream = append(stream, frame('D', strings.Repeat("r", i))...)
	}
	stream = append(stream, frame('D', big)...)
	stream = append(stream, frame('C', "SELECT 21\x00")...)
	stream = append(stream, frame('Z', "I")...)

	want := func(t *testing.T, rd *Reader) {
		t.Helper()
		for i := range 20 {
			m, err := rd.Next()
			if err != nil || m.Type != 'D' || string(m.Body) != strings.Repeat("r", i) {
				t.Fatalf("message %d: %q %q %v", i, m.Type, m.Body, err)
			}
		}
		if m, err := rd.Next(); err != nil || string(m.Body) != big {
			t.Fatalf("the message larger than the window: %d bytes, %v", len(m.Body), err)
		}
		if m, err := rd.Next(); err != nil || m.Type != 'C' || string(m.Body) != "SELECT 21\x00" {
			t.Fatalf("the message after the large one: %q %q %v", m.Type, m.Body, err)
		}
		if m, err := rd.Next(); err != nil || m.Type != 'Z' {
			t.Fatalf("the last message: %q %v", m.Type, err)
		}
		if _, err := rd.Next(); !errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("after the last message: %v, want io.EOF", err)
		}
	}
	t.Run("whole reads", func(t *testing.T) { want(t, NewReaderSize(bytes.NewReader(stream), 64)) })
	t.Run("a byte per read", func(t *testing.T) {
		want(t, NewReaderSize(&oneByteReader{data: stream, err: io.EOF}, 64))
	})

	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"header cut short", []byte{'D', 0, 0}},
		{"body cut short", frame('D', "twelve chars")[:10]},
		{"large body cut short", frame('D', big)[:80]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rd := NewReaderSize(bytes.NewReader(tc.in), 64)
			if _, err := rd.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("got %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

// A read that returns bytes and an error together delivers the bytes first,
// and the error is the answer to the next read rather than lost: a network
// error is not guaranteed to come back a second time.
func TestWindowedReaderKeepsAnErrorThatCameWithBytes(t *testing.T) {
	boom := errors.New("connection reset")
	in := append(frame('D', "one"), frame('D', "two")...)
	fired := false
	rd := NewReaderSize(readerFunc(func(p []byte) (int, error) {
		n := copy(p, in)
		in = in[n:]
		if len(in) == 0 && !fired {
			fired = true
			return n, boom
		}
		if len(in) == 0 {
			return n, io.EOF
		}
		return n, nil
	}), 64)
	for _, body := range []string{"one", "two"} {
		if m, err := rd.Next(); err != nil || string(m.Body) != body {
			t.Fatalf("got %q %v, want %q", m.Body, err, body)
		}
	}
	if _, err := rd.Next(); !errors.Is(err, boom) {
		t.Fatalf("after the bytes: %v, want the error that came with them", err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// The limit holds with a window too: a header claiming more than it is
// refused from the header, before the body is waited for or anything is
// allocated for it.
func TestWindowedReaderRefusesOversizedFromTheHeader(t *testing.T) {
	rd := NewReaderSize(bytes.NewReader([]byte{'D', 0x7f, 0xff, 0xff, 0xff}), 64)
	if _, err := rd.Next(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
	if cap(rd.buf) != 0 {
		t.Errorf("%d bytes allocated for a message that was refused", cap(rd.buf))
	}
}
