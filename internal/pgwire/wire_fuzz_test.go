package pgwire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// The verification S8 names: every network input is bounded before allocation,
// checked by fuzzing rather than by reading the code. The reader's whole
// contract against hostile input is here — it must not panic, must not
// allocate past its limit whatever a length prefix claims, and must fail with
// something a caller can act on rather than with a surprise.
func FuzzReaderNext(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{'D', 0, 0, 0, 4})                     // empty body
	f.Add([]byte{'D', 0, 0, 0, 8, 'a', 'b', 'c', 'd'}) // ordinary
	f.Add([]byte{'D', 0x7F, 0xFF, 0xFF, 0xFF})         // the largest int32
	f.Add([]byte{'D', 0xFF, 0xFF, 0xFF, 0xFF})         // negative as int32
	f.Add([]byte{'D', 0, 0, 0, 3})                     // shorter than the prefix
	f.Add([]byte{'D', 0, 0, 0, 100, 'a'})              // promises more than it sends
	f.Add([]byte{0, 0, 0, 8, 'u', 's', 'e', 'r'})      // startup shape

	const limit = 4096

	f.Fuzz(func(t *testing.T, in []byte) {
		rd := NewReader(bytes.NewReader(in))
		rd.SetMaxMessage(limit)

		// Bounded work as well as bounded memory: a valid stream of empty
		// messages is legitimate and long, so the loop is capped rather than
		// trusted to end.
		for range 64 {
			m, err := rd.Next()
			if err != nil {
				// Every failure must be one of the reader's own, or an I/O
				// end. Anything else means an error escaped unclassified.
				switch {
				case errors.Is(err, io.EOF),
					errors.Is(err, io.ErrUnexpectedEOF),
					errors.Is(err, ErrMessageTooLarge),
					errors.Is(err, ErrMalformedMessage):
				default:
					t.Fatalf("unclassified error %v for input %q", err, in)
				}
				break
			}
			if len(m.Body) > limit {
				t.Fatalf("body of %d bytes past the %d limit", len(m.Body), limit)
			}
		}

		// The limit bounds the buffer, not merely the bodies handed out: a
		// prefix claiming a gigabyte must never have been believed.
		if cap(rd.buf) > limit {
			t.Fatalf("the reader holds %d bytes for a %d limit", cap(rd.buf), limit)
		}
	})
}

// The same for the startup path, which has no type byte to anchor it and so is
// the one an attacker reaches first — before authentication, on any connection
// that opens.
func FuzzReaderNextStartup(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 4})
	f.Add([]byte{0, 0, 0, 9, 'u', 's', 'e', 'r', 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x7F, 0xFF, 0xFF, 0xFF})

	const limit = 1024

	f.Fuzz(func(t *testing.T, in []byte) {
		rd := NewReader(bytes.NewReader(in))
		rd.SetMaxMessage(limit)

		m, err := rd.NextStartup()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF),
				errors.Is(err, io.ErrUnexpectedEOF),
				errors.Is(err, ErrMessageTooLarge),
				errors.Is(err, ErrMalformedMessage):
			default:
				t.Fatalf("unclassified error %v for input %q", err, in)
			}
			return
		}
		if len(m.Body) > limit {
			t.Fatalf("body of %d bytes past the %d limit", len(m.Body), limit)
		}
		if cap(rd.buf) > limit {
			t.Fatalf("the reader holds %d bytes for a %d limit", cap(rd.buf), limit)
		}
	})
}

// Whatever the Writer produces, the Reader reads back unchanged — the property
// that keeps an encoder bug from becoming a protocol desynchronisation nobody
// can diagnose. Fuzzed over the body rather than over the wire form, so the
// input is always a legitimate message.
func FuzzWriterReaderRoundTrip(f *testing.F) {
	f.Add(byte('Q'), []byte("SELECT 1"))
	f.Add(byte('S'), []byte{})
	f.Add(byte('B'), []byte{0, 0, 0})

	f.Fuzz(func(t *testing.T, typ byte, body []byte) {
		if typ == 0 {
			t.Skip("type zero is the startup message, which has its own reader")
		}
		var out bytes.Buffer
		wr := NewWriter(&out)
		wr.Message(typ, body)
		if err := wr.Flush(); err != nil {
			t.Fatalf("Flush returned %v", err)
		}

		rd := NewReader(bytes.NewReader(out.Bytes()))
		rd.SetMaxMessage(len(body) + LengthSize)
		m, err := rd.Next()
		if err != nil {
			t.Fatalf("reading back a message of %d bytes: %v", len(body), err)
		}
		if m.Type != typ {
			t.Errorf("Type = %q, want %q", m.Type, typ)
		}
		if !bytes.Equal(m.Body, body) {
			t.Errorf("Body = %q, want %q", m.Body, body)
		}
		if _, err := rd.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("got %v after the message, want io.EOF — extra bytes were written", err)
		}
	})
}
