package postgres

import (
	"bytes"
	"errors"
	"testing"
)

// The backend parsers read network input too, so S8 covers them as squarely as
// the framing does: an ErrorResponse arrives before authentication completes,
// and a DataRow is the message an attacker who controls a row controls.
func FuzzParseError(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte("SERROR\x00C23505\x00\x00"))
	f.Add([]byte("SERROR"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, body []byte) {
		e, err := ParseError(body)
		switch {
		case err != nil && !errors.Is(err, ErrProtocol):
			t.Fatalf("unclassified error %v", err)
		case err == nil && e == nil:
			t.Fatal("ParseError returned neither an error nor a value")
		case err == nil:
			_ = e.Error() // rendering must not panic on anything that parsed
		}
	})
}

func FuzzParseRowDescription(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add(rowDesc([]string{"id"}, []uint32{20}))
	f.Add([]byte{0, 1})

	f.Fuzz(func(t *testing.T, body []byte) {
		fields, err := ParseRowDescription(nil, body)
		if err != nil {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		// A parse that succeeded must have produced exactly the number of
		// fields the body declared, or the offsets above it are wrong.
		if len(body) >= 2 {
			if want := int(body[0])<<8 | int(body[1]); len(fields) != want {
				t.Fatalf("parsed %d fields for a body declaring %d", len(fields), want)
			}
		}
	})
}

// Rows.Append is fed the one message whose contents a caller may not control:
// whatever the server sends, it must fail cleanly or hold exactly what it was
// given, never index past its own buffer.
func FuzzRowsAppend(f *testing.F) {
	f.Add(2, []byte{0, 2, 0, 0, 0, 1, 'a', 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(1, dataRow([]byte("x")))
	f.Add(1, []byte{0, 1})
	f.Add(0, []byte{0, 0})

	f.Fuzz(func(t *testing.T, fields int, body []byte) {
		if fields < 0 || fields > 64 {
			t.Skip("a width outside what a fuzzer can usefully explore")
		}
		r := NewRows(fields, 4)
		if err := r.Append(body); err != nil {
			if !errors.Is(err, ErrProtocol) && !errors.Is(err, ErrTooManyRows) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		// Every value of an accepted row must be readable, which is what
		// proves the offsets describe the buffer they index.
		for row := range r.Len() {
			for col := range r.Fields() {
				b, isNull := r.Value(row, col)
				if isNull && b != nil {
					t.Fatalf("(%d,%d) is NULL but carries %q", row, col, b)
				}
			}
		}
	})
}

// The decoders read whatever a column contained, which on a shared database is
// whatever some other writer put there. Each must fail cleanly or return a
// value — never index past the slice it was handed.
func FuzzDecoders(f *testing.F) {
	f.Add([]byte{1})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(bytes.Repeat([]byte{0xFF}, 16))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		// Widths are checked before reading, so every one of these either
		// answers or reports ErrCodec, whatever the bytes are.
		decoders := []func([]byte) error{
			func(b []byte) error { _, err := DecodeBool(b); return err },
			func(b []byte) error { _, err := DecodeInt2(b); return err },
			func(b []byte) error { _, err := DecodeInt4(b); return err },
			func(b []byte) error { _, err := DecodeInt8(b); return err },
			func(b []byte) error { _, err := DecodeFloat4(b); return err },
			func(b []byte) error { _, err := DecodeFloat8(b); return err },
			func(b []byte) error { _, err := DecodeUUID(b); return err },
			func(b []byte) error { _, err := DecodeTimestampTZ(b); return err },
			func(b []byte) error { _, err := DecodeDate(b); return err },
		}
		for i, dec := range decoders {
			if err := dec(b); err != nil && !errors.Is(err, ErrCodec) {
				t.Fatalf("decoder %d returned unclassified error %v", i, err)
			}
		}
		_ = DecodeText(b) // a string of any bytes is a string
	})
}

// The array decoder walks a header it did not write, so it is the one with
// offsets to get wrong.
func FuzzDecodeInt8Array(f *testing.F) {
	f.Add(AppendInt8Array(nil, []int64{1, 2, 3}))
	f.Add(AppendInt8Array(nil, nil))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeInt8Array(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		// Whatever it accepted must be re-encodable to something it accepts
		// again with the same values: a decoder that read a length from the
		// wrong offset would not survive that.
		again, err := DecodeInt8Array(nil, AppendInt8Array(nil, got))
		if err != nil {
			t.Fatalf("re-decoding what was accepted: %v", err)
		}
		if len(again) != len(got) {
			t.Fatalf("re-decode gave %d elements, first pass gave %d", len(again), len(got))
		}
		for i := range got {
			if again[i] != got[i] {
				t.Fatalf("element %d: %d then %d", i, got[i], again[i])
			}
		}
	})
}
