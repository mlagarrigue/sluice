package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// The codecs added most recently, against arbitrary bytes.
//
// Two properties, and they are the ones a decoder can break without anyone
// noticing. It must not panic on bytes a hostile or broken server could send —
// a decoder that indexes with a length it read is one bad message away from a
// crash. And whatever it accepts must survive a round trip: a value that comes
// back different is a wrong answer, which this package ranks below a failure.

func FuzzDecodeTime(f *testing.F) {
	f.Add(AppendTime(nil, TimeOfDay{}))
	f.Add(AppendTime(nil, TimeOfDay{Micros: microsPerDay}))
	f.Add(AppendTime(nil, TimeOfDay{Micros: -1}))
	f.Add(make([]byte, 7))
	f.Add(make([]byte, 9))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeTime(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		// String must not panic on anything that decoded, including the
		// negative and out-of-range readings the wire can carry.
		_ = v.String()
		_ = v.InRange()

		if got := AppendTime(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("DecodeTime(%x) -> %v -> %x", b, v, got)
		}
	})
}

func FuzzDecodeTimeTZ(f *testing.F) {
	f.Add(AppendTimeTZ(nil, TimeTZ{Micros: 9 * 3_600_000_000, OffsetSeconds: -7200}))
	f.Add(AppendTimeTZ(nil, TimeTZ{}))
	f.Add(make([]byte, 11))
	f.Add(make([]byte, 13))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeTimeTZ(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		// Zone flips the sign; doing it twice must return the wire value, or
		// the two conventions have drifted apart.
		if got := -int32(v.Zone()); got != v.OffsetSeconds { //nolint:gosec // G115: an offset in seconds
			t.Fatalf("Zone() round trip: %d became %d", v.OffsetSeconds, got)
		}
		if got := AppendTimeTZ(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("DecodeTimeTZ(%x) -> %+v -> %x", b, v, got)
		}
	})
}

func FuzzDecodeMacaddr(f *testing.F) {
	f.Add(AppendMacaddr(nil, [6]byte{1, 2, 3, 4, 5, 6}))
	f.Add(AppendMacaddr8(nil, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	f.Add(make([]byte, 5))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		if v, err := DecodeMacaddr(b); err == nil {
			if got := AppendMacaddr(nil, v); !bytes.Equal(got, b) {
				t.Fatalf("DecodeMacaddr(%x) -> %x -> %x", b, v, got)
			}
		} else if !errors.Is(err, ErrCodec) {
			t.Fatalf("unclassified error %v for %x", err, b)
		}

		// A six-byte address must never decode as an eight-byte one, whatever
		// the bytes: the server converts between them by inserting ff:fe, so
		// accepting the wrong width returns something that still looks like a
		// MAC address.
		if len(b) == 6 {
			if _, err := DecodeMacaddr8(b); err == nil {
				t.Fatalf("DecodeMacaddr8 accepted six bytes: %x", b)
			}
		}
	})
}

// The bit-string decoder reads a length in *bits* and then indexes with the
// bytes that follow. Trusting that length is how a decoder reads past its
// slice, so the property is that it never panics and never accepts a count the
// payload cannot support.
func FuzzDecodeBits(f *testing.F) {
	f.Add(AppendBits(nil, Bits{Len: 1, Bytes: []byte{0x80}}))
	f.Add(AppendBits(nil, Bits{Len: 10, Bytes: []byte{0xb1, 0x40}}))
	f.Add(AppendBits(nil, Bits{Len: 0}))
	// A count claiming far more bits than the body can hold.
	f.Add(append(binary.BigEndian.AppendUint32(nil, 1<<20), 0x80))
	// A negative count.
	f.Add(binary.BigEndian.AppendUint32(nil, ^uint32(0)))
	f.Add(make([]byte, 3))

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeBits(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		// Every declared bit must be readable, and the one past the end must
		// not be — that boundary is what stops At indexing outside Bytes.
		for i := range v.Len {
			if _, err := v.At(i); err != nil {
				t.Fatalf("DecodeBits(%x) accepted %d bits but At(%d) failed: %v", b, v.Len, i, err)
			}
		}
		if _, err := v.At(v.Len); err == nil {
			t.Fatalf("DecodeBits(%x): At(%d) succeeded past the declared length", b, v.Len)
		}
		if got := len(v.String()); got != v.Len {
			t.Fatalf("DecodeBits(%x): String() is %d characters for %d bits", b, got, v.Len)
		}
		if got := AppendBits(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("DecodeBits(%x) -> %+v -> %x", b, v, got)
		}
	})
}

// NULL-bearing arrays, where the header's flags word and the -1 element length
// have to agree. A decoder that read -1 as unsigned would take it for four
// billion bytes and index with it.
func FuzzDecodeNullableInt8Array(f *testing.F) {
	f.Add(AppendNullableInt8Array(nil, []Null[int64]{Some(int64(1)), None[int64]()}))
	f.Add(AppendNullableInt8Array(nil, nil))
	f.Add(AppendInt8Array(nil, []int64{1, 2}))
	f.Add(make([]byte, 12))
	// A header claiming more elements than the body holds.
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 20, 127, 255, 255, 255, 0, 0, 0, 1})

	f.Fuzz(func(t *testing.T, b []byte) {
		vals, err := DecodeNullableInt8Array(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if len(vals) == 0 {
			return
		}
		// Decoding twice rather than comparing bytes: the flags word is
		// normalised on re-encoding, and what must not change is the values.
		again, err := DecodeNullableInt8Array(nil, AppendNullableInt8Array(nil, vals))
		if err != nil {
			t.Fatalf("DecodeNullableInt8Array(%x) -> %v, which failed to decode: %v", b, vals, err)
		}
		if len(again) != len(vals) {
			t.Fatalf("not idempotent: %d elements then %d", len(vals), len(again))
		}
		for i := range vals {
			if again[i] != vals[i] {
				t.Fatalf("element %d changed on a second pass: %+v then %+v", i, vals[i], again[i])
			}
		}

		// And the plain decoder must refuse exactly the arrays that carry a
		// NULL, rather than turning one into a zero.
		_, plainErr := DecodeInt8Array(nil, b)
		hasNull := false
		for _, v := range vals {
			if !v.Valid {
				hasNull = true
				break
			}
		}
		if hasNull && plainErr == nil {
			t.Fatalf("DecodeInt8Array accepted an array containing a NULL: %x", b)
		}
	})
}

func FuzzDecodeNullableTextArray(f *testing.F) {
	f.Add(AppendNullableTextArray(nil, []Null[string]{Some(""), None[string](), Some("x")}))
	f.Add(AppendTextArray(nil, []string{"a"}))
	f.Add(make([]byte, 12))

	f.Fuzz(func(t *testing.T, b []byte) {
		vals, err := DecodeNullableTextArray(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		// An empty string and a NULL are different values, and the one thing
		// a decoder must never do is produce a present empty string where the
		// wire said absent. Nothing here can check that from the outside; what
		// it can check is that the pair survives re-encoding, below.
		again, err := DecodeNullableTextArray(nil, AppendNullableTextArray(nil, vals))
		if err != nil {
			t.Fatalf("re-encoding %v failed to decode: %v", vals, err)
		}
		if len(again) != len(vals) {
			t.Fatalf("not idempotent: %d then %d", len(vals), len(again))
		}
		for i := range vals {
			if again[i].Valid != vals[i].Valid || again[i].Value != vals[i].Value {
				t.Fatalf("element %d changed: %+v then %+v", i, vals[i], again[i])
			}
		}
	})
}
