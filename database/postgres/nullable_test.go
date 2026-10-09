package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// The header's flags word says whether any element is NULL, and the server
// sets it. An encoder that always wrote zero would produce arrays PostgreSQL
// accepts and never generates — the same asymmetry the empty-array header once
// had, and the reason that one was found late.
func TestNullableArrayHeaderDeclaresNulls(t *testing.T) {
	withNull := AppendNullableInt8Array(nil, []Null[int64]{Some(int64(1)), None[int64]()})
	without := AppendNullableInt8Array(nil, []Null[int64]{Some(int64(1)), Some(int64(2))})

	if got := binary.BigEndian.Uint32(withNull[4:8]); got != 1 {
		t.Errorf("flags = %d for an array containing a NULL, want 1", got)
	}
	if got := binary.BigEndian.Uint32(without[4:8]); got != 0 {
		t.Errorf("flags = %d for an array with no NULL, want 0", got)
	}
}

// A NULL element carries a length of -1 and no bytes. Read as unsigned it is
// four billion, which is why nextElement exists rather than each decoder
// checking for itself.
func TestNullableArrayNullElementIsMinusOne(t *testing.T) {
	raw := AppendNullableInt8Array(nil, []Null[int64]{None[int64]()})
	body := raw[arrayHeaderSize:]
	if len(body) != 4 {
		t.Fatalf("a NULL element occupies %d bytes, want 4 (its length and nothing else)", len(body))
	}
	if got := int32(binary.BigEndian.Uint32(body)); got != -1 { //nolint:gosec // G115: the protocol field is int32
		t.Errorf("NULL element length = %d, want -1", got)
	}
}

func TestNullableInt8ArrayRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []Null[int64]
	}{
		{"empty", nil},
		{"no nulls", []Null[int64]{Some(int64(1)), Some(int64(-2))}},
		{"leading null", []Null[int64]{None[int64](), Some(int64(7))}},
		{"trailing null", []Null[int64]{Some(int64(7)), None[int64]()}},
		{"all null", []Null[int64]{None[int64](), None[int64](), None[int64]()}},
		{"zero is not null", []Null[int64]{Some(int64(0)), None[int64]()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := AppendNullableInt8Array(nil, tc.in)
			out, err := DecodeNullableInt8Array(nil, raw)
			if err != nil {
				t.Fatalf("decoding: %v", err)
			}
			if !slices.Equal(out, tc.in) {
				t.Errorf("round trip gave %v, want %v", out, tc.in)
			}
		})
	}
}

// The distinction that earns the type its keep: in Go an empty string and a
// missing one are both "", and in SQL they are different values.
func TestNullableTextArrayKeepsEmptyDistinctFromNull(t *testing.T) {
	in := []Null[string]{Some(""), None[string](), Some("x")}
	out, err := DecodeNullableTextArray(nil, AppendNullableTextArray(nil, in))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(out, in) {
		t.Fatalf("round trip gave %v, want %v", out, in)
	}
	if out[0].Valid == out[1].Valid {
		t.Error("the empty string and the NULL came back the same")
	}
}

func TestNullableInt4AndUUIDArrayRoundTrip(t *testing.T) {
	ints := []Null[int32]{Some(int32(3)), None[int32](), Some(int32(-4))}
	gotInts, err := DecodeNullableInt4Array(nil, AppendNullableInt4Array(nil, ints))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotInts, ints) {
		t.Errorf("int4[]: %v, want %v", gotInts, ints)
	}

	u := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	uuids := []Null[[16]byte]{Some(u), None[[16]byte]()}
	gotUUIDs, err := DecodeNullableUUIDArray(nil, AppendNullableUUIDArray(nil, uuids))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotUUIDs, uuids) {
		t.Errorf("uuid[]: %v, want %v", gotUUIDs, uuids)
	}
}

// The non-nullable decoders still refuse a NULL rather than inventing a zero.
// That refusal is the reason the nullable ones exist, and dropping it while
// adding them would be a silent change of meaning for every existing caller.
func TestNonNullableDecodersStillRefuseNull(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		raw := AppendNullableTextArray(nil, []Null[string]{None[string]()})
		if _, err := DecodeTextArray(nil, raw); err == nil {
			t.Fatal("DecodeTextArray accepted a NULL element")
		}
	})
	t.Run("int8", func(t *testing.T) {
		raw := AppendNullableInt8Array(nil, []Null[int64]{None[int64]()})
		if _, err := DecodeInt8Array(nil, raw); err == nil {
			t.Fatal("DecodeInt8Array accepted a NULL element")
		}
	})
	t.Run("int4", func(t *testing.T) {
		raw := AppendNullableInt4Array(nil, []Null[int32]{None[int32]()})
		if _, err := DecodeInt4Array(nil, raw); err == nil {
			t.Fatal("DecodeInt4Array accepted a NULL element")
		}
	})
	t.Run("uuid", func(t *testing.T) {
		raw := AppendNullableUUIDArray(nil, []Null[[16]byte]{None[[16]byte]()})
		if _, err := DecodeUUIDArray(nil, raw); err == nil {
			t.Fatal("DecodeUUIDArray accepted a NULL element")
		}
	})
}

// A nullable array with no NULLs must be byte-identical to what the plain
// encoder produces. If it were not, the same data would reach the server two
// ways and only one of them would have been tested against it.
func TestNullableEncodingMatchesPlainWhenNothingIsNull(t *testing.T) {
	plain := AppendInt8Array(nil, []int64{1, 2, 3})
	nullable := AppendNullableInt8Array(nil, []Null[int64]{Some(int64(1)), Some(int64(2)), Some(int64(3))})
	if !bytes.Equal(plain, nullable) {
		t.Errorf("the two encoders disagree:\n plain    % x\n nullable % x", plain, nullable)
	}

	plainEmpty := AppendInt8Array(nil, nil)
	nullableEmpty := AppendNullableInt8Array(nil, nil)
	if !bytes.Equal(plainEmpty, nullableEmpty) {
		t.Errorf("empty arrays differ:\n plain    % x\n nullable % x", plainEmpty, nullableEmpty)
	}
}

// A length that overruns the buffer must be refused, not trusted — the decoder
// reads a length from bytes the server chose and then indexes with it.
func TestNullableArrayRejectsAnOverlongElement(t *testing.T) {
	raw := AppendNullableTextArray(nil, []Null[string]{Some("ab")})
	// Claim sixteen bytes where two arrived.
	binary.BigEndian.PutUint32(raw[arrayHeaderSize:], 16)
	if _, err := DecodeNullableTextArray(nil, raw); !errors.Is(err, ErrCodec) {
		t.Fatalf("error = %v, want ErrCodec", err)
	}
}

// A negative length that is not -1 is not a NULL, it is corruption.
func TestNullableArrayRejectsANegativeLength(t *testing.T) {
	raw := AppendNullableInt8Array(nil, []Null[int64]{Some(int64(1))})
	binary.BigEndian.PutUint32(raw[arrayHeaderSize:], ^uint32(1)) // -2
	if _, err := DecodeNullableInt8Array(nil, raw); !errors.Is(err, ErrCodec) {
		t.Fatalf("error = %v, want ErrCodec", err)
	}
}

// The zero Null is absent, not present-and-zero. Anything else would make an
// uninitialised element a real value.
func TestZeroNullIsAbsent(t *testing.T) {
	var n Null[int64]
	if n.Valid {
		t.Error("the zero Null reports itself as present")
	}
	if got := Some(int64(0)); !got.Valid {
		t.Error("Some(0) reports itself as absent")
	}
}
