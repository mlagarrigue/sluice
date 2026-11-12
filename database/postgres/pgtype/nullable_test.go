package pgtype

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"
)

// present is a valid Null, spelled short for the tables of these tests. The
// package has no constructor: a literal is the one way to build a Null.
func present[T any](v T) Null[T] { return Null[T]{Value: v, Valid: true} }

// The header's flags word says whether any element is NULL, and the server
// sets it. An encoder that always wrote zero would produce arrays PostgreSQL
// accepts and never generates — the same asymmetry the empty-array header once
// had, and the reason that one was found late.
func TestNullableArrayHeaderDeclaresNulls(t *testing.T) {
	withNull := appendNulls(Int8Codec(), nil, []Null[int64]{present(int64(1)), {}})
	without := appendNulls(Int8Codec(), nil, []Null[int64]{present(int64(1)), present(int64(2))})

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
	raw := appendNulls(Int8Codec(), nil, []Null[int64]{{}})
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
		{"no nulls", []Null[int64]{present(int64(1)), present(int64(-2))}},
		{"leading null", []Null[int64]{{}, present(int64(7))}},
		{"trailing null", []Null[int64]{present(int64(7)), {}}},
		{"all null", []Null[int64]{{}, {}, {}}},
		{"zero is not null", []Null[int64]{present(int64(0)), {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := appendNulls(Int8Codec(), nil, tc.in)
			out, err := decodeNulls(Int8Codec(), nil, raw)
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
	in := []Null[string]{present(""), {}, present("x")}
	out, err := decodeNulls(TextCodec(), nil, appendNulls(TextCodec(), nil, in))
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
	ints := []Null[int32]{present(int32(3)), {}, present(int32(-4))}
	gotInts, err := decodeNulls(Int4Codec(), nil, appendNulls(Int4Codec(), nil, ints))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotInts, ints) {
		t.Errorf("int4[]: %v, want %v", gotInts, ints)
	}

	u := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	uuids := []Null[[16]byte]{present(u), {}}
	gotUUIDs, err := decodeNulls(UUIDCodec(), nil, appendNulls(UUIDCodec(), nil, uuids))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotUUIDs, uuids) {
		t.Errorf("uuid[]: %v, want %v", gotUUIDs, uuids)
	}
}

// A NULL comes back marked, never as the zero value alone: for text, the
// empty string, which is the confusion Null exists to remove.
func TestDecodeArrayNeverReadsANullAsAZero(t *testing.T) {
	wantNulls(t, "text", TextCodec(), appendNulls(TextCodec(), nil, []Null[string]{{}, present("")}), []bool{true, false})
	wantNulls(t, "int8", Int8Codec(), appendNulls(Int8Codec(), nil, []Null[int64]{{}}), []bool{true})
	wantNulls(t, "int4", Int4Codec(), appendNulls(Int4Codec(), nil, []Null[int32]{present[int32](0), {}}), []bool{false, true})
	wantNulls(t, "uuid", UUIDCodec(), appendNulls(UUIDCodec(), nil, []Null[[16]byte]{{}}), []bool{true})
}

// A nullable array with no NULLs must be byte-identical to what the plain
// encoder produces. If it were not, the same data would reach the server two
// ways and only one of them would have been tested against it.
func TestNullableEncodingMatchesPlainWhenNothingIsNull(t *testing.T) {
	plain := appendSlice(Int8Codec(), nil, []int64{1, 2, 3})
	nullable := appendNulls(Int8Codec(), nil, []Null[int64]{present(int64(1)), present(int64(2)), present(int64(3))})
	if !bytes.Equal(plain, nullable) {
		t.Errorf("the two encoders disagree:\n plain    % x\n nullable % x", plain, nullable)
	}

	plainEmpty := appendSlice(Int8Codec(), nil, nil)
	nullableEmpty := appendNulls(Int8Codec(), nil, nil)
	if !bytes.Equal(plainEmpty, nullableEmpty) {
		t.Errorf("empty arrays differ:\n plain    % x\n nullable % x", plainEmpty, nullableEmpty)
	}
}

// A length that overruns the buffer must be refused, not trusted — the decoder
// reads a length from bytes the server chose and then indexes with it.
func TestNullableArrayRejectsAnOverlongElement(t *testing.T) {
	raw := appendNulls(TextCodec(), nil, []Null[string]{present("ab")})
	// Claim sixteen bytes where two arrived.
	binary.BigEndian.PutUint32(raw[arrayHeaderSize:], 16)
	if _, err := decodeNulls(TextCodec(), nil, raw); !errors.Is(err, ErrCodec) {
		t.Fatalf("error = %v, want ErrCodec", err)
	}
}

// A negative length that is not -1 is not a NULL, it is corruption. The
// element's bytes are cut off, so that a decoder reading -2 as a NULL finds
// a well-formed array and only the length check can refuse it.
func TestNullableArrayRejectsANegativeLength(t *testing.T) {
	raw := appendNulls(Int8Codec(), nil, []Null[int64]{present(int64(1))})
	binary.BigEndian.PutUint32(raw[arrayHeaderSize:], ^uint32(1)) // -2
	raw = raw[:arrayHeaderSize+4]
	_, err := decodeNulls(Int8Codec(), nil, raw)
	if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), "declares a length of -2") {
		t.Fatalf("error = %v, want ErrCodec naming the length -2", err)
	}
}
