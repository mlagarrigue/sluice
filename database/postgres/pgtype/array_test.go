package pgtype

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestScalarArrayRoundTrips(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		in := []bool{true, false, true}
		got, err := decodeSlice(BoolCodec(), nil, appendSlice(BoolCodec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("int2", func(t *testing.T) {
		in := []int16{0, 1, -1, math.MaxInt16, math.MinInt16}
		got, err := decodeSlice(Int2Codec(), nil, appendSlice(Int2Codec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("float4", func(t *testing.T) {
		in := []float32{0, 1.5, -1.5, math.MaxFloat32}
		got, err := decodeSlice(Float4Codec(), nil, appendSlice(Float4Codec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("float8", func(t *testing.T) {
		in := []float64{0, 1.5, -1.5, math.MaxFloat64, math.Inf(1), math.Inf(-1)}
		got, err := decodeSlice(Float8Codec(), nil, appendSlice(Float8Codec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("bytea", func(t *testing.T) {
		in := [][]byte{{}, {1, 2, 3}, {0xff}}
		got, err := decodeSlice(ByteaCodec(), nil, appendSlice(ByteaCodec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(in) {
			t.Fatalf("got %d elements, want %d", len(got), len(in))
		}
		for i := range in {
			if !bytes.Equal(got[i], in[i]) {
				t.Errorf("element %d = % x, want % x", i, got[i], in[i])
			}
		}
	})

	t.Run("timestamptz", func(t *testing.T) {
		in := []time.Time{
			time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		got, err := decodeSlice(TimestampTZCodec(), nil, appendSlice(TimestampTZCodec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(in) {
			t.Fatalf("got %d elements, want %d", len(got), len(in))
		}
		for i := range in {
			if !got[i].Equal(in[i]) {
				t.Errorf("element %d = %v, want %v", i, got[i], in[i])
			}
		}
	})

	t.Run("date", func(t *testing.T) {
		in := []time.Time{
			time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		got, err := decodeSlice(DateCodec(), nil, appendSlice(DateCodec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		for i := range in {
			if !got[i].Equal(in[i]) {
				t.Errorf("element %d = %v, want %v", i, got[i], in[i])
			}
		}
	})

	t.Run("numeric", func(t *testing.T) {
		var in []Numeric
		for _, s := range []string{"0", "1.5", "-99999999999999999999", "0.000000000000000001"} {
			n, err := ParseNumeric(s)
			if err != nil {
				t.Fatal(err)
			}
			in = append(in, n)
		}
		got, err := decodeSlice(NumericCodec(), nil, appendSlice(NumericCodec(), nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(in) {
			t.Fatalf("got %d elements, want %d", len(got), len(in))
		}
		for i := range in {
			if got[i].String() != in[i].String() {
				t.Errorf("element %d = %s, want %s", i, got[i].String(), in[i].String())
			}
		}
	})
}

// NaN is a value the column holds; NULL is the absence of one. An array codec
// that collapsed either into the other would make a query about missing data
// answer about undefined arithmetic.
func TestFloat8ArrayKeepsNaNDistinctFromNull(t *testing.T) {
	in := []Null[float64]{present(math.NaN()), {}, present[float64](0.0)}
	got, err := decodeNulls(Float8Codec(), nil, appendNulls(Float8Codec(), nil, in))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d elements, want 3", len(got))
	}
	if !got[0].Valid || !math.IsNaN(got[0].Value) {
		t.Errorf("NaN came back as %+v", got[0])
	}
	if got[1].Valid {
		t.Error("the NULL came back as present")
	}
	if !got[2].Valid || got[2].Value != 0 {
		t.Errorf("zero came back as %+v", got[2])
	}
}

// SQL's boolean has three states and Go's has two. Flattening unknown into
// false would answer a different question.
func TestNullableBoolArrayKeepsUnknown(t *testing.T) {
	in := []Null[bool]{present(true), {}, present(false)}
	got, err := decodeNulls(BoolCodec(), nil, appendNulls(BoolCodec(), nil, in))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, in) {
		t.Fatalf("got %v, want %v", got, in)
	}
	if got[1].Valid == got[2].Valid {
		t.Error("unknown and false came back the same")
	}
}

// A NULL element comes back marked in Nulls with a zero beside it, never
// as a zero alone: the named decoders of a []T refused it, and an Array has
// a place for it.
func TestDecodeArrayMarksANullRatherThanReadingAZero(t *testing.T) {
	wantNulls(t, "float8", Float8Codec(), appendNulls(Float8Codec(), nil, []Null[float64]{{}}), []bool{true})
	wantNulls(t, "bool", BoolCodec(), appendNulls(BoolCodec(), nil, []Null[bool]{present(true), {}}), []bool{false, true})
	// A variable-width element is the case that matters most: no width check
	// stands behind it, and bytea would read a NULL as empty bytes.
	wantNulls(t, "bytea", ByteaCodec(), appendNulls(ByteaCodec(), nil, []Null[[]byte]{{}, present([]byte{})}), []bool{true, false})
}

// bytea elements must be copied out: the wire buffer is reused per batch.
func TestByteaArrayCopiesOutOfTheWireBuffer(t *testing.T) {
	raw := appendSlice(ByteaCodec(), nil, [][]byte{{0xaa, 0xbb}})
	got, err := decodeSlice(ByteaCodec(), nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0 // the batch moves on
	}
	if got[0][0] != 0xaa || got[0][1] != 0xbb {
		t.Errorf("the decoded element changed with the wire buffer: % x", got[0])
	}
}

// The element OID in the header is the only thing separating timestamp from
// timestamptz, whose bytes are identical. A decoder that ignored it would read
// a clock reading as an instant, silently, and be out by the server's offset.
func TestTimestampArrayIsNotTimestampTZArray(t *testing.T) {
	when := []time.Time{time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)}

	asTZ := appendSlice(TimestampTZCodec(), nil, when)
	if _, err := decodeSlice(TimestampCodec(), nil, asTZ); !errors.Is(err, ErrCodec) {
		t.Error("a timestamptz[] decoded as timestamp[]")
	}
	asPlain := appendSlice(TimestampCodec(), nil, when)
	if _, err := decodeSlice(TimestampTZCodec(), nil, asPlain); !errors.Is(err, ErrCodec) {
		t.Error("a timestamp[] decoded as timestamptz[]")
	}
	// And their element payloads really are the same, which is what makes the
	// OID check load-bearing rather than belt-and-braces.
	if !bytes.Equal(asTZ[arrayHeaderSize:], asPlain[arrayHeaderSize:]) {
		t.Error("the two encodings differ in their payload, so this test proves less than it claims")
	}
}

// A variable-width element's length is written in place after the fact. If the
// back-patch were wrong, every element after the first would be misread.
func TestVariableWidthElementLengthsArePatchedCorrectly(t *testing.T) {
	in := [][]byte{{1}, {2, 2}, {3, 3, 3}, {}}
	raw := appendSlice(ByteaCodec(), nil, in)

	body := raw[arrayHeaderSize:]
	for i, want := range []int{1, 2, 3, 0} {
		if len(body) < 4 {
			t.Fatalf("ran out of bytes at element %d", i)
		}
		got := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if got != want {
			t.Fatalf("element %d declares %d bytes, want %d", i, got, want)
		}
		body = body[4+got:]
	}
	if len(body) != 0 {
		t.Errorf("%d bytes left after the last element", len(body))
	}
}

// keepsBuffer checks the all-or-nothing contract on a decoding failure: the
// error comes back, and so does the caller's buffer, truncated and with its
// capacity intact — a nil in its place would cost a pipeline that logs a bad
// row and carries on its reuse buffer.
func keepsBuffer[T any](t *testing.T, name string, good []byte, dec func([]T, []byte) ([]T, error)) {
	t.Helper()
	for _, bad := range []struct {
		why string
		b   []byte
	}{
		{"truncated element", good[:len(good)-1]},
		{"trailing bytes", append(slices.Clip(good), 0)},
	} {
		buf := make([]T, 3, 64)
		got, err := dec(buf, bad.b)
		if !errors.Is(err, ErrCodec) {
			t.Errorf("%s, %s: error = %v, want ErrCodec", name, bad.why, err)
		}
		if got == nil || len(got) != 0 || cap(got) != 64 {
			t.Errorf("%s, %s: returned len %d cap %d (nil: %v), want the caller's buffer truncated", name, bad.why, len(got), cap(got), got == nil)
		}
	}
}

func TestArrayDecodersKeepTheBufferOnError(t *testing.T) {
	u := [16]byte{1, 2, 3}
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	keepsBuffer(t, "int8", appendSlice(Int8Codec(), nil, []int64{1, 2}), sliceDecoder(Int8Codec()))
	keepsBuffer(t, "int4", appendSlice(Int4Codec(), nil, []int32{1, 2}), sliceDecoder(Int4Codec()))
	keepsBuffer(t, "text", appendSlice(TextCodec(), nil, []string{"a", "bc"}), sliceDecoder(TextCodec()))
	keepsBuffer(t, "uuid", appendSlice(UUIDCodec(), nil, [][16]byte{u, u}), sliceDecoder(UUIDCodec()))
	keepsBuffer(t, "bool", appendSlice(BoolCodec(), nil, []bool{true, false}), sliceDecoder(BoolCodec()))
	keepsBuffer(t, "int2", appendSlice(Int2Codec(), nil, []int16{1, 2}), sliceDecoder(Int2Codec()))
	keepsBuffer(t, "float8", appendSlice(Float8Codec(), nil, []float64{1, 2}), sliceDecoder(Float8Codec()))
	keepsBuffer(t, "timestamptz", appendSlice(TimestampTZCodec(), nil, []time.Time{ts, ts}), sliceDecoder(TimestampTZCodec()))
	keepsBuffer(t, "nullable int8", appendNulls(Int8Codec(), nil, []Null[int64]{present[int64](1), {}, present[int64](2)}), nullsDecoder(Int8Codec()))
	keepsBuffer(t, "nullable int4", appendNulls(Int4Codec(), nil, []Null[int32]{present[int32](1), {}, present[int32](2)}), nullsDecoder(Int4Codec()))
	keepsBuffer(t, "nullable text", appendNulls(TextCodec(), nil, []Null[string]{present("a"), {}, present("bc")}), nullsDecoder(TextCodec()))
	keepsBuffer(t, "nullable uuid", appendNulls(UUIDCodec(), nil, []Null[[16]byte]{present(u), {}, present(u)}), nullsDecoder(UUIDCodec()))
	keepsBuffer(t, "nullable int2", appendNulls(Int2Codec(), nil, []Null[int16]{present[int16](1), {}, present[int16](2)}), nullsDecoder(Int2Codec()))
	keepsBuffer(t, "nullable float8", appendNulls(Float8Codec(), nil, []Null[float64]{present[float64](1.5), {}, present[float64](2.5)}), nullsDecoder(Float8Codec()))
}

// int2[] with NULLs: the round trip, the header's NULL flag, and a NULL
// element landing as an invalid Null rather than as a zero.
func TestNullableInt2ArrayRoundTrip(t *testing.T) {
	in := []Null[int16]{present[int16](math.MinInt16), {}, present[int16](0), present[int16](math.MaxInt16), {}}
	enc := appendNulls(Int2Codec(), nil, in)
	if flags := binary.BigEndian.Uint32(enc[4:8]); flags != 1 {
		t.Errorf("has-NULL flag = %d, want 1", flags)
	}
	if oid := binary.BigEndian.Uint32(enc[8:12]); oid != OIDInt2 {
		t.Errorf("element OID = %d, want int2 (%d)", oid, OIDInt2)
	}
	got, err := decodeNulls(Int2Codec(), nil, enc)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, in) {
		t.Errorf("round trip = %v, want %v", got, in)
	}

	// No NULLs: the flag is clear, and the non-nullable decoder reads it too.
	plain := appendNulls(Int2Codec(), nil, []Null[int16]{present[int16](7), present[int16](-7)})
	if flags := binary.BigEndian.Uint32(plain[4:8]); flags != 0 {
		t.Errorf("has-NULL flag = %d without NULLs, want 0", flags)
	}
	if vals, err := decodeSlice(Int2Codec(), nil, plain); err != nil || !slices.Equal(vals, []int16{7, -7}) {
		t.Errorf("DecodeArray = %v, %v", vals, err)
	}
	// With a NULL, the element is marked rather than read as a zero.
	wantNulls(t, "int2", Int2Codec(), enc, []bool{false, true, false, false, true})

	// Empty: zero dimensions, decoded to an empty slice.
	empty := appendNulls(Int2Codec(), nil, nil)
	if got, err := decodeNulls(Int2Codec(), []Null[int16]{{}}, empty); err != nil || len(got) != 0 {
		t.Errorf("empty = %v, %v", got, err)
	}

	// Wrong element type is refused, not reinterpreted.
	if _, err := decodeNulls(Int2Codec(), nil, appendNulls(Int4Codec(), nil, []Null[int32]{present[int32](1)})); !errors.Is(err, ErrCodec) {
		t.Errorf("int4[] decoded as int2[]: %v, want ErrCodec", err)
	}
	// An element of the wrong width is refused by the element's own
	// decoder, and the error says which element: a decoder whose error was
	// swallowed would leave the stream misaligned and fail later, on some
	// other count, or not at all.
	bad := slices.Clone(enc)
	binary.BigEndian.PutUint32(bad[arrayHeaderSize:], 4)
	if _, err := decodeNulls(Int2Codec(), nil, bad); !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), "array element 0") {
		t.Errorf("a 4-byte int2 element: %v, want ErrCodec for array element 0", err)
	}
}

// The generic arrays were tested by round trip alone, which an encoder and a
// decoder that agree on the same mistake pass together. These are the bytes
// array_send writes, built from its layout — ndim, has-NULL flag, element
// OID, then length and lower bound per dimension, then length-prefixed
// elements — and both halves are held to them.
func TestGenericArraysMatchTheServerBytes(t *testing.T) {
	const header = "00000001" // one dimension
	t.Run("bool", func(t *testing.T) {
		wire := mustHex(t, header+"00000000"+"00000010"+"0000000200000001"+"0000000101"+"0000000100")
		in := []bool{true, false}
		if got := appendSlice(BoolCodec(), nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendArray = % x\n                want % x", got, wire)
		}
		if got, err := decodeSlice(BoolCodec(), nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeArray = %v, %v", got, err)
		}
	})
	t.Run("int2", func(t *testing.T) {
		wire := mustHex(t, header+"00000000"+"00000015"+"0000000200000001"+"000000020001"+"00000002ffff")
		in := []int16{1, -1}
		if got := appendSlice(Int2Codec(), nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendArray = % x\n                want % x", got, wire)
		}
		if got, err := decodeSlice(Int2Codec(), nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeArray = %v, %v", got, err)
		}
	})
	t.Run("nullable float8", func(t *testing.T) {
		wire := mustHex(t, header+"00000001"+"000002bd"+"0000000200000001"+"000000083ff8000000000000"+"ffffffff")
		in := []Null[float64]{present[float64](1.5), {}}
		if got := appendNulls(Float8Codec(), nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendArray = % x\n                          want % x", got, wire)
		}
		if got, err := decodeNulls(Float8Codec(), nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeArray = %v, %v", got, err)
		}
	})
	t.Run("bytea", func(t *testing.T) {
		wire := mustHex(t, header+"00000000"+"00000011"+"0000000200000001"+"0000000101"+"00000000")
		in := [][]byte{{1}, {}}
		if got := appendSlice(ByteaCodec(), nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendArray = % x\n                 want % x", got, wire)
		}
		got, err := decodeSlice(ByteaCodec(), nil, wire)
		if err != nil || len(got) != 2 || !bytes.Equal(got[0], in[0]) || len(got[1]) != 0 {
			t.Errorf("DecodeArray = %v, %v", got, err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		// Zero dimensions and no dimension words, as the server sends '{}'.
		wire := mustHex(t, "00000000"+"00000000"+"00000010")
		if got := appendSlice(BoolCodec(), nil, nil); !bytes.Equal(got, wire) {
			t.Errorf("AppendArray(nil) = % x, want % x", got, wire)
		}
		if got, err := decodeSlice(BoolCodec(), nil, wire); err != nil || len(got) != 0 {
			t.Errorf("DecodeArray = %v, %v", got, err)
		}
	})
}

// The other types of the codec table, held to what int2 is held to: the
// round trip keeps every element, NULL included; the header's flag says a
// NULL is present; the NULL is marked rather than read as a zero; and an
// empty array decodes to nothing.
func TestNullableArrayRoundTrips(t *testing.T) {
	at := time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	price, err := ParseNumeric("19.90")
	if err != nil {
		t.Fatal(err)
	}

	nullableRoundTrip(t, "float4", OIDFloat4,
		[]Null[float32]{present[float32](1.5), {}, present[float32](-math.MaxFloat32)},
		Float4Codec(),
		func(a, b float32) bool { return a == b })
	nullableRoundTrip(t, "bytea", OIDBytea,
		[]Null[[]byte]{present([]byte{1, 2, 3}), {}, present([]byte{})},
		ByteaCodec(),
		bytes.Equal)
	nullableRoundTrip(t, "timestamptz", OIDTimestampTZ,
		[]Null[time.Time]{present(at), {}, present(InfinityTime())},
		TimestampTZCodec(),
		time.Time.Equal)
	nullableRoundTrip(t, "timestamp", OIDTimestamp,
		[]Null[time.Time]{present(at), {}, present(at.Add(time.Hour))},
		TimestampCodec(),
		time.Time.Equal)
	nullableRoundTrip(t, "date", OIDDate,
		[]Null[time.Time]{present(day), {}, present(day.AddDate(0, 0, 1))},
		DateCodec(),
		time.Time.Equal)
	nullableRoundTrip(t, "numeric", OIDNumeric,
		[]Null[Numeric]{present(price), {}, present(Numeric{Sign: NumericNaN})},
		NumericCodec(),
		func(a, b Numeric) bool { return a.String() == b.String() })

	// An empty bytea element and a NULL one are both zero-length and must
	// not be confused: the first is a value, the second is not.
	got, err := decodeNulls(ByteaCodec(), nil, appendNulls(ByteaCodec(), nil, []Null[[]byte]{present([]byte{}), {}}))
	if err != nil || len(got) != 2 || !got[0].Valid || got[1].Valid {
		t.Errorf("bytea empty-vs-NULL = %v, %v; want [valid empty, invalid]", got, err)
	}
	// What comes back is a copy, not a window on the wire buffer.
	raw := appendNulls(ByteaCodec(), nil, []Null[[]byte]{present([]byte{7})})
	got, _ = decodeNulls(ByteaCodec(), nil, raw)
	raw[len(raw)-1] = 9
	if got[0].Value[0] != 7 {
		t.Error("a nullable bytea element aliased the wire buffer")
	}

	// The buffer survives a bad batch, like every other decoder's.
	keepsBuffer(t, "nullable float4", appendNulls(Float4Codec(), nil, []Null[float32]{present[float32](1), {}, present[float32](2)}), nullsDecoder(Float4Codec()))
	keepsBuffer(t, "nullable bytea", appendNulls(ByteaCodec(), nil, []Null[[]byte]{present([]byte{1}), {}, present([]byte{2})}), nullsDecoder(ByteaCodec()))
	keepsBuffer(t, "nullable timestamptz", appendNulls(TimestampTZCodec(), nil, []Null[time.Time]{present(at), {}, present(at)}), nullsDecoder(TimestampTZCodec()))
	keepsBuffer(t, "nullable timestamp", appendNulls(TimestampCodec(), nil, []Null[time.Time]{present(at), {}, present(at)}), nullsDecoder(TimestampCodec()))
	keepsBuffer(t, "nullable date", appendNulls(DateCodec(), nil, []Null[time.Time]{present(day), {}, present(day)}), nullsDecoder(DateCodec()))
	keepsBuffer(t, "nullable numeric", appendNulls(NumericCodec(), nil, []Null[Numeric]{present(price), {}, present(price)}), nullsDecoder(NumericCodec()))
}

func nullableRoundTrip[T any](t *testing.T, name string, elem uint32, in []Null[T], c Codec[T], eq func(a, b T) bool) {
	t.Helper()
	raw := appendNulls(c, nil, in)
	if flags := binary.BigEndian.Uint32(raw[4:8]); flags != 1 {
		t.Errorf("%s: has-NULL flag = %d, want 1", name, flags)
	}
	if oid := binary.BigEndian.Uint32(raw[8:12]); oid != elem {
		t.Errorf("%s: element OID = %d, want %d", name, oid, elem)
	}
	got, err := decodeNulls(c, nil, raw)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(got) != len(in) {
		t.Fatalf("%s: round trip = %v, want %v", name, got, in)
	}
	for i := range in {
		if got[i].Valid != in[i].Valid || (in[i].Valid && !eq(got[i].Value, in[i].Value)) {
			t.Errorf("%s: element %d = %v, want %v", name, i, got[i], in[i])
		}
	}
	nulls := make([]bool, len(in))
	for i, v := range in {
		nulls[i] = !v.Valid
	}
	wantNulls(t, name, c, raw, nulls)
	if got, err := decodeNulls(c, []Null[T]{{}}, appendNulls(c, nil, nil)); err != nil || len(got) != 0 {
		t.Errorf("%s: empty = %v, %v", name, got, err)
	}
}

// An array decoder's errors say what it found, from the header on: the
// empty array's trailing bytes, the width, the length cut short.
func TestArrayErrorsSayWhatTheyFound(t *testing.T) {
	header := func(count int64) []byte {
		b := binary.BigEndian.AppendUint32(nil, 1)
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint32(b, OIDInt8)
		b = binary.BigEndian.AppendUint32(b, uint32(count)) //nolint:gosec // G115: test vectors
		return binary.BigEndian.AppendUint32(b, 1)
	}
	word := func(w int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(w)) } //nolint:gosec // G115: -1 on purpose
	empty := append(appendSlice(Int8Codec(), nil, nil), 0)
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"a byte after an empty array", empty, "1 bytes after the last array element"},
		{"an element of the wrong width", append(append(header(1), word(4)...), 0, 0, 0, 1), "array element 0: pgtype: value does not match its declared type: int8 is 4 bytes, want 8"},
		{"a length and no value", append(header(1), word(8)...), "array element 0 declares 8 bytes, 0 remain"},
		{"a value cut short", append(append(header(1), word(8)...), 0, 0, 0), "array element 0 declares 8 bytes, 3 remain"},
		{"no length", append(header(1), 0, 0), "array declares more elements than its 2 bytes can hold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeSlice(Int8Codec(), nil, tc.b); !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrCodec saying %q", err, tc.want)
			}
		})
	}
	// One dimension of no elements is a header array_send never writes —
	// the empty array is zero dimensions — and is refused as one.
	if _, err := decodeSlice(Int8Codec(), []int64{9}, header(0)); !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), "dimension 0 has 0 elements") {
		t.Errorf("one dimension of no elements: %v, want ErrCodec naming the dimension", err)
	}
}
