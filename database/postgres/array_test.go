package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"
	"time"
)

// The generic skeleton has to produce exactly what the hand-written encoders
// produce, or the same data reaches the server two ways and only one of them
// was ever tested against it. int4 is the overlap: it is written by hand for
// the key path and is reachable through the skeleton's shape here.
func TestGenericArraySkeletonMatchesTheHandWrittenHeader(t *testing.T) {
	// Same header, same lower bound, same empty-array form.
	hand := AppendInt4Array(nil, []int32{1, 2, 3})
	generic := appendArrayOf(nil, OIDInt4, []int32{1, 2, 3}, AppendInt4)
	if !bytes.Equal(hand, generic) {
		t.Errorf("encoders disagree:\n hand    % x\n generic % x", hand, generic)
	}

	handEmpty := AppendInt4Array(nil, nil)
	genericEmpty := appendArrayOf(nil, OIDInt4, []int32(nil), AppendInt4)
	if !bytes.Equal(handEmpty, genericEmpty) {
		t.Errorf("empty arrays disagree:\n hand    % x\n generic % x", handEmpty, genericEmpty)
	}
}

func TestScalarArrayRoundTrips(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		in := []bool{true, false, true}
		got, err := DecodeBoolArray(nil, AppendBoolArray(nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("int2", func(t *testing.T) {
		in := []int16{0, 1, -1, math.MaxInt16, math.MinInt16}
		got, err := DecodeInt2Array(nil, AppendInt2Array(nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("float4", func(t *testing.T) {
		in := []float32{0, 1.5, -1.5, math.MaxFloat32}
		got, err := DecodeFloat4Array(nil, AppendFloat4Array(nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("float8", func(t *testing.T) {
		in := []float64{0, 1.5, -1.5, math.MaxFloat64, math.Inf(1), math.Inf(-1)}
		got, err := DecodeFloat8Array(nil, AppendFloat8Array(nil, in))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, in) {
			t.Errorf("got %v, want %v", got, in)
		}
	})

	t.Run("bytea", func(t *testing.T) {
		in := [][]byte{{}, {1, 2, 3}, {0xff}}
		got, err := DecodeByteaArray(nil, AppendByteaArray(nil, in))
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
		got, err := DecodeTimestampTZArray(nil, AppendTimestampTZArray(nil, in))
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
		got, err := DecodeDateArray(nil, AppendDateArray(nil, in))
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
		got, err := DecodeNumericArray(nil, AppendNumericArray(nil, in))
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
	in := []Null[float64]{Some(math.NaN()), None[float64](), Some(0.0)}
	got, err := DecodeNullableFloat8Array(nil, AppendNullableFloat8Array(nil, in))
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
	in := []Null[bool]{Some(true), None[bool](), Some(false)}
	got, err := DecodeNullableBoolArray(nil, AppendNullableBoolArray(nil, in))
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

// The non-nullable generic decoders refuse a NULL rather than producing a
// zero, matching the hand-written ones.
func TestGenericDecodersRefuseNull(t *testing.T) {
	raw := appendNullableArrayOf(nil, OIDFloat8, []Null[float64]{None[float64]()}, AppendFloat8)
	if _, err := DecodeFloat8Array(nil, raw); !errors.Is(err, ErrCodec) {
		t.Fatalf("DecodeFloat8Array accepted a NULL: %v", err)
	}
	rawBool := appendNullableArrayOf(nil, OIDBool, []Null[bool]{None[bool]()}, AppendBool)
	if _, err := DecodeBoolArray(nil, rawBool); !errors.Is(err, ErrCodec) {
		t.Fatalf("DecodeBoolArray accepted a NULL: %v", err)
	}
}

// bytea elements must be copied out: the wire buffer is reused per batch.
func TestByteaArrayCopiesOutOfTheWireBuffer(t *testing.T) {
	raw := AppendByteaArray(nil, [][]byte{{0xaa, 0xbb}})
	got, err := DecodeByteaArray(nil, raw)
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

	asTZ := AppendTimestampTZArray(nil, when)
	if _, err := DecodeTimestampArray(nil, asTZ); !errors.Is(err, ErrCodec) {
		t.Error("a timestamptz[] decoded as timestamp[]")
	}
	asPlain := AppendTimestampArray(nil, when)
	if _, err := DecodeTimestampTZArray(nil, asPlain); !errors.Is(err, ErrCodec) {
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
	raw := AppendByteaArray(nil, in)

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
	keepsBuffer(t, "int8", AppendInt8Array(nil, []int64{1, 2}), DecodeInt8Array)
	keepsBuffer(t, "int4", AppendInt4Array(nil, []int32{1, 2}), DecodeInt4Array)
	keepsBuffer(t, "text", AppendTextArray(nil, []string{"a", "bc"}), DecodeTextArray)
	keepsBuffer(t, "uuid", AppendUUIDArray(nil, [][16]byte{u, u}), DecodeUUIDArray)
	keepsBuffer(t, "bool", AppendBoolArray(nil, []bool{true, false}), DecodeBoolArray)
	keepsBuffer(t, "int2", AppendInt2Array(nil, []int16{1, 2}), DecodeInt2Array)
	keepsBuffer(t, "float8", AppendFloat8Array(nil, []float64{1, 2}), DecodeFloat8Array)
	keepsBuffer(t, "timestamptz", AppendTimestampTZArray(nil, []time.Time{ts, ts}), DecodeTimestampTZArray)
	keepsBuffer(t, "nullable int8", AppendNullableInt8Array(nil, []Null[int64]{Some[int64](1), {}, Some[int64](2)}), DecodeNullableInt8Array)
	keepsBuffer(t, "nullable int4", AppendNullableInt4Array(nil, []Null[int32]{Some[int32](1), {}, Some[int32](2)}), DecodeNullableInt4Array)
	keepsBuffer(t, "nullable text", AppendNullableTextArray(nil, []Null[string]{Some("a"), {}, Some("bc")}), DecodeNullableTextArray)
	keepsBuffer(t, "nullable uuid", AppendNullableUUIDArray(nil, []Null[[16]byte]{Some(u), {}, Some(u)}), DecodeNullableUUIDArray)
	keepsBuffer(t, "nullable int2", AppendNullableInt2Array(nil, []Null[int16]{Some[int16](1), {}, Some[int16](2)}), DecodeNullableInt2Array)
	keepsBuffer(t, "nullable float8", AppendNullableFloat8Array(nil, []Null[float64]{Some(1.5), {}, Some(2.5)}), DecodeNullableFloat8Array)
}

// int2[] with NULLs: the round trip, the header's NULL flag, and a NULL
// element landing as an invalid Null rather than as a zero.
func TestNullableInt2ArrayRoundTrip(t *testing.T) {
	in := []Null[int16]{Some[int16](math.MinInt16), {}, Some[int16](0), Some[int16](math.MaxInt16), {}}
	enc := AppendNullableInt2Array(nil, in)
	if flags := binary.BigEndian.Uint32(enc[4:8]); flags != 1 {
		t.Errorf("has-NULL flag = %d, want 1", flags)
	}
	if oid := binary.BigEndian.Uint32(enc[8:12]); oid != OIDInt2 {
		t.Errorf("element OID = %d, want int2 (%d)", oid, OIDInt2)
	}
	got, err := DecodeNullableInt2Array(nil, enc)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, in) {
		t.Errorf("round trip = %v, want %v", got, in)
	}

	// No NULLs: the flag is clear, and the non-nullable decoder reads it too.
	plain := AppendNullableInt2Array(nil, []Null[int16]{Some[int16](7), Some[int16](-7)})
	if flags := binary.BigEndian.Uint32(plain[4:8]); flags != 0 {
		t.Errorf("has-NULL flag = %d without NULLs, want 0", flags)
	}
	if vals, err := DecodeInt2Array(nil, plain); err != nil || !slices.Equal(vals, []int16{7, -7}) {
		t.Errorf("DecodeInt2Array = %v, %v", vals, err)
	}
	// With a NULL, the non-nullable decoder refuses rather than writing a zero.
	if _, err := DecodeInt2Array(nil, enc); !errors.Is(err, ErrCodec) {
		t.Errorf("DecodeInt2Array over a NULL element = %v, want ErrCodec", err)
	}

	// Empty: zero dimensions, decoded to an empty slice.
	empty := AppendNullableInt2Array(nil, nil)
	if got, err := DecodeNullableInt2Array([]Null[int16]{{}}, empty); err != nil || len(got) != 0 {
		t.Errorf("empty = %v, %v", got, err)
	}

	// Wrong element type is refused, not reinterpreted.
	if _, err := DecodeNullableInt2Array(nil, AppendNullableInt4Array(nil, []Null[int32]{Some[int32](1)})); !errors.Is(err, ErrCodec) {
		t.Errorf("int4[] decoded as int2[]: %v, want ErrCodec", err)
	}
	// An element of the wrong width is refused.
	bad := slices.Clone(enc)
	binary.BigEndian.PutUint32(bad[arrayHeaderSize:], 4)
	if _, err := DecodeNullableInt2Array(nil, bad); err == nil {
		t.Error("a 4-byte int2 element was accepted")
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
		if got := AppendBoolArray(nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendBoolArray = % x\n                want % x", got, wire)
		}
		if got, err := DecodeBoolArray(nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeBoolArray = %v, %v", got, err)
		}
	})
	t.Run("int2", func(t *testing.T) {
		wire := mustHex(t, header+"00000000"+"00000015"+"0000000200000001"+"000000020001"+"00000002ffff")
		in := []int16{1, -1}
		if got := AppendInt2Array(nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendInt2Array = % x\n                want % x", got, wire)
		}
		if got, err := DecodeInt2Array(nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeInt2Array = %v, %v", got, err)
		}
	})
	t.Run("nullable float8", func(t *testing.T) {
		wire := mustHex(t, header+"00000001"+"000002bd"+"0000000200000001"+"000000083ff8000000000000"+"ffffffff")
		in := []Null[float64]{Some(1.5), {}}
		if got := AppendNullableFloat8Array(nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendNullableFloat8Array = % x\n                          want % x", got, wire)
		}
		if got, err := DecodeNullableFloat8Array(nil, wire); err != nil || !slices.Equal(got, in) {
			t.Errorf("DecodeNullableFloat8Array = %v, %v", got, err)
		}
	})
	t.Run("bytea", func(t *testing.T) {
		wire := mustHex(t, header+"00000000"+"00000011"+"0000000200000001"+"0000000101"+"00000000")
		in := [][]byte{{1}, {}}
		if got := AppendByteaArray(nil, in); !bytes.Equal(got, wire) {
			t.Errorf("AppendByteaArray = % x\n                 want % x", got, wire)
		}
		got, err := DecodeByteaArray(nil, wire)
		if err != nil || len(got) != 2 || !bytes.Equal(got[0], in[0]) || len(got[1]) != 0 {
			t.Errorf("DecodeByteaArray = %v, %v", got, err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		// Zero dimensions and no dimension words, as the server sends '{}'.
		wire := mustHex(t, "00000000"+"00000000"+"00000010")
		if got := AppendBoolArray(nil, nil); !bytes.Equal(got, wire) {
			t.Errorf("AppendBoolArray(nil) = % x, want % x", got, wire)
		}
		if got, err := DecodeBoolArray(nil, wire); err != nil || len(got) != 0 {
			t.Errorf("DecodeBoolArray = %v, %v", got, err)
		}
	})
}

// The six nullable pairs added for the codec table, held to what the int2
// one is held to: the round trip keeps every element, NULL included; the
// header's flag says a NULL is present; the plain decoder refuses the same
// bytes rather than writing a zero; and an empty array decodes to nothing.
func TestNullableArrayRoundTrips(t *testing.T) {
	at := time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	price, err := ParseNumeric("19.90")
	if err != nil {
		t.Fatal(err)
	}

	nullableRoundTrip(t, "float4", OIDFloat4,
		[]Null[float32]{Some[float32](1.5), {}, Some[float32](-math.MaxFloat32)},
		AppendNullableFloat4Array, DecodeNullableFloat4Array, DecodeFloat4Array,
		func(a, b float32) bool { return a == b })
	nullableRoundTrip(t, "bytea", OIDBytea,
		[]Null[[]byte]{Some([]byte{1, 2, 3}), {}, Some([]byte{})},
		AppendNullableByteaArray, DecodeNullableByteaArray, DecodeByteaArray,
		bytes.Equal)
	nullableRoundTrip(t, "timestamptz", OIDTimestampTZ,
		[]Null[time.Time]{Some(at), {}, Some(InfinityTime())},
		AppendNullableTimestampTZArray, DecodeNullableTimestampTZArray, DecodeTimestampTZArray,
		time.Time.Equal)
	nullableRoundTrip(t, "timestamp", OIDTimestamp,
		[]Null[time.Time]{Some(at), {}, Some(at.Add(time.Hour))},
		AppendNullableTimestampArray, DecodeNullableTimestampArray, DecodeTimestampArray,
		time.Time.Equal)
	nullableRoundTrip(t, "date", OIDDate,
		[]Null[time.Time]{Some(day), {}, Some(day.AddDate(0, 0, 1))},
		AppendNullableDateArray, DecodeNullableDateArray, DecodeDateArray,
		time.Time.Equal)
	nullableRoundTrip(t, "numeric", OIDNumeric,
		[]Null[Numeric]{Some(price), {}, Some(Numeric{Sign: NumericNaN})},
		AppendNullableNumericArray, DecodeNullableNumericArray, DecodeNumericArray,
		func(a, b Numeric) bool { return a.String() == b.String() })

	// An empty bytea element and a NULL one are both zero-length and must
	// not be confused: the first is a value, the second is not.
	got, err := DecodeNullableByteaArray(nil, AppendNullableByteaArray(nil, []Null[[]byte]{Some([]byte{}), {}}))
	if err != nil || len(got) != 2 || !got[0].Valid || got[1].Valid {
		t.Errorf("bytea empty-vs-NULL = %v, %v; want [valid empty, invalid]", got, err)
	}
	// What comes back is a copy, not a window on the wire buffer.
	raw := AppendNullableByteaArray(nil, []Null[[]byte]{Some([]byte{7})})
	got, _ = DecodeNullableByteaArray(nil, raw)
	raw[len(raw)-1] = 9
	if got[0].Value[0] != 7 {
		t.Error("a nullable bytea element aliased the wire buffer")
	}

	// The buffer survives a bad batch, like every other decoder's.
	keepsBuffer(t, "nullable float4", AppendNullableFloat4Array(nil, []Null[float32]{Some[float32](1), {}, Some[float32](2)}), DecodeNullableFloat4Array)
	keepsBuffer(t, "nullable bytea", AppendNullableByteaArray(nil, []Null[[]byte]{Some([]byte{1}), {}, Some([]byte{2})}), DecodeNullableByteaArray)
	keepsBuffer(t, "nullable timestamptz", AppendNullableTimestampTZArray(nil, []Null[time.Time]{Some(at), {}, Some(at)}), DecodeNullableTimestampTZArray)
	keepsBuffer(t, "nullable timestamp", AppendNullableTimestampArray(nil, []Null[time.Time]{Some(at), {}, Some(at)}), DecodeNullableTimestampArray)
	keepsBuffer(t, "nullable date", AppendNullableDateArray(nil, []Null[time.Time]{Some(day), {}, Some(day)}), DecodeNullableDateArray)
	keepsBuffer(t, "nullable numeric", AppendNullableNumericArray(nil, []Null[Numeric]{Some(price), {}, Some(price)}), DecodeNullableNumericArray)
}

func nullableRoundTrip[T any](t *testing.T, name string, elem uint32, in []Null[T],
	enc func([]byte, []Null[T]) []byte,
	dec func([]Null[T], []byte) ([]Null[T], error),
	plain func([]T, []byte) ([]T, error),
	eq func(a, b T) bool,
) {
	t.Helper()
	raw := enc(nil, in)
	if flags := binary.BigEndian.Uint32(raw[4:8]); flags != 1 {
		t.Errorf("%s: has-NULL flag = %d, want 1", name, flags)
	}
	if oid := binary.BigEndian.Uint32(raw[8:12]); oid != elem {
		t.Errorf("%s: element OID = %d, want %d", name, oid, elem)
	}
	got, err := dec(nil, raw)
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
	if _, err := plain(nil, raw); !errors.Is(err, ErrCodec) {
		t.Errorf("%s: the plain decoder over a NULL element = %v, want ErrCodec", name, err)
	}
	if got, err := dec([]Null[T]{{}}, enc(nil, nil)); err != nil || len(got) != 0 {
		t.Errorf("%s: empty = %v, %v", name, got, err)
	}
}
