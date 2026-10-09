package postgres

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

// The golden vectors are the point of this file, and they are written by hand
// from the protocol's specification rather than produced by the encoder they
// check.
//
// A round-trip test — encode, decode, compare — cannot catch a codec that is
// consistently wrong: the encoder and the decoder agree with each other about
// a mistake, and the property holds while every byte on the wire is
// unreadable to anyone else. That is not hypothetical. The field report this
// project keeps records a production pipeline whose version had to be bumped
// because a geometry codec truncated floats at six decimals: same input,
// different bytes, no error anywhere, caught two milestones late.
//
// So each vector below states what PostgreSQL's binary format actually is. If
// one of them disagrees with a real server, the vector is what gets corrected
// — but the disagreement is visible, which is the whole difference.
//
// One caveat learned the hard way, worth more than the vectors themselves: **a
// golden vector is only as good as the independence of its derivation.** Two
// of the values below were written from memory on the first pass and were
// simply wrong — an IEEE 754 mantissa and a microsecond count across
// twenty-four years are not numbers a person derives by eye, so what they
// recorded was a second guess rather than a reference. Both were then computed
// with a separate implementation and both times the encoder had been right.
// A vector that cannot be derived structurally must say where it came from,
// and the ones that can — 1.0's exponent, the epoch being zero, one day being
// 1, -1 being all-ones — are the ones worth trusting on sight.

func TestGoldenVectorsScalars(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		want []byte
	}{
		{"bool true", AppendBool(nil, true), []byte{1}},
		{"bool false", AppendBool(nil, false), []byte{0}},

		{"int2 zero", AppendInt2(nil, 0), []byte{0x00, 0x00}},
		{"int2 one", AppendInt2(nil, 1), []byte{0x00, 0x01}},
		{"int2 minus one", AppendInt2(nil, -1), []byte{0xFF, 0xFF}},
		{"int2 max", AppendInt2(nil, math.MaxInt16), []byte{0x7F, 0xFF}},
		{"int2 min", AppendInt2(nil, math.MinInt16), []byte{0x80, 0x00}},

		{"int4 one", AppendInt4(nil, 1), []byte{0, 0, 0, 1}},
		{"int4 minus one", AppendInt4(nil, -1), []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"int4 min", AppendInt4(nil, math.MinInt32), []byte{0x80, 0, 0, 0}},

		{"int8 one", AppendInt8(nil, 1), []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{"int8 minus one", AppendInt8(nil, -1), bytes.Repeat([]byte{0xFF}, 8)},
		{"int8 max", AppendInt8(nil, math.MaxInt64), []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},

		// IEEE 754: 1.0 is sign 0, exponent 127 biased, mantissa 0.
		{"float4 one", AppendFloat4(nil, 1), []byte{0x3F, 0x80, 0x00, 0x00}},
		{"float4 minus two", AppendFloat4(nil, -2), []byte{0xC0, 0x00, 0x00, 0x00}},
		{"float8 one", AppendFloat8(nil, 1), []byte{0x3F, 0xF0, 0, 0, 0, 0, 0, 0}},
		{"float8 half", AppendFloat8(nil, 0.5), []byte{0x3F, 0xE0, 0, 0, 0, 0, 0, 0}},
		// The value a text codec rounding at six decimals would destroy, which
		// is the failure these vectors exist for. Unlike the ones above, this
		// mantissa is not derivable by hand: it was computed with a separate
		// IEEE 754 implementation (Python's struct.pack('>d')), which is what
		// makes it a reference rather than a second guess.
		{"float8 many decimals", AppendFloat8(nil, 1.2345678901234567), []byte{0x3F, 0xF3, 0xC0, 0xCA, 0x42, 0x8C, 0x59, 0xFB}},

		{"text", AppendText(nil, "héllo"), []byte("h\xc3\xa9llo")},
		{"bytea", AppendBytea(nil, []byte{0, 1, 0xFF}), []byte{0, 1, 0xFF}},

		{
			"uuid", AppendUUID(nil, [16]byte{
				0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
				0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
			}),
			[]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Equal(tt.got, tt.want) {
				t.Errorf("encoded % x, want % x", tt.got, tt.want)
			}
		})
	}
}

// Timestamps count from 2000-01-01, not from the Unix epoch. Getting that
// constant wrong shifts every value by thirty years while every round-trip
// test still passes, which is precisely why these are stated as absolute
// numbers.
func TestGoldenVectorsTime(t *testing.T) {
	tests := []struct {
		name  string
		got   []byte
		want  []byte
		about string
	}{
		{
			"timestamptz at the postgres epoch",
			AppendTimestampTZ(nil, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
			AppendInt8(nil, 0),
			"2000-01-01 is zero",
		},
		{
			"timestamptz one second later",
			AppendTimestampTZ(nil, time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC)),
			AppendInt8(nil, 1_000_000),
			"microseconds, not milliseconds or nanoseconds",
		},
		{
			"timestamptz at the unix epoch",
			AppendTimestampTZ(nil, time.Unix(0, 0)),
			AppendInt8(nil, -946684800*1_000_000),
			"the unix epoch is 30 years before the postgres one, and negative",
		},
		{
			"timestamptz keeps microseconds",
			AppendTimestampTZ(nil, time.Date(2024, 3, 1, 12, 30, 45, 123456000, time.UTC)),
			// Also computed externally rather than by hand: a microsecond
			// count across twenty-four years is not a number to eyeball.
			AppendInt8(nil, 762611445123456),
			"sub-second precision survives",
		},
		{
			"date at the postgres epoch",
			AppendDate(nil, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
			AppendInt4(nil, 0),
			"days, counted from the same epoch",
		},
		{
			"date one day later",
			AppendDate(nil, time.Date(2000, 1, 2, 23, 59, 59, 0, time.UTC)),
			AppendInt4(nil, 1),
			"the time of day is dropped, not rounded",
		},
		{
			"date before the epoch",
			AppendDate(nil, time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)),
			AppendInt4(nil, -1),
			"dates before 2000 are negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Equal(tt.got, tt.want) {
				t.Errorf("encoded % x, want % x (%s)", tt.got, tt.want, tt.about)
			}
		})
	}
}

// A timestamp carrying a zone must encode the instant, not the wall clock
// reading: PostgreSQL stores an absolute point in time and applies a zone on
// the way out.
func TestTimestampTZIsZoneIndependent(t *testing.T) {
	utc := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	east := utc.In(time.FixedZone("UTC+2", 2*3600))

	if !bytes.Equal(AppendTimestampTZ(nil, utc), AppendTimestampTZ(nil, east)) {
		t.Error("the same instant encoded differently in two zones — the wall clock was sent instead of the instant")
	}
}

// The array header is what `= ANY($1)` rides on, and it is fixed enough to
// state byte for byte.
func TestGoldenVectorInt8Array(t *testing.T) {
	got := AppendInt8Array(nil, []int64{1, -1})
	want := []byte{
		0, 0, 0, 1, // one dimension
		0, 0, 0, 0, // no NULL bitmap
		0, 0, 0, 20, // element type: int8, OID 20
		0, 0, 0, 2, // two elements
		0, 0, 0, 1, // lower bound 1
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, // 1
		0, 0, 0, 8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // -1
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoded % x,\nwant     % x", got, want)
	}
}

func TestDecodeScalars(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		for _, tt := range []struct {
			b    []byte
			want bool
		}{{[]byte{0}, false}, {[]byte{1}, true}} {
			got, err := DecodeBool(tt.b)
			if err != nil || got != tt.want {
				t.Errorf("DecodeBool(% x) = %v, %v", tt.b, got, err)
			}
		}
		// A byte that is neither means the column is not a bool, however
		// tempting it is to read it as truthy.
		if _, err := DecodeBool([]byte{2}); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeBool(2) = %v, want ErrCodec", err)
		}
	})

	t.Run("integers", func(t *testing.T) {
		if v, err := DecodeInt2(AppendInt2(nil, -12345)); err != nil || v != -12345 {
			t.Errorf("int2 round trip = %v, %v", v, err)
		}
		if v, err := DecodeInt4(AppendInt4(nil, math.MinInt32)); err != nil || v != math.MinInt32 {
			t.Errorf("int4 round trip = %v, %v", v, err)
		}
		if v, err := DecodeInt8(AppendInt8(nil, math.MaxInt64)); err != nil || v != math.MaxInt64 {
			t.Errorf("int8 round trip = %v, %v", v, err)
		}
	})

	t.Run("floats keep every bit", func(t *testing.T) {
		const v = 1.2345678901234567
		got, err := DecodeFloat8(AppendFloat8(nil, v))
		if err != nil || got != v {
			t.Errorf("float8 round trip = %v, %v — precision was lost", got, err)
		}
		nan, err := DecodeFloat8(AppendFloat8(nil, math.NaN()))
		if err != nil || !math.IsNaN(nan) {
			t.Errorf("NaN did not survive: %v, %v", nan, err)
		}
		inf, err := DecodeFloat8(AppendFloat8(nil, math.Inf(1)))
		if err != nil || !math.IsInf(inf, 1) {
			t.Errorf("infinity did not survive: %v, %v", inf, err)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		want := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		got, err := DecodeUUID(AppendUUID(nil, want))
		if err != nil || got != want {
			t.Errorf("uuid round trip = %v, %v", got, err)
		}
	})
}

// Every decoder must check its width before reading: a length is what the
// server said, and a decoder that trusts it reads past its slice.
func TestDecodersRejectWrongWidth(t *testing.T) {
	tests := []struct {
		name string
		f    func([]byte) error
	}{
		{"bool", func(b []byte) error { _, err := DecodeBool(b); return err }},
		{"int2", func(b []byte) error { _, err := DecodeInt2(b); return err }},
		{"int4", func(b []byte) error { _, err := DecodeInt4(b); return err }},
		{"int8", func(b []byte) error { _, err := DecodeInt8(b); return err }},
		{"float4", func(b []byte) error { _, err := DecodeFloat4(b); return err }},
		{"float8", func(b []byte) error { _, err := DecodeFloat8(b); return err }},
		{"uuid", func(b []byte) error { _, err := DecodeUUID(b); return err }},
		{"timestamptz", func(b []byte) error { _, err := DecodeTimestampTZ(b); return err }},
		{"date", func(b []byte) error { _, err := DecodeDate(b); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, b := range [][]byte{nil, {}, make([]byte, 3), make([]byte, 17)} {
				if err := tt.f(b); !errors.Is(err, ErrCodec) {
					t.Errorf("%d bytes: got %v, want ErrCodec", len(b), err)
				}
			}
		})
	}
}

func TestDecodeTimeRoundTrip(t *testing.T) {
	instants := []time.Time{
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 1, 12, 30, 45, 123456000, time.UTC),
		time.Date(1900, 6, 15, 8, 0, 0, 0, time.UTC),
	}
	for _, want := range instants {
		got, err := DecodeTimestampTZ(AppendTimestampTZ(nil, want))
		if err != nil {
			t.Fatalf("%v: %v", want, err)
		}
		if !got.Equal(want) {
			t.Errorf("timestamptz round trip: got %v, want %v", got, want)
		}
	}

	for _, want := range instants {
		day := time.Date(want.Year(), want.Month(), want.Day(), 0, 0, 0, 0, time.UTC)
		got, err := DecodeDate(AppendDate(nil, want))
		if err != nil {
			t.Fatalf("%v: %v", want, err)
		}
		if !got.Equal(day) {
			t.Errorf("date round trip: got %v, want %v", got, day)
		}
	}
}

func TestInt8ArrayRoundTrip(t *testing.T) {
	tests := [][]int64{
		{},
		{0},
		{1, 2, 3},
		{math.MinInt64, -1, 0, 1, math.MaxInt64},
	}
	for _, want := range tests {
		got, err := DecodeInt8Array(nil, AppendInt8Array(nil, want))
		if err != nil {
			t.Fatalf("%v: %v", want, err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("array round trip: got %v, want %v", got, want)
		}
	}
}

// An empty array arrives as zero dimensions, not one dimension of length zero.
// A decoder that expected a dimension header would fail on the ordinary result
// of a query that matched nothing.
func TestDecodeEmptyArrayFromServerForm(t *testing.T) {
	serverEmpty := []byte{
		0, 0, 0, 0, // zero dimensions
		0, 0, 0, 0, // no NULL bitmap
		0, 0, 0, 20, // element type int8
	}
	got, err := DecodeInt8Array(nil, serverEmpty)
	if err != nil {
		t.Fatalf("decoding the server's empty array: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want an empty slice", got)
	}
}

func TestDecodeInt8ArrayRejectsMalformed(t *testing.T) {
	valid := AppendInt8Array(nil, []int64{1, 2})
	tests := []struct {
		name string
		b    []byte
	}{
		{"header too short", valid[:8]},
		{"wrong element type", func() []byte {
			b := slices.Clone(valid)
			b[11] = 23 // int4 instead of int8
			return b
		}()},
		{"two dimensions", func() []byte {
			b := slices.Clone(valid)
			b[3] = 2
			return b
		}()},
		{"element shorter than declared", valid[:len(valid)-4]},
		{"trailing bytes", append(slices.Clone(valid), 0)},
		{"null element", func() []byte {
			b := AppendInt8Array(nil, []int64{1})
			// Rewrite the element's length as -1.
			copy(b[arrayHeaderSize:], []byte{0xFF, 0xFF, 0xFF, 0xFF})
			return b[:arrayHeaderSize+4]
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeInt8Array(nil, tt.b); !errors.Is(err, ErrCodec) {
				t.Errorf("got %v, want ErrCodec", err)
			}
		})
	}
}

// The reuse the batch model rests on: encoding a batch of parameters into one
// buffer, and decoding an array into a caller's slice, must not allocate per
// value once the buffers have grown.
func TestCodecsReuseBuffers(t *testing.T) {
	buf := make([]byte, 0, 1024)
	for range 3 {
		buf = buf[:0]
		for i := range 64 {
			buf = AppendInt8(buf, int64(i))
		}
	}
	if cap(buf) != 1024 {
		t.Errorf("the parameter buffer regrew to %d — encoding allocated per value", cap(buf))
	}

	dst := make([]int64, 0, 64)
	wire := AppendInt8Array(nil, make([]int64, 64))
	for range 3 {
		var err error
		dst, err = DecodeInt8Array(dst, wire)
		if err != nil {
			t.Fatal(err)
		}
	}
	if cap(dst) != 64 {
		t.Errorf("the destination slice regrew to %d — decoding allocated per value", cap(dst))
	}
}

// The three array codecs share a header, so one golden vector for it plus a
// round trip each is what covers them: the element encoding is the scalar
// codec already checked above.
func TestGoldenVectorInt4Array(t *testing.T) {
	got := AppendInt4Array(nil, []int32{7, -1})
	want := []byte{
		0, 0, 0, 1, // one dimension
		0, 0, 0, 0, // no NULL bitmap
		0, 0, 0, 23, // element type: int4, OID 23
		0, 0, 0, 2, // two elements
		0, 0, 0, 1, // lower bound 1
		0, 0, 0, 4, 0, 0, 0, 7,
		0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF,
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoded % x,\nwant     % x", got, want)
	}
}

func TestGoldenVectorTextArray(t *testing.T) {
	got := AppendTextArray(nil, []string{"a", ""})
	want := []byte{
		0, 0, 0, 1,
		0, 0, 0, 0,
		0, 0, 0, 25, // element type: text, OID 25
		0, 0, 0, 2,
		0, 0, 0, 1,
		0, 0, 0, 1, 'a',
		0, 0, 0, 0, // the empty string, which is not NULL
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoded % x,\nwant     % x", got, want)
	}
}

func TestInt4AndTextArrayRoundTrip(t *testing.T) {
	t.Run("int4", func(t *testing.T) {
		for _, want := range [][]int32{{}, {0}, {1, -1, math.MaxInt32, math.MinInt32}} {
			got, err := DecodeInt4Array(nil, AppendInt4Array(nil, want))
			if err != nil {
				t.Fatalf("%v: %v", want, err)
			}
			if !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	})
	t.Run("text", func(t *testing.T) {
		for _, want := range [][]string{{}, {""}, {"a", "héllo", "with\x00nul"}} {
			got, err := DecodeTextArray(nil, AppendTextArray(nil, want))
			if err != nil {
				t.Fatalf("%v: %v", want, err)
			}
			if !slices.Equal(got, want) {
				t.Errorf("got %q, want %q", got, want)
			}
		}
	})
}

// An array decoded as the wrong element type is refused rather than
// reinterpreted: int4 and int8 both decode from four or eight bytes, and
// guessing would produce plausible numbers that are not the stored ones.
func TestArrayDecodersCheckElementType(t *testing.T) {
	ints := AppendInt8Array(nil, []int64{1})
	if _, err := DecodeInt4Array(nil, ints); !errors.Is(err, ErrCodec) {
		t.Errorf("an int8[] decoded as int4[] gave %v, want ErrCodec", err)
	}
	if _, err := DecodeTextArray(nil, ints); !errors.Is(err, ErrCodec) {
		t.Errorf("an int8[] decoded as text[] gave %v, want ErrCodec", err)
	}
	texts := AppendTextArray(nil, []string{"x"})
	if _, err := DecodeInt8Array(nil, texts); !errors.Is(err, ErrCodec) {
		t.Errorf("a text[] decoded as int8[] gave %v, want ErrCodec", err)
	}
}

// A NULL element has nowhere to go in a slice of values, so it is refused
// rather than turned into a zero.
func TestArrayDecodersRejectNullElements(t *testing.T) {
	withNull := func(elem uint32) []byte {
		b := []byte{0, 0, 0, 1, 0, 0, 0, 1}
		b = binary.BigEndian.AppendUint32(b, elem)
		b = binary.BigEndian.AppendUint32(b, 1)
		b = binary.BigEndian.AppendUint32(b, 1)
		return binary.BigEndian.AppendUint32(b, ^uint32(0))
	}
	if _, err := DecodeInt4Array(nil, withNull(OIDInt4)); !errors.Is(err, ErrCodec) {
		t.Errorf("int4[]: got %v, want ErrCodec", err)
	}
	if _, err := DecodeTextArray(nil, withNull(OIDText)); !errors.Is(err, ErrCodec) {
		t.Errorf("text[]: got %v, want ErrCodec", err)
	}
}

// PostgreSQL's infinity and -infinity are the extreme int64 values, and the
// branches decoding them were written and never exercised — which is worse
// than an untested line, because a decoder that mishandles them turns a row
// the server was happy to store into an error or a date in 1954.
func TestDecodeTimestampTZInfinities(t *testing.T) {
	pos, err := DecodeTimestampTZ(AppendInt8(nil, math.MaxInt64))
	if err != nil {
		t.Fatalf("infinity: %v", err)
	}
	if pos.Year() < 10000 {
		t.Errorf("infinity decoded to %v, want a time far past any real one", pos)
	}

	neg, err := DecodeTimestampTZ(AppendInt8(nil, math.MinInt64))
	if err != nil {
		t.Fatalf("-infinity: %v", err)
	}
	if !neg.Equal(NegInfinityTime()) {
		t.Errorf("-infinity decoded to %v, want NegInfinityTime()", neg)
	}
	if !pos.Equal(InfinityTime()) {
		t.Errorf("infinity decoded to %v, want InfinityTime()", pos)
	}

	// The two must not collide with an ordinary value near them.
	ordinary, err := DecodeTimestampTZ(AppendInt8(nil, math.MaxInt64-1))
	if err != nil {
		t.Fatalf("a value next to infinity: %v", err)
	}
	if ordinary.Equal(pos) {
		t.Error("a value one microsecond below infinity decoded as infinity")
	}
}

// The float4 decoder's own error branch, and its NaN handling, which the
// float8 tests covered and this one did not.
func TestDecodeFloat4EdgeCases(t *testing.T) {
	if _, err := DecodeFloat4([]byte{1, 2, 3}); !errors.Is(err, ErrCodec) {
		t.Errorf("three bytes: got %v, want ErrCodec", err)
	}
	nan, err := DecodeFloat4(AppendFloat4(nil, float32(math.NaN())))
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(float64(nan)) {
		t.Errorf("NaN decoded as %v", nan)
	}
	inf, err := DecodeFloat4(AppendFloat4(nil, float32(math.Inf(-1))))
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsInf(float64(inf), -1) {
		t.Errorf("-Inf decoded as %v", inf)
	}
}

// The array header validator's remaining refusals, each of which stands
// between a misread stream and a plausible wrong answer.
func TestArrayHeaderRefusals(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
	}{
		{"header far too short", []byte{0, 0, 0}},
		{"one dimension but no dimension header", []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20}},
		{"negative element count", func() []byte {
			b := AppendInt8Array(nil, []int64{1})
			copy(b[12:16], []byte{0xFF, 0xFF, 0xFF, 0xFF})
			return b
		}()},
		{"three dimensions", func() []byte {
			b := AppendInt8Array(nil, []int64{1})
			b[3] = 3
			return b
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeInt8Array(nil, tt.b); !errors.Is(err, ErrCodec) {
				t.Errorf("got %v, want ErrCodec", err)
			}
		})
	}
}

// # timestamp without time zone

func TestTimestampAgainstServerVectors(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		hex  string
	}{
		{"the epoch itself", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), "0000000000000000"},
		{"a reading with microseconds", time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC), "0002b5975f49e614"},
		{"one second before the epoch", time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC), "fffffffffff0bdc0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := mustHex(t, tc.hex)
			if got := AppendTimestamp(nil, tc.t); !bytes.Equal(got, want) {
				t.Errorf("AppendTimestamp = %x, want %x", got, want)
			}
			back, err := DecodeTimestamp(want)
			if err != nil {
				t.Fatalf("DecodeTimestamp: %v", err)
			}
			if !back.Equal(tc.t) {
				t.Errorf("DecodeTimestamp = %v, want %v", back, tc.t)
			}
		})
	}
}

// A timestamp has no zone, so the encoder must read the clock the caller
// wrote and not convert it. A value built in a zone eight hours from UTC must
// produce the same bytes as the same reading built in UTC — otherwise the
// server stores an hour nobody asked for.
func TestTimestampIgnoresTheLocation(t *testing.T) {
	somewhere := time.FixedZone("UTC+8", 8*3600)
	inZone := time.Date(2024, 3, 1, 12, 34, 56, 789012000, somewhere)
	inUTC := time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)

	if got, want := AppendTimestamp(nil, inZone), AppendTimestamp(nil, inUTC); !bytes.Equal(got, want) {
		t.Errorf("the same clock reading in two zones encoded differently: %x vs %x", got, want)
	}
	// And it is emphatically not the same as timestamptz, which does convert.
	if bytes.Equal(AppendTimestampTZ(nil, inZone), AppendTimestamp(nil, inZone)) {
		t.Error("timestamp and timestamptz encoded a zoned reading identically; one of them is wrong")
	}
}

func TestDecodeTimestampRefusesTheWrongWidth(t *testing.T) {
	if _, err := DecodeTimestamp(make([]byte, 4)); err == nil {
		t.Error("DecodeTimestamp accepted four bytes")
	}
}

// # uuid[]

func TestUUIDArrayAgainstServerVector(t *testing.T) {
	// ARRAY['00000000-...-0001', 'ffffffff-...-ffff']::uuid[]
	want := mustHex(t, "000000010000000000000b86000000020000000100000010"+
		"00000000000000000000000000000001"+
		"00000010ffffffffffffffffffffffffffffffff")

	values := [][16]byte{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
	}
	if got := AppendUUIDArray(nil, values); !bytes.Equal(got, want) {
		t.Errorf("AppendUUIDArray = %x,\n                want %x", got, want)
	}
	back, err := DecodeUUIDArray(nil, want)
	if err != nil {
		t.Fatalf("DecodeUUIDArray: %v", err)
	}
	if len(back) != 2 || back[0] != values[0] || back[1] != values[1] {
		t.Errorf("DecodeUUIDArray = %x, want %x", back, values)
	}
}

func TestDecodeUUIDArrayRefusals(t *testing.T) {
	// An empty array is zero dimensions, which is the ordinary result of a
	// query that matched nothing rather than an error.
	empty, err := DecodeUUIDArray(nil, AppendUUIDArray(nil, nil))
	if err != nil || len(empty) != 0 {
		t.Errorf("DecodeUUIDArray(empty) = %v, %v; want an empty result", empty, err)
	}
	// Empty still names its element type, and the wrong one is refused: the
	// type check must not depend on whether the array holds anything.
	if _, err := DecodeUUIDArray(nil, AppendInt8Array(nil, nil)); !errors.Is(err, ErrCodec) {
		t.Errorf("DecodeUUIDArray accepted an empty int8[]: %v", err)
	}
	// An array of the wrong element type is a column read as something it is
	// not, and every value after it would be plausible nonsense.
	wrong := AppendInt8Array(nil, []int64{1})
	if _, err := DecodeUUIDArray(nil, wrong); err == nil {
		t.Error("DecodeUUIDArray accepted an int8[]")
	}
}

// The range PostgreSQL actually holds, which is very nearly the whole int64
// microsecond span — and far wider than a time.Duration.
//
// This is the regression these vectors exist for: the decoder used to reach
// the result through `pgEpoch.Add(time.Duration(micros) * time.Microsecond)`,
// and a Duration counts nanoseconds in an int64, so it spans about ±292 years.
// Multiplying by 1000 wrapped rather than saturating, and 2300-01-01 came back
// as 1715-06-13 with no error at all. The encoder had guarded against exactly
// this since it was written; the decoder had not, which is how a guard gets
// written once for a round trip that needs it twice.
//
// Vectors from the server: SELECT encode(timestamp_send(...), 'hex').
func TestTimestampSurvivesTheWholeRange(t *testing.T) {
	tests := []struct {
		sql string
		hex string
		t   time.Time
	}{
		{"1700-01-01", "ffde5dcb74228000", time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC)},
		{
			// A microsecond before the epoch. Every other value here lands on
			// a whole second, so the branch that carries a negative remainder
			// into the previous second — Go truncates division toward zero —
			// was never reached by any of them.
			"one microsecond before the epoch", "ffffffffffffffff",
			time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC),
		},
		{
			"a sub-second reading well before the epoch", "fffc96188bb04340",
			time.Date(1969, 7, 20, 20, 17, 40, 123456000, time.UTC),
		},
		{
			"one microsecond after a whole second before the epoch", "fffffffffff0bdc1",
			time.Date(1999, 12, 31, 23, 59, 59, 1000, time.UTC),
		},
		{"2300-01-01", "0021a248a9b4e000", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"3000-01-01", "00701ceb81132000", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{
			// The type's own upper bound.
			"294276-12-31 23:59:59.999999", "7fffff5bb3b29fff",
			time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			want := mustHex(t, tc.hex)

			back, err := DecodeTimestamp(want)
			if err != nil {
				t.Fatalf("DecodeTimestamp: %v", err)
			}
			if !back.Equal(tc.t) {
				t.Errorf("DecodeTimestamp = %v, want %v", back, tc.t)
			}
			if got := AppendTimestamp(nil, tc.t); !bytes.Equal(got, want) {
				t.Errorf("AppendTimestamp = %x, want %x", got, want)
			}
			// timestamptz shares the eight bytes, and shared the defect.
			tz, err := DecodeTimestampTZ(want)
			if err != nil {
				t.Fatalf("DecodeTimestampTZ: %v", err)
			}
			if !tz.Equal(tc.t) {
				t.Errorf("DecodeTimestampTZ = %v, want %v", tz, tc.t)
			}
		})
	}
}

// An array the server numbers from something other than 1.
//
// `'[5:7]={10,20,30}'::bigint[]` is legal, and `arr[5]` is its first value.
// These decoders return a Go slice, numbered from zero, so accepting it would
// hand back every index shifted by four with nothing to say so — a caller
// computing a position from the result would be wrong by a constant it cannot
// see. Refused for the same reason a NULL element is: there is nowhere honest
// to put it.
//
// Vector from the server: SELECT encode(array_send('[5:7]={10,20,30}'::bigint[]), 'hex').
func TestArrayRefusesANonStandardLowerBound(t *testing.T) {
	shifted := mustHex(t, "000000010000000000000014000000030000000500000008000000000000000a"+
		"00000008000000000000001400000008000000000000001e")
	ordinary := mustHex(t, "000000010000000000000014000000030000000100000008000000000000000a"+
		"00000008000000000000001400000008000000000000001e")

	// The two differ in one word, which is the point: everything else about
	// them is identical, so nothing but this check separates them.
	got, err := DecodeInt8Array(nil, ordinary)
	if err != nil {
		t.Fatalf("the ordinary array was refused: %v", err)
	}
	if !slices.Equal(got, []int64{10, 20, 30}) {
		t.Fatalf("ordinary array = %v, want [10 20 30]", got)
	}

	_, err = DecodeInt8Array(nil, shifted)
	if err == nil {
		t.Fatal("DecodeInt8Array accepted an array numbered from 5")
	}
	if !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want it to wrap ErrCodec", err)
	}
	if !strings.Contains(err.Error(), "numbered from 5") {
		t.Errorf("error = %v, want it to name the lower bound", err)
	}

	// Every array decoder shares the header rules, so every one refuses it.
	if _, err := DecodeInt4Array(nil, shifted); err == nil {
		t.Error("DecodeInt4Array accepted it")
	}
	if _, err := DecodeTextArray(nil, shifted); err == nil {
		t.Error("DecodeTextArray accepted it")
	}
	if _, err := DecodeUUIDArray(nil, shifted); err == nil {
		t.Error("DecodeUUIDArray accepted it")
	}
}

// An empty array is zero dimensions, and the encoder must say so.
//
// The decoders here have always known it — `arrayBody` returns the empty
// result for ndim 0 and its doc says the server sends it that way — while the
// encoders wrote a one-dimensional header with a count of zero. Twenty bytes
// where the server sends twelve, accepted by the server and produced by
// nothing. The same shape as every other defect found in this package: a rule
// written once for a conversion that needs it on both sides.
func TestEmptyArrayIsZeroDimensions(t *testing.T) {
	// From the server: SELECT encode(array_send(ARRAY[]::bigint[]), 'hex').
	want := mustHex(t, "000000000000000000000014")

	if got := AppendInt8Array(nil, nil); !bytes.Equal(got, want) {
		t.Errorf("AppendInt8Array(nil) = %x, want %x", got, want)
	}
	if got := AppendInt8Array(nil, []int64{}); !bytes.Equal(got, want) {
		t.Errorf("AppendInt8Array(empty) = %x, want %x", got, want)
	}

	// And it decodes back to nothing, which is the ordinary result of a query
	// that matched nothing rather than an error.
	back, err := DecodeInt8Array(nil, want)
	if err != nil {
		t.Fatalf("DecodeInt8Array: %v", err)
	}
	if len(back) != 0 {
		t.Errorf("DecodeInt8Array = %v, want empty", back)
	}

	// Every array encoder shares the header, so every one produces twelve
	// bytes for an empty list.
	for name, got := range map[string][]byte{
		"int4[]": AppendInt4Array(nil, nil),
		"text[]": AppendTextArray(nil, nil),
		"uuid[]": AppendUUIDArray(nil, nil),
	} {
		if len(got) != 12 {
			t.Errorf("Append%s(nil) is %d bytes, want 12", name, len(got))
		}
		if binary.BigEndian.Uint32(got[:4]) != 0 {
			t.Errorf("Append%s(nil) declares %d dimensions, want 0", name, binary.BigEndian.Uint32(got[:4]))
		}
	}
}

// A date is the calendar day t reads in its own location, as a timestamp is
// its clock reading: midnight in Paris on 2024-01-01 is that date, not the
// UTC date of the same instant, which is the day before.
func TestAppendDateUsesTheCivilDateInItsZone(t *testing.T) {
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	tokyo := time.FixedZone("UTC+9", 9*3600)
	honolulu := time.FixedZone("UTC-10", -10*3600)
	want := AppendDate(nil, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	for _, in := range []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, paris),
		time.Date(2024, 1, 1, 0, 30, 0, 0, tokyo),
		time.Date(2024, 1, 1, 23, 30, 0, 0, honolulu),
	} {
		if got := AppendDate(nil, in); !bytes.Equal(got, want) {
			d, _ := DecodeDate(got)
			t.Errorf("AppendDate(%v) = %v, want 2024-01-01", in, d.Format(time.DateOnly))
		}
	}
	arr := AppendDateArray(nil, []time.Time{time.Date(2024, 1, 1, 0, 0, 0, 0, paris)})
	back, err := DecodeDateArray(nil, arr)
	if err != nil || len(back) != 1 || back[0].Format(time.DateOnly) != "2024-01-01" {
		t.Errorf("AppendDateArray in Paris = %v, %v, want [2024-01-01]", back, err)
	}
}

// The infinities survive decode then encode for every type that has them,
// and a value outside the wire range saturates outside PostgreSQL's range
// rather than wrapping into it or becoming an infinity.
func TestInfinitiesRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		dec  func([]byte) (time.Time, error)
		enc  func([]byte, time.Time) []byte
		want time.Time
	}{
		{"timestamptz infinity", AppendInt8(nil, math.MaxInt64), DecodeTimestampTZ, AppendTimestampTZ, InfinityTime()},
		{"timestamptz -infinity", AppendInt8(nil, math.MinInt64), DecodeTimestampTZ, AppendTimestampTZ, NegInfinityTime()},
		{"timestamp infinity", AppendInt8(nil, math.MaxInt64), DecodeTimestamp, AppendTimestamp, InfinityTime()},
		{"timestamp -infinity", AppendInt8(nil, math.MinInt64), DecodeTimestamp, AppendTimestamp, NegInfinityTime()},
		{"date infinity", AppendInt4(nil, math.MaxInt32), DecodeDate, AppendDate, InfinityTime()},
		{"date -infinity", AppendInt4(nil, math.MinInt32), DecodeDate, AppendDate, NegInfinityTime()},
	} {
		got, err := tc.dec(tc.wire)
		if err != nil || !got.Equal(tc.want) {
			t.Errorf("%s: decoded %v, %v; want %v", tc.name, got, err, tc.want)
			continue
		}
		if back := tc.enc(nil, got); !bytes.Equal(back, tc.wire) {
			t.Errorf("%s: re-encoded %x, want %x", tc.name, back, tc.wire)
		}
		// Shown in another zone, a sentinel is still the same instant.
		if back := tc.enc(nil, got.In(time.FixedZone("UTC+5", 5*3600))); !bytes.Equal(back, tc.wire) {
			t.Errorf("%s in another zone: re-encoded %x, want %x", tc.name, back, tc.wire)
		}
	}

	// Past the wire range: saturated one short of the infinities.
	far := time.Date(5_000_000, 1, 1, 0, 0, 0, 0, time.UTC)
	if got, _ := DecodeInt8(AppendTimestampTZ(nil, far)); got != math.MaxInt64-1 {
		t.Errorf("timestamptz in year 5,000,000 = %d, want MaxInt64-1", got)
	}
	if got, _ := DecodeInt8(AppendTimestamp(nil, far.AddDate(-10_000_000, 0, 0))); got != math.MinInt64+1 {
		t.Errorf("timestamp in year -5,000,000 = %d, want MinInt64+1", got)
	}
	if got, _ := DecodeInt4(AppendDate(nil, far.AddDate(4_000_000, 0, 0))); got != math.MaxInt32-1 {
		t.Errorf("date in year 9,000,000 = %d, want MaxInt32-1", got)
	}
	// The edges of the int64 range are still exact.
	for _, micros := range []int64{math.MaxInt64 - 1, math.MinInt64 + 1, math.MinInt64 + 224192} {
		ts, err := DecodeTimestampTZ(AppendInt8(nil, micros))
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := DecodeInt8(AppendTimestampTZ(nil, ts)); got != micros {
			t.Errorf("edge %d re-encoded as %d", micros, got)
		}
	}
}

// The flags word is 0 or 1 — "has NULLs" — on everything the server writes,
// and array_recv refuses anything else. A decoder that tolerated other values
// would believe the rest of a header it is plainly reading at the wrong place.
func TestArrayRefusesUnknownFlags(t *testing.T) {
	for _, flags := range []string{"00000002", "80000000", "ffffffff"} {
		full := mustHex(t, "00000001"+flags+"000000140000000100000001"+"00000008000000000000000a")
		empty := mustHex(t, "00000000"+flags+"00000014")
		for name, b := range map[string][]byte{"one element": full, "empty": empty} {
			if _, err := DecodeInt8Array(nil, b); !errors.Is(err, ErrCodec) {
				t.Errorf("flags %s, %s: error = %v, want ErrCodec", flags, name, err)
			}
			if _, err := DecodeNullableInt8Array(nil, b); !errors.Is(err, ErrCodec) {
				t.Errorf("flags %s, %s (nullable): error = %v, want ErrCodec", flags, name, err)
			}
		}
	}
	// Both values the server writes still decode.
	for _, flags := range []string{"00000000", "00000001"} {
		b := mustHex(t, "00000001"+flags+"000000140000000100000001"+"00000008000000000000000a")
		if got, err := DecodeInt8Array(nil, b); err != nil || !slices.Equal(got, []int64{10}) {
			t.Errorf("flags %s: %v, %v; want [10]", flags, got, err)
		}
	}
}
