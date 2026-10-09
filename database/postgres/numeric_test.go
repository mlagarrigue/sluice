package postgres

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The vectors below are hand-derivable from the format, which is what the
// earlier lesson says a golden vector must be to be worth having: ndigits,
// weight, sign and scale as four int16, then the base-10000 digits.
//
// -0.01 is the one to read if only one is read. The integer part is empty, so
// there are no integer groups and the weight is -1 (0xffff); the sign is
// 0x4000; the scale is 2; and the single digit is the fraction "01" padded
// right to "0100" — one hundred, 0x0064. Every byte of it is accounted for by
// the description rather than by this package's own encoder.
func TestGoldenVectorsNumeric(t *testing.T) {
	tests := []struct {
		in   string
		want []byte
	}{
		{"0", []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"1", []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"1.5", []byte{0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0x13, 0x88}},
		{"12345.678", []byte{0, 3, 0, 1, 0, 0, 0, 3, 0, 1, 0x09, 0x29, 0x1a, 0x7c}},
		{"-0.01", []byte{0, 1, 0xff, 0xff, 0x40, 0x00, 0, 2, 0x00, 0x64}},
		{"10.00", []byte{0, 1, 0, 0, 0, 0, 0, 2, 0x00, 0x0a}},
		{"99999999.99", []byte{0, 3, 0, 1, 0, 0, 0, 2, 0x27, 0x0f, 0x27, 0x0f, 0x26, 0xac}},
		{"NaN", []byte{0, 0, 0, 0, 0xC0, 0x00, 0, 0}},
		// These two describe what ParseNumeric produces, which is not quite
		// what the server sends: PostgreSQL writes dscale = 32 for an
		// infinity, not 0 (see TestNumericSpecialValuesAgainstServerVectors).
		// Both are accepted and compare equal, so the divergence is harmless
		// — but it is a divergence, and this vector was written believing it
		// was the server's. It is the third hand-derived vector in this
		// repository to be wrong about something, and the first to pass its
		// test anyway: it compares ParseNumeric against AppendNumeric, and
		// they share the belief.
		{"Infinity", []byte{0, 0, 0, 0, 0xD0, 0x00, 0, 0}},
		{"-Infinity", []byte{0, 0, 0, 0, 0xF0, 0x00, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			n, err := ParseNumeric(tt.in)
			if err != nil {
				t.Fatalf("ParseNumeric(%q) returned %v", tt.in, err)
			}
			if got := AppendNumeric(nil, n); !bytes.Equal(got, tt.want) {
				t.Errorf("encoded % x, want % x", got, tt.want)
			}
		})
	}
}

// The property the type exists for: a value goes in as text and comes back as
// the same text, through the wire, with nothing rounded on the way.
func TestNumericRoundTripIsExact(t *testing.T) {
	values := []string{
		"0", "1", "-1", "1.5", "0.1", "-0.01", "10.00", "12345.678",
		"99999999.99", "0.000000000000000000001",
		// Beyond what float64 can represent, which is the whole point: a
		// float64 has about seventeen significant digits and this has
		// thirty-eight.
		"12345678901234567890.12345678901234567890",
		"-99999999999999999999999999999999999999",
		"NaN", "Infinity", "-Infinity",
	}
	for _, want := range values {
		t.Run(want, func(t *testing.T) {
			n, err := ParseNumeric(want)
			if err != nil {
				t.Fatalf("ParseNumeric returned %v", err)
			}
			back, err := DecodeNumeric(AppendNumeric(nil, n))
			if err != nil {
				t.Fatalf("DecodeNumeric returned %v", err)
			}
			if got := back.String(); got != want {
				t.Errorf("round trip gave %q, want %q", got, want)
			}
		})
	}
}

// The scale is data, not decoration: numeric(12,2) holding ten is "10.00", and
// a type that dropped the trailing zeros would be describing a different
// value than the schema declared.
func TestNumericKeepsItsScale(t *testing.T) {
	tests := []struct{ in, want string }{
		{"10", "10"},
		{"10.0", "10.0"},
		{"10.00", "10.00"},
		{"0.00", "0.00"},
		{"-0.0", "-0.0"},
	}
	for _, tt := range tests {
		n, err := ParseNumeric(tt.in)
		if err != nil {
			t.Fatalf("ParseNumeric(%q): %v", tt.in, err)
		}
		if got := n.String(); got != tt.want {
			t.Errorf("%q rendered as %q, want %q", tt.in, got, tt.want)
		}
	}
}

// Float64 is the one lossy way out, and its name is the warning. The test
// states the loss rather than hiding it.
func TestNumericFloat64IsNamedAndLossy(t *testing.T) {
	exact := "12345678901234567890.12345678901234567890"
	n, err := ParseNumeric(exact)
	if err != nil {
		t.Fatal(err)
	}
	if n.String() != exact {
		t.Fatalf("the value itself was not exact: %q", n.String())
	}
	if f := n.Float64(); f == 0 || math.IsInf(f, 0) {
		t.Errorf("Float64 = %v, want a finite approximation", f)
	}
	// The approximation is not the value, which is the reason the conversion
	// has to be asked for.
	if got := strconv.FormatFloat(n.Float64(), 'f', -1, 64); got == exact {
		t.Error("float64 held all thirty-eight digits, which it cannot")
	}

	// The special values convert to their float counterparts.
	nan, _ := ParseNumeric("NaN")
	if !math.IsNaN(nan.Float64()) {
		t.Error("NaN did not convert to NaN")
	}
	pos, _ := ParseNumeric("Infinity")
	if !math.IsInf(pos.Float64(), 1) {
		t.Error("Infinity did not convert to +Inf")
	}
	neg, _ := ParseNumeric("-Infinity")
	if !math.IsInf(neg.Float64(), -1) {
		t.Error("-Infinity did not convert to -Inf")
	}
}

func TestNumericRejectsMalformed(t *testing.T) {
	tests := []string{"", ".", "abc", "1.2.3", "1e5", "- 1", "1,5", "０"}
	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			if _, err := ParseNumeric(in); !errors.Is(err, ErrCodec) {
				t.Errorf("ParseNumeric(%q) = %v, want ErrCodec", in, err)
			}
		})
	}
}

func TestDecodeNumericRejectsMalformed(t *testing.T) {
	valid := AppendNumeric(nil, mustParse(t, "12345.678"))
	tests := []struct {
		name string
		b    []byte
	}{
		{"header too short", valid[:6]},
		{"fewer digits than declared", valid[:len(valid)-2]},
		{"more bytes than declared", append(bytes.Clone(valid), 0, 0)},
		{"unknown sign", []byte{0, 0, 0, 0, 0x11, 0x11, 0, 0}},
		{"digit out of range", []byte{0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x11}}, // 10001
		{"NaN carrying digits", []byte{0, 1, 0, 0, 0xC0, 0x00, 0, 0, 0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeNumeric(tt.b); !errors.Is(err, ErrCodec) {
				t.Errorf("got %v, want ErrCodec", err)
			}
		})
	}
}

func TestNumericIsNaNAndIsInf(t *testing.T) {
	nan := mustParse(t, "NaN")
	if !nan.IsNaN() {
		t.Error("NaN does not report itself")
	}
	if inf, _ := nan.IsInf(); inf {
		t.Error("NaN reported itself as infinite")
	}
	pos := mustParse(t, "Infinity")
	if inf, positive := pos.IsInf(); !inf || !positive {
		t.Errorf("Infinity: inf %v, positive %v", inf, positive)
	}
	neg := mustParse(t, "-Infinity")
	if inf, positive := neg.IsInf(); !inf || positive {
		t.Errorf("-Infinity: inf %v, positive %v", inf, positive)
	}
	ordinary := mustParse(t, "1.5")
	if ordinary.IsNaN() {
		t.Error("1.5 reported itself as NaN")
	}
}

// The zero value is a valid zero, so a struct field that was never set decodes
// and renders rather than failing.
func TestNumericZeroValue(t *testing.T) {
	var n Numeric
	if got := n.String(); got != "0" {
		t.Errorf("the zero Numeric renders as %q, want \"0\"", got)
	}
	back, err := DecodeNumeric(AppendNumeric(nil, n))
	if err != nil {
		t.Fatalf("round-tripping the zero value: %v", err)
	}
	if back.String() != "0" {
		t.Errorf("round trip gave %q", back.String())
	}
}

func mustParse(t *testing.T, s string) Numeric {
	t.Helper()
	n, err := ParseNumeric(s)
	if err != nil {
		t.Fatalf("ParseNumeric(%q): %v", s, err)
	}
	return n
}

// The three values that are not numbers, against the server's own bytes.
//
// The surprise here is dscale, and it is the reason this test exists rather
// than a comment: PostgreSQL sends **32** for both infinities and **0** for
// NaN. Zeroing it looks like normalisation and refusing it looks like
// tightening; both were considered, and the second would have rejected every
// infinity a real server sends. The bytes decided.
//
// Vectors: SELECT encode(numeric_send('NaN'::numeric), 'hex'), and so on.
func TestNumericSpecialValuesAgainstServerVectors(t *testing.T) {
	tests := []struct {
		sql string
		hex string
		n   Numeric
	}{
		{"NaN", "00000000c0000000", Numeric{Sign: NumericNaN}},
		{"Infinity", "00000000d0000020", Numeric{Sign: NumericPosInf, Dscale: 32}},
		{"-Infinity", "00000000f0000020", Numeric{Sign: NumericNegInf, Dscale: 32}},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			want, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeNumeric(want)
			if err != nil {
				t.Fatalf("DecodeNumeric: %v", err)
			}
			if got.Sign != tc.n.Sign || got.Dscale != tc.n.Dscale || len(got.Digits) != 0 {
				t.Errorf("DecodeNumeric = %+v, want %+v", got, tc.n)
			}
			// And back out as the server sent it. An encoder that normalised
			// dscale away would make a value read from a column and written
			// back differ from what arrived.
			if back := AppendNumeric(nil, got); !bytes.Equal(back, want) {
				t.Errorf("AppendNumeric = %x, want %x", back, want)
			}
		})
	}
}

// A value that is nothing but a header must be exactly a header.
//
// This branch returned before the length check below it, so it accepted any
// amount of trailing padding — the one decoder in this package that took a
// prefix it found agreeable and ignored the rest, while its neighbours all
// report "N bytes after the last field". Found by fuzzing the round trip.
func TestNumericSpecialValueRefusesTrailingBytes(t *testing.T) {
	nan, err := hex.DecodeString("00000000c0000000")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNumeric(nan); err != nil {
		t.Fatalf("the exact header was refused: %v", err)
	}

	padded := append(slices.Clone(nan), 0x30)
	got, err := DecodeNumeric(padded)
	if err == nil {
		t.Fatalf("DecodeNumeric accepted %x, returning %+v", padded, got)
	}
	if !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want it to wrap ErrCodec", err)
	}
	if !strings.Contains(err.Error(), "9 bytes, want 8") {
		t.Errorf("error = %v, want it to name the lengths", err)
	}
}

// ParseNumeric must produce the shape the server produces, not merely a shape
// the server accepts.
//
// These two are different, and the difference is invisible from inside: a
// value with a run of leading zero digit groups is arithmetically identical to
// the same value without them, so the server stores it, compares it equal, and
// says nothing. What it costs is bytes — `0.000000000000000000001` went out as
// six digit groups where the server sends one — on a type this package exists
// to move by the thousand.
//
// The cause was a guard stopping the leading-zero strip at weight zero, while
// a negative weight is exactly how the format expresses a value below 1. It
// was found by a decoder check refusing a zero first group, which was itself
// found by fuzzing the text round trip.
//
// Vectors: SELECT encode(numeric_send('...'::numeric), 'hex').
func TestParseNumericProducesTheServersShape(t *testing.T) {
	tests := []struct{ sql, hex string }{
		{"0", "0000000000000000"},
		{"0.00", "0000000000000002"},
		{"1.5", "000200000000000100011388"},
		{"0.0001", "0001ffff000000040001"},
		{"0.00001", "0001fffe0000000503e8"},
		{"0.00010000", "0001ffff000000080001"},
		{"0.000000000000000000001", "0001fffa0000001503e8"},
		{"10000.0001", "0003000100000004000100000001"},
		{"100000000", "00010002000000000001"},
		{"1.00000000", "00010000000000080001"},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			want, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			n, err := ParseNumeric(tc.sql)
			if err != nil {
				t.Fatalf("ParseNumeric: %v", err)
			}
			if got := AppendNumeric(nil, n); !bytes.Equal(got, want) {
				t.Errorf("AppendNumeric = %x,\n           want %x", got, want)
			}
			// And the server's own bytes must decode to the same thing, which
			// is what makes the two shapes one shape rather than two that
			// happen to agree on value.
			back, err := DecodeNumeric(want)
			if err != nil {
				t.Fatalf("DecodeNumeric: %v", err)
			}
			if back.String() != n.String() {
				t.Errorf("the server's bytes render %q, ParseNumeric's render %q", back.String(), n.String())
			}
		})
	}
}

// Lossy is not the same as sloppy: Float64 returns the float64 nearest the
// exact decimal, which is what strconv.ParseFloat returns for the same
// digits. Summing digit × 10000^k in float64 rounds once per term and missed
// the nearest float on about one value in five.
func TestNumericFloat64IsCorrectlyRounded(t *testing.T) {
	cases := []string{
		"0.1", "0.3", "1.1", "2.675", "123.456",
		"9007199254740993",        // 2^53 + 1: a tie, rounds to even
		"9007199254740995",        // 2^53 + 3: a tie, rounds up to even
		"0.000000000000000000001", // far below the first group
		strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64),
		"2.2250738585072011e-308",
		"4.9406564584124654e-324",
		"-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049406564584124654",
	}
	// And a deterministic spread: eight significant digits at varied scales,
	// the shape money and measurements take.
	x := uint64(0x9E3779B97F4A7C15)
	for range 20000 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		mant := strconv.FormatUint(x%1_000_000_000_000_000, 10)
		cut := int(x>>59) % (len(mant) + 1)
		cases = append(cases, mant[:cut]+"."+mant[cut:]+"0")
	}
	bad := 0
	for _, s := range cases {
		want, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("ParseFloat(%q): %v", s, err)
		}
		if strings.ContainsAny(s, "e") {
			// ParseNumeric reads plain decimals; expand the exponent form.
			s = strconv.FormatFloat(want, 'f', -1, 64)
		}
		if strings.HasPrefix(s, ".") {
			s = "0" + s
		}
		n, err := ParseNumeric(s)
		if err != nil {
			t.Fatalf("ParseNumeric(%q): %v", s, err)
		}
		if got := n.Float64(); got != want {
			bad++
			if bad <= 5 {
				t.Errorf("Float64(%s) = %v, want %v", s, got, want)
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d of %d values not correctly rounded", bad, len(cases))
	}
}

// Digits outside 0..9999 cannot come off the wire, but Digits is an exported
// field and a hand-built value still converts.
func TestNumericFloat64OfHandBuiltDigits(t *testing.T) {
	n := Numeric{Weight: 0, Digits: []int16{-3}}
	if got := n.Float64(); got != -3 {
		t.Errorf("Float64 = %v, want -3", got)
	}
}

// Float64 on a money-shaped value: the decimal form goes through
// strconv.ParseFloat, and its buffer stays on the stack.
func BenchmarkNumericFloat64(b *testing.B) {
	n, err := ParseNumeric("123456.78")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var sink float64
	for b.Loop() {
		sink += n.Float64()
	}
	_ = sink
}
