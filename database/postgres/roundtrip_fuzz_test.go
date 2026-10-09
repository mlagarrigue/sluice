package postgres

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"testing"
)

// The same property the new codecs are held to, applied to the ones that
// shipped first: **every value a decoder accepts must re-encode to the bytes
// it came from**.
//
// These have golden vectors, and golden vectors check the values a person
// thought to write down. This checks the ones nobody did. It is what found an
// inet that rewrote itself and an array that renumbered itself, both in code
// that had vectors and passed them.
//
// Where a type has a value that is equal to itself in more than one encoding —
// a float NaN, whose payload the hardware may canonicalise — the property is
// stated on the meaning rather than on the bytes, and says so.

func fuzzRoundTrip[T comparable](
	f *testing.F,
	decode func([]byte) (T, error),
	encode func([]byte, T) []byte,
	seeds ...[]byte,
) {
	f.Helper()
	for _, s := range seeds {
		f.Add(s)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := decode(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := encode(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, v, got)
		}
	})
}

func FuzzRoundTripBool(f *testing.F) {
	fuzzRoundTrip(f, DecodeBool, AppendBool, []byte{0}, []byte{1}, []byte{2}, []byte{0, 0})
}

func FuzzRoundTripInt2(f *testing.F) {
	fuzzRoundTrip(f, DecodeInt2, AppendInt2, []byte{0, 0}, []byte{255, 255}, []byte{128, 0})
}

func FuzzRoundTripInt4(f *testing.F) {
	fuzzRoundTrip(f, DecodeInt4, AppendInt4, make([]byte, 4), bytes.Repeat([]byte{255}, 4))
}

func FuzzRoundTripInt8(f *testing.F) {
	fuzzRoundTrip(f, DecodeInt8, AppendInt8, make([]byte, 8), bytes.Repeat([]byte{255}, 8))
}

func FuzzRoundTripUUID(f *testing.F) {
	fuzzRoundTrip(f,
		DecodeUUID, AppendUUID,
		make([]byte, 16), bytes.Repeat([]byte{255}, 16), make([]byte, 15))
}

// Floats carry a value that is not comparable to itself — NaN — and whose bit
// pattern the hardware is entitled to canonicalise on the way through a
// register. The property is therefore stated twice: bit-identical for
// everything else, and "still a NaN" for that.
func FuzzRoundTripFloat8(f *testing.F) {
	f.Add(AppendFloat8(nil, 0))
	f.Add(AppendFloat8(nil, 1.5))
	f.Add(AppendFloat8(nil, math.Inf(-1)))
	f.Add(AppendFloat8(nil, math.NaN()))
	f.Add(AppendFloat8(nil, math.SmallestNonzeroFloat64))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeFloat8(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		got := AppendFloat8(nil, v)
		if math.IsNaN(v) {
			back, err := DecodeFloat8(got)
			if err != nil || !math.IsNaN(back) {
				t.Fatalf("a NaN did not survive: %x -> %v -> %x", b, v, got)
			}
			return
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, v, got)
		}
		// Negative zero is a distinct bit pattern that compares equal to zero,
		// so equality on the value would have let it be flattened. Pin the
		// sign bit on the decoded result rather than trusting byte equality
		// alone to have meant it.
		if v == 0 {
			back, err := DecodeFloat8(got)
			if err != nil || math.Signbit(back) != math.Signbit(v) {
				t.Fatalf("zero lost its sign: %x -> %x", b, got)
			}
		}
	})
}

func FuzzRoundTripFloat4(f *testing.F) {
	f.Add(AppendFloat4(nil, 0))
	f.Add(AppendFloat4(nil, 1.5))
	f.Add(AppendFloat4(nil, float32(math.Inf(1))))
	f.Add(AppendFloat4(nil, float32(math.NaN())))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeFloat4(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		got := AppendFloat4(nil, v)
		if math.IsNaN(float64(v)) {
			back, err := DecodeFloat4(got)
			if err != nil || !math.IsNaN(float64(back)) {
				t.Fatalf("a NaN did not survive: %x -> %v -> %x", b, v, got)
			}
			return
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, v, got)
		}
	})
}

// numeric is the type this package says it would rather not have than have
// nearly right: an exact decimal, where a value read through float64 is the
// silent-wrongness failure the whole design refuses. If any codec here has to
// survive arbitrary bytes, it is this one.
func FuzzRoundTripNumeric(f *testing.F) {
	f.Add(AppendNumeric(nil, Numeric{}))
	f.Add(AppendNumeric(nil, Numeric{Sign: NumericNaN}))
	f.Add(AppendNumeric(nil, Numeric{Digits: []int16{1}, Weight: 0}))
	f.Add(AppendNumeric(nil, Numeric{Sign: NumericNegative, Digits: []int16{1, 5000}, Weight: 0, Dscale: 4}))
	f.Add([]byte{})
	f.Add(make([]byte, 8))

	f.Fuzz(func(t *testing.T, b []byte) {
		n, err := DecodeNumeric(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendNumeric(nil, n); !bytes.Equal(got, b) {
			t.Fatalf("DecodeNumeric(%x) -> %+v -> %x: the value did not survive", b, n, got)
		}
		// Rendering must not panic on anything the decoder accepted, because
		// String is what a diagnostic reaches for and a diagnostic that
		// crashes is worse than the value it was explaining.
		_ = n.String()
		_ = n.Float64()
	})
}

func FuzzRoundTripDate(f *testing.F) {
	f.Add(AppendDate(nil, pgEpoch))
	f.Add(make([]byte, 4))
	f.Add(bytes.Repeat([]byte{255}, 4))
	f.Add([]byte{127, 255, 255, 255}) // date 'infinity'
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		d, err := DecodeDate(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendDate(nil, d); !bytes.Equal(got, b) {
			t.Fatalf("DecodeDate(%x) -> %v -> %x: the value did not survive", b, d, got)
		}
	})
}

func FuzzRoundTripInt8Array(f *testing.F) {
	f.Add(AppendInt8Array(nil, []int64{1, 2, 3}))
	f.Add(AppendInt8Array(nil, nil))
	f.Add(make([]byte, 12))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeInt8Array(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if len(v) == 0 {
			return // the empty array is zero dimensions, not a header to reproduce
		}
		// Idempotence rather than byte equality: the array header carries a
		// flags word saying whether NULLs may be present, which these
		// decoders have no use for — a NULL element is refused per element,
		// with a better message — so a re-encoding normalises it. The values
		// are what must not change.
		again, err := DecodeInt8Array(nil, AppendInt8Array(nil, v))
		if err != nil || !slices.Equal(again, v) {
			t.Fatalf("DecodeInt8Array(%x) is not idempotent: %v then %v (%v)", b, v, again, err)
		}
	})
}

func FuzzRoundTripTextArray(f *testing.F) {
	f.Add(AppendTextArray(nil, []string{"a", "", "ccc"}))
	f.Add(AppendTextArray(nil, nil))
	f.Add(make([]byte, 12))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeTextArray(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if len(v) == 0 {
			return
		}
		// Idempotence rather than byte equality: the array header carries a
		// flags word saying whether NULLs may be present, which these
		// decoders have no use for — a NULL element is refused per element,
		// with a better message — so a re-encoding normalises it. The values
		// are what must not change.
		again, err := DecodeTextArray(nil, AppendTextArray(nil, v))
		if err != nil || !slices.Equal(again, v) {
			t.Fatalf("DecodeTextArray(%x) is not idempotent: %q then %q (%v)", b, v, again, err)
		}
	})
}

// The text round trip, which is a different thing from the wire round trip and
// the one a person actually sees.
//
// A numeric leaves this package as text far more often than as bytes: a log
// line, a CSV export, a JSON field, an error message. If a value renders to
// something that parses back to a *different* value, the loss happens outside
// the database, where nothing checks it — and it is the same loss the whole
// type exists to refuse, wearing different clothes.
//
// The property is stated on the rendering rather than on the struct, because
// two structs can hold one value: the wire may carry a leading zero group that
// the parser normalises away, and that is a difference in representation, not
// in the number. What must not change is what the value *says it is*.
func FuzzNumericTextRoundTrip(f *testing.F) {
	f.Add(AppendNumeric(nil, Numeric{}))
	f.Add(AppendNumeric(nil, Numeric{Sign: NumericNaN}))
	f.Add(AppendNumeric(nil, Numeric{Sign: NumericPosInf, Dscale: 32}))
	f.Add(AppendNumeric(nil, Numeric{Digits: []int16{1}, Weight: 0}))
	f.Add(AppendNumeric(nil, Numeric{Digits: []int16{1}, Weight: 0, Dscale: 2}))
	f.Add(AppendNumeric(nil, Numeric{Sign: NumericNegative, Digits: []int16{1, 5000}, Dscale: 4}))
	f.Add(AppendNumeric(nil, Numeric{Digits: []int16{1, 2345}, Weight: 1, Dscale: 3}))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		n, err := DecodeNumeric(b)
		if err != nil {
			return // only well-formed values have a rendering worth judging
		}
		text := n.String()

		back, err := ParseNumeric(text)
		if err != nil {
			t.Fatalf("DecodeNumeric(%x).String() = %q, which ParseNumeric refuses: %v", b, text, err)
		}
		if again := back.String(); again != text {
			t.Fatalf("DecodeNumeric(%x) renders %q, which parses back and renders %q", b, text, again)
		}
	})
}
