package pgtype

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// fastMatchesGeneral holds a type's array loops to the general path they
// replace: the same Codec with its loops removed — what a Codec built
// outside this package is — must write the same bytes and read back the
// same values, NULLs and errors included.
func fastMatchesGeneral[T any](t *testing.T, c Codec[T], values []T) {
	t.Helper()
	if c.decodeElems == nil || c.appendElems == nil {
		t.Fatalf("OID %d: the codec has no array loops", c.OID)
	}
	general := c
	general.decodeElems, general.appendElems = nil, nil
	shapes := map[string]Array[T]{}
	for _, n := range []int{0, 1, 3, 4, 5, 9, len(values)} {
		elems := make([]T, n)
		for i := range elems {
			elems[i] = values[i%len(values)]
		}
		shapes["n="+strconv.Itoa(n)] = Array[T]{Elems: elems}
		// A mask with no NULL in it is no NULL: written as the array without.
		shapes["n="+strconv.Itoa(n)+",mask"] = Array[T]{Elems: elems, Nulls: make([]bool, n)}
		for _, at := range []int{0, 1, 2, 3, 4, n - 1} {
			if at < 0 || at >= n {
				continue
			}
			withNull := append([]T(nil), elems...)
			var zero T
			withNull[at] = zero
			nulls := make([]bool, n)
			nulls[at] = true
			shapes["n="+strconv.Itoa(n)+",null@"+strconv.Itoa(at)] = Array[T]{Elems: withNull, Nulls: nulls}
		}
		// Several NULLs: the loops resume after each one, on both sides.
		for name, isNull := range map[string]func(i int) bool{
			"all":      func(int) bool { return true },
			"odd":      func(i int) bool { return i%2 == 1 },
			"1-2":      func(i int) bool { return i == 1 || i == 2 },
			"1 in 10":  func(i int) bool { return i%10 == 3 },
			"not last": func(i int) bool { return i != n-1 },
		} {
			withNulls := append([]T(nil), elems...)
			nulls := make([]bool, n)
			for i := range nulls {
				if nulls[i] = isNull(i); nulls[i] {
					var zero T
					withNulls[i] = zero
				}
			}
			shapes["n="+strconv.Itoa(n)+",nulls "+name] = Array[T]{Elems: withNulls, Nulls: nulls}
		}
	}
	if len(values) >= 4 {
		elems := append([]T(nil), values[:4]...)
		shapes["2x2 from 0"] = Array[T]{Dims: []ArrayDim{{Len: 2, Lower: 0}, {Len: 2, Lower: 1}}, Elems: elems}
	}
	for name, a := range shapes {
		fast, slow := AppendArray(nil, a, c), AppendArray(nil, a, general)
		if !bytes.Equal(fast, slow) {
			t.Errorf("OID %d, %s: the type's loop wrote\n%x\nthe general path\n%x", c.OID, name, fast, slow)
			continue
		}
		got, err := DecodeArray(Array[T]{}, fast, c)
		want, werr := DecodeArray(Array[T]{}, fast, general)
		if (err == nil) != (werr == nil) || !reflect.DeepEqual(got, want) {
			t.Errorf("OID %d, %s: the type's loop read %v, %v; the general path %v, %v", c.OID, name, got, err, want, werr)
		}
	}
	// A dst that carried a mask, reused for an array with no NULL, comes
	// back with none; and with one, with a mask of its own length.
	if n := len(values); n >= 4 {
		nulls := make([]bool, n)
		nulls[1] = true
		withNull := Array[T]{Elems: append([]T(nil), values...), Nulls: nulls}
		var zero T
		withNull.Elems[1] = zero
		prev, err := DecodeArray(Array[T]{}, AppendArray(nil, withNull, c), c)
		if err != nil || len(prev.Nulls) != n {
			t.Fatalf("OID %d: decoding a NULL at 1: %v, mask of %d", c.OID, err, len(prev.Nulls))
		}
		got, err := DecodeArray(prev, AppendArray(nil, Array[T]{Elems: values}, c), c)
		if err != nil || len(got.Nulls) != 0 || len(got.Elems) != n {
			t.Errorf("OID %d: a reused dst kept a mask of %d for an array with no NULL (%v)", c.OID, len(got.Nulls), err)
		}
	}
	// An element of a length the type does not have, after three that
	// are right: the loop leaves it to the general path, whose error it is.
	if len(values) >= 3 {
		raw := AppendArray(nil, Array[T]{Elems: values[:3]}, c)
		raw = binary.BigEndian.AppendUint32(raw, 3)
		raw = append(raw, 1, 2, 3)
		binary.BigEndian.PutUint32(raw[12:], 4) // four elements now
		_, err := DecodeArray(Array[T]{}, raw, c)
		_, werr := DecodeArray(Array[T]{}, raw, general)
		if (err == nil) != (werr == nil) || (err != nil && err.Error() != werr.Error()) {
			t.Errorf("OID %d, a three-byte element: the type's loop said %v, the general path %v", c.OID, err, werr)
		}
	}
}

func TestArrayLoopsMatchTheGeneralPath(t *testing.T) {
	base := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	fastMatchesGeneral(t, BoolCodec(), []bool{true, false, true, true, false})
	fastMatchesGeneral(t, Int2Codec(), []int16{1, -2, 300, -32768, 32767})
	fastMatchesGeneral(t, Int4Codec(), []int32{1, -2, 1 << 30, -1 << 31, 7})
	fastMatchesGeneral(t, Int8Codec(), []int64{1, -2, 1 << 62, -1 << 63, 7})
	fastMatchesGeneral(t, Float4Codec(), []float32{1.5, -2, 0, 3e38, -1e-38})
	fastMatchesGeneral(t, Float8Codec(), []float64{1.5, -2, 0, 1e300, -1e-300})
	fastMatchesGeneral(t, UUIDCodec(), [][16]byte{{1}, {2, 3}, {15: 9}, {}, {4}})
	fastMatchesGeneral(t, TimestampTZCodec(), []time.Time{base, base.Add(time.Second), base.AddDate(-30, 0, 0), base.Add(time.Microsecond), base})
	fastMatchesGeneral(t, TimestampCodec(), []time.Time{base, base.Add(time.Minute), base.AddDate(5, 0, 0), base, base})
	fastMatchesGeneral(t, DateCodec(), []time.Time{base, base.AddDate(0, 0, 1), base.AddDate(-100, 0, 0), base, base})
	fastMatchesGeneral(t, TextCodec(), []string{"a", "", "été", "longer text", "x"})
	fastMatchesGeneral(t, ByteaCodec(), [][]byte{{1}, {}, {0, 255}, []byte("abc"), {9}})
	n := func(s string) Numeric {
		v, err := ParseNumeric(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	fastMatchesGeneral(t, NumericCodec(), []Numeric{n("1.25"), n("-3"), n("0"), n("123456789.000001"), n("7")})
}
