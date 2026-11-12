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
		elems := make([]Null[T], n)
		for i := range elems {
			elems[i] = Null[T]{Value: values[i%len(values)], Valid: true}
		}
		shapes["n="+strconv.Itoa(n)] = Array[T]{Elems: elems}
		for _, at := range []int{0, 1, 2, 3, 4, n - 1} {
			if at < 0 || at >= n {
				continue
			}
			withNull := append([]Null[T](nil), elems...)
			withNull[at] = Null[T]{}
			shapes["n="+strconv.Itoa(n)+",null@"+strconv.Itoa(at)] = Array[T]{Elems: withNull}
		}
	}
	if len(values) >= 4 {
		elems := make([]Null[T], 4)
		for i := range elems {
			elems[i] = Null[T]{Value: values[i], Valid: true}
		}
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
	// An element of a length the type does not have, after three that
	// are right: the loop leaves it to the general path, whose error it is.
	if len(values) >= 3 {
		raw := AppendArray(nil, Array[T]{Elems: []Null[T]{{Value: values[0], Valid: true}, {Value: values[1], Valid: true}, {Value: values[2], Valid: true}}}, c)
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
