package pgtype

import (
	"encoding/binary"
	"fmt"
	"slices"
	"time"
)

// The element loops of the common types: DecodeArray and AppendArray run
// these, through the codecs this package builds (Codec.decodeElems and
// Codec.appendElems), instead of a call through Decode or Append per
// element. Written out per type on purpose: a loop that took the decoder as
// a value paid the indirect call per element it exists to save — a thousand
// int4 values read three times slower than here.
//
// An Array keeps its values in a []T and its NULLs apart (Array.Nulls), so
// these loops read and write plain values, as a loop over a []int32 does:
// the per-element flag a []Null[T] carried cost the int4 encoding 17 to 24%
// against such a loop, whatever the loop was made of. A decoding loop reads
// the elements it expects — the type's width, or a length that fits — and
// stops at the first it does not (a NULL, a wrong length), which the
// general path (otherElements) takes from there, so its own loop carries no
// slow path and the errors are the general path's. An encoding loop is only
// run on an array with no NULL. The widest values (uuid, numeric, the
// times) are decoded into their place in dst rather than built and copied
// there.

// decodeBoolElems reads the bool elements of an array body it expects.
func decodeBoolElems(dst []bool, body []byte, n int) (out []bool, rest []byte, err error) {
	for range n {
		if len(body) < 5 || binary.BigEndian.Uint32(body) != 1 {
			break
		}
		v, err := DecodeBool(body[4:5])
		if err != nil {
			return dst, body, elementError(len(dst), err)
		}
		dst = append(dst, v)
		body = body[5:]
	}
	return dst, body, nil
}

// appendBoolElems writes the elements of a bool array with no NULL.
func appendBoolElems(dst []byte, elems []bool) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 1)
		dst = AppendBool(dst, v)
	}
	return dst
}

// decodeInt2Elems reads the int2 elements of an array body it expects.
func decodeInt2Elems(dst []int16, body []byte, n int) (out []int16, rest []byte, err error) {
	for range n {
		if len(body) < 6 || binary.BigEndian.Uint32(body) != 2 {
			break
		}
		dst = append(dst, int16(binary.BigEndian.Uint16(body[4:6]))) //nolint:gosec // G115: two's complement
		body = body[6:]
	}
	return dst, body, nil
}

// appendInt2Elems writes the elements of an int2 array with no NULL.
func appendInt2Elems(dst []byte, elems []int16) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 2)
		dst = AppendInt2(dst, v)
	}
	return dst
}

// decodeInt4Elems reads the int4 elements of an array body it expects.
func decodeInt4Elems(dst []int32, body []byte, n int) (out []int32, rest []byte, err error) {
	dst = slices.Grow(dst, n)
	for range n {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			break
		}
		k := len(dst)
		dst = dst[:k+1]
		dst[k] = int32(binary.BigEndian.Uint32(body[4:8])) //nolint:gosec // G115: two's complement
		body = body[8:]
	}
	return dst, body, nil
}

// appendInt4Elems writes the elements of an int4 array with no NULL.
func appendInt4Elems(dst []byte, elems []int32) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, v)
	}
	return dst
}

// decodeInt8Elems reads the int8 elements of an array body it expects.
func decodeInt8Elems(dst []int64, body []byte, n int) (out []int64, rest []byte, err error) {
	dst = slices.Grow(dst, n)
	for range n {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		k := len(dst)
		dst = dst[:k+1]
		dst[k] = int64(binary.BigEndian.Uint64(body[4:12])) //nolint:gosec // G115: two's complement
		body = body[12:]
	}
	return dst, body, nil
}

// appendInt8Elems writes the elements of an int8 array with no NULL.
func appendInt8Elems(dst []byte, elems []int64) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, v)
	}
	return dst
}

// decodeFloat4Elems reads the float4 elements of an array body it expects.
func decodeFloat4Elems(dst []float32, body []byte, n int) (out []float32, rest []byte, err error) {
	for range n {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			break
		}
		v, err := DecodeFloat4(body[4:8])
		if err != nil {
			return dst, body, elementError(len(dst), err)
		}
		dst = append(dst, v)
		body = body[8:]
	}
	return dst, body, nil
}

// appendFloat4Elems writes the elements of a float4 array with no NULL.
func appendFloat4Elems(dst []byte, elems []float32) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendFloat4(dst, v)
	}
	return dst
}

// decodeFloat8Elems reads the float8 elements of an array body it expects.
func decodeFloat8Elems(dst []float64, body []byte, n int) (out []float64, rest []byte, err error) {
	for range n {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		v, err := DecodeFloat8(body[4:12])
		if err != nil {
			return dst, body, elementError(len(dst), err)
		}
		dst = append(dst, v)
		body = body[12:]
	}
	return dst, body, nil
}

// appendFloat8Elems writes the elements of a float8 array with no NULL.
func appendFloat8Elems(dst []byte, elems []float64) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendFloat8(dst, v)
	}
	return dst
}

// decodeUUIDElems reads the uuid elements of an array body it expects, each
// copied into its place.
func decodeUUIDElems(dst [][16]byte, body []byte, n int) (out [][16]byte, rest []byte, err error) {
	dst = slices.Grow(dst, n)
	for range n {
		if len(body) < 20 || binary.BigEndian.Uint32(body) != 16 {
			break
		}
		k := len(dst)
		dst = dst[:k+1]
		dst[k] = [16]byte(body[4:20])
		body = body[20:]
	}
	return dst, body, nil
}

// appendUUIDElems writes the elements of a uuid array with no NULL.
func appendUUIDElems(dst []byte, elems [][16]byte) []byte {
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, elems[i][:]...)
	}
	return dst
}

// decodeTimeElems reads the elements of a timestamptz, timestamp or date
// array body it expects — width bytes each, dec's — each decoded into its
// place.
func decodeTimeElems(dst []time.Time, body []byte, n int, width uint32, dec func([]byte) (time.Time, error)) ([]time.Time, []byte, error) {
	w := int(width) + 4
	for range n {
		if len(body) < w || binary.BigEndian.Uint32(body) != width {
			break
		}
		dst = append(dst, time.Time{})
		var err error
		if dst[len(dst)-1], err = dec(body[4:w]); err != nil {
			return dst[:len(dst)-1], body, elementError(len(dst)-1, err)
		}
		body = body[w:]
	}
	return dst, body, nil
}

func decodeTimestampTZElems(dst []time.Time, body []byte, n int) (out []time.Time, rest []byte, err error) {
	return decodeTimeElems(dst, body, n, 8, DecodeTimestampTZ)
}

func decodeTimestampElems(dst []time.Time, body []byte, n int) (out []time.Time, rest []byte, err error) {
	return decodeTimeElems(dst, body, n, 8, DecodeTimestamp)
}

func decodeDateElems(dst []time.Time, body []byte, n int) (out []time.Time, rest []byte, err error) {
	return decodeTimeElems(dst, body, n, 4, DecodeDate)
}

// appendTimestampTZElems writes the elements of a timestamptz array with no
// NULL.
func appendTimestampTZElems(dst []byte, elems []time.Time) []byte {
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendTimestampTZ(dst, elems[i])
	}
	return dst
}

// appendTimestampElems writes the elements of a timestamp array with no
// NULL.
func appendTimestampElems(dst []byte, elems []time.Time) []byte {
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendTimestamp(dst, elems[i])
	}
	return dst
}

// appendDateElems writes the elements of a date array with no NULL.
func appendDateElems(dst []byte, elems []time.Time) []byte {
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendDate(dst, elems[i])
	}
	return dst
}

// decodeTextElems reads the text elements of an array body it expects.
func decodeTextElems(dst []string, body []byte, n int) (out []string, rest []byte, err error) {
	for range n {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		dst = append(dst, string(body[4:4+size]))
		body = body[4+size:]
	}
	return dst, body, nil
}

// appendTextElems writes the elements of a text array with no NULL.
func appendTextElems(dst []byte, elems []string) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v))) //nolint:gosec // G115: a value's length
		dst = append(dst, v...)
	}
	return dst
}

// decodeByteaElems reads the bytea elements of an array body it expects,
// each copied out of the body.
func decodeByteaElems(dst [][]byte, body []byte, n int) (out [][]byte, rest []byte, err error) {
	for range n {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		v, err := cloneBytes(body[4 : 4+size])
		if err != nil {
			return dst, body, elementError(len(dst), err)
		}
		dst = append(dst, v)
		body = body[4+size:]
	}
	return dst, body, nil
}

// appendByteaElems writes the elements of a bytea array with no NULL.
func appendByteaElems(dst []byte, elems [][]byte) []byte {
	for _, v := range elems {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v))) //nolint:gosec // G115: a value's length
		dst = append(dst, v...)
	}
	return dst
}

// decodeNumericElems reads the numeric elements of an array body it
// expects, each decoded into its place.
func decodeNumericElems(dst []Numeric, body []byte, n int) (out []Numeric, rest []byte, err error) {
	for range n {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		dst = append(dst, Numeric{})
		var err error
		if dst[len(dst)-1], err = DecodeNumeric(body[4 : 4+size]); err != nil {
			return dst[:len(dst)-1], body, elementError(len(dst)-1, err)
		}
		body = body[4+size:]
	}
	return dst, body, nil
}

// appendNumericElems writes the elements of a numeric array with no NULL.
func appendNumericElems(dst []byte, elems []Numeric) []byte {
	for i := range elems {
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = AppendNumeric(dst, elems[i])
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what was written
	}
	return dst
}

// otherElements reads elements i to n of an array body one at a time,
// through dec: the general path, which a type's loop leaves to at the
// first element it does not expect, and which every Codec built outside
// this package takes. The first NULL starts a.Nulls, every element before
// it marked not NULL; from there each element is marked.
func otherElements[T any](a Array[T], body []byte, i, n int, dec func([]byte) (T, error)) (Array[T], []byte, error) {
	for ; i < n; i++ {
		val, rest, isNull, err := nextElement(body, i)
		if err != nil {
			return a, body, err
		}
		body = rest
		if isNull {
			if len(a.Nulls) == 0 {
				a.Nulls = slices.Grow(a.Nulls[:0], len(a.Elems)+1)[:len(a.Elems)]
				clear(a.Nulls)
			}
			var zero T
			a.Elems = append(a.Elems, zero)
			a.Nulls = append(a.Nulls, true)
			continue
		}
		v, err := dec(val)
		if err != nil {
			return a, body, elementError(i, err)
		}
		a.Elems = append(a.Elems, v)
		if len(a.Nulls) > 0 {
			a.Nulls = append(a.Nulls, false)
		}
	}
	return a, body, nil
}

// elementError is the error of array element i, as every array decoder
// words it.
func elementError(i int, err error) error {
	return fmt.Errorf("array element %d: %w", i, err)
}
