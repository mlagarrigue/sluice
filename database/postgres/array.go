package postgres

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Array type OIDs for the element types this file covers.
const (
	OIDBoolArray        uint32 = 1000
	OIDByteaArray       uint32 = 1001
	OIDInt2Array        uint32 = 1005
	OIDFloat4Array      uint32 = 1021
	OIDFloat8Array      uint32 = 1022
	OIDDateArray        uint32 = 1182
	OIDTimestampArray   uint32 = 1115
	OIDTimestampTZArray uint32 = 1185
	OIDNumericArray     uint32 = 1231
)

// # Arrays of the remaining scalars
//
// The four arrays written by hand elsewhere — int8, int4, text, uuid — exist
// because a batch of *keys* travels as one `= ANY($1)` parameter. That is an
// argument about what goes into a query, and by it float8[] has no place here.
//
// The ones below exist for the other direction, which that argument does not
// cover: a column's type is chosen by whoever wrote the schema, not by this
// package, and a connector that cannot read a `boolean[]` column is a
// connector with a hole in it. Encoders come with them for symmetry and
// because a round trip is the cheapest test either half has.
//
// They are built on one generic skeleton rather than copied six times. The
// four hand-written ones are deliberately *not* converted to it: they sit on
// the batched-read hot path, two of them are watched by the regression gate,
// and a per-element indirect call is exactly the kind of change that costs
// three percent and is discovered a month later. The duplication is the price
// of not moving them, and it is written down here rather than left to look
// like an oversight.

// appendArrayOf writes a one-dimensional array whose elements are encoded by
// enc. Element lengths are written in place, so a variable-width element costs
// no scratch buffer.
func appendArrayOf[T any](dst []byte, elem uint32, values []T, enc func([]byte, T) []byte) []byte {
	dst = appendNullableArrayHeader(dst, elem, len(values), false)
	for _, v := range values {
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = enc(dst, v)
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what enc wrote
	}
	return dst
}

// appendNullableArrayOf is appendArrayOf with absent elements.
func appendNullableArrayOf[T any](dst []byte, elem uint32, values []Null[T], enc func([]byte, T) []byte) []byte {
	dst = appendNullableArrayHeader(dst, elem, len(values), anyNull(values))
	for _, v := range values {
		if !v.Valid {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = enc(dst, v.Value)
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what enc wrote
	}
	return dst
}

// decodeArrayOf reads a one-dimensional array, refusing NULL elements — the
// same refusal the hand-written decoders make, and for the same reason: a
// caller asking for []float64 has nowhere to put an absent value except a
// zero, and a zero that means "missing" is the silent wrongness this package
// exists to avoid.
func decodeArrayOf[T any](dst []T, b []byte, elem uint32, dec func([]byte) (T, error)) ([]T, error) {
	body, n, err := arrayBody(b, elem)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		val, rest, isNull, err := nextElement(body, i)
		if err != nil {
			return dst[:0], err
		}
		if isNull {
			return dst[:0], fmt.Errorf("%w: array element %d is NULL", ErrCodec, i)
		}
		body = rest
		v, err := dec(val)
		if err != nil {
			return dst[:0], fmt.Errorf("array element %d: %w", i, err)
		}
		dst = append(dst, v)
	}
	if err := trailing(body); err != nil {
		return dst[:0], err
	}
	return dst, nil
}

// decodeNullableArrayOf reads a one-dimensional array, NULLs included.
func decodeNullableArrayOf[T any](dst []Null[T], b []byte, elem uint32, dec func([]byte) (T, error)) ([]Null[T], error) {
	body, n, err := arrayBody(b, elem)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		val, rest, isNull, err := nextElement(body, i)
		if err != nil {
			return dst[:0], err
		}
		body = rest
		if isNull {
			dst = append(dst, Null[T]{})
			continue
		}
		v, err := dec(val)
		if err != nil {
			return dst[:0], fmt.Errorf("array element %d: %w", i, err)
		}
		dst = append(dst, Some(v))
	}
	if err := trailing(body); err != nil {
		return dst[:0], err
	}
	return dst, nil
}

// AppendBoolArray appends a one-dimensional boolean[].
func AppendBoolArray(dst []byte, values []bool) []byte {
	return appendArrayOf(dst, OIDBool, values, AppendBool)
}

// DecodeBoolArray decodes a one-dimensional boolean[], appending to dst.
func DecodeBoolArray(dst []bool, b []byte) ([]bool, error) {
	return decodeArrayOf(dst, b, OIDBool, DecodeBool)
}

// AppendNullableBoolArray appends a boolean[] whose elements may be NULL.
//
// This is the array where the three-valued logic is not academic: SQL's
// boolean is true, false, or unknown, and a Go bool has two values. Anything
// that flattened the third into false would be answering a different question.
func AppendNullableBoolArray(dst []byte, values []Null[bool]) []byte {
	return appendNullableArrayOf(dst, OIDBool, values, AppendBool)
}

// DecodeNullableBoolArray decodes a boolean[], NULLs included.
func DecodeNullableBoolArray(dst []Null[bool], b []byte) ([]Null[bool], error) {
	return decodeNullableArrayOf(dst, b, OIDBool, DecodeBool)
}

// AppendInt2Array appends a one-dimensional smallint[].
func AppendInt2Array(dst []byte, values []int16) []byte {
	return appendArrayOf(dst, OIDInt2, values, AppendInt2)
}

// DecodeInt2Array decodes a one-dimensional smallint[], appending to dst.
func DecodeInt2Array(dst []int16, b []byte) ([]int16, error) {
	return decodeArrayOf(dst, b, OIDInt2, DecodeInt2)
}

// AppendNullableInt2Array appends a smallint[] whose elements may be NULL.
func AppendNullableInt2Array(dst []byte, values []Null[int16]) []byte {
	return appendNullableArrayOf(dst, OIDInt2, values, AppendInt2)
}

// DecodeNullableInt2Array decodes a smallint[], NULLs included.
func DecodeNullableInt2Array(dst []Null[int16], b []byte) ([]Null[int16], error) {
	return decodeNullableArrayOf(dst, b, OIDInt2, DecodeInt2)
}

// AppendFloat4Array appends a one-dimensional real[].
func AppendFloat4Array(dst []byte, values []float32) []byte {
	return appendArrayOf(dst, OIDFloat4, values, AppendFloat4)
}

// DecodeFloat4Array decodes a one-dimensional real[], appending to dst.
func DecodeFloat4Array(dst []float32, b []byte) ([]float32, error) {
	return decodeArrayOf(dst, b, OIDFloat4, DecodeFloat4)
}

// AppendFloat8Array appends a one-dimensional double precision[].
func AppendFloat8Array(dst []byte, values []float64) []byte {
	return appendArrayOf(dst, OIDFloat8, values, AppendFloat8)
}

// DecodeFloat8Array decodes a one-dimensional double precision[], appending to
// dst.
func DecodeFloat8Array(dst []float64, b []byte) ([]float64, error) {
	return decodeArrayOf(dst, b, OIDFloat8, DecodeFloat8)
}

// AppendNullableFloat8Array appends a double precision[] whose elements may be
// NULL.
//
// NULL and NaN are different absences and both survive here: NaN is a value
// the column holds and compares unequal to itself, NULL is the absence of one.
// Collapsing either into the other would make a query about missing data
// answer about undefined arithmetic instead.
func AppendNullableFloat8Array(dst []byte, values []Null[float64]) []byte {
	return appendNullableArrayOf(dst, OIDFloat8, values, AppendFloat8)
}

// DecodeNullableFloat8Array decodes a double precision[], NULLs included.
func DecodeNullableFloat8Array(dst []Null[float64], b []byte) ([]Null[float64], error) {
	return decodeNullableArrayOf(dst, b, OIDFloat8, DecodeFloat8)
}

// AppendByteaArray appends a one-dimensional bytea[].
//
// The elements are variable-width, which is why this file writes lengths in
// place rather than measuring first.
func AppendByteaArray(dst []byte, values [][]byte) []byte {
	return appendArrayOf(dst, OIDBytea, values, AppendBytea)
}

// DecodeByteaArray decodes a one-dimensional bytea[], appending to dst.
//
// Elements are copied out: the wire buffer is reused per batch, so a borrowed
// slice would change under whoever kept it.
func DecodeByteaArray(dst [][]byte, b []byte) ([][]byte, error) {
	return decodeArrayOf(dst, b, OIDBytea, func(v []byte) ([]byte, error) {
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	})
}

// AppendTimestampTZArray appends a one-dimensional timestamptz[].
func AppendTimestampTZArray(dst []byte, values []time.Time) []byte {
	return appendArrayOf(dst, OIDTimestampTZ, values, AppendTimestampTZ)
}

// DecodeTimestampTZArray decodes a one-dimensional timestamptz[], appending to
// dst.
//
// Every element comes back in UTC, as [DecodeTimestampTZ] returns it: the wire
// carries an instant and no zone, so the zone a caller sees is this package's
// choice rather than the server's. Stated here because an array of them is
// where someone is most likely to compare against a local time and find the
// whole slice shifted.
func DecodeTimestampTZArray(dst []time.Time, b []byte) ([]time.Time, error) {
	return decodeArrayOf(dst, b, OIDTimestampTZ, DecodeTimestampTZ)
}

// AppendTimestampArray appends a one-dimensional timestamp[] — without time
// zone, which is a clock reading rather than an instant. Its bytes are
// identical to timestamptz[]'s, which is why the element OID in the header is
// the only thing that keeps them apart.
func AppendTimestampArray(dst []byte, values []time.Time) []byte {
	return appendArrayOf(dst, OIDTimestamp, values, AppendTimestamp)
}

// DecodeTimestampArray decodes a one-dimensional timestamp[], appending to dst.
func DecodeTimestampArray(dst []time.Time, b []byte) ([]time.Time, error) {
	return decodeArrayOf(dst, b, OIDTimestamp, DecodeTimestamp)
}

// AppendDateArray appends a one-dimensional date[].
func AppendDateArray(dst []byte, values []time.Time) []byte {
	return appendArrayOf(dst, OIDDate, values, AppendDate)
}

// DecodeDateArray decodes a one-dimensional date[], appending to dst.
func DecodeDateArray(dst []time.Time, b []byte) ([]time.Time, error) {
	return decodeArrayOf(dst, b, OIDDate, DecodeDate)
}

// AppendNumericArray appends a one-dimensional numeric[].
func AppendNumericArray(dst []byte, values []Numeric) []byte {
	return appendArrayOf(dst, OIDNumeric, values, AppendNumeric)
}

// DecodeNumericArray decodes a one-dimensional numeric[], appending to dst.
func DecodeNumericArray(dst []Numeric, b []byte) ([]Numeric, error) {
	return decodeArrayOf(dst, b, OIDNumeric, DecodeNumeric)
}
