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
// A decoding loop reads the elements it expects — the type's width, or a
// length that fits — and leaves the rest of the array, from the first it
// does not (a NULL, a wrong length), to otherElements, so its own loop
// carries no slow path and the errors are the general path's. An encoding
// loop looks for a NULL first and, finding none, writes every element
// without asking: the question per element cost half again as much. The
// widest values (uuid, numeric, the times) are decoded into their place
// in dst rather than built and copied there.

// decodeBoolElems reads n bool elements of an array body.
func decodeBoolElems(dst []Null[bool], body []byte, n int) ([]Null[bool], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 5 || binary.BigEndian.Uint32(body) != 1 {
			break
		}
		v, err := DecodeBool(body[4:5])
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[bool]{Value: v, Valid: true})
		body = body[5:]
	}
	return otherElements(dst, body, i, n, DecodeBool)
}

// appendBoolElems writes the elements of a bool array, and says
// whether one was NULL.
func appendBoolElems(dst []byte, elems []Null[bool]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendBool), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 1)
		dst = AppendBool(dst, elems[i].Value)
	}
	return dst, false
}

// decodeInt2Elems reads n int2 elements of an array body.
func decodeInt2Elems(dst []Null[int16], body []byte, n int) ([]Null[int16], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 6 || binary.BigEndian.Uint32(body) != 2 {
			break
		}
		v, err := DecodeInt2(body[4:6])
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[int16]{Value: v, Valid: true})
		body = body[6:]
	}
	return otherElements(dst, body, i, n, DecodeInt2)
}

// appendInt2Elems writes the elements of a int2 array, and says
// whether one was NULL.
func appendInt2Elems(dst []byte, elems []Null[int16]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendInt2), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 2)
		dst = AppendInt2(dst, elems[i].Value)
	}
	return dst, false
}

// decodeInt4Elems reads n int4 elements of an array body.
func decodeInt4Elems(dst []Null[int32], body []byte, n int) ([]Null[int32], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			break
		}
		dst = append(dst, Null[int32]{Value: int32(binary.BigEndian.Uint32(body[4:8])), Valid: true}) //nolint:gosec // G115: two's complement
		body = body[8:]
	}
	return otherElements(dst, body, i, n, DecodeInt4)
}

// appendInt4Elems writes the elements of a int4 array, and says
// whether one was NULL.
func appendInt4Elems(dst []byte, elems []Null[int32]) ([]byte, bool) {
	i := 0
	for ; i+4 <= len(elems); i += 4 {
		e := elems[i : i+4 : i+4]
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, e[0].Value)
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, e[1].Value)
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, e[2].Value)
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, e[3].Value)
		if !e[0].Valid || !e[1].Valid || !e[2].Valid || !e[3].Valid {
			dst = dst[:at]
			break
		}
	}
	return appendRestElems(dst, elems[i:], AppendInt4)
}

// decodeInt8Elems reads n int8 elements of an array body.
func decodeInt8Elems(dst []Null[int64], body []byte, n int) ([]Null[int64], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		dst = append(dst, Null[int64]{Value: int64(binary.BigEndian.Uint64(body[4:12])), Valid: true}) //nolint:gosec // G115: two's complement
		body = body[12:]
	}
	return otherElements(dst, body, i, n, DecodeInt8)
}

// appendInt8Elems writes the elements of a int8 array, and says
// whether one was NULL.
func appendInt8Elems(dst []byte, elems []Null[int64]) ([]byte, bool) {
	i := 0
	for ; i+4 <= len(elems); i += 4 {
		e := elems[i : i+4 : i+4]
		if !e[0].Valid || !e[1].Valid || !e[2].Valid || !e[3].Valid {
			break
		}
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, e[0].Value)
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, e[1].Value)
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, e[2].Value)
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, e[3].Value)
	}
	return appendRestElems(dst, elems[i:], AppendInt8)
}

// decodeFloat4Elems reads n float4 elements of an array body.
func decodeFloat4Elems(dst []Null[float32], body []byte, n int) ([]Null[float32], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			break
		}
		v, err := DecodeFloat4(body[4:8])
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[float32]{Value: v, Valid: true})
		body = body[8:]
	}
	return otherElements(dst, body, i, n, DecodeFloat4)
}

// appendFloat4Elems writes the elements of a float4 array, and says
// whether one was NULL.
func appendFloat4Elems(dst []byte, elems []Null[float32]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendFloat4), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendFloat4(dst, elems[i].Value)
	}
	return dst, false
}

// decodeFloat8Elems reads n float8 elements of an array body.
func decodeFloat8Elems(dst []Null[float64], body []byte, n int) ([]Null[float64], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		v, err := DecodeFloat8(body[4:12])
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[float64]{Value: v, Valid: true})
		body = body[12:]
	}
	return otherElements(dst, body, i, n, DecodeFloat8)
}

// appendFloat8Elems writes the elements of a float8 array, and says
// whether one was NULL.
func appendFloat8Elems(dst []byte, elems []Null[float64]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendFloat8), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendFloat8(dst, elems[i].Value)
	}
	return dst, false
}

// decodeUUIDElems reads n uuid elements of an array body.
func decodeUUIDElems(dst []Null[[16]byte], body []byte, n int) ([]Null[[16]byte], []byte, error) {
	dst = slices.Grow(dst, n)
	i := 0
	for ; i < n; i++ {
		if len(body) < 20 || binary.BigEndian.Uint32(body) != 16 {
			break
		}
		k := len(dst)
		dst = dst[:k+1]
		dst[k].Value = [16]byte(body[4:20])
		dst[k].Valid = true
		body = body[20:]
	}
	return otherElements(dst, body, i, n, DecodeUUID)
}

// appendUUIDElems writes the elements of a uuid array, and says
// whether one was NULL.
func appendUUIDElems(dst []byte, elems []Null[[16]byte]) ([]byte, bool) {
	i := 0
	for ; i+4 <= len(elems); i += 4 {
		e := elems[i : i+4 : i+4]
		if !e[0].Valid || !e[1].Valid || !e[2].Valid || !e[3].Valid {
			break
		}
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, e[0].Value[:]...)
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, e[1].Value[:]...)
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, e[2].Value[:]...)
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, e[3].Value[:]...)
	}
	return appendRestElems(dst, elems[i:], AppendUUID)
}

// decodeTimestampTZElems reads n timestamptz elements of an array body.
func decodeTimestampTZElems(dst []Null[time.Time], body []byte, n int) ([]Null[time.Time], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		dst = append(dst, Null[time.Time]{})
		e := &dst[len(dst)-1]
		var err error
		if e.Value, err = DecodeTimestampTZ(body[4:12]); err != nil {
			return dst, body, elementError(i, err)
		}
		e.Valid = true
		body = body[12:]
	}
	return otherElements(dst, body, i, n, DecodeTimestampTZ)
}

// appendTimestampTZElems writes the elements of a timestamptz array, and says
// whether one was NULL.
func appendTimestampTZElems(dst []byte, elems []Null[time.Time]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendTimestampTZ), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendTimestampTZ(dst, elems[i].Value)
	}
	return dst, false
}

// decodeTimestampElems reads n timestamp elements of an array body.
func decodeTimestampElems(dst []Null[time.Time], body []byte, n int) ([]Null[time.Time], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 12 || binary.BigEndian.Uint32(body) != 8 {
			break
		}
		dst = append(dst, Null[time.Time]{})
		e := &dst[len(dst)-1]
		var err error
		if e.Value, err = DecodeTimestamp(body[4:12]); err != nil {
			return dst, body, elementError(i, err)
		}
		e.Valid = true
		body = body[12:]
	}
	return otherElements(dst, body, i, n, DecodeTimestamp)
}

// appendTimestampElems writes the elements of a timestamp array, and says
// whether one was NULL.
func appendTimestampElems(dst []byte, elems []Null[time.Time]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendTimestamp), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendTimestamp(dst, elems[i].Value)
	}
	return dst, false
}

// decodeDateElems reads n date elements of an array body.
func decodeDateElems(dst []Null[time.Time], body []byte, n int) ([]Null[time.Time], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			break
		}
		dst = append(dst, Null[time.Time]{})
		e := &dst[len(dst)-1]
		var err error
		if e.Value, err = DecodeDate(body[4:8]); err != nil {
			return dst, body, elementError(i, err)
		}
		e.Valid = true
		body = body[8:]
	}
	return otherElements(dst, body, i, n, DecodeDate)
}

// appendDateElems writes the elements of a date array, and says
// whether one was NULL.
func appendDateElems(dst []byte, elems []Null[time.Time]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendDate), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendDate(dst, elems[i].Value)
	}
	return dst, false
}

// decodeTextElems reads n text elements of an array body.
func decodeTextElems(dst []Null[string], body []byte, n int) ([]Null[string], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		dst = append(dst, Null[string]{Value: string(body[4 : 4+size]), Valid: true})
		body = body[4+size:]
	}
	return otherElements(dst, body, i, n, decodeTextValue)
}

// appendTextElems writes the elements of a text array, and says
// whether one was NULL.
func appendTextElems(dst []byte, elems []Null[string]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendText), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(elems[i].Value))) //nolint:gosec // G115: a value's length
		dst = append(dst, elems[i].Value...)
	}
	return dst, false
}

// decodeByteaElems reads n bytea elements of an array body.
func decodeByteaElems(dst []Null[[]byte], body []byte, n int) ([]Null[[]byte], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		v, err := cloneBytes(body[4 : 4+size])
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[[]byte]{Value: v, Valid: true})
		body = body[4+size:]
	}
	return otherElements(dst, body, i, n, cloneBytes)
}

// appendByteaElems writes the elements of a bytea array, and says
// whether one was NULL.
func appendByteaElems(dst []byte, elems []Null[[]byte]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendBytea), true
		}
	}
	for i := range elems {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(elems[i].Value))) //nolint:gosec // G115: a value's length
		dst = append(dst, elems[i].Value...)
	}
	return dst, false
}

// decodeNumericElems reads n numeric elements of an array body.
func decodeNumericElems(dst []Null[Numeric], body []byte, n int) ([]Null[Numeric], []byte, error) {
	i := 0
	for ; i < n; i++ {
		if len(body) < 4 {
			break
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			break
		}
		dst = append(dst, Null[Numeric]{})
		e := &dst[len(dst)-1]
		var err error
		if e.Value, err = DecodeNumeric(body[4 : 4+size]); err != nil {
			return dst, body, elementError(i, err)
		}
		e.Valid = true
		body = body[4+size:]
	}
	return otherElements(dst, body, i, n, DecodeNumeric)
}

// appendNumericElems writes the elements of a numeric array, and says
// whether one was NULL.
func appendNumericElems(dst []byte, elems []Null[Numeric]) ([]byte, bool) {
	for i := range elems {
		if !elems[i].Valid {
			return appendNullElems(dst, elems, AppendNumeric), true
		}
	}
	for i := range elems {
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = AppendNumeric(dst, elems[i].Value)
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what was written
	}
	return dst, false
}

// appendRestElems writes the elements a type's loop left, one at a time,
// and says whether one was NULL. The loops of the narrowest types check
// four elements at once and leave the rest here: a check per element, or a
// pass of its own looking for a NULL, cost them half again as much.
func appendRestElems[T any](dst []byte, elems []Null[T], enc func([]byte, T) []byte) ([]byte, bool) {
	hasNull := false
	for _, v := range elems {
		hasNull = hasNull || !v.Valid
		dst = appendNullValue(dst, v, enc)
	}
	return dst, hasNull
}

// appendNullElems is the general loop of an array with a NULL.
func appendNullElems[T any](dst []byte, elems []Null[T], enc func([]byte, T) []byte) []byte {
	for _, v := range elems {
		dst = appendNullValue(dst, v, enc)
	}
	return dst
}

// otherElements reads elements i to n of an array body one at a time,
// through dec: the general path, which a type's loop leaves to at the
// first element it does not expect, and which every Codec built outside
// this package takes.
func otherElements[T any](dst []Null[T], body []byte, i, n int, dec func([]byte) (T, error)) ([]Null[T], []byte, error) {
	for ; i < n; i++ {
		val, rest, isNull, err := nextElement(body, i)
		if err != nil {
			return dst, body, err
		}
		body = rest
		if isNull {
			dst = append(dst, Null[T]{})
			continue
		}
		v, err := dec(val)
		if err != nil {
			return dst, body, elementError(i, err)
		}
		dst = append(dst, Null[T]{Value: v, Valid: true})
	}
	return dst, body, nil
}

// elementError is the error of array element i, as every array decoder
// words it.
func elementError(i int, err error) error {
	return fmt.Errorf("array element %d: %w", i, err)
}
