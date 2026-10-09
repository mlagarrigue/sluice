package postgres

import (
	"encoding/binary"
	"fmt"
)

// Null carries a value that may be absent, without spending a pointer on
// saying so.
//
//	vals, err := DecodeNullableInt8Array(vals[:0], raw)
//	for _, v := range vals {
//	    if !v.Valid {
//	        continue // SQL NULL
//	    }
//	    use(v.Value)
//	}
//
// # Why not []*int64
//
// Because a pointer per element is an allocation per element, and the whole
// point of the array codecs is that a thousand keys travel without a thousand
// allocations. A `[]Null[int64]` is one contiguous block, `Valid` sits beside
// the value, and the batch stays flat — which is the same reason [Rows] decodes
// into one buffer plus offsets rather than a slice per row.
//
// # Valid, not IsNull
//
// The zero value of Null is *invalid* — an absent value, not a zero one. That
// is deliberate: a `Null[int64]{}` that read as "the number 0, present" would
// make every uninitialised element a silently real value, which is the exact
// confusion this type exists to remove.
type Null[T any] struct {
	Value T
	Valid bool
}

// Some returns a present value.
func Some[T any](v T) Null[T] { return Null[T]{Value: v, Valid: true} }

// None returns an absent value. It is the zero Null, named so a caller writing
// a literal list does not have to know that.
func None[T any]() Null[T] { return Null[T]{} }

// # NULL inside an array
//
// PostgreSQL's binary array format says NULL twice, and both have to agree:
// the header's flags word is 1 when any element is NULL, and each NULL element
// carries a length of -1 instead of its bytes. There is no bitmap — that
// belongs to the on-disk representation, not to what travels on the wire.
//
// This package's encoders wrote flags = 0 unconditionally, which was true only
// because they refused NULLs. Now that they do not, the two halves are written
// in one place so they cannot drift apart.

// appendNullableArrayHeader is [appendArrayHeader] with the flags word told
// the truth.
func appendNullableArrayHeader(dst []byte, elem uint32, n int, hasNull bool) []byte {
	if n == 0 {
		// Still zero dimensions: an empty array has no elements to be NULL,
		// whatever the caller's slice type says.
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		return binary.BigEndian.AppendUint32(dst, elem)
	}
	var flags uint32
	if hasNull {
		flags = 1
	}
	dst = binary.BigEndian.AppendUint32(dst, 1)         // one dimension
	dst = binary.BigEndian.AppendUint32(dst, flags)     // NULLs present
	dst = binary.BigEndian.AppendUint32(dst, elem)      // element type
	dst = binary.BigEndian.AppendUint32(dst, uint32(n)) //nolint:gosec // G115: a list's length
	return binary.BigEndian.AppendUint32(dst, 1)        // lower bound
}

// nullElement is the length a NULL element carries: -1, as an unsigned word.
const nullElement = ^uint32(0)

// nextElement returns the bytes of the next array element and whether it is
// NULL, advancing the body. It is the one place the -1 convention is read, so
// a decoder cannot forget it and mistake -1 for a very large length.
func nextElement(body []byte, i int) (value, rest []byte, isNull bool, err error) {
	if len(body) < 4 {
		return nil, nil, false, fmt.Errorf("%w: array element %d has no length", ErrCodec, i)
	}
	size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
	body = body[4:]
	if size == -1 {
		return nil, body, true, nil
	}
	if size < 0 {
		return nil, nil, false, fmt.Errorf("%w: array element %d declares a length of %d", ErrCodec, i, size)
	}
	if size > len(body) {
		return nil, nil, false, fmt.Errorf("%w: array element %d declares %d bytes, %d remain", ErrCodec, i, size, len(body))
	}
	return body[:size], body[size:], false, nil
}

// AppendNullableInt8Array appends a one-dimensional int8[] whose elements may
// be NULL.
//
// [AppendInt8Array] stays the one to reach for when they cannot be: it takes a
// plain []int64 and costs nothing to call. This is for the column that has
// NULLs in it, which is most columns.
func AppendNullableInt8Array(dst []byte, values []Null[int64]) []byte {
	dst = appendNullableArrayHeader(dst, OIDInt8, len(values), anyNull(values))
	for _, v := range values {
		if !v.Valid {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, 8)
		dst = AppendInt8(dst, v.Value)
	}
	return dst
}

// DecodeNullableInt8Array decodes a one-dimensional int8[], NULLs included.
func DecodeNullableInt8Array(dst []Null[int64], b []byte) ([]Null[int64], error) {
	body, n, err := arrayBody(b, OIDInt8)
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
			dst = append(dst, Null[int64]{})
			continue
		}
		v, err := DecodeInt8(val)
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

// AppendNullableInt4Array appends a one-dimensional int4[] whose elements may
// be NULL.
func AppendNullableInt4Array(dst []byte, values []Null[int32]) []byte {
	dst = appendNullableArrayHeader(dst, OIDInt4, len(values), anyNull(values))
	for _, v := range values {
		if !v.Valid {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, v.Value)
	}
	return dst
}

// DecodeNullableInt4Array decodes a one-dimensional int4[], NULLs included.
func DecodeNullableInt4Array(dst []Null[int32], b []byte) ([]Null[int32], error) {
	body, n, err := arrayBody(b, OIDInt4)
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
			dst = append(dst, Null[int32]{})
			continue
		}
		v, err := DecodeInt4(val)
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

// AppendNullableTextArray appends a one-dimensional text[] whose elements may
// be NULL.
//
// This is the one where the distinction earns its keep: an empty string and a
// NULL are the same zero value in Go and different values in SQL, and a codec
// that collapsed them would turn `WHERE name = ”` and `WHERE name IS NULL`
// into the same query without saying so.
func AppendNullableTextArray(dst []byte, values []Null[string]) []byte {
	dst = appendNullableArrayHeader(dst, OIDText, len(values), anyNull(values))
	for _, v := range values {
		if !v.Valid {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v.Value))) //nolint:gosec // G115: bounded by the caller's value
		dst = append(dst, v.Value...)
	}
	return dst
}

// DecodeNullableTextArray decodes a one-dimensional text[], NULLs included.
// Elements are copied out, for the reason [DecodeTextArray] gives.
func DecodeNullableTextArray(dst []Null[string], b []byte) ([]Null[string], error) {
	body, n, err := arrayBody(b, OIDText)
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
			dst = append(dst, Null[string]{})
			continue
		}
		dst = append(dst, Some(string(val)))
	}
	if err := trailing(body); err != nil {
		return dst[:0], err
	}
	return dst, nil
}

// AppendNullableUUIDArray appends a one-dimensional uuid[] whose elements may
// be NULL.
func AppendNullableUUIDArray(dst []byte, values []Null[[16]byte]) []byte {
	dst = appendNullableArrayHeader(dst, OIDUUID, len(values), anyNull(values))
	for _, v := range values {
		if !v.Valid {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, v.Value[:]...)
	}
	return dst
}

// DecodeNullableUUIDArray decodes a one-dimensional uuid[], NULLs included.
func DecodeNullableUUIDArray(dst []Null[[16]byte], b []byte) ([]Null[[16]byte], error) {
	body, n, err := arrayBody(b, OIDUUID)
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
			dst = append(dst, Null[[16]byte]{})
			continue
		}
		u, err := DecodeUUID(val)
		if err != nil {
			return dst[:0], fmt.Errorf("array element %d: %w", i, err)
		}
		dst = append(dst, Some(u))
	}
	if err := trailing(body); err != nil {
		return dst[:0], err
	}
	return dst, nil
}

func anyNull[T any](values []Null[T]) bool {
	for _, v := range values {
		if !v.Valid {
			return true
		}
	}
	return false
}

func trailing(body []byte) error {
	if len(body) != 0 {
		return fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return nil
}
