package pgtype

import (
	"encoding/binary"
	"fmt"
)

// Null carries a value that may be absent, without spending a pointer on
// saying so: a nullable column of an entity, a nullable bound of a range.
//
//	type Product struct {
//	    Weight pgtype.Null[float64] `db:"weight"` // NULL when !Weight.Valid
//	}
//
// An array marks its NULL elements apart, in [Array.Nulls], so that an
// array without one is a plain slice.
//
// # Why not *int64
//
// Because a pointer per value is an allocation per value, and a batch of a
// thousand rows would pay a thousand of them. A Null sits beside its value,
// and the batch stays flat — which is the same reason the connector's Rows
// decodes into one buffer plus offsets rather than a slice per row.
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

// # NULL inside an array
//
// PostgreSQL's binary array format says NULL twice, and both have to agree:
// the header's flags word is 1 when any element is NULL, and each NULL element
// carries a length of -1 instead of its bytes. There is no bitmap — that
// belongs to the on-disk representation, not to what travels on the wire.
// [AppendArray] writes both from [Array.Nulls], and [DecodeArray] reads both
// back into it.

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

func trailing(body []byte) error {
	if len(body) != 0 {
		return fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return nil
}
