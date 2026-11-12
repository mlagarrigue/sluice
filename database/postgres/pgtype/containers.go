package pgtype

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

// # The containers
//
// An array, a range, a multirange and a record each wrap values of other
// types, and each is written once here over a [Codec] of what it wraps. The
// bytes are those of the server's array_send, range_send, multirange_send and
// record_send (src/backend/utils/adt), read in the source rather than inferred
// from what a server happens to produce.

// Array is a PostgreSQL array of any number of dimensions, the elements laid
// out in row-major order — the last dimension varies fastest, as the server
// stores them.
//
//	// '{{1,2,3},{4,5,6}}'::int4[]
//	pgtype.Array[int32]{
//		Dims:  []pgtype.ArrayDim{{Len: 2, Lower: 1}, {Len: 3, Lower: 1}},
//		Elems: []int32{1, 2, 3, 4, 5, 6},
//	}
//
//	// '{7,NULL,9}'::int4[]
//	pgtype.Array[int32]{Elems: []int32{7, 0, 9}, Nulls: []bool{false, true, false}}
//
// Dims left nil means one dimension holding every element, numbered from 1:
// `Array[T]{Elems: values}` is the one-dimensional array most callers want,
// and takes the caller's slice as it is. An array with no elements is the
// empty array, which the server sends with no dimensions at all, and
// [DecodeArray] returns with Dims empty.
//
// The values are a []T and the NULLs a mask beside them, rather than one
// [Null] per element: an array with no NULL — the common case — is then a
// plain slice to fill and to read, and its elements are written and read by
// the same loop a []int32 gets, where the flag beside every element cost
// 17 to 24% on int4.
type Array[T any] struct {
	Dims  []ArrayDim
	Elems []T
	// Nulls marks the NULL elements: Nulls[i] says Elems[i] is NULL, its
	// value then ignored when encoding and zero when decoded. Empty means
	// no element is NULL; set, it runs parallel to Elems, and an index past
	// its end is not NULL. DecodeArray sets it only for an array that has a
	// NULL, and reuses its storage.
	Nulls []bool
}

// ArrayDim is one dimension of an [Array]: how many elements it spans, and
// the index of the first. The server numbers from 1 unless told otherwise —
// `'[0:2]={1,2,3}'::int4[]` is numbered from 0 — and the number is part of
// the value: `arr[1]` names a different element in each.
type ArrayDim struct {
	Len   int32
	Lower int32
}

// maxArrayDims is the server's MAXDIM: array_recv refuses an array of more
// dimensions, and array_send never writes one.
const maxArrayDims = 6

// AppendArray appends an array whose elements c encodes.
//
// The dimensions are written as given. When they do not multiply to
// len(a.Elems) the server refuses the parameter, which is the error a caller
// with a wrong shape should get — an encoder that cannot return one has
// nothing better to do than leave the shape to be checked where it is used.
func AppendArray[T any](dst []byte, a Array[T], c Codec[T]) []byte {
	dims := a.Dims
	if len(dims) == 0 && len(a.Elems) > 0 {
		dims = []ArrayDim{{Len: int32(len(a.Elems)), Lower: 1}} //nolint:gosec // G115: a slice's length
	}
	// The NULL flag is written once the elements say whether one is NULL.
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(dims))) //nolint:gosec // G115: a slice's length
	flags := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, 0)
	dst = binary.BigEndian.AppendUint32(dst, c.OID)
	for _, d := range dims {
		dst = binary.BigEndian.AppendUint32(dst, uint32(d.Len))   //nolint:gosec // G115: the protocol field is int32
		dst = binary.BigEndian.AppendUint32(dst, uint32(d.Lower)) //nolint:gosec // G115: the protocol field is int32
	}
	switch {
	case slices.Contains(a.Nulls, true) && c.appendElems != nil:
		// The type's loop writes each run of values between two NULLs: one
		// NULL in ten left to the general path cost int4 three times the
		// array without (BenchmarkArrayAppendInt41000Nulls).
		binary.BigEndian.PutUint32(dst[flags:], 1)
		for i := 0; i < len(a.Elems); {
			j := i
			for j < len(a.Elems) && (j >= len(a.Nulls) || !a.Nulls[j]) {
				j++
			}
			dst = c.appendElems(dst, a.Elems[i:j])
			if j < len(a.Elems) {
				dst = binary.BigEndian.AppendUint32(dst, nullElement)
				j++
			}
			i = j
		}
	case slices.Contains(a.Nulls, true):
		binary.BigEndian.PutUint32(dst[flags:], 1)
		for i, v := range a.Elems {
			if i < len(a.Nulls) && a.Nulls[i] {
				dst = binary.BigEndian.AppendUint32(dst, nullElement)
				continue
			}
			dst = appendValue(dst, v, c.Append)
		}
	case c.appendElems != nil:
		dst = c.appendElems(dst, a.Elems)
	default:
		for _, v := range a.Elems {
			dst = appendValue(dst, v, c.Append)
		}
	}
	return dst
}

// appendNullValue writes one length-prefixed value, or the length -1 that
// says NULL. The length is written in place, so a variable-width value costs
// no scratch buffer.
func appendNullValue[T any](dst []byte, v Null[T], enc func([]byte, T) []byte) []byte {
	if !v.Valid {
		return binary.BigEndian.AppendUint32(dst, nullElement)
	}
	return appendValue(dst, v.Value, enc)
}

// appendValue writes one length-prefixed value.
func appendValue[T any](dst []byte, v T, enc func([]byte, T) []byte) []byte {
	at := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, 0)
	dst = enc(dst, v)
	binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what enc wrote
	return dst
}

// DecodeArray decodes an array of any shape whose elements c decodes,
// reusing the storage of dst.
//
// The element type in the header must be c.OID: an int8[] read with the
// codec of int4 is refused rather than misread, and so is an empty one, which
// still names its element type.
func DecodeArray[T any](dst Array[T], b []byte, c Codec[T]) (Array[T], error) {
	dst.Dims, dst.Elems, dst.Nulls = dst.Dims[:0], dst.Elems[:0], dst.Nulls[:0]
	if len(b) < 12 {
		return dst, fmt.Errorf("%w: array header is %d bytes, want at least 12", ErrCodec, len(b))
	}
	ndim := int32(binary.BigEndian.Uint32(b[0:4])) //nolint:gosec // G115: the protocol field is int32
	flags := binary.BigEndian.Uint32(b[4:8])
	switch got := binary.BigEndian.Uint32(b[8:12]); {
	case got != c.OID:
		return dst, fmt.Errorf("%w: array element type is OID %d, want %d", ErrCodec, got, c.OID)
	case flags > 1:
		return dst, fmt.Errorf("%w: array flags are %#x, want 0 or 1", ErrCodec, flags)
	case ndim < 0 || ndim > maxArrayDims:
		return dst, fmt.Errorf("%w: array has %d dimensions, the server allows 0 to %d", ErrCodec, ndim, maxArrayDims)
	case len(b) < 12+8*int(ndim):
		return dst, fmt.Errorf("%w: array of %d dimensions has a %d-byte header", ErrCodec, ndim, len(b))
	}
	body := b[12+8*int(ndim):]
	// The element count is bounded by the bytes that carry the elements —
	// four at least each, the length word of a NULL — before anything is
	// sized from it. A header claiming 2^31 elements in a 30-byte value is
	// refused here, not allocated for.
	n := 1
	if ndim == 0 {
		n = 0
	}
	for i := range int(ndim) {
		d := ArrayDim{
			Len:   int32(binary.BigEndian.Uint32(b[12+8*i:])), //nolint:gosec // G115: the protocol field is int32
			Lower: int32(binary.BigEndian.Uint32(b[16+8*i:])), //nolint:gosec // G115: the protocol field is int32
		}
		// A dimension of no elements is an empty array, which the server
		// sends as zero dimensions and never as this; and a dimension whose
		// last index does not fit an int32 is one array_recv refuses.
		if d.Len <= 0 || int64(d.Lower)+int64(d.Len)-1 > math.MaxInt32 {
			return dst.reset(), fmt.Errorf("%w: array dimension %d has %d elements from %d", ErrCodec, i, d.Len, d.Lower)
		}
		if int(d.Len) > len(body)/4/n {
			return dst.reset(), fmt.Errorf("%w: array declares more elements than its %d bytes can hold", ErrCodec, len(body))
		}
		n *= int(d.Len)
		dst.Dims = append(dst.Dims, d)
	}
	var err error
	if c.decodeElems != nil {
		// The type's loop reads what it expects; the general path takes the
		// one element it stops at — a NULL, an unexpected length — and the
		// loop resumes after it. Handing the general path everything from
		// the first NULL on cost int4 five times the array without, at one
		// NULL in ten (BenchmarkArrayDecodeInt41000Nulls).
		for len(dst.Elems) < n {
			start := len(dst.Elems)
			if dst.Elems, body, err = c.decodeElems(dst.Elems, body, n-start); err != nil {
				return dst.reset(), err
			}
			if m, k := len(dst.Nulls), len(dst.Elems)-start; m > 0 && k > 0 {
				dst.Nulls = slices.Grow(dst.Nulls, k)[:m+k]
				clear(dst.Nulls[m:])
			}
			if len(dst.Elems) == n {
				break
			}
			if len(body) >= 4 && binary.BigEndian.Uint32(body) == nullElement {
				if len(dst.Nulls) == 0 { // the first NULL: the mask, sized once
					dst.Nulls = slices.Grow(dst.Nulls[:0], n)[:len(dst.Elems)]
					clear(dst.Nulls)
				}
				var zero T
				dst.Elems = append(dst.Elems, zero)
				dst.Nulls = append(dst.Nulls, true)
				body = body[4:]
				continue
			}
			if dst, body, err = otherElements(dst, body, len(dst.Elems), len(dst.Elems)+1, c.Decode); err != nil {
				return dst.reset(), err
			}
		}
	}
	if dst, body, err = otherElements(dst, body, len(dst.Elems), n, c.Decode); err != nil {
		return dst.reset(), err
	}
	if err := trailing(body); err != nil {
		return dst.reset(), err
	}
	return dst, nil
}

// reset is the array a failed decode returns: dst's storage, nothing in it.
func (a Array[T]) reset() Array[T] {
	return Array[T]{Dims: a.Dims[:0], Elems: a.Elems[:0], Nulls: a.Nulls[:0]}
}

// Range is a PostgreSQL range: two bounds, each included or not, either
// absent, or the empty range.
//
//	// '[2024-01-01,2024-02-01)'::daterange
//	pgtype.Range[time.Time]{
//		Lower: pgtype.RangeBound[time.Time]{Value: jan1, Inclusive: true},
//		Upper: pgtype.RangeBound[time.Time]{Value: feb1},
//	}
//
// The server puts a range in canonical form before it stores or sends it:
// an int4range written `[1,5]` comes back `[1,6)`, and `[3,3)` comes back
// empty. A Range decoded is the canonical one, so comparing it with the one
// encoded is comparing two spellings of what may be one range.
//
// Range is comparable with == when T is, and two equal Ranges are the same
// range only once both are canonical.
type Range[T any] struct {
	Lower, Upper RangeBound[T]
	// Empty is the range of no values. Its bounds are ignored when encoding
	// and zero when decoded: the server keeps none.
	Empty bool
}

// RangeBound is one end of a [Range].
//
// Unbounded is the absent bound — `(,5)` — which is not the same as a bound
// whose value is infinite: `[-infinity,infinity]` is a tsrange with two
// bounds, both included, and it does not contain the timestamps outside them
// because there are none. The server calls the absent bound "infinite"
// (lower_inf, upper_inf); this package does not, because Go's time and float
// types have values that are.
type RangeBound[T any] struct {
	Value T
	// Inclusive says the bound's value is in the range. An absent bound is
	// never inclusive, as the server keeps it.
	Inclusive bool
	// Unbounded says there is no bound on this side; Value is then ignored.
	Unbounded bool
}

// The flag byte a range starts with (rangetypes.h). RANGE_LB_NULL and
// RANGE_UB_NULL are reserved and never sent; RANGE_CONTAIN_EMPTY marks index
// pages and never leaves them.
const (
	rangeEmpty   = 0x01
	rangeLBInc   = 0x02
	rangeUBInc   = 0x04
	rangeLBInf   = 0x08
	rangeUBInf   = 0x10
	rangeAnyFlag = rangeEmpty | rangeLBInc | rangeUBInc | rangeLBInf | rangeUBInf
)

// AppendRange appends a range whose bounds c encodes.
//
// The flags are written as range_send writes them: the empty range carries no
// other, and an absent bound is never inclusive, whatever r.Lower.Inclusive
// says. The bytes of a canonical range are then the server's own.
func AppendRange[T any](dst []byte, r Range[T], c Codec[T]) []byte {
	if r.Empty {
		return append(dst, rangeEmpty)
	}
	var flags byte
	switch {
	case r.Lower.Unbounded:
		flags |= rangeLBInf
	case r.Lower.Inclusive:
		flags |= rangeLBInc
	}
	switch {
	case r.Upper.Unbounded:
		flags |= rangeUBInf
	case r.Upper.Inclusive:
		flags |= rangeUBInc
	}
	dst = append(dst, flags)
	if !r.Lower.Unbounded {
		dst = appendValue(dst, r.Lower.Value, c.Append)
	}
	if !r.Upper.Unbounded {
		dst = appendValue(dst, r.Upper.Value, c.Append)
	}
	return dst
}

// DecodeRange decodes a range whose bounds c decodes.
//
// The flags are held to what range_send writes: a flag it never sets, an
// empty range with any other, or an absent bound marked inclusive is refused,
// as reading the value at the wrong offset — which is what each of them
// means — and not believed.
func DecodeRange[T any](b []byte, c Codec[T]) (Range[T], error) {
	if len(b) < 1 {
		return Range[T]{}, fmt.Errorf("%w: range is empty, want at least its flags", ErrCodec)
	}
	flags := b[0]
	switch {
	case flags&^rangeAnyFlag != 0:
		return Range[T]{}, fmt.Errorf("%w: range flags are %#x", ErrCodec, flags)
	case flags&rangeEmpty != 0 && flags != rangeEmpty:
		return Range[T]{}, fmt.Errorf("%w: empty range with flags %#x", ErrCodec, flags)
	case flags&(rangeLBInf|rangeLBInc) == rangeLBInf|rangeLBInc, flags&(rangeUBInf|rangeUBInc) == rangeUBInf|rangeUBInc:
		return Range[T]{}, fmt.Errorf("%w: range flags %#x make an absent bound inclusive", ErrCodec, flags)
	case flags == rangeEmpty:
		if len(b) != 1 {
			return Range[T]{}, fmt.Errorf("%w: %d bytes after an empty range", ErrCodec, len(b)-1)
		}
		return Range[T]{Empty: true}, nil
	}
	r := Range[T]{
		Lower: RangeBound[T]{Inclusive: flags&rangeLBInc != 0, Unbounded: flags&rangeLBInf != 0},
		Upper: RangeBound[T]{Inclusive: flags&rangeUBInc != 0, Unbounded: flags&rangeUBInf != 0},
	}
	body := b[1:]
	var err error
	if !r.Lower.Unbounded {
		if r.Lower.Value, body, err = rangeBound(body, "lower", c.Decode); err != nil {
			return Range[T]{}, err
		}
	}
	if !r.Upper.Unbounded {
		if r.Upper.Value, body, err = rangeBound(body, "upper", c.Decode); err != nil {
			return Range[T]{}, err
		}
	}
	if len(body) != 0 {
		return Range[T]{}, fmt.Errorf("%w: %d bytes after the range's bounds", ErrCodec, len(body))
	}
	return r, nil
}

// rangeBound reads one length-prefixed bound. A range has no NULL bound, so
// the length -1 an array uses for one is refused with the other negatives.
func rangeBound[T any](body []byte, side string, dec func([]byte) (T, error)) (v T, rest []byte, err error) {
	if len(body) < 4 {
		return v, nil, fmt.Errorf("%w: range %s bound has no length", ErrCodec, side)
	}
	size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
	if size < 0 || size > len(body)-4 {
		return v, nil, fmt.Errorf("%w: range %s bound declares %d bytes, %d remain", ErrCodec, side, size, len(body)-4)
	}
	if v, err = dec(body[4 : 4+size]); err != nil {
		return v, nil, fmt.Errorf("range %s bound: %w", side, err)
	}
	return v, body[4+size:], nil
}

// Multirange is a PostgreSQL multirange (PostgreSQL 14): ranges that neither
// overlap nor touch, in order. The server merges what it is given into that
// form, so `{[1,3),[2,5)}` comes back `{[1,5)}`; a Multirange decoded is
// always the merged one.
type Multirange[T any] []Range[T]

// AppendMultirange appends a multirange whose ranges' bounds c encodes.
func AppendMultirange[T any](dst []byte, m Multirange[T], c Codec[T]) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m))) //nolint:gosec // G115: a slice's length
	for _, r := range m {
		at := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = AppendRange(dst, r, c)
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(len(dst)-at-4)) //nolint:gosec // G115: bounded by what AppendRange wrote
	}
	return dst
}

// DecodeMultirange decodes a multirange whose ranges' bounds c decodes,
// reusing the storage of dst.
func DecodeMultirange[T any](dst Multirange[T], b []byte, c Codec[T]) (Multirange[T], error) {
	dst = dst[:0]
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: multirange is %d bytes, want at least 4", ErrCodec, len(b))
	}
	// Each range is five bytes at least, a length and its flags: the count is
	// held to the bytes that follow before anything is sized from it.
	n := int(int32(binary.BigEndian.Uint32(b))) //nolint:gosec // G115: the protocol field is int32
	body := b[4:]
	if n < 0 || n > len(body)/5 {
		return dst, fmt.Errorf("%w: multirange declares %d ranges in %d bytes", ErrCodec, n, len(body))
	}
	for i := range n {
		if len(body) < 4 {
			return dst[:0], fmt.Errorf("%w: multirange range %d has no length", ErrCodec, i)
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		if size < 0 || size > len(body)-4 {
			return dst[:0], fmt.Errorf("%w: multirange range %d declares %d bytes, %d remain", ErrCodec, i, size, len(body)-4)
		}
		r, err := DecodeRange(body[4:4+size], c)
		if err != nil {
			return dst[:0], fmt.Errorf("multirange range %d: %w", i, err)
		}
		dst = append(dst, r)
		body = body[4+size:]
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last range", ErrCodec, len(body))
	}
	return dst, nil
}

// Record is a composite value — a row of a table type, a `CREATE TYPE … AS`,
// or the anonymous `ROW(…)` a query builds — as its fields arrive: each one's
// type OID and its bytes, still encoded. A field's type is known only once
// the value is read, which is why the fields stay bytes here and are decoded
// one by one with [DecodeRecordField] and the Codec of the type the caller
// expects.
//
//	rec, err := pgtype.DecodeRecord(rec, raw)
//	id, err := pgtype.DecodeRecordField(rec[0], pgtype.Int8Codec())
//
// The server sends no field names, and skips the columns a table has
// dropped: field i is the i-th live column.
type Record []RecordField

// RecordField is one field of a [Record]. A nil Value is NULL, and an empty
// one is a value of no bytes, as in a statement's parameter.
type RecordField struct {
	OID   uint32
	Value []byte
}

// AppendRecord appends a composite value. The server reads it only as a
// named composite type — record_recv refuses the anonymous record — and
// checks each field's OID against the type's column when both are built-in.
func AppendRecord(dst []byte, r Record) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r))) //nolint:gosec // G115: a slice's length
	for _, f := range r {
		dst = binary.BigEndian.AppendUint32(dst, f.OID)
		if f.Value == nil {
			dst = binary.BigEndian.AppendUint32(dst, nullElement)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(f.Value))) //nolint:gosec // G115: a value's length
		dst = append(dst, f.Value...)
	}
	return dst
}

// DecodeRecord decodes a composite value, reusing the storage of dst.
//
// The fields' bytes are copied, once, into one buffer they share: they are
// decoded after this returns, and the wire buffer they came from is
// overwritten by the next batch.
func DecodeRecord(dst Record, b []byte) (Record, error) {
	dst = dst[:0]
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: record is %d bytes, want at least 4", ErrCodec, len(b))
	}
	// Each field is eight bytes at least, its OID and its length.
	n := int(int32(binary.BigEndian.Uint32(b))) //nolint:gosec // G115: the protocol field is int32
	if n < 0 || n > (len(b)-4)/8 {
		return dst, fmt.Errorf("%w: record declares %d fields in %d bytes", ErrCodec, n, len(b)-4)
	}
	own := append([]byte(nil), b...)
	body := own[4:]
	for i := range n {
		if len(body) < 8 {
			return dst[:0], fmt.Errorf("%w: record field %d has no type and length", ErrCodec, i)
		}
		f := RecordField{OID: binary.BigEndian.Uint32(body)}
		size := int(int32(binary.BigEndian.Uint32(body[4:]))) //nolint:gosec // G115: the protocol field is int32
		body = body[8:]
		switch {
		case size == -1:
		case size < 0 || size > len(body):
			return dst[:0], fmt.Errorf("%w: record field %d declares %d bytes, %d remain", ErrCodec, i, size, len(body))
		default:
			f.Value, body = body[:size:size], body[size:]
		}
		dst = append(dst, f)
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last record field", ErrCodec, len(body))
	}
	return dst, nil
}

// DecodeRecordField decodes one field of a [Record] with the Codec of the
// type the caller expects, refusing a field of another type: the field
// carries its OID, and a bigint read as an integer is the misreading it is
// there to prevent.
func DecodeRecordField[T any](f RecordField, c Codec[T]) (Null[T], error) {
	if f.OID != c.OID {
		return Null[T]{}, fmt.Errorf("%w: record field has type OID %d, want %d", ErrCodec, f.OID, c.OID)
	}
	if f.Value == nil {
		return Null[T]{}, nil
	}
	v, err := c.Decode(f.Value)
	if err != nil {
		return Null[T]{}, err
	}
	return Null[T]{Value: v, Valid: true}, nil
}
