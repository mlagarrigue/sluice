package pgtype

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

// # Types an extension creates
//
// hstore, citext, ltree and pgvector's vector have no OID of their own: each
// database that runs CREATE EXTENSION gives the type the next free one, so
// the number differs from one database to the next and cannot be a constant
// here. Their Codecs take the OIDs as arguments — read once per database by
// postgres.Conn.LookupType — and are otherwise built-in Codecs like the rest.
//
// The text these types carry is sent in the client encoding, which this
// connector sets to UTF8 at startup; the lengths hstore writes are of the
// converted text, so they agree with what follows them.

// CitextCodec returns the Codec of citext, the case-insensitive text of the
// citext extension: text on the wire, compared without case by the server
// only.
func CitextCodec(oid, arrayOID uint32) Codec[string] {
	return Codec[string]{OID: oid, ArrayOID: arrayOID, Append: AppendText, Decode: decodeTextValue}
}

// Hstore is a value of the hstore extension's type: keys, each with a value
// that may be NULL. A key cannot be NULL, and appears once.
type Hstore map[string]Null[string]

// HstoreCodec returns the Codec of hstore.
func HstoreCodec(oid, arrayOID uint32) Codec[Hstore] {
	return Codec[Hstore]{OID: oid, ArrayOID: arrayOID, Append: AppendHstore, Decode: DecodeHstore}
}

// AppendHstore appends an hstore: the pair count, then each key and value,
// length first, a NULL value as the length -1.
//
// The pairs are written in the order hstore_send writes them — shorter keys
// first, then by byte — so the bytes do not depend on Go's map order and
// are the server's own.
func AppendHstore(dst []byte, h Hstore) []byte {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(a), len(b)), cmp.Compare(a, b))
	})
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(h))) //nolint:gosec // G115: a map's length
	for _, k := range keys {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(k))) //nolint:gosec // G115: a string's length
		dst = append(dst, k...)
		dst = appendNullValue(dst, h[k], AppendText)
	}
	return dst
}

// DecodeHstore decodes an hstore. A NULL key, a key sent twice, or a count
// the bytes cannot hold is refused: hstore_send writes none of them.
func DecodeHstore(b []byte) (Hstore, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("%w: hstore is %d bytes, want at least 4", ErrCodec, len(b))
	}
	// Each pair is eight bytes at least, the key's length and the value's.
	n := int(int32(binary.BigEndian.Uint32(b))) //nolint:gosec // G115: the protocol field is int32
	body := b[4:]
	if n < 0 || n > len(body)/8 {
		return nil, fmt.Errorf("%w: hstore declares %d pairs in %d bytes", ErrCodec, n, len(body))
	}
	h := make(Hstore, n)
	for i := range n {
		key, rest, isNull, err := nextElement(body, i)
		switch {
		case err != nil:
			return nil, fmt.Errorf("hstore key: %w", err)
		case isNull:
			return nil, fmt.Errorf("%w: hstore key %d is NULL", ErrCodec, i)
		}
		val, rest, isNull, err := nextElement(rest, i)
		if err != nil {
			return nil, fmt.Errorf("hstore value: %w", err)
		}
		body = rest
		k := string(key)
		if _, dup := h[k]; dup {
			return nil, fmt.Errorf("%w: hstore key %q appears twice", ErrCodec, k)
		}
		if isNull {
			h[k] = Null[string]{}
		} else {
			h[k] = Null[string]{Value: string(val), Valid: true}
		}
	}
	if err := trailing(body); err != nil {
		return nil, err
	}
	return h, nil
}

// ltreeVersion is the version byte ltree_send, lquery_send and ltxtq_send
// write before the text (contrib/ltree/ltree_io.c); ltree_recv refuses any
// other.
const ltreeVersion = 1

// LtreeCodec returns the Codec of ltree, a label path such as `top.science`.
func LtreeCodec(oid, arrayOID uint32) Codec[string] {
	return Codec[string]{OID: oid, ArrayOID: arrayOID, Append: AppendLtree, Decode: DecodeLtree}
}

// LqueryCodec returns the Codec of lquery, ltree's pattern type
// (`*.science.*`), which travels in the same form as ltree.
func LqueryCodec(oid, arrayOID uint32) Codec[string] {
	return Codec[string]{OID: oid, ArrayOID: arrayOID, Append: AppendLtree, Decode: DecodeLtree}
}

// LtxtqueryCodec returns the Codec of ltxtquery, ltree's full-text pattern
// type (`science & !physics`), which travels in the same form as ltree.
func LtxtqueryCodec(oid, arrayOID uint32) Codec[string] {
	return Codec[string]{OID: oid, ArrayOID: arrayOID, Append: AppendLtree, Decode: DecodeLtree}
}

// AppendLtree appends an ltree, an lquery or an ltxtquery: a version byte,
// then the text. The server parses the text; a malformed path is its
// refusal to make.
func AppendLtree(dst []byte, s string) []byte {
	return append(append(dst, ltreeVersion), s...)
}

// DecodeLtree decodes an ltree, an lquery or an ltxtquery, refusing a version
// byte other than the one the server writes.
func DecodeLtree(b []byte) (string, error) {
	switch {
	case len(b) == 0:
		return "", fmt.Errorf("%w: ltree has no version byte", ErrCodec)
	case b[0] != ltreeVersion:
		return "", fmt.Errorf("%w: ltree version %d, want %d", ErrCodec, b[0], ltreeVersion)
	}
	return string(b[1:]), nil
}

// vectorMaxDim is pgvector's VECTOR_MAX_DIM: vector_recv refuses more.
const vectorMaxDim = 16000

// VectorCodec returns the Codec of pgvector's vector.
func VectorCodec(oid, arrayOID uint32) Codec[[]float32] {
	return Codec[[]float32]{OID: oid, ArrayOID: arrayOID, Append: AppendVector, Decode: func(b []byte) ([]float32, error) {
		return DecodeVector(nil, b)
	}}
}

// AppendVector appends a pgvector vector: the dimension and an unused word as
// two int16, then each element as a float4. The server refuses an empty
// vector, more than 16,000 dimensions, NaN and infinities; those are its
// refusals to make.
func AppendVector(dst []byte, v []float32) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(v))) //nolint:gosec // G115: bounded by the server's refusal past 16000
	dst = binary.BigEndian.AppendUint16(dst, 0)
	for _, x := range v {
		dst = binary.BigEndian.AppendUint32(dst, math.Float32bits(x))
	}
	return dst
}

// DecodeVector decodes a pgvector vector, appending its elements to dst. The
// dimension is held to what vector_send writes — 1 to 16,000, the unused word
// zero — and to the bytes that follow.
func DecodeVector(dst []float32, b []byte) ([]float32, error) {
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: vector is %d bytes, want at least 4", ErrCodec, len(b))
	}
	dim := int(int16(binary.BigEndian.Uint16(b))) //nolint:gosec // G115: the field is int16
	switch unused := binary.BigEndian.Uint16(b[2:]); {
	case dim < 1 || dim > vectorMaxDim:
		return dst, fmt.Errorf("%w: vector has %d dimensions, want 1 to %d", ErrCodec, dim, vectorMaxDim)
	case unused != 0:
		return dst, fmt.Errorf("%w: vector's unused word is %d", ErrCodec, unused)
	case len(b) != 4+4*dim:
		return dst, fmt.Errorf("%w: vector of %d dimensions is %d bytes, want %d", ErrCodec, dim, len(b), 4+4*dim)
	}
	dst = slices.Grow(dst, dim)
	for i := range dim {
		dst = append(dst, math.Float32frombits(binary.BigEndian.Uint32(b[4+4*i:])))
	}
	return dst, nil
}
