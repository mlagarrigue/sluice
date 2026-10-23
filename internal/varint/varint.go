// Package varint is QUIC's variable-length integer (RFC 9000 §16), shared by
// net/quic and the HTTP/3 layer of net/httpstream, which frames its own
// streams with the same encoding (RFC 9114 §7.1). It is internal: the
// encoding is mechanics of those two packages, not something their callers
// handle.
package varint

import "errors"

// Max is the largest value a variable-length integer can carry: the encoding
// spends two bits of the first byte on the length, so 62 bits are left.
const Max = 1<<62 - 1

// ErrTruncated reports a buffer that ends inside an integer.
var ErrTruncated = errors.New("varint: the buffer ends mid-integer")

// Read decodes a variable-length integer and returns the rest of the buffer.
//
// The two most significant bits of the first byte give the length — one, two,
// four or eight bytes — and the value occupies what is left. The encoding is
// not canonical: 0 may be written in any of the four widths, and a decoder
// that refused the long forms would refuse traffic the RFC permits.
func Read(b []byte) (v uint64, rest []byte, err error) {
	if len(b) == 0 {
		return 0, nil, ErrTruncated
	}
	n := 1 << (b[0] >> 6) // 1, 2, 4 or 8
	if len(b) < n {
		return 0, nil, ErrTruncated
	}
	v = uint64(b[0] & 0x3f)
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, b[n:], nil
}

// Append writes v in the shortest form that holds it.
//
// Shortest rather than fixed-width: the encoding allows either, and a
// protocol whose lengths are mostly small pays for the choice on every frame.
// Append panics on a value past [Max], which is a programming error rather
// than a datum — there is no way to write it.
func Append(dst []byte, v uint64) []byte {
	switch {
	case v <= 63:
		return append(dst, byte(v))
	case v <= 16383:
		return append(dst, byte(v>>8)|0x40, byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	case v <= 1073741823:
		return append(dst, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	case v <= Max:
		return append(dst, byte(v>>56)|0xc0, byte(v>>48), byte(v>>40), byte(v>>32), //nolint:gosec // G115: varint encoding keeps the low octet of each shift
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	}
	panic("varint: a value larger than a variable-length integer can carry")
}
