package httpstream

// HPACK encoding, deliberately minimal.
//
// Decoding must be complete because the peer chooses the representation.
// Encoding may choose, and this chooses the simplest one the format allows:
// every field as a literal without indexing, names uncompressed, no Huffman.
// That is conformant HPACK — a decoder must accept it — and it costs bytes on
// the wire rather than correctness.
//
// The trade is deliberate. An encoder that indexes has to keep a dynamic table
// in step with the peer's, and a table that drifts corrupts every later block
// on the connection silently. That is the failure this package spent its
// decoder's documentation on, and there is no reason to buy it back on the
// way out for a header or two of bandwidth.

// appendIndexed writes a field whose name is in the static table but whose
// value is not, as a literal-without-indexing with a name index (§6.2.2).
// Generic over string and []byte so the response path feeds Header fields in
// without a per-field conversion to string, which escapes and allocates.
//
// A credential-bearing field goes out as never-indexed instead (§6.2.3, same
// shape, one flag bit): this encoder indexes nothing either way, but the bit
// travels with the field and binds every intermediary that re-encodes it not
// to put the value in a table, where §7.1's compression-ratio attacks probe it.
func appendHPACK[N, V ~string | ~[]byte](dst []byte, name N, value V) []byte {
	flags := byte(0x00)
	if sensitiveField(name) {
		flags = 0x10
	}
	if idx := staticNameIndex(name); idx > 0 {
		dst = appendHPACKInt(dst, uint64(idx), 4, flags)
	} else {
		dst = append(dst, flags) // a literal name follows
		dst = appendHPACKString(dst, name)
	}
	return appendHPACKString(dst, value)
}

// sensitiveField reports the response fields RFC 7541 §7.1.3 has in mind for
// never-indexed: the ones that carry a credential.
func sensitiveField[N ~string | ~[]byte](name N) bool {
	switch string(name) {
	case "set-cookie", "authorization", "proxy-authorization":
		return true
	}
	return false
}

// appendHPACKStatus writes :status, which is the one field worth taking from
// the static table by value: 200 and 204 are indexed entries on their own.
func appendHPACKStatus(dst []byte, status int) []byte {
	switch status {
	case 200:
		return append(dst, 0x88) // index 8
	case 204:
		return append(dst, 0x89) // index 9
	case 206:
		return append(dst, 0x8a)
	case 304:
		return append(dst, 0x8b)
	case 400:
		return append(dst, 0x8c)
	case 404:
		return append(dst, 0x8d)
	case 500:
		return append(dst, 0x8e)
	}
	return appendHPACK(dst, ":status", itoa(status))
}

// staticNameIndex returns the first static-table index whose name matches, or
// zero. The table is small and this runs once per response header, so it is a
// scan rather than a map nobody would see the benefit of.
func staticNameIndex[N ~string | ~[]byte](name N) int {
	for i, e := range &staticTable {
		// A string(x) that only feeds a comparison does not allocate.
		if e.Name == string(name) {
			return i + 1 // the table is one-based
		}
	}
	return 0
}

// appendHPACKInt writes RFC 7541 §5.1's prefixed integer, with the given
// prefix width and the flag bits that go above it.
func appendHPACKInt(dst []byte, v uint64, n uint8, flags byte) []byte {
	mask := uint64(1)<<n - 1
	if v < mask {
		return append(dst, flags|byte(v)) //nolint:gosec // G115: v is below the prefix mask here
	}
	dst = append(dst, flags|byte(mask)) //nolint:gosec // G115: the mask is at most 8 bits
	v -= mask
	for v >= 0x80 {
		dst = append(dst, byte(v&0x7f)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// appendHPACKString writes a string literal without Huffman coding.
func appendHPACKString[S ~string | ~[]byte](dst []byte, s S) []byte {
	dst = appendHPACKInt(dst, uint64(len(s)), 7, 0x00)
	return append(dst, s...)
}

// itoa is strconv.Itoa without the import, for the handful of status codes
// that are not in the static table.
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// lowerASCII reports whether a field name is already lowercase, which HTTP/2
// requires (RFC 9113 §8.2.1). A name with an uppercase letter is malformed
// rather than normalised: normalising is how a header means one thing to this
// hop and another to the next. Generic so the hot paths hand it their []byte
// names directly instead of converting per field.
func lowerASCII[S ~string | ~[]byte](s S) bool {
	for i := range len(s) {
		if c := s[i]; 'A' <= c && c <= 'Z' {
			return false
		}
	}
	return true
}
