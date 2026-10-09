package postgres

import (
	"fmt"
	"net/netip"
)

// OIDs of PostgreSQL's two network address types.
//
// They share one binary layout and differ in a flag: `cidr` requires the bits
// below the netmask to be zero, `inet` keeps them. That is why a host address
// with a netmask — 192.168.1.1/24, an interface's own configuration — is an
// inet and cannot be a cidr.
const (
	OIDCIDR uint32 = 650
	OIDInet uint32 = 869
)

// PostgreSQL's own address family numbers.
//
// These are **not** the operating system's AF_INET and AF_INET6. PostgreSQL
// defines PGSQL_AF_INET as AF_INET on the platform it was built on plus a
// fixed offset for IPv6, precisely so that a value written on one platform
// reads the same on another — the system constants disagree across platforms
// (AF_INET6 is 10 on Linux, 30 on the BSDs, 23 on Windows). Using
// syscall.AF_INET6 here would produce bytes that are correct on exactly one
// operating system.
const (
	pgAFInet  = 2
	pgAFInet6 = 3
)

// inetHeaderSize is family, netmask bits, the is_cidr flag, and the address
// length — one byte each, before the address itself.
const inetHeaderSize = 4

// AppendInet appends an inet value: an address with a netmask length.
//
// A bare address is a prefix whose bits cover the whole address — netip's
// [netip.Addr.BitLen] — which is what PostgreSQL means by an inet with no
// mask. Note that Go and PostgreSQL render that case differently: netip prints
// "192.168.1.1/32" where PostgreSQL prints "192.168.1.1". The bytes are the
// same; only the text differs.
//
// The address family follows the address's own width. `netip.MustParseAddr`
// gives a four-byte address for "1.2.3.4" and a sixteen-byte one for
// "::ffff:1.2.3.4", and those go out as IPv4 and IPv6 respectively — because
// that is what PostgreSQL considers them, and it answers false when they are
// compared.
//
// The host bits are preserved rather than masked off. Masking would be a
// different value: 192.168.1.1/24 is a host on a network, and 192.168.1.0/24
// is the network. Silently turning one into the other is the kind of
// correctness this package refuses to trade for tidiness.
//
// An invalid prefix — the zero netip.Prefix, or one whose length does not fit
// its address — has no encoding. It is written with address family 0, which
// the server and [DecodeInet] both refuse as "invalid address family": an
// error that names the problem, rather than the IPv6 family with 255 netmask
// bits that a plain encoding of it would claim.
func AppendInet(dst []byte, p netip.Prefix) []byte { return appendNetworkAddr(dst, p, 0) }

// AppendCIDR appends a cidr value.
//
// The server requires the bits below the netmask to be zero and rejects a
// value where they are not. That check is left to it: it is the same rule
// applied by the same code that will store the value, and duplicating it here
// would mean two places to be wrong.
//
// An invalid prefix is written as [AppendInet] writes one, for the server to
// refuse.
func AppendCIDR(dst []byte, p netip.Prefix) []byte { return appendNetworkAddr(dst, p, 1) }

func appendNetworkAddr(dst []byte, p netip.Prefix, isCIDR byte) []byte {
	if !p.IsValid() {
		return append(dst, 0, 0, isCIDR, 0)
	}
	addr := p.Addr()

	// The family follows the address's own width, and an IPv4-mapped IPv6
	// address stays IPv6.
	//
	// Unmapping it would be tidier and would be wrong: PostgreSQL reports
	// family 6 for '::ffff:1.2.3.4'::inet, and answers false to
	// '::ffff:1.2.3.4'::inet = '1.2.3.4'::inet. They are two values, so a
	// client that turned one into the other on the way out would write
	// something the caller did not read — and reading a column and writing it
	// back is the most ordinary thing a client does. netip keeps the
	// distinction ([netip.Addr.Is4] against [netip.Addr.Is4In6]); this
	// follows it rather than deciding on the caller's behalf.
	family := byte(pgAFInet6)
	if addr.Is4() {
		family = pgAFInet
	}
	raw := addr.AsSlice()
	dst = append(dst, family, byte(p.Bits()), isCIDR, byte(len(raw))) //nolint:gosec // G115: a prefix length and an address length, both under 128
	return append(dst, raw...)
}

// DecodeInet decodes an inet or a cidr into a prefix.
//
// Both are accepted: they share a layout, and which one a column holds is
// already stated by its type. The flag that separates them carries nothing the
// prefix does not.
func DecodeInet(b []byte) (netip.Prefix, error) {
	if len(b) < inetHeaderSize {
		return netip.Prefix{}, fmt.Errorf("%w: network address is %d bytes, want at least %d", ErrCodec, len(b), inetHeaderSize)
	}
	family, bits, size := b[0], b[1], int(b[3])

	var want int
	switch family {
	case pgAFInet:
		want = 4
	case pgAFInet6:
		want = 16
	default:
		return netip.Prefix{}, fmt.Errorf("%w: network address family %d is neither IPv4 (%d) nor IPv6 (%d)", ErrCodec, family, pgAFInet, pgAFInet6)
	}
	if size != want {
		// The length byte and the family must agree. They are two statements
		// about the same thing, and believing the wrong one reads an address
		// out of whatever follows it.
		return netip.Prefix{}, fmt.Errorf("%w: family %d declares a %d-byte address, want %d", ErrCodec, family, size, want)
	}
	if len(b) != inetHeaderSize+size {
		return netip.Prefix{}, fmt.Errorf("%w: network address is %d bytes, want %d", ErrCodec, len(b), inetHeaderSize+size)
	}

	addr, ok := netip.AddrFromSlice(b[inetHeaderSize:])
	if !ok {
		return netip.Prefix{}, fmt.Errorf("%w: %d address bytes are not an address", ErrCodec, size)
	}
	if int(bits) > addr.BitLen() {
		return netip.Prefix{}, fmt.Errorf("%w: netmask is %d bits for a %d-bit address", ErrCodec, bits, addr.BitLen())
	}
	return netip.PrefixFrom(addr, int(bits)), nil
}
