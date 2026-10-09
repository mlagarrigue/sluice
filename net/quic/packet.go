package quic

import "fmt"

// MaxConnectionIDLen is the longest connection identifier QUIC version 1
// allows (RFC 9000 §17.2). The field that carries it is a byte, so a peer can
// name a longer one; refusing it here is what keeps a length from becoming a
// read past the datagram.
const maxConnectionIDLen = 20

// Long-header packet types (RFC 9000 §17.2), in the two bits below the fixed
// bit.
const (
	packetInitial   = 0x0
	packetZeroRTT   = 0x1
	packetHandshake = 0x2
	packetRetry     = 0x3
)

// Version1 is the QUIC version this package speaks. A version it does not
// know is not guessed at: the header format itself is version-specific past
// the first five bytes.
const version1 = 0x00000001

// LongHeader is a parsed long-header packet, up to but not including its
// protected payload.
//
// Everything borrows the datagram.
type longHeader struct {
	Type    byte
	Version uint32
	DCID    []byte
	SCID    []byte

	// Token is an Initial packet's, empty otherwise.
	Token []byte

	// Length is the declared length of the packet number and payload
	// together, which is what separates one packet from the next in a
	// coalesced datagram.
	Length uint64

	// PNOffset is where the packet number starts, relative to the start of
	// the packet. Header protection needs it, and it cannot be recovered
	// afterwards because the packet-number length is itself protected.
	PNOffset int

	// Raw is the whole packet: header, packet number and payload, exactly
	// Length bytes past PNOffset.
	Raw []byte
}

// ParseLongHeader decodes one long-header packet off the front of a datagram
// and returns what follows it.
//
// A datagram may carry several packets end to end (RFC 9000 §12.2), which is
// why this returns a remainder rather than requiring the datagram to hold
// exactly one.
//
// The packet number is *not* decoded: it is protected, and unprotecting it
// needs keys this function does not have. [Opener.Open] does both together,
// because doing them apart is how a header gets used before it has been
// authenticated.
func parseLongHeader(b []byte) (longHeader, []byte, error) {
	var h longHeader
	if len(b) < 7 {
		return h, nil, ErrTruncated
	}
	first := b[0]
	if first&0x80 == 0 {
		return h, nil, fmt.Errorf("%w: not a long header", ErrQUIC)
	}
	if first&0x40 == 0 {
		// The fixed bit. It is called fixed because it is always one, and a
		// packet without it is not QUIC — it is what lets a demultiplexer
		// tell QUIC from other things on the same port.
		return h, nil, fmt.Errorf("%w: the fixed bit is clear", ErrQUIC)
	}
	h.Type = (first >> 4) & 0x03
	h.Version = uint32(b[1])<<24 | uint32(b[2])<<16 | uint32(b[3])<<8 | uint32(b[4])
	if h.Version != version1 {
		// Version 0 is version negotiation and anything else is a version
		// whose header this package cannot read past here.
		return h, nil, fmt.Errorf("%w: version %#x", ErrQUIC, h.Version)
	}
	rest := b[5:]

	var err error
	if h.DCID, rest, err = connectionID(rest); err != nil {
		return h, nil, err
	}
	if h.SCID, rest, err = connectionID(rest); err != nil {
		return h, nil, err
	}

	if h.Type == packetRetry {
		// Retry carries a token and a sixteen-byte integrity tag, and no
		// length or packet number at all.
		if len(rest) < 16 {
			return h, nil, ErrTruncated
		}
		h.Token = rest[:len(rest)-16]
		h.Raw = b
		return h, nil, nil
	}

	if h.Type == packetInitial {
		var n uint64
		if n, rest, err = Varint(rest); err != nil {
			return h, nil, err
		}
		if n > uint64(len(rest)) {
			return h, nil, ErrTruncated
		}
		if h.Token, rest, err = bytesN(rest, int(n)); err != nil { //nolint:gosec // G115: checked above
			return h, nil, err
		}
	}

	if h.Length, rest, err = Varint(rest); err != nil {
		return h, nil, err
	}
	if h.Length > uint64(len(rest)) {
		return h, nil, ErrTruncated
	}
	h.PNOffset = len(b) - len(rest)
	end := h.PNOffset + int(h.Length) //nolint:gosec // G115: checked against len(rest)
	h.Raw = b[:end]
	return h, b[end:], nil
}

// ShortHeader is a 1-RTT packet's header. Its connection identifier has no
// length on the wire — the endpoint chose it and is expected to recognise it
// — so the caller says how long its own identifiers are.
type shortHeader struct {
	DCID []byte

	// PNOffset is where the packet number starts, as in [LongHeader].
	PNOffset int

	// Raw is the whole packet: a short-header packet has no length field and
	// therefore runs to the end of the datagram, which is why it can only be
	// the last packet in one.
	Raw []byte
}

// ParseShortHeader decodes a 1-RTT packet, given the length of the connection
// identifiers this endpoint issued.
func parseShortHeader(b []byte, dcidLen int) (shortHeader, error) {
	var h shortHeader
	if dcidLen < 0 || dcidLen > maxConnectionIDLen {
		return h, fmt.Errorf("%w: a connection identifier length of %d", ErrQUIC, dcidLen)
	}
	if len(b) < 1+dcidLen {
		return h, ErrTruncated
	}
	if b[0]&0x80 != 0 {
		return h, fmt.Errorf("%w: not a short header", ErrQUIC)
	}
	if b[0]&0x40 == 0 {
		return h, fmt.Errorf("%w: the fixed bit is clear", ErrQUIC)
	}
	h.DCID = b[1 : 1+dcidLen]
	h.PNOffset = 1 + dcidLen
	h.Raw = b
	return h, nil
}

// appendVersionNegotiation writes a Version Negotiation packet (RFC 9000
// §17.2.1): version zero, the triggering packet's identifiers echoed swapped
// — dcid is the sender's source identifier, scid the destination it named —
// and the versions this endpoint actually serves. The low seven bits of the
// first byte carry no meaning in this packet; RFC 9000 suggests randomising
// them against fingerprinting, a traffic-analysis concern this package does
// not take on (the Retry builder makes the same call).
func appendVersionNegotiation(dst, dcid, scid []byte, versions ...uint32) []byte {
	dst = append(dst, 0xc0, 0, 0, 0, 0, byte(len(dcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes; version 0 is what marks the packet
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(scid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	dst = append(dst, scid...)
	for _, v := range versions {
		dst = append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: big-endian serialisation keeps the low octet of each shift
	}
	return dst
}

// connectionID reads a length byte and that many bytes.
func connectionID(b []byte) (id, rest []byte, err error) {
	if len(b) == 0 {
		return nil, nil, ErrTruncated
	}
	n := int(b[0])
	if n > maxConnectionIDLen {
		return nil, nil, fmt.Errorf("%w: a connection identifier of %d bytes, over the %d allowed",
			ErrQUIC, n, maxConnectionIDLen)
	}
	return bytesN(b[1:], n)
}
