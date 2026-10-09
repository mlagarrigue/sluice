package quic

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// Transport parameters, RFC 9000 §18. They travel inside the TLS handshake
// rather than in QUIC frames, which is what makes them authenticated by the
// same exchange that establishes the keys.

// Parameter identifiers this package writes and reads.
const (
	paramOriginalDCID          = 0x00
	paramMaxIdleTimeout        = 0x01
	paramStatelessResetToken   = 0x02
	paramMaxUDPPayloadSize     = 0x03
	paramInitialMaxData        = 0x04
	paramInitialMaxStreamBidiL = 0x05
	paramInitialMaxStreamBidiR = 0x06
	paramInitialMaxStreamUni   = 0x07
	paramInitialMaxStreamsBidi = 0x08
	paramInitialMaxStreamsUni  = 0x09
	paramAckDelayExponent      = 0x0a
	paramMaxAckDelay           = 0x0b
	paramPreferredAddress      = 0x0d
	paramActiveConnIDLimit     = 0x0e
	paramInitialSourceCID      = 0x0f
	paramRetrySourceCID        = 0x10
)

// TransportParameters are the limits each endpoint imposes on the other.
//
// Every one of them is a bound, so a zero field is a real announcement — an
// endpoint that announces no data credit has announced none, and a peer that
// honours it can send nothing at all. The zero value is therefore not usable
// and [Dial], [Accept] and [NewListener] refuse it with [ErrZeroParameters]:
// start from [DefaultParameters] and change what you need.
type TransportParameters struct {
	// MaxIdleTimeout ends a connection nobody is using. Zero means no
	// timeout, which is the one default the RFC allows and this package does
	// not: [DefaultParameters] fills it in.
	MaxIdleTimeout time.Duration

	// MaxUDPPayloadSize is the largest datagram this endpoint will read.
	MaxUDPPayloadSize uint64

	// InitialMaxData bounds everything the peer may send on all streams
	// together, and the three below bound it per stream and per stream count.
	InitialMaxData                 uint64
	InitialMaxStreamDataUni        uint64
	InitialMaxStreamDataBidiLocal  uint64
	InitialMaxStreamDataBidiRemote uint64
	InitialMaxStreamsBidi          uint64
	InitialMaxStreamsUni           uint64

	// AckDelayExponent scales the peer's ACK delay fields; zero means the
	// protocol default of 3. Read so a peer that announces another value
	// does not skew this end's round-trip estimate.
	AckDelayExponent uint64

	// MaxAckDelay is how long the peer may sit on an acknowledgement,
	// which loss recovery adds to its probe timeout. Zero means the
	// protocol default of 25.
	MaxAckDelay time.Duration

	// OriginalDCID is the connection identifier the client first used, echoed
	// by the server. It is what ties the handshake to the packets that
	// carried it, so an attacker cannot splice one connection's handshake
	// onto another's packets.
	originalDCID []byte

	// InitialSourceCID is the identifier this endpoint used on its own first
	// packet, for the same reason.
	initialSourceCID []byte

	// RetrySourceCID is the identifier a server put on the Retry packet it
	// sent, echoed so the client can bind the connection it ends up with to
	// the Retry that redirected it — present only when a Retry happened.
	retrySourceCID []byte

	// ActiveConnIDLimit is how many of the peer's connection identifiers
	// this endpoint will keep active at once (§18.2; the protocol minimum
	// and default is 2). Zero or 1 is replaced with this package's working
	// default of 4 when the connection is built, so there is always a spare
	// for the peer to rotate into.
	ActiveConnIDLimit uint64

	// sawStatelessResetToken and sawPreferredAddress record that the peer
	// announced these server-only parameters (§18.2). A *client* announcing
	// either is a protocol violation a server must refuse with
	// TRANSPORT_PARAMETER_ERROR, so their presence is remembered for
	// handlePeerParameters to judge with the direction known.
	sawStatelessResetToken bool
	sawPreferredAddress    bool

	// StatelessResetToken is the server's reset token for the connection
	// identifier the handshake runs on (§10.3): a later datagram ending in
	// these 16 bytes is the server saying, without any connection state
	// left, that this connection is dead. A client stores it against the
	// peer's sequence-0 identifier; a server sends one only when its
	// listener can honour it statelessly.
	statelessResetToken []byte

	// PreferredAddress is the server's invitation to move the connection
	// to another address once the handshake confirms (§9.6). A
	// [Listener] fills it in from [ListenerConfig.PreferredAddress]; a
	// client reads it and migrates on its own (see [Conn.PreferredAddress]).
	// Server-only: a client announcing one is refused.
	preferredAddress *preferredAddress
}

// PreferredAddress is RFC 9000 §18.2's preferred_address: up to one
// address per family a server would rather be reached at, the connection
// identifier (sequence 1) a client must use there, and that identifier's
// stateless reset token. A family not offered is nil here and all-zero on
// the wire.
type preferredAddress struct {
	ipv4 *net.UDPAddr
	ipv6 *net.UDPAddr
	// CID is 1 to 20 bytes: §18.2 forbids a zero-length one, since a
	// client cannot route a path change under the identifier it is
	// leaving.
	cid                 []byte
	statelessResetToken [16]byte
}

// addrFor picks the offered address of the same family as cur, nil when
// cur is not a UDP address or no address of its family was offered.
func (p *preferredAddress) addrFor(cur net.Addr) net.Addr {
	ua, ok := cur.(*net.UDPAddr)
	if !ok || p == nil {
		return nil
	}
	if ua.IP.To4() != nil {
		if p.ipv4 == nil {
			return nil
		}
		return p.ipv4
	}
	if p.ipv6 == nil {
		return nil
	}
	return p.ipv6
}

// preferredAddressLen is the fixed part of the parameter: 4+2 for IPv4,
// 16+2 for IPv6, the identifier's length byte, and the 16-byte token.
const preferredAddressLen = 4 + 2 + 16 + 2 + 1 + 16

func appendPreferredAddress(dst []byte, p *preferredAddress) []byte {
	var v4 [4]byte
	var v4port uint16
	if p.ipv4 != nil {
		copy(v4[:], p.ipv4.IP.To4())
		v4port = uint16(p.ipv4.Port) //nolint:gosec // G115: a UDPAddr port is already 0..65535
	}
	var v6 [16]byte
	var v6port uint16
	if p.ipv6 != nil {
		copy(v6[:], p.ipv6.IP.To16())
		v6port = uint16(p.ipv6.Port) //nolint:gosec // G115: as above
	}
	dst = append(dst, v4[:]...)
	dst = append(dst, byte(v4port>>8), byte(v4port))
	dst = append(dst, v6[:]...)
	dst = append(dst, byte(v6port>>8), byte(v6port), byte(len(p.cid))) //nolint:gosec // G115: a port keeps its low octets; a connection ID is at most 20 bytes
	dst = append(dst, p.cid...)
	return append(dst, p.statelessResetToken[:]...)
}

func parsePreferredAddress(val []byte) (*preferredAddress, error) {
	if len(val) < preferredAddressLen {
		return nil, fmt.Errorf("%w: a preferred_address of %d bytes; at least %d are needed", ErrQUIC, len(val), preferredAddressLen)
	}
	cidLen := int(val[24])
	if cidLen == 0 || cidLen > 20 {
		// §18.2: a zero-length identifier is TRANSPORT_PARAMETER_ERROR;
		// over 20 bytes is not an identifier version 1 can carry (§17.2).
		return nil, &transportError{
			code: transportParameterError,
			err:  fmt.Errorf("%w: a preferred_address with a %d-byte connection identifier", ErrQUIC, cidLen),
		}
	}
	if len(val) != preferredAddressLen+cidLen {
		return nil, fmt.Errorf("%w: a preferred_address of %d bytes carrying a %d-byte identifier", ErrQUIC, len(val), cidLen)
	}
	p := &preferredAddress{cid: append([]byte(nil), val[25:25+cidLen]...)}
	copy(p.statelessResetToken[:], val[25+cidLen:])
	// An all-zero address and port means the family is not offered.
	if !allZero(val[0:6]) {
		p.ipv4 = &net.UDPAddr{IP: net.IP(append([]byte(nil), val[0:4]...)), Port: int(val[4])<<8 | int(val[5])}
	}
	if !allZero(val[6:24]) {
		p.ipv6 = &net.UDPAddr{IP: net.IP(append([]byte(nil), val[6:22]...)), Port: int(val[22])<<8 | int(val[23])}
	}
	return p, nil
}

// ErrZeroParameters reports a zero [TransportParameters]: no limit of any
// kind announced, which no caller can mean literally.
var ErrZeroParameters = errors.New("quic: zero TransportParameters announce no credit at all — start from DefaultParameters()")

// check refuses the zero value, which would announce a connection that
// cannot move.
func (p TransportParameters) check() error {
	if p.MaxIdleTimeout == 0 && p.MaxUDPPayloadSize == 0 && p.InitialMaxData == 0 &&
		p.InitialMaxStreamDataUni == 0 && p.InitialMaxStreamDataBidiLocal == 0 && p.InitialMaxStreamDataBidiRemote == 0 &&
		p.InitialMaxStreamsBidi == 0 && p.InitialMaxStreamsUni == 0 && p.ActiveConnIDLimit == 0 {
		return ErrZeroParameters
	}
	return nil
}

// DefaultParameters are modest limits for a connection that serves requests:
// enough for a batch of them, not enough to be worth attacking.
func DefaultParameters() TransportParameters {
	return TransportParameters{
		MaxIdleTimeout:                 30 * time.Second,
		MaxUDPPayloadSize:              1452, // an Ethernet MTU less IPv6 and UDP headers
		InitialMaxData:                 1 << 20,
		InitialMaxStreamDataUni:        1 << 16,
		InitialMaxStreamDataBidiLocal:  1 << 18,
		InitialMaxStreamDataBidiRemote: 1 << 18,
		// RFC 9114 §6.1: an HTTP/3 server SHOULD grant at least 100 request
		// streams, and this transport default is what ServeH3 inherits.
		InitialMaxStreamsBidi: 100,
		InitialMaxStreamsUni:  8,
		// Enough identifiers for a NAT rebind and a deliberate rotation to
		// overlap, few enough that the pool is not worth attacking.
		ActiveConnIDLimit: 4,
	}
}

// AppendParameters encodes the parameters as the TLS extension carries them:
// a sequence of identifier, length and value, all variable-length integers
// except the value.
func appendParameters(dst []byte, p TransportParameters) []byte {
	num := func(id, v uint64) {
		dst = AppendVarint(dst, id)
		dst = AppendVarint(dst, uint64(varintBytes(v))) //nolint:gosec // G115: varintBytes returns 1 to 8
		dst = AppendVarint(dst, v)
	}
	raw := func(id uint64, v []byte) {
		if len(v) == 0 {
			return
		}
		dst = AppendVarint(dst, id)
		dst = AppendVarint(dst, uint64(len(v)))
		dst = append(dst, v...)
	}
	num(paramMaxIdleTimeout, uint64(max(p.MaxIdleTimeout, 0)/time.Millisecond))
	num(paramMaxUDPPayloadSize, p.MaxUDPPayloadSize)
	num(paramInitialMaxData, p.InitialMaxData)
	num(paramInitialMaxStreamBidiL, p.InitialMaxStreamDataBidiLocal)
	num(paramInitialMaxStreamBidiR, p.InitialMaxStreamDataBidiRemote)
	num(paramInitialMaxStreamUni, p.InitialMaxStreamDataUni)
	num(paramInitialMaxStreamsBidi, p.InitialMaxStreamsBidi)
	num(paramInitialMaxStreamsUni, p.InitialMaxStreamsUni)
	if p.ActiveConnIDLimit != 0 {
		// Written only when set: zero is not an encodable announcement here
		// — the protocol floor is 2, and omission means the default.
		num(paramActiveConnIDLimit, p.ActiveConnIDLimit)
	}
	raw(paramOriginalDCID, p.originalDCID)
	raw(paramInitialSourceCID, p.initialSourceCID)
	raw(paramRetrySourceCID, p.retrySourceCID)
	raw(paramStatelessResetToken, p.statelessResetToken)
	if p.preferredAddress != nil {
		raw(paramPreferredAddress, appendPreferredAddress(nil, p.preferredAddress))
	}
	return dst
}

// ParseParameters decodes what a peer announced.
//
// An identifier this package does not know is skipped rather than refused:
// the space is extensible on purpose, and a peer that refuses the unknown is
// how a protocol stops being able to change. A *known* identifier repeated
// is refused, though — RFC 9000 §7.4 makes it TRANSPORT_PARAMETER_ERROR,
// because two values for one limit is a peer this end cannot reason about.
func parseParameters(b []byte) (TransportParameters, error) {
	var p TransportParameters
	var seen uint64 // a bit per known identifier; they all sit below 0x10
	for len(b) > 0 {
		id, rest, err := Varint(b)
		if err != nil {
			return p, err
		}
		length, rest, err := Varint(rest)
		if err != nil {
			return p, err
		}
		if length > uint64(len(rest)) {
			return p, fmt.Errorf("%w: a transport parameter of %d bytes, %d remain", ErrQUIC, length, len(rest))
		}
		val, rest, err := bytesN(rest, int(length)) //nolint:gosec // G115: checked above
		if err != nil {
			return p, err
		}
		b = rest

		if id < 64 {
			bit := uint64(1) << id
			if known(id) && seen&bit != 0 {
				return p, fmt.Errorf("%w: transport parameter %#x appears twice", ErrQUIC, id)
			}
			seen |= bit
		}

		switch id {
		case paramOriginalDCID:
			p.originalDCID = val
		case paramInitialSourceCID:
			p.initialSourceCID = val
		case paramRetrySourceCID:
			p.retrySourceCID = val
		case paramStatelessResetToken:
			p.sawStatelessResetToken = true
			if len(val) != 16 {
				return p, fmt.Errorf("%w: a stateless_reset_token of %d bytes", ErrQUIC, len(val))
			}
			p.statelessResetToken = val
		case paramPreferredAddress:
			p.sawPreferredAddress = true
			pa, err := parsePreferredAddress(val)
			if err != nil {
				return p, err
			}
			p.preferredAddress = pa
		case paramActiveConnIDLimit:
			// §18.2: active_connection_id_limit below 2 is
			// TRANSPORT_PARAMETER_ERROR. The value caps how many identifiers
			// this end issues for itself (quic/migration.go).
			v, _, err := Varint(val)
			if err != nil {
				return p, err
			}
			if v < 2 {
				return p, &transportError{
					code: transportParameterError,
					err:  fmt.Errorf("%w: active_connection_id_limit %d is below the minimum of 2", ErrQUIC, v),
				}
			}
			p.ActiveConnIDLimit = v
		case paramMaxIdleTimeout, paramMaxUDPPayloadSize, paramInitialMaxData,
			paramInitialMaxStreamBidiL, paramInitialMaxStreamBidiR, paramInitialMaxStreamUni,
			paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni,
			paramAckDelayExponent, paramMaxAckDelay:
			v, _, err := Varint(val)
			if err != nil {
				return p, err
			}
			switch id {
			case paramMaxIdleTimeout:
				p.MaxIdleTimeout = idleDuration(v)
			case paramMaxUDPPayloadSize:
				p.MaxUDPPayloadSize = v
			case paramInitialMaxData:
				p.InitialMaxData = v
			case paramInitialMaxStreamBidiL:
				p.InitialMaxStreamDataBidiLocal = v
			case paramInitialMaxStreamBidiR:
				p.InitialMaxStreamDataBidiRemote = v
			case paramInitialMaxStreamUni:
				p.InitialMaxStreamDataUni = v
			case paramInitialMaxStreamsBidi:
				p.InitialMaxStreamsBidi = v
			case paramInitialMaxStreamsUni:
				p.InitialMaxStreamsUni = v
			case paramAckDelayExponent:
				p.AckDelayExponent = v
			case paramMaxAckDelay:
				p.MaxAckDelay = idleDuration(v)
			}
		}
	}
	return p, nil
}

// known reports whether an identifier is one this package decodes, which is
// the set the duplicate check applies to.
func known(id uint64) bool {
	switch id {
	case paramOriginalDCID, paramMaxIdleTimeout, paramMaxUDPPayloadSize,
		paramInitialMaxData, paramInitialMaxStreamBidiL, paramInitialMaxStreamBidiR,
		paramInitialMaxStreamUni, paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni,
		paramAckDelayExponent, paramMaxAckDelay, paramInitialSourceCID, paramRetrySourceCID,
		paramStatelessResetToken, paramPreferredAddress, paramActiveConnIDLimit:
		return true
	}
	return false
}
