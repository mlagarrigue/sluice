package quic

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"
)

// Transport parameters, RFC 9000 §18. They travel inside the TLS handshake
// rather than in QUIC frames, which is what makes them authenticated by the
// same exchange that establishes the keys.

// Parameter identifiers this package writes and reads.
const (
	paramOriginalDCID           = 0x00
	paramMaxIdleTimeout         = 0x01
	paramStatelessResetToken    = 0x02
	paramMaxUDPPayloadSize      = 0x03
	paramInitialMaxData         = 0x04
	paramInitialMaxStreamBidiL  = 0x05
	paramInitialMaxStreamBidiR  = 0x06
	paramInitialMaxStreamUni    = 0x07
	paramInitialMaxStreamsBidi  = 0x08
	paramInitialMaxStreamsUni   = 0x09
	paramAckDelayExponent       = 0x0a
	paramMaxAckDelay            = 0x0b
	paramDisableActiveMigration = 0x0c
	paramPreferredAddress       = 0x0d
	paramActiveConnIDLimit      = 0x0e
	paramInitialSourceCID       = 0x0f
	paramRetrySourceCID         = 0x10
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
	// Announced, it also caps the size the peer probes the path for
	// (RFC 9000 §14.3); what this endpoint sends is decided by its own
	// probe of the path, never by the peer's announcement alone — see the
	// package documentation. Zero means 1452, like MaxDatagramSize; below
	// 1200 or above 65527 (§18.2's bounds), [Dial], [Accept] and
	// [NewListener] refuse it.
	MaxUDPPayloadSize uint64

	// InitialMaxData bounds everything the peer may send on all streams
	// together, and the three below bound it per stream and per stream count.
	InitialMaxData                 uint64
	InitialMaxStreamDataUni        uint64
	InitialMaxStreamDataBidiLocal  uint64
	InitialMaxStreamDataBidiRemote uint64
	InitialMaxStreamsBidi          uint64
	InitialMaxStreamsUni           uint64

	// AckDelayExponent scales the peer's ACK delay fields. Read so a peer
	// that announces another value does not skew this end's round-trip
	// estimate. RFC 9000 §18.2 gives the default of 3 to a peer that is
	// *silent*, not to one that announces 0: sawAckDelayExponent tells the
	// two apart, and the field here is whatever the peer wrote.
	AckDelayExponent uint64

	// MaxAckDelay is how long the peer may sit on an acknowledgement,
	// which loss recovery adds to its probe timeout. As above, the §18.2
	// default of 25 ms is for an absent parameter (sawMaxAckDelay), and an
	// announced 0 is read as 0.
	MaxAckDelay time.Duration

	// sawAckDelayExponent and sawMaxAckDelay record that the peer announced
	// the parameter at all, since 0 is a legal value of both and not the
	// default (RFC 9000 §18.2). Neither is announced by this package, so
	// appendParameters has no use for them.
	sawAckDelayExponent bool
	sawMaxAckDelay      bool

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
	// for the peer to rotate into. It also caps how many identifiers this
	// endpoint issues for itself, whatever the peer's own limit invites:
	// each is a routing entry a [Listener] holds, so a figure raised for
	// the peer is one this end pays for too.
	ActiveConnIDLimit uint64

	// DisableActiveMigration announces that this endpoint cannot follow a
	// peer that changes address (RFC 9000 §18.2, a zero-length value; §9
	// says a deployment without connection continuity SHOULD announce it).
	// A client that reads it from its server refuses [Conn.Rebind] with
	// [ErrMigrationDisabled] until it has moved to a preferred_address the
	// same server offered — §18.2 forbids a new local address only towards
	// the address the handshake used. A server that announces it changes
	// nothing in how this package behaves: §9 lets such a server either
	// drop a client's packets from a new address or validate the path and
	// follow, and it follows (see maybeMigrate); the announcement is for
	// deployments whose load balancer, not this server, would lose the
	// client.
	DisableActiveMigration bool

	// Congestion chooses the congestion controller this endpoint sends
	// with. It is local: never encoded on the wire, never read from the
	// peer, which runs whatever it likes (RFC 9002 §7 leaves the choice to
	// each sender). Experimental: the default is NewReno until another
	// algorithm has been measured against a real client, and the zero
	// value is that default.
	Congestion CongestionConfig

	// MaxDatagramSize is the largest datagram this endpoint sends, once
	// its probe of the path (DPLPMTUD, RFC 9000 §14.3) proves the path
	// carries it. Local, like Congestion: never encoded on the wire. Zero
	// means 1452, an Ethernet MTU less the IPv6 and UDP headers; above
	// that, the search climbs towards it — 8952 for a 9000-byte jumbo
	// path. What is probed is also bounded by the peer's announced
	// MaxUDPPayloadSize, so a jumbo path needs both ends to announce one.
	// Below 1200 (what every path must carry, §14.1) or above 65527 (the
	// largest UDP payload), [Dial], [Accept] and [NewListener] refuse it.
	// The size reached is [Stats.DatagramSize].
	MaxDatagramSize int

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

// CongestionConfig is the congestion-control choice a connection is built
// with ([TransportParameters.Congestion]): the algorithm, and the two
// choices its RFCs leave to the sender. Every zero value is the
// algorithm's default; the other parameters are the RFC's. Experimental.
type CongestionConfig struct {
	// Algorithm names the controller. Unknown values run the default.
	Algorithm CongestionAlgorithm

	// SlowStart chooses how a loss-based controller (NewReno, CUBIC) ends
	// its initial slow start. The zero value is the algorithm's default:
	// HyStart++ for CUBIC, the standard slow start for NewReno. BBR's
	// Startup has its own exit and ignores it.
	// Unknown values run the default.
	SlowStart SlowStart

	// DisableFastConvergence turns off CUBIC's fast convergence (RFC 9438
	// §4.7): after a loss below the previous one, the flow remembers the
	// window it lost at instead of less, and climbs back to it sooner. The
	// RFC wants it on where flows share the path, the zero value, and off
	// for a flow alone on it. Other algorithms ignore it.
	DisableFastConvergence bool
}

// SlowStart names how the initial slow start ends ([CongestionConfig]).
// Slow start doubles the window each round trip until something says the
// path is full; the choice is what that something is.
type SlowStart uint8

const (
	// SlowStartDefault is the algorithm's own choice.
	SlowStartDefault SlowStart = iota

	// SlowStartStandard ends the slow start on the first loss or
	// congestion mark (RFC 9002 §7.3.1): the window overshoots the path by
	// up to the bottleneck's buffer before it learns.
	SlowStartStandard

	// SlowStartHyStart is RFC 9406's HyStart++: the slow start ends when
	// the round trip rises by a few milliseconds over the previous round's
	// — the bottleneck's buffer starting to fill — after a few rounds of
	// growth at a quarter of the rate to confirm the rise. A loss that
	// comes first still ends it.
	SlowStartHyStart
)

// CongestionAlgorithm names a congestion controller this package
// implements. Each one answers the same questions from the sender (when a
// packet may leave, how fast) and differs in how it reads loss.
type CongestionAlgorithm uint8

const (
	// CongestionNewReno is RFC 9002 §7's controller: the window grows by
	// one datagram per round trip and halves on each loss epoch. The
	// default, and the one every benchmark figure of this package was
	// measured with.
	CongestionNewReno CongestionAlgorithm = iota

	// CongestionCubic is RFC 9438's controller: after a loss the window
	// shrinks to seven tenths and grows back along a cubic curve, flat
	// around the size the loss happened at and steep away from it, so a
	// long round trip no longer costs a window a datagram at a time.
	// Experimental: measured on this package's emulated paths only.
	CongestionCubic

	// CongestionBBRv1 is BBR version 1
	// (draft-cardwell-iccrg-bbr-congestion-control-00): the sender paces
	// at the highest delivery rate measured over the last ten round trips
	// and keeps about one bandwidth-delay product in flight, probing for
	// more every eight round trips and draining the path for 200 ms every
	// ten seconds to re-measure its propagation delay. Loss does not shrink
	// its rate, so a path that drops a packet in a hundred is held at its
	// bandwidth; a CE mark is read as a loss that arrived, which opens the
	// same recovery period and shrinks the rate no more; the version has no
	// bound on what it takes from a Reno or CUBIC flow sharing the link.
	// Experimental: measured on this package's emulated paths only.
	CongestionBBRv1

	// CongestionBBR is BBR as the IETF drafts it (draft-ietf-ccwg-bbr, the
	// version Linux ships as BBRv3): the same model of the path as
	// CongestionBBRv1 — the highest delivery rate and the lowest round trip
	// seen recently — with loss read as evidence about how much the path
	// holds. A round that loses more than 2% while probing bounds the
	// volume in flight, 15% of which is then left free for other flows;
	// a loss between probes cuts the rate and the volume to 0.7 of what
	// they were, until the next probe, two to three seconds later or as
	// soon as a Reno flow would have grown back. Startup also ends on
	// loss, so a shallow buffer is not overrun for three round trips.
	// Experimental: measured on this package's emulated paths only.
	CongestionBBR
)

// String is the name the dashboard prints: "newreno", "cubic", "bbr1",
// "bbr".
func (a CongestionAlgorithm) String() string {
	switch a {
	case CongestionNewReno:
		return "newreno"
	case CongestionCubic:
		return "cubic"
	case CongestionBBRv1:
		return "bbr1"
	case CongestionBBR:
		return "bbr"
	}
	return "CongestionAlgorithm(" + strconv.Itoa(int(a)) + ")"
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
	// Every refusal here is TRANSPORT_PARAMETER_ERROR: §18.2 names it for
	// the identifier's length, §20.1 for a parameter "badly formatted".
	if len(val) < preferredAddressLen {
		return nil, parameterError(fmt.Errorf("%w: a preferred_address of %d bytes; at least %d are needed", ErrQUIC, len(val), preferredAddressLen))
	}
	cidLen := int(val[24])
	if cidLen == 0 || cidLen > 20 {
		// A zero-length identifier is what §18.2 names; over 20 bytes is
		// not an identifier version 1 can carry (§17.2).
		return nil, parameterError(fmt.Errorf("%w: a preferred_address with a %d-byte connection identifier", ErrQUIC, cidLen))
	}
	if len(val) != preferredAddressLen+cidLen {
		return nil, parameterError(fmt.Errorf("%w: a preferred_address of %d bytes carrying a %d-byte identifier", ErrQUIC, len(val), cidLen))
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
// cannot move, and a MaxDatagramSize no path can carry.
func (p TransportParameters) check() error {
	if p.MaxIdleTimeout == 0 && p.MaxUDPPayloadSize == 0 && p.InitialMaxData == 0 &&
		p.InitialMaxStreamDataUni == 0 && p.InitialMaxStreamDataBidiLocal == 0 && p.InitialMaxStreamDataBidiRemote == 0 &&
		p.InitialMaxStreamsBidi == 0 && p.InitialMaxStreamsUni == 0 && p.ActiveConnIDLimit == 0 {
		return ErrZeroParameters
	}
	if n := p.MaxDatagramSize; n != 0 && (n < pmtuBaseSize || n > pmtuMaxSize) {
		return fmt.Errorf("quic: MaxDatagramSize %d is outside %d..%d", n, pmtuBaseSize, pmtuMaxSize)
	}
	// RFC 9000 §18.2: below 1200 the announcement is invalid, and a
	// conforming peer closes the handshake with TRANSPORT_PARAMETER_ERROR;
	// above 65527 it names more than a UDP payload can hold.
	if n := p.MaxUDPPayloadSize; n != 0 && (n < pmtuBaseSize || n > pmtuMaxSize) {
		return fmt.Errorf("quic: MaxUDPPayloadSize %d is outside %d..%d", n, pmtuBaseSize, pmtuMaxSize)
	}
	return nil
}

// udpPayloadSize is the max_udp_payload_size this endpoint announces: the
// field, or 1452 when it is left at zero — never 0, which §18.2 makes
// invalid, and never less than the read buffer (recvBufSize) can hold.
func (p TransportParameters) udpPayloadSize() uint64 {
	if p.MaxUDPPayloadSize == 0 {
		return pmtuCommonSize
	}
	return p.MaxUDPPayloadSize
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
	num(paramMaxUDPPayloadSize, p.udpPayloadSize())
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
	if p.DisableActiveMigration {
		dst = AppendVarint(dst, paramDisableActiveMigration)
		dst = AppendVarint(dst, 0) // §18.2: a zero-length value
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

// parameterError is how every refusal of what the peer announced leaves
// the parser and handlePeerParameters: RFC 9000 §20.1 defines
// TRANSPORT_PARAMETER_ERROR for parameters "badly formatted, included an
// invalid value, omitted a mandatory transport parameter, included a
// forbidden transport parameter" — a block cut mid-varint or mid-value is
// the first case, §7.4 adds the duplicate, §18.2 each parameter's own
// bounds and the server-only ones a client sends, and §7.3 offers it (or
// PROTOCOL_VIOLATION) for identifiers that do not match the handshake's
// packets. One code for the whole family, so a peer reads "your
// parameters" rather than "my internals".
func parameterError(err error) error {
	return &transportError{code: transportParameterError, err: err}
}

// parseParameters decodes what a peer announced.
//
// An identifier this package does not know is skipped rather than refused:
// the space is extensible on purpose, and a peer that refuses the unknown is
// how a protocol stops being able to change. An identifier repeated is
// refused, though, known or not — RFC 9000 §7.4 makes it
// TRANSPORT_PARAMETER_ERROR, because two values for one limit is a peer
// this end cannot reason about.
func parseParameters(b []byte) (TransportParameters, error) {
	var p TransportParameters
	// §7.4: no identifier twice, known or not — two values for one limit
	// is a peer this end cannot reason about, and an unknown one that the
	// peer itself repeats is the same malformed block. The defined
	// identifiers all sit below 64 and take a bit each; anything above
	// (extensions, §18.1's reserved values) goes in a slice no longer than
	// the block has parameters.
	var seen uint64
	var seenHigh []uint64
	for len(b) > 0 {
		// A block that stops mid-varint or mid-value is a parameter
		// "badly formatted" (§20.1), and TRANSPORT_PARAMETER_ERROR names it
		// better than the INTERNAL_ERROR an unwrapped parse error would close
		// with.
		id, rest, err := Varint(b)
		if err != nil {
			return p, parameterError(fmt.Errorf("%w: a transport parameter identifier cut short", ErrQUIC))
		}
		length, rest, err := Varint(rest)
		if err != nil {
			return p, parameterError(fmt.Errorf("%w: transport parameter %#x has its length cut short", ErrQUIC, id))
		}
		if length > uint64(len(rest)) {
			return p, parameterError(fmt.Errorf("%w: a transport parameter of %d bytes, %d remain", ErrQUIC, length, len(rest)))
		}
		val := rest[:length]
		b = rest[length:]

		if id < 64 {
			bit := uint64(1) << id
			if seen&bit != 0 {
				return p, parameterError(fmt.Errorf("%w: transport parameter %#x appears twice", ErrQUIC, id))
			}
			seen |= bit
		} else {
			if slices.Contains(seenHigh, id) {
				return p, parameterError(fmt.Errorf("%w: transport parameter %#x appears twice", ErrQUIC, id))
			}
			seenHigh = append(seenHigh, id)
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
				return p, parameterError(fmt.Errorf("%w: a stateless_reset_token of %d bytes", ErrQUIC, len(val)))
			}
			p.statelessResetToken = val
		case paramPreferredAddress:
			p.sawPreferredAddress = true
			pa, err := parsePreferredAddress(val)
			if err != nil {
				return p, err
			}
			p.preferredAddress = pa
		case paramDisableActiveMigration:
			// §18.2: "a zero-length value" — a peer that puts bytes in it
			// announced something this package cannot read, which §7.4
			// makes an invalid value.
			if len(val) != 0 {
				return p, parameterError(fmt.Errorf("%w: a disable_active_migration of %d bytes", ErrQUIC, len(val)))
			}
			p.DisableActiveMigration = true
		case paramActiveConnIDLimit:
			// §18.2: active_connection_id_limit below 2 is
			// TRANSPORT_PARAMETER_ERROR. The value caps how many identifiers
			// this end issues for itself (quic/migration.go).
			v, err := integerParameter(id, val)
			if err != nil {
				return p, err
			}
			if v < 2 {
				return p, parameterError(fmt.Errorf("%w: active_connection_id_limit %d is below the minimum of 2", ErrQUIC, v))
			}
			p.ActiveConnIDLimit = v
		case paramMaxIdleTimeout, paramMaxUDPPayloadSize, paramInitialMaxData,
			paramInitialMaxStreamBidiL, paramInitialMaxStreamBidiR, paramInitialMaxStreamUni,
			paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni,
			paramAckDelayExponent, paramMaxAckDelay:
			v, err := integerParameter(id, val)
			if err != nil {
				return p, err
			}
			switch id {
			case paramMaxIdleTimeout:
				p.MaxIdleTimeout = idleDuration(v)
			case paramMaxUDPPayloadSize:
				// §18.2: "Values below 1200 are invalid" — 0 included,
				// which is not the absence the default stands in for.
				if v < pmtuBaseSize {
					return p, parameterError(fmt.Errorf("%w: max_udp_payload_size %d is below the protocol minimum", ErrQUIC, v))
				}
				p.MaxUDPPayloadSize = v
			case paramInitialMaxData:
				p.InitialMaxData = v
			case paramInitialMaxStreamBidiL:
				p.InitialMaxStreamDataBidiLocal = v
			case paramInitialMaxStreamBidiR:
				p.InitialMaxStreamDataBidiRemote = v
			case paramInitialMaxStreamUni:
				p.InitialMaxStreamDataUni = v
			case paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni:
				// §4.6: over 2^60 the limit names stream identifiers no
				// varint can carry; in a parameter that is
				// TRANSPORT_PARAMETER_ERROR (the frame's twin is
				// FRAME_ENCODING_ERROR, parseFrame).
				if v > maxStreamsLimit {
					return p, parameterError(fmt.Errorf("%w: initial_max_streams %d is over 2^60", ErrQUIC, v))
				}
				if id == paramInitialMaxStreamsBidi {
					p.InitialMaxStreamsBidi = v
				} else {
					p.InitialMaxStreamsUni = v
				}
			case paramAckDelayExponent:
				p.AckDelayExponent = v
				p.sawAckDelayExponent = true
			case paramMaxAckDelay:
				p.MaxAckDelay = idleDuration(v)
				p.sawMaxAckDelay = true
			}
		}
	}
	if seen&(1<<paramMaxUDPPayloadSize) == 0 {
		// §18.2: "The default for this parameter is the maximum permitted
		// UDP payload of 65527." Left at zero, the peer would be read as
		// accepting nothing, and the path search (pmtuCeilingLocked) would
		// never leave 1200.
		p.MaxUDPPayloadSize = pmtuMaxSize
	}
	return p, nil
}

// maxStreamsLimit is the largest stream count a MAX_STREAMS frame or an
// initial_max_streams_* parameter may carry (RFC 9000 §4.6): 2^60 streams of
// one type and direction reach stream identifier 2^62-1, the varint ceiling.
const maxStreamsLimit = 1 << 60

// integerParameter decodes a parameter whose whole value is one varint. The
// value's announced length must match the integer's own, both ways: bytes
// left after it, or a varint cut short by it, are a value this end cannot
// read, which §7.4 makes TRANSPORT_PARAMETER_ERROR.
func integerParameter(id uint64, val []byte) (uint64, error) {
	v, rest, err := Varint(val)
	if err != nil || len(rest) != 0 {
		return 0, parameterError(fmt.Errorf("%w: transport parameter %#x is %d bytes, not one integer", ErrQUIC, id, len(val)))
	}
	return v, nil
}
