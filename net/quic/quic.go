// Package quic is an experimental QUIC version 1 implementation: the packet
// layer — variable-length integers, frames, headers, RFC 9001 packet
// protection — and a connection on top of it, with the transport machinery
// of RFC 9002: acknowledgements, loss detection, retransmission, NewReno
// congestion control, and flow control enforced in both directions.
//
// # Read this before using it
//
// Interoperability is proven against exactly one peer that shares no code:
// quic-go, on each side of the handshake, in continuous integration since
// 2026-10-10 (the interop/ module — handshake, an echo across flow-control
// windows, a close the peer sees). Everything else is checked against
// itself, against RFC 9001 Appendix A's published vectors, and against a
// test harness that drops, reorders and forges datagrams. One peer is not
// the public QUIC Interop Runner, whose matrix crosses many; until this
// package is green there, treat a second implementation as untested.
// The deployments it is written for — a client behind a NAT that rebinds,
// a server on a host with several addresses (Linux only, through
// [net/quic/udp]) — are gathered in docs/guide/experimental.md ("Supported
// deployments"); each row names where the code decides it.
//
// # Why it exists
//
// The library's claim is that a batch is the unit of transport. HTTP/1.1
// gives one only when a client pipelines; HTTP/2 gives one because it
// multiplexes; QUIC gives one because a **datagram** carries frames for
// several streams at once and always has. One UDP read is a batch by
// construction, with no waiting, no pipelining, and nothing unusual asked of
// anyone — [Conn.OnStreamFrames] hands it over as the transport formed it,
// with the byte streams already reordered.
//
// # What is here
//
//   - A [Conn]: TLS 1.3 handshake via [crypto/tls.QUICConn], packet
//     protection per RFC 9001, streams with interval reassembly, per-stream
//     failure via [Stream.Reset] and [Stream.StopSending].
//   - Recovery per RFC 9002: ACK generation with delayed acknowledgements,
//     packet- and time-threshold loss detection, probe timeouts,
//     retransmission, NewReno with slow start and recovery periods.
//   - Flow control per RFC 9000 §4, both levels and both directions:
//     senders wait for credit, receivers grant it as data is consumed, and
//     a peer that overruns its grant loses the connection.
//   - Lifecycle: CONNECTION_CLOSE both ways, HANDSHAKE_DONE, the negotiated
//     idle timeout, transport-parameter validation with the §7.3
//     identifier-binding checks, version-negotiation and Retry handling on
//     the client (the Retry integrity tag is verified per RFC 9001 §5.8).
//   - A server [Listener]: one shared [net.PacketConn] demultiplexed to
//     many connections by connection identifier, the §8.1 anti-amplification
//     limit on every address until it validates, and server-generated Retry
//     ([ListenerConfig.AlwaysRetry]) for the deployments that want the
//     address proved before a connection exists at all. On Linux a
//     Listener on a UDP socket bound to 0.0.0.0 or [::] answers each
//     datagram from the address it reached (IP_PKTINFO /
//     IPV6_RECVPKTINFO), so a multi-homed server does not look like a
//     path change to its clients. [Accept] remains for the
//     one-connection-per-socket case.
//   - Connection-identifier rotation and server-side passive migration
//     (RFC 9000 §5.1, §9): every connection issues additional identifiers
//     once its handshake confirms — a client's are what let a server
//     follow it across a NAT rebind (§9.5) — every connection pools what
//     its peer issues and honours RETIRE_CONNECTION_ID and Retire Prior To
//     — switching what it sends under when a peer (a load balancer,
//     typically) forces rotation — and a server whose client's packets
//     start arriving from a new address follows it there: path probed with
//     PATH_CHALLENGE, amplification-limited until the answer arrives,
//     congestion and round-trip state reset (§9.4), a fresh destination
//     identifier adopted when one is pooled (§9.5). The probe is expanded
//     to 1200 bytes as far as the amplification budget allows, and a path
//     validated by a smaller one is validated again at full size (§8.2.1).
//     The path left behind is challenged too (§9.3.3), and the peer
//     returning to it ends the move; a probe unanswered after three
//     retransmissions sends the connection back to the last validated
//     path (§9.3.2) rather than leaving it throttled — a peer gone from
//     both dies by the ordinary idle timeout, not by a second kill path.
//   - Client-initiated migration and the server's preferred address
//     (§9.2, §9.6): [Conn.Rebind] moves a client onto a new socket behind
//     a PATH_CHALLENGE, with the connection ending if the new path never
//     answers; a client offered a preferred address by its server probes
//     it once the handshake confirms and moves there on its own when the
//     path validates. Both reset congestion and round-trip state as §9.4
//     asks. The server side is [ListenerConfig.PreferredAddress]: a second
//     socket the listener reads alongside its main one and announces in
//     every handshake.
//   - Datagram coalescing (§12.2): the ACK a datagram earns and the CRYPTO
//     flight it provokes leave together, Initial and Handshake packets in
//     one datagram, with §14.1's padding carried by the packet that closes
//     it rather than by the Initial alone.
//   - Stateless resets (§10.3): a connection recognises a reset by the
//     tokens its peer announced — NEW_CONNECTION_ID's, and the handshake's
//     stateless_reset_token — and dies immediately rather than by idle
//     timeout; a [Listener] answers unroutable short-header datagrams with
//     a reset whose token derives from a per-listener key, which is what
//     lets it be honoured with no connection state left.
//   - The failure model of §5.2 and §12.2: nothing an unauthenticated
//     sender can put in a datagram ends a connection. Garbage, forgeries
//     and strays are counted ([Conn.Stats], and the listener's
//     own [Listener.DiscardedPackets] in front) and dropped.
//   - Key update (RFC 9001 §6): a peer's rotation is followed, old read
//     keys are kept three probe timeouts for stragglers, and this end
//     rotates its own keys before the AEAD's confidentiality limit (§6.6)
//     rather than closing — a long-lived connection no longer dies of its
//     packet count. [Stats.KeyUpdates] counts the rotations.
//
// # What is not here
//
//   - **Server migration.** A server never changes its own address (§9):
//     it may announce a preferred one (see above), nothing else; a
//     client's preferred_address is refused (§18.2).
//   - 0-RTT, ECN and session resumption.
//
// # The socket is the caller's
//
// Every entry point takes a [net.PacketConn] and never opens one. Two
// things RFC 9000 asks of the socket itself — the IPv4 Don't Fragment bit
// (§14) and RFC 6437 flow labels on IPv6 (§9.7) — are socket options with
// a different name on each operating system, so they live in the
// [github.com/mlagarrigue/sluice/net/quic/udp] helper: its Listen opens a UDP
// socket with them applied, its Configure applies them to one the caller
// already holds. A plain [net.ListenPacket] socket works too; it is just
// fragmentable.
//
// # On the cryptography
//
// None of it is invented here. The key schedule is HKDF from [crypto/hkdf]
// with the labels RFC 9001 §5.1 publishes, the packet protection is the
// AEAD of the negotiated suite — AES-GCM from [crypto/cipher], or
// ChaCha20-Poly1305 from golang.org/x/crypto, the module's one dependency
// (ADR 0002) — and the header protection is one block of the same suite's
// cipher over a sample, as §5.4 specifies. What this package contributes is
// the plumbing, and the plumbing is pinned two ways: Appendix A.1's
// published derivation vectors, and seal/open round trips for what the
// vectors do not reach.
package quic

import (
	"errors"
	"fmt"
)

// ErrQUIC reports bytes this package will not parse.
var ErrQUIC = errors.New("quic: malformed")

// ErrTruncated reports a buffer that ends inside what it was describing. It
// is separated from [ErrQUIC] because a datagram is atomic: a packet that
// runs off the end of one is malformed, where the same bytes arriving as a
// prefix of a stream would merely be incomplete.
var ErrTruncated = fmt.Errorf("%w: the buffer ends mid-field", ErrQUIC)

// MaxVarint is the largest value a variable-length integer can carry: the
// encoding spends two bits of the first byte on the length, so 62 bits are
// left (RFC 9000 §16).
const MaxVarint = 1<<62 - 1

// Varint decodes a variable-length integer and returns the rest of the
// buffer.
//
// The two most significant bits of the first byte give the length — one, two,
// four or eight bytes — and the value occupies what is left. The encoding is
// not canonical: 0 may be written in any of the four widths, and a decoder
// that refused the long forms would refuse traffic the RFC permits.
func Varint(b []byte) (v uint64, rest []byte, err error) {
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

// AppendVarint writes v in the shortest form that holds it.
//
// Shortest rather than fixed-width: the encoding allows either, and a
// protocol whose lengths are mostly small pays for the choice on every frame.
// AppendVarint panics on a value past [MaxVarint], which is a programming
// error rather than a datum — there is no way to write it.
func AppendVarint(dst []byte, v uint64) []byte {
	switch {
	case v <= 63:
		return append(dst, byte(v))
	case v <= 16383:
		return append(dst, byte(v>>8)|0x40, byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	case v <= 1073741823:
		return append(dst, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	case v <= MaxVarint:
		return append(dst, byte(v>>56)|0xc0, byte(v>>48), byte(v>>40), byte(v>>32), //nolint:gosec // G115: varint encoding keeps the low octet of each shift
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: varint encoding keeps the low octet of each shift
	}
	panic("quic: a value larger than a variable-length integer can carry")
}

// varintBytes reports how many bytes AppendVarint would use, which a writer
// needs before it knows what it is about to write.
func varintBytes(v uint64) int {
	switch {
	case v <= 63:
		return 1
	case v <= 16383:
		return 2
	case v <= 1073741823:
		return 4
	default:
		return 8
	}
}

// bytesN takes n bytes off the front, refusing rather than reslicing past the
// end — the one mistake a packet parser makes that turns a short datagram
// into a read of somebody else's memory.
func bytesN(b []byte, n int) (taken, rest []byte, err error) {
	if n < 0 || len(b) < n {
		return nil, nil, ErrTruncated
	}
	return b[:n], b[n:], nil
}
