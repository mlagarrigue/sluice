// Package quic is an experimental QUIC version 1 implementation: the packet
// layer — variable-length integers, frames, headers, RFC 9001 packet
// protection — and a connection on top of it, with the transport machinery
// of RFC 9002: acknowledgements, loss detection, retransmission, congestion
// control (NewReno by default, CUBIC and the IETF draft of BBR to choose),
// and flow control enforced in both directions.
//
// # Read this before using it
//
// Interoperability is proven against exactly one peer that shares no code:
// quic-go, on each side of the handshake, in continuous integration (the
// interop/ module — handshake, an echo across flow-control
// windows, a close the peer sees). Everything else is checked against
// itself, against RFC 9001 Appendix A's published vectors, and against a
// test harness that drops, reorders and forges datagrams. The QUIC Interop
// Runner, run in this repository's CI, adds ngtcp2 to quic-go; two peers
// are not every peer, so treat a third implementation as untested.
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
//     failure via [Stream.Reset] and [Stream.StopSending], and read and
//     write deadlines ([Stream.SetReadDeadline], [Stream.SetWriteDeadline])
//     with the [net.Conn] contract: [os.ErrDeadlineExceeded]. What the
//     handshake settled is readable — [Conn.ConnectionState] for the
//     negotiated protocol, server name and peer certificates — and so are
//     the two addresses, [Conn.LocalAddr] and [Conn.RemoteAddr], the
//     latter following the peer across a migration.
//   - Recovery per RFC 9002: ACK generation with delayed acknowledgements,
//     packet- and time-threshold loss detection, probe timeouts,
//     retransmission; NewReno with slow start and recovery periods by
//     default, CUBIC or BBR through [TransportParameters.Congestion].
//   - Flow control per RFC 9000 §4, both levels and both directions:
//     senders wait for credit, receivers grant it as data is consumed, and
//     a peer that overruns its grant loses the connection.
//   - Lifecycle: CONNECTION_CLOSE both ways — at once with [Conn.Close],
//     or once everything sent is acknowledged with [Conn.Shutdown] —
//     HANDSHAKE_DONE, the negotiated idle timeout and the
//     [Conn.SetKeepAlive] PING that holds a connection under it; why a
//     connection ended is one typed value from [Conn.Err] ([ErrIdleTimeout],
//     [ErrStatelessReset], [ErrVersionNegotiation], [ConnectionError]),
//     transport-parameter validation with the §7.3
//     identifier-binding checks, version-negotiation and Retry handling on
//     the client (the Retry integrity tag is verified per RFC 9001 §5.8).
//   - A server [Listener]: one shared [net.PacketConn] demultiplexed to
//     many connections by connection identifier, the §8.1 anti-amplification
//     limit on every address until it validates, and server-generated Retry
//     ([ListenerConfig.AlwaysRetry]) for the deployments that want the
//     address proved before a connection exists at all. On Linux,
//     macOS and Windows a Listener on a UDP socket bound to 0.0.0.0 or [::]
//     answers each datagram from the address it reached (IP_PKTINFO /
//     IPV6_PKTINFO), so a multi-homed server does not look like a
//     path change to its clients. [Listener.Addr] reports where it
//     listens. A server, even one that serves a single connection, is a
//     Listener.
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
//   - Datagram size (§14): datagrams start at the 1200 bytes §14.1
//     guarantees on any path and rise by DPLPMTUD (§14.3, RFC 8899):
//     PING+PADDING probes, 1452 bytes first — an Ethernet MTU less IPv6
//     and UDP headers — then a search between the size confirmed and the
//     smallest lost, to 20 bytes, up to
//     [TransportParameters.MaxDatagramSize] and the peer's
//     [TransportParameters.MaxUDPPayloadSize]; after the handshake and
//     after each migration. A path that shrinks under the connection (a
//     black hole: three probe timeouts, or full-size packets lost while
//     smaller ones get through) brings the size back to 1200 and the
//     search runs again. No ICMP (§14.2.1 does not require it); a lost
//     probe is not a congestion event (§14.4). [Stats.DatagramSize] is the
//     size reached. See docs/guide/limits.md.
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
//     rather than closing — a long-lived connection does not die of its
//     packet count. [Stats.KeyUpdates] counts the rotations.
//   - ECN (§13.4), on Linux: the field is read off every datagram and
//     reported in ACK frames, this end marks its own packets ECT(0) once
//     the path validates, and a rise in the CE count a peer reports is a
//     congestion event for the controller. Elsewhere the field is neither
//     read nor set, and [Stats] shows zero.
//   - Session resumption and 0-RTT (RFC 9001 §4.5, §4.6; ADR 0019), on
//     request. A [Listener] issues tickets ([ListenerConfig.SessionTickets])
//     and accepts 0-RTT ([ListenerConfig.EarlyData]) once per ticket and
//     only for its own; [DialEarly] sends the first requests in 0-RTT
//     under the parameters remembered with the ticket, and a refusal fails
//     the streams that carried them ([ErrEarlyDataRejected]).
//
// # What is not here
//
//   - **Server migration.** A server never changes its own address (§9):
//     it may announce a preferred one (see above), nothing else; a
//     client's preferred_address is refused (§18.2).
//   - **Versions other than 1.** QUIC version 2 (RFC 9369) and compatible
//     version negotiation (RFC 9368, the version_information parameter)
//     are not implemented: the only version spoken is 1. A Version
//     Negotiation packet from a server that does not speak it ends the
//     dial with [ErrVersionNegotiation], and a version_information
//     parameter from a peer is skipped as any unknown parameter is
//     (RFC 9000 §7.4.2). Version 2 differs from 1 in constants only and
//     exists against middlebox ossification, which no peer of this
//     package has shown; it is to be added the day one does.
//
// # The socket is the caller's
//
// Every entry point takes a [net.PacketConn] and never opens one. Two
// things RFC 9000 asks of the socket itself — the IPv4 Don't Fragment bit
// (§14) and RFC 6437 flow labels on IPv6 (§9.7) — are socket options with
// a different name on each operating system, so they live in the
// [github.com/mlagarrigue/sluice/net/quic/udp] helper: its Listen opens a UDP
// socket with them applied, its Configure applies them to one the caller
// already holds. When the socket given is a [net.UDPConn], [Dial] and
// [NewListener] apply them themselves, best effort, as they ask it to
// report ECN; a plain [net.ListenPacket] socket therefore works as one
// from udp.Listen. A socket of any other type is used as it comes, and
// is fragmentable unless the caller configured it.
//
// # The TLS callbacks are the caller's
//
// The callbacks of the [tls.Config] given to [Dial] and [NewListener] — VerifyPeerCertificate, VerifyConnection, GetCertificate,
// GetConfigForClient and the rest — run on the connection's own receive
// goroutine, inside the handshake: crypto/tls drives them from the CRYPTO
// bytes that goroutine feeds it, and returns to it only when the handshake
// waits for more. A callback that blocks — an OCSP query, a directory
// lookup — therefore stops the reading of every datagram of that one
// connection for as long as it runs: acknowledgements and Handshake packets
// arriving meanwhile wait in its queue, and only the timer loop keeps
// retransmitting. Other connections behind a Listener are untouched, since
// the Listener's loop never waits on a connection; what overflows the
// stalled connection's queue ([ListenerConfig.ConnQueueLen]) is dropped and
// resent by the peer. RFC 9001 §4.1.4 asks an endpoint to keep answering
// what it can while TLS works asynchronously; this package buffers the
// packets whose keys are missing but does not answer during a callback, so
// the slow work belongs before the handshake — a staple fetched in
// advance, a cache in front of the lookup — and the callback under the
// path's round trip. Moving the callbacks off the receive goroutine would
// take a TLS goroutine per handshake and its events replayed under the
// connection's lock; it is worth doing the day a measured callback
// outlasts the round trip.
//
// # On the cryptography
//
// None of it is invented here. The key schedule is HKDF from [crypto/hkdf]
// with the labels RFC 9001 §5.1 publishes, the packet protection is the
// AEAD of the negotiated suite — AES-GCM from [crypto/cipher], or
// ChaCha20-Poly1305 from golang.org/x/crypto, the only module outside the
// standard library this package imports (ADR 0002) — and the header protection is one block of the same suite's
// cipher over a sample, as §5.4 specifies. What this package contributes is
// the plumbing, and the plumbing is pinned two ways: Appendix A.1's
// published derivation vectors, and seal/open round trips for what the
// vectors do not reach.
//
// What crypto/tls refuses, this package reports as crypto/tls says it: a
// TLS alert leaves in a CONNECTION_CLOSE with code 0x0100 plus the alert
// (RFC 9001 §4.8), which is also how the codes that RFC mandates — 0x0178
// for no application protocol (§8.1), 0x016d for missing transport
// parameters (§8.2), 0x010a for a KeyUpdate message (§6) — reach the wire.
// Four refusals RFC 9001 names PROTOCOL_VIOLATION (0x0a) go out under
// their alert's code instead: CRYPTO data of a level already finished,
// past the end received (§4.1.3, internal_error); data of a level left
// unconsumed when the next level's keys arrive (§4.1.3,
// unexpected_message); a CertificateRequest after the handshake, on a
// client (§4.4, unexpected_message); a NewSessionTicket whose early_data
// is not 0xffffffff (§4.6.1, illegal_parameter). The connection closes in
// each case; only the code differs. The alert is all this package sees,
// and the same alert serves other causes — an internal_error is also a
// real one — so no translation is made. To revisit when crypto/tls states
// the cause of an alert. The TLS 1.3 compatibility mode (RFC 8446 Appendix
// D.4, legacy_session_id) is neither requested nor policed here; the
// limits guide says why.
package quic

import (
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// ErrQUIC marks the errors this package reports about QUIC itself: bytes it
// will not parse, and a connection or stream used in a state that does not
// allow it. errors.Is(err, ErrQUIC) tells them from a context's and the
// socket's own errors; the message says which, and [Conn.Err] whether the
// connection ended. Nothing is gained by retrying the same call.
var ErrQUIC = errors.New("quic: malformed")

// ErrTruncated reports a buffer that ends inside what it was describing. It
// is separated from [ErrQUIC] because a datagram is atomic: a packet that
// runs off the end of one is malformed, where the same bytes arriving as a
// prefix of a stream would merely be incomplete.
var ErrTruncated = fmt.Errorf("%w: the buffer ends mid-field", ErrQUIC)

// errPacketNumberPastCeiling refuses a packet whose decoded number is past
// 2^62-1 (RFC 9000 §12.3); see decodePacketNumber.
var errPacketNumberPastCeiling = fmt.Errorf("%w: the packet number is past 2^62-1", ErrQUIC)

// readVarint is varint.Read with this package's own [ErrTruncated], which
// its callers test for.
func readVarint(b []byte) (v uint64, rest []byte, err error) {
	v, rest, err = varint.Read(b)
	if err != nil {
		return 0, nil, ErrTruncated
	}
	return v, rest, nil
}

// varintBytes reports how many bytes varint.Append would use, which a writer
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
