// Package quic is an experimental QUIC version 1 implementation: the packet
// layer — variable-length integers, frames, headers, RFC 9001 packet
// protection — and a connection on top of it, with the transport machinery
// of RFC 9002: acknowledgements, loss detection, retransmission, congestion
// control (NewReno by default, CUBIC and the IETF draft of BBR to choose),
// and flow control enforced in both directions.
//
// Interoperability is proven against quic-go, on each side of the
// handshake, in continuous integration (the interop/ module), and weekly
// against the implementations of the QUIC Interop Runner, of which three —
// quic-go, ngtcp2 and msquic — are judged case by case; treat the others as
// untested. A section number with no RFC named, in this package, is RFC
// 9000's.
//
// Why it is here: a QUIC datagram carries frames for several streams at
// once, so one UDP read is a batch by construction — the library's thesis
// at the transport level. [Conn.OnStreamFrames] hands it over as the
// transport formed it, with the byte streams already reordered.
//
//	client, err := quic.Dial(ctx, pc, addr, tlsConfig, quic.DefaultConfig())
//	s, err := client.OpenStream(ctx)
//
//	ln, err := quic.NewListener(pc, tlsConfig, quic.DefaultConfig(), quic.ListenerConfig{})
//	server, err := ln.Accept()
//
// The socket is the caller's: every entry point takes a [net.PacketConn]
// and never opens one; [github.com/mlagarrigue/sluice/net/quic/udp] opens
// or configures one with the options QUIC asks of it. The callbacks of the
// [tls.Config] run on the connection's receive goroutine, so a slow one
// stalls that connection. Under GODEBUG=fips140=only, [Dial], [DialEarly]
// and [NewListener] refuse with an error that wraps [errors.ErrUnsupported]:
// that mode refuses the AES-GCM nonces and the Initial keys RFC 9001
// prescribes.
//
// Where to read on:
//   - docs/guide/experimental.md — what the package is for, and what
//     "experimental" means here;
//   - docs/reference/quic.md — the settings, what is implemented and what
//     is not, the socket and the TLS callbacks in detail;
//   - docs/guide/deployments.md — a client behind a NAT that rebinds, a
//     server with several addresses;
//   - docs/reference/protocol-positions.md — where the package departs from
//     an RFC, and the cryptography it borrows from crypto/tls.
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
// [ErrEarlyDataRejected] stands apart: a refused 0-RTT is no misuse, and
// sending again on a new stream is its remedy.
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

// appendVarintOn encodes v on exactly width bytes (1, 2, 4 or 8), which may
// be more than varintBytes(v): RFC 9000 §16 lets every field but the frame
// type use a longer encoding than its value needs. v must fit the width.
func appendVarintOn(dst []byte, v uint64, width int) []byte {
	switch width {
	case 1:
		return append(dst, byte(v)) //nolint:gosec // G115: keeps the low octet of each shift
	case 2:
		return append(dst, 0x40|byte(v>>8), byte(v)) //nolint:gosec // G115: keeps the low octet of each shift
	case 4:
		return append(dst, 0x80|byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: keeps the low octet of each shift
	default:
		return append(dst, 0xc0|byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec // G115: keeps the low octet of each shift
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
