package quic

import (
	"net"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// ECN codepoints: the two low bits of the IP TOS byte or traffic class
// (RFC 3168 §5), as net/quic/udp reports and sets them.
const (
	ecnNotECT uint8 = 0
	ecnECT1   uint8 = 1
	ecnECT0   uint8 = 2
	ecnCE     uint8 = 3
)

// ecnTestPackets is how many packets leave marked ECT(0) on a new path
// before this end stops marking and waits for the peer's acknowledgements
// to say whether the marks survive (RFC 9000 §13.4.2 suggests ten).
const ecnTestPackets = 10

// ecnPhase is where one path stands in ECN validation.
type ecnPhase uint8

const (
	ecnTesting ecnPhase = iota // the first packets leave marked; their ACKs decide
	ecnUnknown                 // test packets sent, nothing marked until an ACK validates them
	ecnCapable                 // validated: every packet leaves marked
	ecnFailed                  // validation failed, or the socket cannot mark: nothing marked
)

// ecnState is one path's ECN validation (RFC 9000 §13.4.2, a reduction of
// Appendix A.4): mark the first packets ECT(0), then mark everything once
// an ACK proves the marks arrive and are reported, or nothing once one
// proves they do not. It is reset with the rest of the path state on
// every path change (resetPathStateLocked). The counts the peer reports
// are validated per packet-number space, in validateECNLocked; the
// verdict is the path's.
//
// What A.4 does and this does not: notice that every test packet was
// lost. The state then stays unknown, which marks nothing — the same
// outcome as failed, reached without a verdict.
type ecnState struct {
	phase  ecnPhase
	tested int
}

// reset arms validation for a new path, or disables it where the socket
// cannot set the field at all.
func (e *ecnState) reset(markable bool) {
	*e = ecnState{phase: ecnFailed}
	if markable {
		e.phase = ecnTesting
	}
}

// mark returns the codepoint the next datagram leaves with, and counts it
// against the test budget.
func (e *ecnState) mark() uint8 {
	switch e.phase {
	case ecnTesting:
		e.tested++
		if e.tested >= ecnTestPackets {
			e.phase = ecnUnknown
		}
		return ecnECT0
	case ecnCapable:
		return ecnECT0
	}
	return ecnNotECT
}

// validateECNLocked checks the ECN counts of one ACK frame against what
// this end sent in that space (§13.4.2.1), and moves the path to capable
// or failed. out is what markAcked made of the frame. It returns how many
// more packets the frame reports as CE-marked than the last valid one in
// that space (§13.4.2.1's "increase in the ECN-CE count"): the congestion
// signal the controller reads, zero when the counts fail validation or
// the frame is old news. Callers hold mu.
func (c *Conn) validateECNLocked(space int, f Frame, out ackOutcome) (newCE int) {
	if c.ecn.phase == ecnFailed || !out.raisedLargest {
		// A frame that does not increase the largest acknowledged number
		// is old or reordered news, and MUST NOT fail validation.
		return 0
	}
	sp := &c.spaces[space]
	counts, ok := ackECNCounts(f)
	if !ok {
		// A peer that reports no counts for packets that left marked
		// either cannot read the field or sits behind a path that
		// clears it; either way the marks buy nothing.
		if out.newlyAckedECT0 > 0 {
			c.ecn.phase = ecnFailed
		}
		return 0
	}
	prev := sp.ecnLast
	switch {
	case counts[0] < prev[0] || counts[1] < prev[1] || counts[2] < prev[2]:
		// Counts only grow; a decrease is a peer that lost track.
		c.ecn.phase = ecnFailed
	case counts[0]-prev[0]+counts[2]-prev[2] < uint64(out.newlyAckedECT0): //nolint:gosec // G115: a count of packets, positive
		// Fewer ECT(0)-or-CE arrivals than packets that left ECT(0): the
		// network cleared the mark on some of them.
		c.ecn.phase = ecnFailed
	case counts[1] != 0 || counts[0]+counts[2] > sp.ecnSent:
		// A codepoint this end never applied, or more marked arrivals
		// than departures: something on the path remarks packets.
		c.ecn.phase = ecnFailed
	default:
		sp.ecnLast = counts
		if out.newlyAckedECT0 > 0 {
			c.ecn.phase = ecnCapable
		}
		// Bounded by ecnSent, a count of this end's packets: fits an int.
		return int(counts[2] - prev[2]) //nolint:gosec // G115: see above
	}
	return 0
}

// ecnSocket asks pc's kernel to report the ECN field of every datagram
// read from it, and reports whether the socket can carry marks at all —
// one answer for both directions, since a platform that offers one offers
// the other. Idempotent: a Listener's shared socket is asked once per
// connection.
func ecnSocket(pc net.PacketConn) bool {
	switch pc := pc.(type) {
	case *net.UDPConn:
		return udp.EnableECN(pc) == nil
	case *sourcedConn:
		return udp.EnableECN(pc.UDPConn) == nil
	}
	return false
}

// dfSocket sets the Don't Fragment option on pc, with the per-flow IPv6
// label where the platform has one (udp.Configure): RFC 9000 §14 asks for
// DF "if possible", and on the caller's *net.UDPConn it is — the same
// socket ecnSocket already configures. Best effort, like ecnSocket: a
// platform without the option, or a socket of another type, leaves the
// datagrams fragmentable and the transport's search (pmtu.go) the only
// guard. Idempotent: a socket udp.Listen already configured, or a
// Listener's shared one, takes the option again without change.
//
// It reports false when pc is a socket it configures and the option could
// not be set — a platform without it, a socket that refused it: the path
// search must not run there (Conn.fragmentable). A socket of another type
// reports true: it is the caller's, configured as the caller chose.
func dfSocket(pc net.PacketConn) bool {
	switch pc := pc.(type) {
	case *net.UDPConn:
		return udp.Configure(pc) == nil
	case *sourcedConn:
		return udp.Configure(pc.UDPConn) == nil
	}
	return true
}

// readDatagram reads one datagram from pc and, where the socket reports it
// (ecnOK, from ecnSocket), the ECN field it arrived with. oob is scratch
// for the control messages, at least udp.PacketInfoLen long.
func readDatagram(pc net.PacketConn, ecnOK bool, buf, oob []byte) (n int, from net.Addr, ecn uint8, err error) {
	if uc, ok := pc.(*net.UDPConn); ok && ecnOK {
		n, oobn, _, addr, err := uc.ReadMsgUDP(buf, oob)
		if err != nil {
			return 0, nil, 0, err
		}
		ecn, _ = udp.PacketECN(oob[:oobn])
		return n, addr, ecn, nil
	}
	n, from, err = pc.ReadFrom(buf)
	return n, from, 0, err
}

// writeMarked sends one datagram with its ECN field set to ecn, through
// the socket's control message. A socket this cannot drive sends it
// unmarked; the records say marked, and the peer's counts then fail
// validation, which is the right outcome for a path that cannot mark.
// oob is scratch, returned grown for reuse.
func writeMarked(pc net.PacketConn, b []byte, to net.Addr, ecn uint8, oob []byte) (scratch []byte, err error) {
	ua, isUDP := to.(*net.UDPAddr)
	var uc *net.UDPConn
	switch pc := pc.(type) {
	case *net.UDPConn:
		uc, oob = pc, oob[:0]
	case *sourcedConn:
		uc, oob = pc.UDPConn, append(oob[:0], pc.oob...)
	}
	if uc == nil || !isUDP {
		_, err = pc.WriteTo(b, to)
		return oob, err
	}
	ap := ua.AddrPort()
	oob = udp.AppendECN(oob, ecn, ap.Addr())
	_, _, err = uc.WriteMsgUDPAddrPort(b, oob, ap)
	return oob, err
}

// ackECNCounts returns the ECT(0), ECT(1) and CE counts of an ACK frame of
// type 0x03 (§19.3.2), false for a plain ACK. parseACK proved the frame
// decodes; the walk here only finds where the ranges end.
func ackECNCounts(f Frame) (counts [3]uint64, ok bool) {
	if f.Type&1 == 0 {
		return counts, false
	}
	var count, skip uint64
	b, err := takeVarints(f.Data, &count, &skip)
	if err != nil {
		return counts, false
	}
	for range count {
		if b, err = takeVarints(b, &skip, &skip); err != nil {
			return counts, false
		}
	}
	if _, err = takeVarints(b, &counts[0], &counts[1], &counts[2]); err != nil {
		return counts, false
	}
	return counts, true
}
