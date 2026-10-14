package quic

import "time"

// Datagram Packetization Layer PMTU Discovery (RFC 8899, applied to QUIC by
// RFC 9000 §14.3): the proof that a path carries datagrams larger than the
// 1200 bytes §14.1 guarantees. Without it every datagram stays at 1200 —
// about 7 % of payload forgone on an Ethernet path.
//
// The search is deliberately the simplest RFC 8899 allows (§5.3.2 leaves
// the probe sizes to the implementation): one size, pmtuProbeSize bounded
// by the peer's max_udp_payload_size, tried up to pmtuMaxProbes times, then
// 1200 until the next path. 1452 is the one size that changes anything on
// the common path (1500-byte Ethernet less IPv6 and UDP headers); a path
// that drops it (1280, the IPv6 minimum, or a tunnel) loses the 6 % and
// nothing else. No binary search, no table of common sizes, no ICMP
// (§14.2.1 does not require it, and net.PacketConn does not deliver it);
// a tunnel between the two sizes is the case that would revise this.
//
// A probe is a 1-RTT packet of one PING and PADDING to the probed size
// ("probing using padding data", RFC 8899 §4.1), alone in its datagram. It
// counts in flight — §14.4: probes consume congestion window, RFC 9002 §2:
// PADDING counts toward bytes in flight — but its loss is not a congestion
// event (§14.4: a packet larger than the current datagram size is more
// likely dropped by the network than by congestion) and nothing of it is
// retransmitted (PING and PADDING carry nothing). The acknowledgement of
// the probe is the proof (RFC 8899 §4.2, QUIC being an acknowledged PL);
// QUIC runs no CONFIRMATION_TIMER afterwards (§14.3.2).
//
// DPLPMTUD is per path (§14.3): a migration, a rebind or a move to the
// server's preferred address starts the search over from 1200, since the
// old path's proof says nothing about the new one.

const (
	// pmtuProbeSize is the one datagram size probed for.
	pmtuProbeSize = 1452
	// pmtuMaxProbes is RFC 8899 §5.1.2's MAX_PROBES: consecutive failed
	// probes before the search completes at the base size.
	pmtuMaxProbes = 3
)

// pmtuState is one path's search. Zero value: nothing probed yet.
type pmtuState struct {
	// done is SEARCH_COMPLETE (RFC 8899 §5.2): the size was confirmed, or
	// every try failed, or the peer's parameter leaves nothing to probe.
	done bool
	// inFlight says a probe is out and not yet acknowledged or lost; probePN
	// is its packet number, what an acknowledgement or a loss is matched
	// against — a probe sent on the previous path is neither.
	inFlight bool
	probePN  uint64
	// size is the datagram size the probe in flight was expanded to.
	size int
	// tries counts the probes sent on this path.
	tries int
}

// pmtuTargetLocked is the size worth probing: the probe size, no more than
// the peer says it reads (RFC 8899 §5.3.1: MAX_PLPMTU bounded by EMTU_R),
// or 0 when that is not above what the path already carries. Callers hold
// mu.
func (c *Conn) pmtuTargetLocked() int {
	if !c.hasPeerParams {
		return 0
	}
	target := min(pmtuProbeSize, int(c.peerParams.MaxUDPPayloadSize)) //nolint:gosec // G115: bounded by pmtuProbeSize
	if target <= c.maxDatagram {
		return 0
	}
	return target
}

// resetPMTULocked restarts the search for a new path: datagrams fall back
// to the 1200 bytes every path carries, and the next probe leaves once the
// path is validated. Callers hold mu.
func (c *Conn) resetPMTULocked() {
	c.maxDatagram = 1200
	c.pmtu = pmtuState{}
}

// maybeProbePMTULocked sends the next probe when one is due: the handshake
// is confirmed (RFC 9000 §14.3.1, the BASE state), no probe is in flight,
// the tries are not exhausted, the path is validated — a probe cut to the
// amplification budget would prove a smaller size than the one asked, and
// a path still under challenge may never be used — and the congestion
// window has room for it (§14.4). What stops it today
// is re-checked after every processed packet, so the probe leaves as soon
// as the path or the window allows. Callers hold mu.
func (c *Conn) maybeProbePMTULocked() {
	if c.pmtu.done || c.pmtu.inFlight || !c.handshakeConfirmed || c.amplActive || c.pathProbe.pending || c.closeSent || c.draining {
		return
	}
	target := c.pmtuTargetLocked()
	if target == 0 || c.pmtu.tries >= pmtuMaxProbes {
		c.pmtu.done = true
		return
	}
	if !c.cc.canSend(target) {
		return
	}
	sp := &c.spaces[spaceApplication]
	if sp.sealer == nil {
		return
	}
	// Straight to writePacketLocked: sendPacketLocked would add the ACK and
	// control frames owed, and a probe carries PING and PADDING only — an
	// ACK lost with it would cost the peer a probe timeout.
	c.pmtu.size = target
	_ = c.writePacketLocked(sp, spaceApplication, []byte{framePing}, nil, sendOpts{noRetrans: true, padTo: target, pmtuProbe: true}, time.Now())
}

// pmtuProbeAckedLocked is the proof: the probe's size is the path's from
// here on. The congestion controller's per-datagram unit follows (RFC 9002
// §7.3.3 grows the window by one datagram per window acknowledged); its
// initial window does not — §7.2 fixed it at the start of the path, and
// re-deriving it now would be a growth no acknowledgement earned. Callers
// hold mu.
func (c *Conn) pmtuProbeAckedLocked(pn uint64) {
	if !c.pmtu.inFlight || pn != c.pmtu.probePN {
		return // a probe of the previous path, acknowledged late
	}
	c.pmtu.inFlight = false
	c.pmtu.done = true
	c.maxDatagram = c.pmtu.size
	c.cc.maxDatagram = c.pmtu.size
}

// pmtuProbeLostLocked moves to the next try, or ends the search at the
// base size once pmtuMaxProbes have failed. Callers hold mu.
func (c *Conn) pmtuProbeLostLocked(pn uint64) {
	if !c.pmtu.inFlight || pn != c.pmtu.probePN {
		return
	}
	c.pmtu.inFlight = false
	c.maybeProbePMTULocked()
}
