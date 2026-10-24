package quic

import "time"

// Datagram Packetization Layer PMTU Discovery (RFC 8899, applied to QUIC by
// RFC 9000 §14.3): the proof that a path carries datagrams larger than the
// 1200 bytes §14.1 guarantees. Without it every datagram stays at 1200 —
// about 7 % of payload forgone on an Ethernet path.
//
// The search (RFC 8899 §5.3) runs between the last size confirmed and the
// smallest size lost, from 1200 up to a ceiling: the caller's
// [TransportParameters.MaxDatagramSize] (1452 when zero), no more than the
// peer's max_udp_payload_size (§5.3.1: MAX_PLPMTU is the local MTU bounded
// by EMTU_R, and "MAY be reduced by an application"). The first probe is
// 1452 — 1500-byte Ethernet less the IPv6 and UDP headers, the path that
// is common enough to settle in one probe, §5.3.2's table of common sizes
// reduced to its one entry that matters — and, while nothing has been
// lost, the next one is the ceiling itself, so a jumbo path a caller
// opened up is reached in two probes. After the first size lost, each
// probe halves the range (§5.3.2: maximize the gain from each step), and
// the search completes once the range is pmtuPrecision bytes or less: a
// 1420-byte tunnel settles within 20 bytes of its payload, the IPv6
// minimum of 1280 at 1212 or more instead of falling back to 1200. A size
// is lost after pmtuMaxProbes unanswered probes in a row (§5.1.2's
// MAX_PROBES), so that one probe a congested queue dropped does not cap
// the path. One probe is in flight at a time; no ICMP (§14.2.1 does not
// require it, and net.PacketConn does not deliver it).
//
// A path that shrinks after the search (a route change, a tunnel set up
// underneath) is a black hole (RFC 8899 §4.3): every full datagram is lost
// and only the small ones pass. QUIC needs no CONFIRMATION_TIMER (RFC 9000
// §14.3.2), so the indicator is §4.3's third, loss of the packets of one
// size: pmtuMaxProbes probe timeouts in a row, or as many packets above
// 1200 bytes declared lost by acknowledgements that acknowledged none of
// that size. Either drops the datagram size to 1200 and starts the search
// over; what was lost is resent cut to the new size (fitFrames). A total
// outage of three probe timeouts reads as one too, and costs one probe of
// 1452 once the path is back. No PMTU_RAISE_TIMER: a completed search is
// not re-run on a path that might have grown (§5.1.1 makes it optional).
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
	// pmtuCommonSize is the first size probed, and the ceiling when
	// [TransportParameters.MaxDatagramSize] is zero.
	pmtuCommonSize = 1452
	// pmtuBaseSize is the size every path carries (RFC 9000 §14.1), where
	// each path starts and where a black hole falls back to.
	pmtuBaseSize = 1200
	// pmtuMaxSize is the largest UDP payload an IPv6 datagram can carry
	// (65535 less the 8-byte UDP header) — the largest ceiling accepted.
	pmtuMaxSize = 65527
	// pmtuPrecision is the range the search stops at: closer than this,
	// the next probe would gain less than 2 % on a 1200-byte datagram.
	pmtuPrecision = 20
	// pmtuMaxProbes is RFC 8899 §5.1.2's MAX_PROBES: consecutive failed
	// probes of one size before that size is lost; and the count of probe
	// timeouts or full-size losses that reads as a black hole.
	pmtuMaxProbes = 3
)

// pmtuState is one path's search. Zero value: nothing probed yet.
type pmtuState struct {
	// done is SEARCH_COMPLETE (RFC 8899 §5.2): the range is narrower than
	// pmtuPrecision, or the ceiling is confirmed, or the peer's parameter
	// leaves nothing to probe.
	done bool
	// inFlight says a probe is out and not yet acknowledged or lost; probePN
	// is its packet number, what an acknowledgement or a loss is matched
	// against — a probe sent on the previous path is neither.
	inFlight bool
	probePN  uint64
	// size is the datagram size the probe in flight was expanded to.
	size int
	// tries counts the probes of size lost in a row (PROBE_COUNT).
	tries int
	// lost is the smallest size lost pmtuMaxProbes times on this path, the
	// top of the range; zero while none has been.
	lost int
	// probes counts every probe sent on this path.
	probes int
	// largeLost counts the packets above pmtuBaseSize declared lost since
	// one of that size was last acknowledged: the black-hole indicator.
	largeLost int
}

// pmtuCeilingLocked is MAX_PLPMTU: the caller's ceiling, no more than the
// peer says it reads (RFC 8899 §5.3.1), or 0 before the peer's parameters
// are known. Callers hold mu.
func (c *Conn) pmtuCeilingLocked() int {
	if !c.hasPeerParams {
		return 0
	}
	ceiling := c.params.MaxDatagramSize
	if ceiling == 0 {
		ceiling = pmtuCommonSize
	}
	return min(ceiling, int(min(c.peerParams.MaxUDPPayloadSize, pmtuMaxSize))) //nolint:gosec // G115: bounded by pmtuMaxSize
}

// pmtuTargetLocked is the size worth probing next, or 0 when the search is
// complete: 1452 first, the ceiling while nothing was lost, then the
// middle of the range between the size confirmed and the smallest lost.
// Callers hold mu.
func (c *Conn) pmtuTargetLocked() int {
	ceiling := c.pmtuCeilingLocked()
	confirmed := c.maxDatagram
	if c.pmtu.lost == 0 {
		switch {
		case ceiling <= confirmed:
			return 0
		case confirmed < pmtuCommonSize && pmtuCommonSize < ceiling:
			return pmtuCommonSize
		}
		return ceiling
	}
	top := min(c.pmtu.lost, ceiling+1)
	if top-confirmed <= pmtuPrecision {
		return 0
	}
	return (confirmed + top) / 2
}

// resetPMTULocked restarts the search: datagrams fall back to the 1200
// bytes every path carries, and the next probe leaves once the path is
// validated. Callers hold mu.
func (c *Conn) resetPMTULocked() {
	c.maxDatagram = pmtuBaseSize
	c.pmtu = pmtuState{}
}

// maybeProbePMTULocked sends the next probe when one is due: the handshake
// is confirmed (RFC 9000 §14.3.1, the BASE state), no probe is in flight,
// the path is validated — a probe cut to the amplification budget would
// prove a smaller size than the one asked, and a path still under
// challenge may never be used — and the congestion window has room for it
// (§14.4). Nor does it leave from a socket Don't Fragment could not be set
// on (fragmentable): RFC 8899 §3 sends a probe with DF, and one a router
// may fragment proves nothing. What stops it today is re-checked after
// every processed packet, so the probe leaves as soon as the path or the
// window allows. Callers hold mu.
func (c *Conn) maybeProbePMTULocked() {
	if c.pmtu.done || c.pmtu.inFlight || c.fragmentable || !c.handshakeConfirmed || c.amplActive || c.pathProbe.pending || c.closeSent || c.draining {
		return
	}
	target := c.pmtuTargetLocked()
	if target == 0 {
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
// here on, and the search moves on to the next size. The congestion
// controller's per-datagram unit follows (RFC 9002 §7.3.3 grows the window
// by one datagram per window acknowledged); its initial window does not.
// RFC 9002 §7.2 says it SHOULD be recalculated with the new size, and the
// SHOULD is set aside on purpose: by the time a probe is acknowledged the
// window has long left its initial value, and re-deriving it would be a
// growth no acknowledgement earned. Callers hold mu.
func (c *Conn) pmtuProbeAckedLocked(pn uint64) {
	if !c.pmtu.inFlight || pn != c.pmtu.probePN {
		return // a probe of the previous path, acknowledged late
	}
	c.pmtu.inFlight = false
	c.pmtu.tries = 0
	if c.pmtu.size > c.maxDatagram {
		c.maxDatagram = c.pmtu.size
		c.cc.setMaxDatagram(c.pmtu.size)
	}
	c.maybeProbePMTULocked()
}

// pmtuProbeLostLocked counts the try, marks the size lost after
// pmtuMaxProbes in a row, and sends the next probe. Callers hold mu.
func (c *Conn) pmtuProbeLostLocked(pn uint64) {
	if !c.pmtu.inFlight || pn != c.pmtu.probePN {
		return
	}
	c.pmtu.inFlight = false
	c.pmtu.tries++
	if c.pmtu.tries >= pmtuMaxProbes {
		c.pmtu.tries = 0
		if c.pmtu.lost == 0 || c.pmtu.size < c.pmtu.lost {
			c.pmtu.lost = c.pmtu.size
		}
	}
	c.maybeProbePMTULocked()
}

// watchBlackHoleLocked reads an ACK's or the loss timer's outcome for the
// black-hole indicator: a packet above 1200 bytes acknowledged clears it,
// one declared lost by an outcome that acknowledged none of that size
// counts towards it. Losses beside such an acknowledgement do not count:
// that is the congestion a full-size flow meets, on a path that plainly
// carries the size. Callers hold mu.
func (c *Conn) watchBlackHoleLocked(out *ackOutcome) {
	if out.largeAcked {
		c.pmtu.largeLost = 0
		return
	}
	c.pmtu.largeLost += out.largeLost
	if c.pmtu.largeLost >= pmtuMaxProbes {
		c.blackHoleLocked()
	}
}

// blackHoleLocked falls back to 1200 bytes and searches again (RFC 8899
// §4.3: "DPLPMTUD sets a lower PLPMTU … then confirms that the new PLPMTU
// can be successfully used"). A no-op at 1200 already. Callers hold mu.
func (c *Conn) blackHoleLocked() {
	if c.maxDatagram <= pmtuBaseSize {
		return
	}
	probe := c.pmtu
	c.resetPMTULocked()
	// A probe still out is the one the next acknowledgement or loss is
	// matched against; its fate settles a size of the new search too.
	c.pmtu.inFlight, c.pmtu.probePN, c.pmtu.size = probe.inFlight, probe.probePN, probe.size
	c.cc.setMaxDatagram(pmtuBaseSize)
	c.pmtuBlackHoles++
}
