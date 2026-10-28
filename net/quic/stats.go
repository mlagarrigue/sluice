package quic

import (
	"time"
)

// Stats is a snapshot of the counters a connection keeps about what it
// could not process, for the dashboard rather than the data path.
type Stats struct {
	// DiscardedPackets is how many incoming datagrams or packets were dropped
	// without being processed: wrong sender, failed parse, failed
	// authentication — and transient socket read errors, which on Windows
	// include the unauthenticated ICMP port-unreachable. A rising count under
	// load is an attacker probing, or a path corrupting datagrams.
	DiscardedPackets uint64

	// NonProductivePackets is how many authenticated, ack-eliciting packets
	// carried no frame that moved this end's state: PING alone, a
	// PATH_CHALLENGE, a MAX_DATA that raised nothing, bytes already
	// received, a *_BLOCKED frame. RFC 9000 §21.9 asks an endpoint to track
	// the cost of processing against progress and to read an excess of
	// such packets as an attack; this is the tracking, and the reading is
	// the operator's. Nothing is closed or dropped on it: each such packet
	// costs this end what any packet costs — one AEAD open and a parse —
	// and closing the connection would spare that only for packets still
	// sent to a dead identifier, while the frames whose effect costs more
	// than their bytes have bounds of their own. A rate that climbs while
	// nothing else moves is the thing to look at. A limit, if a measurement
	// ever shows such a flood costing more than the Initial flood a
	// Listener already absorbs, would be a named Parameters field.
	NonProductivePackets uint64

	// ECT0Received, ECT1Received and CEReceived count the packets processed
	// whose IP header carried that ECN codepoint (RFC 9000 §13.4.1), the
	// counts this end reports back in its ACK frames. All zero where the
	// socket does not report the field (every platform but Linux today), or
	// when the peer does not mark. A rising CEReceived is the network
	// signalling congestion without dropping.
	ECT0Received uint64
	ECT1Received uint64
	CEReceived   uint64

	// KeyUpdates is how many times this end's 1-RTT send keys rotated
	// (RFC 9001 §6), whichever side initiated. Each rotation renews the
	// AEAD's confidentiality budget (§6.6); a count that never moves on a
	// connection that has sent tens of millions of packets is the thing to
	// look at.
	KeyUpdates uint64

	// WriteFailures is how many datagrams this connection failed to hand to
	// its socket. Sends are best effort — loss recovery resends what a failed
	// write lost, and a CONNECTION_CLOSE that fails to leave is covered by
	// the peer's idle timer — so no write error ends the connection or
	// reaches a caller; a count that keeps rising is a socket that has
	// stopped working where the connection would otherwise just look slow.
	WriteFailures uint64

	// UnhandledTLSEvents is how many crypto/tls QUIC events this connection
	// received of a kind this package does not handle — most likely one a
	// newer Go release added that this package does not know yet.
	UnhandledTLSEvents uint64

	// SmoothedRTT is the round-trip estimate loss recovery runs on
	// (RFC 9002 §5.3, smoothed_rtt): an exponentially weighted mean of the
	// samples, moved by every ACK that newly covers the largest packet it
	// names, the peer's declared ACK delay subtracted. Zero before the first
	// sample, which the handshake supplies. It is what a probe timeout and
	// the loss time threshold are scaled by.
	SmoothedRTT time.Duration

	// MinRTT is the smallest round trip sampled (§5.2, min_rtt): the path's
	// propagation delay, as close as this end can know it. It only ever
	// falls, except across a migration, which starts it over.
	MinRTT time.Duration

	// RTTVariance is the mean deviation of the samples from SmoothedRTT
	// (§5.3, rttvar). A variance that climbs while the mean holds is a
	// path whose queue is filling and emptying.
	RTTVariance time.Duration

	// LatestRTT is the last round trip sampled (§5.1, latest_rtt), before
	// the peer's declared ACK delay is subtracted: the path as it was one
	// acknowledgement ago, where SmoothedRTT is the path as it has been. A
	// LatestRTT that sits well above MinRTT on a path that loses nothing
	// is a queue building somewhere in front of this sender — the signal
	// a delay-based controller reads, and the one a loss-based one does
	// not.
	LatestRTT time.Duration

	// CongestionWindow is how many bytes the congestion controller lets
	// this end have in flight (RFC 9002 §7): it grows on every ACK, halves
	// on a loss epoch and collapses on persistent congestion (§7.6).
	// A window that stays near 2 × 1200 on a connection that keeps writing
	// is a path losing packets faster than it recovers.
	CongestionWindow int

	// Algorithm is the congestion controller this end sends with
	// ([TransportParameters.Congestion]), as built: an unknown choice reads
	// back as the default it fell to.
	Algorithm CongestionAlgorithm

	// BytesInFlight is how many bytes are sent and neither acknowledged nor
	// declared lost (§B.2). It rises on every ack-eliciting send, falls
	// when an ACK arrives or a loss is declared, and reads zero once
	// everything written has been acknowledged. BytesInFlight approaching
	// CongestionWindow is a sender limited by the path, not by its data.
	BytesInFlight int

	// LostPackets is how many packets loss recovery declared lost (§6.1),
	// by either path that can: an ACK proving a packet three behind or too
	// old, or the loss timer maturing on one left too young to call. A
	// DPLPMTUD probe that goes missing is not counted: its size was the
	// question, and its loss is no congestion event (RFC 9000 §14.4). A
	// retransmission is not a second loss until it is itself declared.
	LostPackets uint64

	// BytesRetransmitted is how many bytes of frames those losses put back
	// on the send queue — payload, not packet size: the headers, the
	// ACK and PADDING frames of a lost packet are not sent again (§6.1,
	// RFC 9000 §13.3) — so it never exceeds LostPackets times the largest
	// datagram the path carried. It counts at the moment of queuing: a
	// byte queued as the connection closes, or in the Initial or Handshake
	// space after its keys are discarded (RFC 9001 §4.9), is counted and
	// never sent. On a path that reorders but loses nothing, every byte
	// here was queued for no reason: the cost of a spurious loss, which
	// LostPackets counts the same as a real one.
	BytesRetransmitted uint64

	// PersistentCongestions is how many times an acknowledgement proved
	// that everything sent over more than three probe intervals was lost
	// (§7.6) and the window collapsed to its minimum. A blackout, a
	// migration to a dead path, or a peer that went away mid-transfer; a
	// count that moves on a path that merely loses packets is a detector
	// firing where the RFC's conditions were not met.
	PersistentCongestions uint64

	// ProbeTimeouts is how many times the probe timeout fired (§6.2):
	// acknowledgements stopped arriving for a whole timeout, and this end
	// sent probes to find out whether anything still gets through. It
	// counts expiries, not probes sent: a pass that probes two packet
	// number spaces together is one. A count that moves on a quiet path is
	// a peer that stalls, or ACKs that do not come back.
	ProbeTimeouts uint64

	// DeliveryRate is how fast the network delivered this end's packets, in
	// bytes per second, as the last reliable sample measured it
	// (draft-ietf-ccwg-bbr-06 §4.1): the bytes an
	// acknowledgement proved delivered, over the longer of the time they
	// took to send and the time they took to be acknowledged. Samples taken
	// while the application had nothing to send, or while BBR's ProbeRTT
	// held the flight down on purpose, are skipped — they measure the
	// sender, not the path — and so are intervals shorter than MinRTT. Zero until the first such sample. It is what the path carried,
	// not what CongestionWindow allowed: over a bottleneck it reads the line
	// rate while the window covers the bandwidth-delay product, and the
	// window per round trip once a loss has cut the window below it.
	DeliveryRate float64

	// DatagramSize is the largest datagram this end sends on the current
	// path: 1200 until a DPLPMTUD probe proves more (RFC 9000 §14.3),
	// then the largest size proven, up to
	// [TransportParameters.MaxDatagramSize]. It starts over at 1200 on
	// every new path.
	DatagramSize int

	// MTUBlackHoles is how many times full-size datagrams stopped getting
	// through after the path had carried them (RFC 8899 §4.3) — three
	// probe timeouts in a row, or three of them lost while only smaller
	// packets were acknowledged — and the datagram size fell back to 1200
	// while the search ran again. A route change or a tunnel set up under
	// the connection; an outage of three probe timeouts counts too.
	MTUBlackHoles uint64
}

// Stats reports the connection's counters, consistently with each other.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		DiscardedPackets:      c.discarded,
		NonProductivePackets:  c.nonProductive,
		ECT0Received:          c.ecnReceived[ecnECT0],
		ECT1Received:          c.ecnReceived[ecnECT1],
		CEReceived:            c.ecnReceived[ecnCE],
		KeyUpdates:            c.ku.updates,
		WriteFailures:         c.writeFailures,
		UnhandledTLSEvents:    c.unhandledTLSEvents.Load(),
		SmoothedRTT:           c.rtt.smoothed,
		MinRTT:                c.rtt.min,
		RTTVariance:           c.rtt.variance,
		LatestRTT:             c.rtt.latest,
		CongestionWindow:      c.cc.window(),
		Algorithm:             c.params.Congestion.Algorithm,
		BytesInFlight:         c.cc.bytesInFlight(),
		LostPackets:           c.lostPackets,
		BytesRetransmitted:    c.bytesRetransmitted,
		PersistentCongestions: c.persistentCongestions,
		ProbeTimeouts:         c.ptoFired,
		DeliveryRate:          c.delivery.rate,
		DatagramSize:          c.maxDatagram,
		MTUBlackHoles:         c.pmtuBlackHoles,
	}
}
