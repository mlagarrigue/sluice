package quic

import (
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// Acknowledgement generation, RFC 9000 §13.2. Without it a conformant peer
// retransmits into silence, backs off, and gives the connection up — sending
// ACKs is not an optimisation, it is what keeps a real peer talking.

// maxAckRanges bounds how many received ranges one space remembers. A peer
// that opens more gaps than this loses acknowledgement of the oldest — which
// costs it a retransmission, not the connection.
const maxAckRanges = 32

// localMaxAckDelay is how long this endpoint may sit on an ACK in the
// application space, and is the protocol's default so it does not need
// announcing. Initial and handshake packets are acknowledged immediately —
// the peer's handshake timers are tuned for that. The timer itself is armed
// at ackTimerDelay, under this bound by ackAlarmMargin.
const localMaxAckDelay = 25 * time.Millisecond

// ackAlarmMargin is the room left under localMaxAckDelay for the alarm to
// fire late: RFC 9000 §18.2 has max_ack_delay "include the receiver's
// expected delays in alarms firing" ("if a receiver sets a timer for 5ms
// and alarms commonly fire up to 1ms late, then it should send a
// max_ack_delay of 6ms"). The delayed ACK rides a time.Timer (timer.go),
// which the runtime fires a fraction of a millisecond late when idle and a
// few milliseconds late under load or on a coarse clock; a fifth of the
// bound covers that without shortening the delay to where a burst's ACKs
// stop coalescing. The peer bounds its RTT samples and sets its probe
// timeout by the 25 ms it assumes, so an ACK that keeps under it never
// reads as a late one.
const ackAlarmMargin = 5 * time.Millisecond

// ackTimerDelay is what the delayed ACK is armed at.
const ackTimerDelay = localMaxAckDelay - ackAlarmMargin

// pnRange is a run of received packet numbers, both ends inclusive.
type pnRange struct{ lo, hi uint64 }

// ackTracker is one space's record of what arrived and what it owes the peer.
type ackTracker struct {
	// ranges is highest-first, which is the order an ACK frame writes them.
	ranges []pnRange
	// largestAt is when the largest packet arrived, for the frame's delay
	// field.
	largestAt time.Time
	// elicitingSinceAck counts ack-eliciting packets since the last ACK was
	// sent; the second one makes the next ACK immediate (§13.2.2).
	elicitingSinceAck int
	// due is when an ACK must go at the latest; zero when nothing is owed.
	due time.Time
	// immediate marks the debt urgent: send with the next packet, not the
	// next timer.
	immediate bool
	// outOfOrder remembers that the newest recorded packet arrived out of
	// order or opened a gap — §13.2.1's cue to acknowledge at once, so the
	// peer's loss detection learns about the reordering within one RTT
	// instead of a delayed-ACK period later.
	outOfOrder bool
	// ecn counts the packets recorded by the ECN codepoint their datagram
	// arrived with, indexed by codepoint (§13.4.1): each coalesced packet
	// counts once, a duplicate never. Reported in every ACK (§19.3.2).
	ecn [4]uint64
	// floor, when hasFloor, is the lowest packet number this tracker can
	// still vouch for: everything below it was once covered by a range
	// trim dropped, or sat in a gap between such ranges, and whether it was
	// processed is no longer known. §12.3: "A receiver MUST discard a newly
	// unprotected packet unless it is certain that it has not processed
	// another packet with the same packet number" — so record refuses
	// them. Nothing legitimate is lost: a peer never resends a packet
	// number, it retransmits the frames under a new one (§12.3), so a
	// number below the floor is a duplicate, a replay or a straggler.
	// Without the floor, a trim that forgot the low ranges outright would
	// let a replayed packet of a forgotten number be recorded and processed
	// anew — its frames idempotent, its ECN count and its ACK counted twice.
	floor    uint64
	hasFloor bool
}

// record notes one received packet number and returns false when it is a
// duplicate — already covered — which the caller drops without processing.
func (a *ackTracker) record(pn uint64, now time.Time) bool {
	if pn > varint.Max {
		// RFC 9000 §12.3: a packet number has to be "representable in
		// whole in the Largest Acknowledged field of an ACK frame". The
		// receive path refuses such a number before the AEAD
		// (decodePacketNumber); this is the tracker's own invariant, kept
		// here so that appendAckFrame can never meet a number AppendVarint
		// would panic on. Refused as "not recorded": nothing is held,
		// nothing is owed.
		return false
	}
	if a.hasFloor && pn < a.floor {
		return false // below what the tracker can vouch for (§12.3)
	}
	// In order means exactly one past the largest seen; anything else that
	// still records is either reordered (below the largest) or opens a gap
	// (more than one past it). The flag is set only for packets actually
	// recorded — a duplicate changes nothing and owes nothing.
	inOrder := len(a.ranges) == 0 || pn == a.ranges[0].hi+1
	if !a.insert(pn, now) {
		return false
	}
	if !inOrder {
		a.outOfOrder = true
	}
	return true
}

// insert files one packet number into the ranges, reporting false for a
// duplicate.
func (a *ackTracker) insert(pn uint64, now time.Time) bool {
	if len(a.ranges) == 0 || pn > a.ranges[0].hi {
		a.largestAt = now
	}
	for i := range a.ranges {
		r := &a.ranges[i]
		switch {
		case pn >= r.lo && pn <= r.hi:
			return false
		case pn == r.hi+1:
			r.hi++
			if i > 0 && a.ranges[i-1].lo == r.hi+1 {
				a.ranges[i-1].lo = r.lo
				a.ranges = append(a.ranges[:i], a.ranges[i+1:]...)
			}
			return true
		case pn == r.lo-1:
			r.lo--
			if i+1 < len(a.ranges) && a.ranges[i+1].hi == r.lo-1 {
				r.lo = a.ranges[i+1].lo
				a.ranges = append(a.ranges[:i+1], a.ranges[i+2:]...)
			}
			return true
		case pn > r.hi:
			a.ranges = append(a.ranges, pnRange{})
			copy(a.ranges[i+1:], a.ranges[i:])
			a.ranges[i] = pnRange{lo: pn, hi: pn}
			a.trim()
			return true
		}
	}
	a.ranges = append(a.ranges, pnRange{lo: pn, hi: pn})
	a.trim()
	return true
}

// trim drops the lowest ranges past the bound. What is forgotten stops being
// acknowledged, and the peer retransmits its frames under new numbers — the
// cost lands on the endpoint that opened the gaps. The floor rises to just
// above the highest number dropped: below it, received and never-received
// numbers can no longer be told apart, and record refuses them all (§12.3).
func (a *ackTracker) trim() {
	if len(a.ranges) > maxAckRanges {
		a.floor, a.hasFloor = a.ranges[maxAckRanges].hi+1, true
		a.ranges = a.ranges[:maxAckRanges]
	}
}

// onAckEliciting notes a packet that must be answered. Initial and handshake
// spaces answer at once; the application space answers the second packet at
// once and the first on a short timer (§13.2.1-2), which is what lets one
// ACK cover a burst — except when the packet arrived out of order or opened
// a gap, where §13.2.1 says to answer immediately so the peer's loss
// detection sees the reordering now rather than a delayed-ACK period later,
// and when the packet was marked CE by the network (ce), where the same
// section says to answer immediately "to reduce the peer's response time to
// congestion events": the mark is worth a round trip, not a delayed-ACK
// period plus one.
func (a *ackTracker) onAckEliciting(space int, now time.Time, ce bool) {
	a.elicitingSinceAck++
	if space != spaceApplication || a.elicitingSinceAck >= 2 || a.outOfOrder || ce {
		a.immediate = true
		a.due = now
		return
	}
	if a.due.IsZero() {
		a.due = now.Add(ackTimerDelay)
	}
}

// owes reports whether an ACK should be in the next packet sent.
func (a *ackTracker) owes(now time.Time) bool {
	if len(a.ranges) == 0 {
		return false
	}
	return a.immediate || (!a.due.IsZero() && !now.Before(a.due))
}

// pending reports an ack-eliciting packet not yet acknowledged: owed now,
// or waiting on the delayed-ACK timer. A packet leaving for another reason
// carries it (sendPacketLocked); only owes decides when one leaves alone.
func (a *ackTracker) pending() bool {
	return len(a.ranges) > 0 && !a.due.IsZero()
}

// appendAck writes the ACK owed, if any, and clears the debt.
func (a *ackTracker) appendAck(dst []byte, now time.Time) []byte {
	if len(a.ranges) == 0 {
		return dst
	}
	dst = a.encodeAck(dst, now)
	a.settle()
	return dst
}

// encodeAck writes the ACK owed without clearing the debt: the caller may
// still find nothing to send and leave the debt for the next packet. settle
// clears it once the frame is on its way.
func (a *ackTracker) encodeAck(dst []byte, now time.Time) []byte {
	if len(a.ranges) == 0 {
		return dst
	}
	delay := uint64(0)
	if !a.largestAt.IsZero() && now.After(a.largestAt) {
		delay = uint64(now.Sub(a.largestAt).Microseconds()) //nolint:gosec // G115: positive, now is after largestAt
	}
	return appendAckFrame(dst, a.ranges, delay, [3]uint64{a.ecn[ecnECT0], a.ecn[ecnECT1], a.ecn[ecnCE]})
}

// settle clears the debt an encoded ACK paid.
func (a *ackTracker) settle() {
	a.elicitingSinceAck = 0
	a.immediate = false
	a.outOfOrder = false
	a.due = time.Time{}
}

// rearm owes again the ACK a settled packet did not deliver: one the
// anti-amplification budget withheld (writePacketLocked). The debt rides
// the next packet sent, or the next flush — flushAcksLocked runs after
// every processed packet, so the peer's next datagram, which raises the
// budget, also releases the ACK. No deadline is armed: the timer would
// only find the budget still spent.
func (a *ackTracker) rearm() {
	if len(a.ranges) > 0 {
		a.immediate = true
	}
}
