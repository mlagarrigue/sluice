package quic

import "time"

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
// the peer's handshake timers are tuned for that.
const localMaxAckDelay = 25 * time.Millisecond

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
}

// record notes one received packet number and returns false when it is a
// duplicate — already covered — which the caller drops without processing.
func (a *ackTracker) record(pn uint64, now time.Time) bool {
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
// acknowledged, and the peer retransmits it — the cost lands on the endpoint
// that opened the gaps.
func (a *ackTracker) trim() {
	if len(a.ranges) > maxAckRanges {
		a.ranges = a.ranges[:maxAckRanges]
	}
}

// onAckEliciting notes a packet that must be answered. Initial and handshake
// spaces answer at once; the application space answers the second packet at
// once and the first on a short timer (§13.2.1-2), which is what lets one
// ACK cover a burst — except when the packet arrived out of order or opened
// a gap, where §13.2.1 says to answer immediately so the peer's loss
// detection sees the reordering now rather than a delayed-ACK period later.
func (a *ackTracker) onAckEliciting(space int, now time.Time) {
	a.elicitingSinceAck++
	if space != spaceApplication || a.elicitingSinceAck >= 2 || a.outOfOrder {
		a.immediate = true
		a.due = now
		return
	}
	if a.due.IsZero() {
		a.due = now.Add(localMaxAckDelay)
	}
}

// owes reports whether an ACK should be in the next packet sent.
func (a *ackTracker) owes(now time.Time) bool {
	if len(a.ranges) == 0 {
		return false
	}
	return a.immediate || (!a.due.IsZero() && !now.Before(a.due))
}

// appendAck writes the ACK owed, if any, and clears the debt.
func (a *ackTracker) appendAck(dst []byte, now time.Time) []byte {
	if len(a.ranges) == 0 {
		return dst
	}
	delay := uint64(0)
	if !a.largestAt.IsZero() && now.After(a.largestAt) {
		delay = uint64(now.Sub(a.largestAt).Microseconds()) //nolint:gosec // G115: positive, now is after largestAt
	}
	dst = appendAckFrame(dst, a.ranges, delay)
	a.elicitingSinceAck = 0
	a.immediate = false
	a.outOfOrder = false
	a.due = time.Time{}
	return dst
}
