package quic

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

// Loss detection and congestion control, RFC 9002 — the part of a transport
// that cannot be observed on a loopback, which is why it is tested against a
// harness that drops packets on purpose.
//
// The shape is the RFC's, cut to what this connection sends: every packet
// carrying retransmittable frames is remembered until acknowledged; an ACK
// updates the round-trip estimate, releases what it covers, and declares
// older packets lost by packet distance or by time; a probe timeout resends
// the oldest unacknowledged payload when ACKs stop arriving at all. Loss puts
// a packet's frames back on a queue, not the packet itself: retransmission is
// new packets with old frames, never the same bytes again (§6.1).

// Timing constants, RFC 9002 §6 and Appendix A.
const (
	// packetThreshold declares a packet lost once a packet sent three or
	// more packets after it has been acknowledged (§6.1.1) — packets, not
	// numbers: see sentTracker.skippedAbove.
	packetThreshold = 3
	// timeThresholdNum/Den scale the round trip by 9/8 for time-based loss
	// (§6.1.2).
	timeThresholdNum = 9
	timeThresholdDen = 8
	// granularity is the timer's floor: below it, loss timers fire on noise.
	granularity = time.Millisecond
	// persistentCongestionThreshold is how many probe intervals of
	// unbroken loss make persistent congestion (§7.6.1, kPersistentCongestionThreshold).
	persistentCongestionThreshold = 3
	// initialRTT stands in before the first sample (§6.2.2).
	initialRTT = 333 * time.Millisecond
)

// rttEstimator is §5's smoothed round-trip state.
type rttEstimator struct {
	latest   time.Duration
	min      time.Duration
	smoothed time.Duration
	variance time.Duration
	sampled  bool
	// firstAt is when the first sample was taken (B.8's first_rtt_sample),
	// zero before: only a loss sent after it counts toward persistent
	// congestion (detectLost).
	firstAt time.Time
}

// sample folds in one measurement, taken when an ACK newly covers the largest
// packet it names and that packet was ack-eliciting (§5.1). ackDelay is the
// peer's declared hold — subtracted so its ACK pacing does not read as path
// latency, but never below the smallest round trip seen, which is the RFC's
// guard against a peer whose declared delay swallows the path entirely.
func (r *rttEstimator) sample(rtt, ackDelay time.Duration) {
	if rtt <= 0 {
		return
	}
	r.latest = rtt
	if !r.sampled {
		r.sampled = true
		r.min = rtt
		r.smoothed = rtt
		r.variance = rtt / 2
		return
	}
	if rtt < r.min {
		r.min = rtt
	}
	adjusted := rtt
	if adjusted-ackDelay >= r.min {
		adjusted -= ackDelay
	}
	diff := r.smoothed - adjusted
	if diff < 0 {
		diff = -diff
	}
	r.variance = (3*r.variance + diff) / 4
	r.smoothed = (7*r.smoothed + adjusted) / 8
}

// resetAfterPersistentCongestion restarts the estimate from the newest
// sample, as from a first one: RFC 9002 §5.2 says an endpoint SHOULD set
// min_rtt to the newest sample once persistent congestion is established
// — otherwise a path whose RTT has risen keeps an old minimum that makes
// every later sample look late, and persistent congestion is declared again
// and again — and §5.3 lets smoothed_rtt be reset with it "after a
// disruptive network event", which this is; the variance follows as on a
// first sample. The first-sample date is kept: what happened before the
// collapse still happened.
func (r *rttEstimator) resetAfterPersistentCongestion() {
	if !r.sampled {
		return
	}
	r.min = r.latest
	r.smoothed = r.latest
	r.variance = r.latest / 2
}

// sampleAt is sample on the ACK path, which also dates the first sample.
func (r *rttEstimator) sampleAt(now time.Time, rtt, ackDelay time.Duration) {
	r.sample(rtt, ackDelay)
	if r.sampled && r.firstAt.IsZero() {
		r.firstAt = now
	}
}

// current is the working estimate: the smoothed value once there is one, the
// RFC's stand-in before.
func (r *rttEstimator) current() time.Duration {
	if !r.sampled {
		return initialRTT
	}
	return r.smoothed
}

// pto is the probe timeout for one space (§6.2.1). maxAckDelay is added only
// in the application space, where the peer is allowed to sit on an ACK.
func (r *rttEstimator) pto(maxAckDelay time.Duration) time.Duration {
	v := 4 * r.variance
	if !r.sampled {
		v = 2 * initialRTT
	}
	if v < granularity {
		v = granularity
	}
	return r.current() + v + maxAckDelay
}

// sentPacket is one packet in flight: what to resend if it is lost, and what
// its acknowledgement releases.
type sentPacket struct {
	pn     uint64
	sentAt time.Time
	size   int
	// payload holds the packet's retransmittable frames, encoded. ACK and
	// PADDING are excluded when the packet is built: acknowledgements are
	// regenerated from current state, never replayed, and padding is a
	// property of the datagram that carried it.
	payload []byte
	// The delivery estimator's snapshot of the connection when this packet
	// left (draft-ietf-ccwg-bbr-06 §4.1.2.2): bytes delivered so far, how
	// long before this send the count last moved and the flight's first
	// send happened — offsets from sentAt rather than the two instants,
	// which keeps the record 32 bytes smaller and the acknowledgement path
	// to one subtraction — the flight this packet joined, itself included
	// (P.tx_in_flight), and the bytes declared lost so far (P.lost). Read
	// once, when the packet is acknowledged (deliveryEstimator.update) or
	// declared lost (declareLost).
	delivered      int
	sendElapsed    time.Duration // P.sent_time - P.first_sent_time
	sinceDelivered time.Duration // P.sent_time - P.delivered_time
	txInFlight     int
	lost           int
	ackEliciting   bool
	// acked is markAcked's working mark, and declared detectLost's: neither
	// survives an ACK frame, release removing both (Conn.handleAck).
	acked    bool
	declared bool
	// pmtuProbe marks a DPLPMTUD probe (pmtu.go): its loss releases its
	// bytes from flight but is no congestion event (RFC 9000 §14.4), and
	// its acknowledgement or loss is reported by number to the search.
	pmtuProbe bool
	// ect0 marks a packet whose datagram left with the ECT(0) codepoint,
	// which the peer's ECN counts must then account for (§13.4.2.1).
	ect0 bool
	// appLimited is the snapshot's last field: whether the sender was
	// application-limited when the packet left.
	appLimited bool
	// afterLoss says the packet tracked just before this one was declared
	// lost by an earlier scan: if this one is lost too, it extends that lost
	// range rather than opening one (declareLost). The timer's scan sets it
	// on the copy it keeps, never on a record it has yet to read: a byte
	// stored just ahead of the record's copy stalls the copy. detectLost
	// copies nothing and sets it in place; release copies later.
	afterLoss bool
}

// sentTracker is one packet-number space's in-flight bookkeeping. Packets are
// appended in order — the packet number only grows — so the slice is sorted
// by construction and pruned from the front as acknowledgements arrive.
type sentTracker struct {
	packets  []sentPacket
	lossTime time.Time // when the earliest time-threshold candidate matures
	// lost remembers the packets declared lost whose payload went to the
	// retransmission queue, the newest last: a late acknowledgement of one
	// — the packet arrived after all, reordered — takes its payload off the
	// queue if it is still there (RFC 9000 §13.3: no retransmission of what
	// was acknowledged, "including after being declared lost"). Bounded at
	// lostMemory entries, the oldest forgotten first: by then the payload
	// has long gone again, and a late acknowledgement has nothing to spare.
	lost []lostRef
	// largestAcked persists across ACK frames (RFC 9002 A.10): loss
	// thresholds compare against everything the peer has ever acknowledged,
	// not just what this one frame newly covered — an ACK that repeats old
	// news must still be able to mature a pending loss.
	largestAcked    uint64
	hasLargestAcked bool
	// skips are the packet numbers this end deliberately never used (RFC
	// 9000 §21.4), the latest at skips[0], nSkips of them: a peer that
	// acknowledges one acknowledged something it cannot have received, and
	// is closed with PROTOCOL_VIOLATION before its invented ACKs open the
	// congestion window — caught is the number it named. The latest gap is
	// enough for that: an attacker acknowledging every number up to the
	// largest it sees hits it within a window. The packet threshold needs
	// the older ones (skippedAbove): the latest is usually still in flight,
	// and the gap a reordered packet straddles is a window below it. Gaps
	// land at least a quarter of a flight apart while the window doubles,
	// so four reach back past the largest acknowledged. nextSkip is where
	// the next gap goes; skipArmed says whether it has been drawn yet.
	skips     [4]uint64
	nSkips    int
	caught    uint64
	nextSkip  uint64
	skipArmed bool
	// pathStart is when the current path began (RFC 9000 §9.4). A packet
	// sent before it was sent on a previous path: its acknowledgement or
	// loss still leaves the record — and a lost payload is still queued
	// for retransmission, this time on the current path — but it "MUST
	// NOT contribute to congestion control or RTT estimation for the new
	// path": no flight bytes released, no window growth, no congestion
	// event, no round-trip sample. Zero until the first migration, which
	// no packet predates. The one mark is cheaper than a path tag per
	// packet: the record is sorted by send time, so one comparison per
	// acknowledged or lost packet is the whole cost.
	pathStart time.Time
}

// onPath reports whether p was sent on the current path (see pathStart).
func (s *sentTracker) onPath(p *sentPacket) bool { return !p.sentAt.Before(s.pathStart) }

// errOptimisticAck is markAcked's verdict on an ACK that covers the skipped
// number; handleAck turns it into the connection error.
var errOptimisticAck = errors.New("quic: the peer acknowledged a packet number that was skipped on purpose")

func (s *sentTracker) record(p sentPacket) {
	s.packets = append(s.packets, p)
}

// skipNumber reports whether pn is the number to leave unused, and draws the
// next gap when it is. window is the congestion window in packets: gaps land
// a window apart on average (uniform in [½w, 1½w)), so one number per window
// is the whole cost, and an optimistic acknowledger — whose gain is a window
// of unearned growth — is caught within the window it tries to steal. The
// draw is from crypto/rand, once per window: a peer must not be able to
// guess where the gap is, which is what makes it a detector rather than a
// convention.
func (s *sentTracker) skipNumber(pn, window uint64) bool {
	if !s.skipArmed {
		s.skipArmed = true
		s.nextSkip = pn + skipGap(window)
		return false
	}
	if pn < s.nextSkip {
		return false
	}
	copy(s.skips[1:], s.skips[:])
	s.skips[0], s.nSkips = pn, min(s.nSkips+1, len(s.skips))
	s.nextSkip = pn + 1 + skipGap(window)
	return true
}

// skippedAbove is how many numbers between pn and the largest acknowledged
// were never sent. The packet threshold counts packets sent after pn
// (§6.1.1, "similar to TCP", whose threshold counts segments), and a
// skipped number is not one: counted, a datagram that two successors
// overtook across a gap was declared lost at a threshold of two, below the
// SHOULD NOT of §6.1.1 — a spurious congestion event about once per window
// of reordering under the threshold. A gap older than the four kept lies
// more than a flight below the largest, past any packet the count could
// still spare.
func (s *sentTracker) skippedAbove(pn uint64) uint64 {
	var n uint64
	for _, k := range s.skips[:s.nSkips] {
		if pn < k && k < s.largestAcked {
			n++
		}
	}
	return n
}

func skipGap(window uint64) uint64 {
	window = max(window, 2)
	var b [8]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24
	return window/2 + binary.LittleEndian.Uint64(b[:])%window
}

// ackOutcome is what processing one ACK frame yields.
type ackOutcome struct {
	// largestMarked is the highest packet the frame newly acknowledged;
	// prevLargest the largest acknowledged before the frame, if hadPrev
	// (detectLost's persistent-congestion span).
	largestMarked    uint64
	prevLargest      uint64
	hadPrev          bool
	declared         int  // packets detectLost marked, for release
	newlyAcked       int  // bytes released from flight (current path only)
	newlyAckedCount  int  // packets released, whichever path sent them
	largestWasNew    bool // the frame's largest was newly acknowledged…
	largestEliciting bool // …ack-eliciting and sent on this path: what a sample needs
	largestSentAt    time.Time
	// latestSentAt is when the most recent tracked packet the frame newly
	// acknowledges was sent — the largest itself when that one is tracked.
	// It dates a congestion event (RFC 9002 B.7) where largestSentAt
	// cannot: only ack-eliciting packets are recorded, and the peer
	// acknowledges every number it received, ACK-only ones included; a
	// frame whose largest is one of those left largestSentAt at zero, which
	// read as "before the recovery began" and dropped the CE mark it
	// carried (§7.3.1 MUST), until 2026-10-21.
	latestSentAt time.Time
	lostBytes    int
	lostCount    uint64 // packets behind lostBytes, for Stats.LostPackets
	// lostRanges is how many lost ranges the batch opens
	// (sentPacket.afterLoss), crossing the first lost packet whose losses
	// crossed BBR's threshold (lossEvent).
	lostRanges      int
	crossing        lostPacket
	lost            [][]byte // payloads to queue for retransmission
	lateAcked       [][]byte // payloads queued for packets the frame acknowledged after their loss (sentTracker.lost)
	sawAckEliciting bool     // any newly acked packet was ack-eliciting
	newlyAckedECT0  int      // newly acked packets that left marked ECT(0), for ECN validation
	raisedLargest   bool     // the frame increased the largest acknowledged: validation may fail on it (§13.4.2.1)
	// latestLossSent is when the *newest* lost packet in this batch was
	// sent, which is what gates a congestion event (RFC 9002 B.8): one
	// pre-recovery straggler in a batch must not shield a genuinely new
	// loss epoch from the window reduction it deserves.
	latestLossSent time.Time
	// lostSpan is the longest interval between the send times of two
	// ack-eliciting packets this batch declared lost with nothing
	// acknowledged between them — what RFC 9002 §7.6.2 measures against
	// the persistent-congestion duration, which the connection knows and
	// the tracker does not (it includes the peer's max_ack_delay).
	lostSpan time.Duration
	// probeAcked and probeLost report a DPLPMTUD probe's fate by packet
	// number (pmtu.go matches it against the probe in flight); probeLostBytes
	// leave flight without the window shrinking.
	probeAcked, probeLost     bool
	probeAckedPN, probeLostPN uint64
	probeLostBytes            int
	// largeAcked and largeLost feed the black-hole indicator (pmtu.go):
	// a packet above 1200 bytes acknowledged on this path, and how many
	// such packets were declared lost.
	largeAcked bool
	largeLost  int
}

// The tracker's side of an ACK frame runs in three steps, in RFC 9002
// A.7's order, with the connection's own steps between them
// (Conn.handleAck): markAcked finds what the frame newly acknowledges; the
// connection takes the round-trip sample and reads the ECN counts;
// detectLost declares what the new largest acknowledged leaves behind, and
// the connection reports those losses — the congestion event, then
// persistent congestion; release hands each acknowledged packet to the
// controller last. The order is what the window sees: one pass used to
// grow the window by a slow-start flight and then halve the result, where
// A.7 halves first and grows nothing sent before the recovery period the
// losses just opened (B.5).

// markAcked marks what the ranges cover and reads the frame's
// acknowledgement side into the outcome: what it newly acknowledges, and
// whether the largest it names is among that. largest is the frame's own
// largest field; decodeAckRanges walks its encoded ranges. Nothing leaves
// the record here: release does that, after detectLost.
func (s *sentTracker) markAcked(largest uint64, rangeData []byte) (ackOutcome, error) {
	var out ackOutcome

	// Both sides are ordered — s.packets ascending by construction (the
	// packet number only grows), the frame's ranges strictly descending by
	// encoding (parseACK proved each one sits gap+2 below the previous) — so
	// one two-pointer merge marks every acknowledged packet without a map and
	// without rescanning the slice per range. An ACK may name billions of
	// packets; only the ones still in s.packets cost anything.
	i := len(s.packets) - 1
	optimistic := false
	err := decodeAckRanges(largest, rangeData, func(lo, hi uint64) {
		for _, k := range s.skips[:s.nSkips] {
			if lo <= k && k <= hi && !optimistic {
				// The range names a number that was never sent (§21.4).
				// The marks already set stay set: the verdict closes the
				// connection, and this tracker with it.
				optimistic, s.caught = true, k
			}
		}
		if optimistic {
			return
		}
		i = s.below(i, hi)
		for i >= 0 && s.packets[i].pn >= lo {
			if p := &s.packets[i]; !p.acked {
				p.acked = true
				out.mark(p, s.onPath(p), largest)
			}
			i--
		}
	})
	if err != nil {
		return out, err
	}
	if optimistic {
		return out, errOptimisticAck
	}
	if len(s.lost) > 0 {
		s.lateAcked(&out, largest, rangeData)
	}

	// The largest ever acknowledged persists (A.10); the one before this
	// frame is remembered for detectLost's persistent-congestion span.
	out.prevLargest, out.hadPrev = s.largestAcked, s.hasLargestAcked
	if out.newlyAckedCount > 0 && (!s.hasLargestAcked || out.largestMarked > s.largestAcked) {
		s.largestAcked = out.largestMarked
		s.hasLargestAcked = true
		out.raisedLargest = true
	}
	return out, nil
}

// below returns the highest index at or under i whose packet number is at
// most hi, or -1. It gallops down — steps of 1, 2, 4, … — then bisects the
// last step, so it costs the logarithm of how far it moves: a step or two
// when the peer is current, and no walk through the window above an old
// largest. An ACK arrives a round trip after the largest packet it names
// was sent, with a window's worth sent since, and stepping down through
// those one at a time made every frame O(window) — O(window²) per round
// trip on a long fat path, or on purpose from a peer acknowledging one old
// packet per frame.
func (s *sentTracker) below(i int, hi uint64) int {
	top := i + 1 // every index from top up holds a number above hi
	for step := 1; i >= 0 && s.packets[i].pn > hi; step <<= 1 {
		top = i
		i -= step
	}
	lo := max(i, 0) // the answer is in [lo-1, top-1]
	for lo < top {
		mid := int(uint(lo+top) >> 1)
		if s.packets[mid].pn <= hi {
			lo = mid + 1
		} else {
			top = mid
		}
	}
	return lo - 1
}

// mark books one newly acknowledged packet. The merge walks down, so the
// first packet booked is the frame's highest.
func (out *ackOutcome) mark(p *sentPacket, onPath bool, largest uint64) {
	if out.newlyAckedCount == 0 {
		out.largestMarked = p.pn
	}
	out.newlyAckedCount++
	if p.ect0 {
		out.newlyAckedECT0++
	}
	if onPath {
		out.newlyAcked += p.size
		if p.size > pmtuBaseSize {
			out.largeAcked = true
		}
	}
	if p.ackEliciting {
		out.sawAckEliciting = true
	}
	if p.pmtuProbe && !out.probeAcked {
		out.probeAcked, out.probeAckedPN = true, p.pn
	}
	if p.pn == largest {
		out.largestWasNew = true
		out.largestEliciting = p.ackEliciting && onPath
		out.largestSentAt = p.sentAt
	}
	if p.sentAt.After(out.latestSentAt) {
		out.latestSentAt = p.sentAt
	}
}

// detectLost declares lost what the largest acknowledged leaves behind
// (§6.1): anything packetThreshold below it, or sent before it and older
// than the time threshold — which the round-trip sample this frame just
// took sets, A.7 sampling before it detects. Whatever is too young for
// either arms the loss timer instead. The scan runs on every ACK, not just
// the ones with news: the timer was cleared here, and an ACK that repeats
// old news used to disarm a pending loss deadline for good — the loss then
// surfaced only on a later new ACK, or on the PTO. Nothing past the
// largest acknowledged can be lost, so the scan stops there. It marks and
// moves nothing: release removes the lost with the acknowledged, in one
// compaction.
//
// The same pass measures the batch for RFC 9002 §7.6: the longest span
// between the send times of two ack-eliciting packets declared lost with
// nothing acknowledged between them — the only state it needs is where the
// current run began, because the order here is the send order. An
// acknowledged packet between two losses starts the run over whether this
// frame acknowledged it or an earlier one did: the earlier ones are all at
// or below the previous largest acknowledged, so a run that straddles that
// number is cut there. (A run can also straddle a packet acknowledged two
// frames ago and below that number; the RFC allows per-space state that
// errs on the side of declaring.) Only a loss sent after the first
// round-trip sample counts (§7.6.2, B.8): before it, the probe timeout ran
// on the initial RTT, and the span would measure a guess.
func (s *sentTracker) detectLost(now time.Time, rtt *rttEstimator, est *deliveryEstimator, out *ackOutcome) {
	s.lossTime = time.Time{}
	if !s.hasLargestAcked {
		return
	}
	threshold := max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
	lostBefore := now.Add(-threshold)
	var runStart time.Time
	var runStartPN uint64
	prevLost := false // the packet before this one was lost, by this scan or (afterLoss) an earlier one
	for i := range s.packets {
		p := &s.packets[i]
		if p.pn > s.largestAcked {
			if prevLost {
				p.afterLoss = true
			}
			break
		}
		if p.acked {
			prevLost = false
			runStart = time.Time{}
			continue
		}
		lostByCount := p.pn+packetThreshold+s.skippedAbove(p.pn) <= s.largestAcked
		lostByTime := p.pn < s.largestAcked && !p.sentAt.After(lostBefore)
		if lostByCount || lostByTime {
			extends := prevLost || p.afterLoss
			prevLost = true
			p.declared = true
			out.declared++
			if s.declareLost(out, p, est, extends) && !rtt.firstAt.IsZero() && p.sentAt.After(rtt.firstAt) {
				if out.hadPrev && runStartPN < out.prevLargest && out.prevLargest < p.pn {
					runStart = time.Time{}
				}
				if runStart.IsZero() {
					runStart, runStartPN = p.sentAt, p.pn
				} else if span := p.sentAt.Sub(runStart); span > out.lostSpan {
					out.lostSpan = span
				}
			}
			continue
		}
		if p.pn < s.largestAcked && s.lossTime.IsZero() {
			// Not lost yet by time, but older than the largest: it will
			// be, if nothing acknowledges it by then.
			s.lossTime = p.sentAt.Add(threshold)
		}
		if prevLost {
			p.afterLoss = true
			prevLost = false
		}
	}
	if est != nil {
		est.lost += out.lostBytes
	}
}

// release removes what markAcked and detectLost marked, and hands each
// acknowledged packet of the current path to the delivery estimator, then
// to cc, in number order (B.5 grows the window per packet). A.7 calls it
// after the losses, so a packet sent before the recovery period they
// opened releases its flight and grows nothing. est yields the frame's
// rate sample; cc and est are nil when a test drives the tracker alone.
// The walk stops at the last marked packet and moves the rest of the
// record in one copy.
func (s *sentTracker) release(now time.Time, rtt *rttEstimator, cc congestionController, est *deliveryEstimator, out *ackOutcome) {
	left := out.newlyAckedCount + out.declared
	if left == 0 {
		return
	}
	i := 0
	for i < len(s.packets) && !s.packets[i].acked && !s.packets[i].declared {
		i++
	}
	kept := s.packets[:i]
	if est != nil && out.newlyAckedCount > 0 {
		est.begin()
	}
	for ; i < len(s.packets) && left > 0; i++ {
		p := &s.packets[i]
		if !p.acked {
			if p.declared {
				left--
			} else {
				kept = append(kept, *p)
			}
			continue
		}
		left--
		if !s.onPath(p) {
			continue
		}
		if est != nil {
			est.update(p)
		}
		if cc != nil {
			ev := ackEvent{now: now, sentAt: p.sentAt, size: p.size, srtt: rtt.current()}
			if est != nil {
				ev.delivered = est.delivered - p.delivered
				ev.txInFlight, ev.lost = p.txInFlight, est.lost-p.lost
				ev.sendElapsed, ev.sinceDelivered, ev.appLimited = p.sendElapsed, p.sinceDelivered, p.appLimited
				// Sent times rise with numbers: the highest newly
				// marked is the frame's newest, on this path if any
				// is, and its sample is the frame's (ackEvent.rated).
				ev.rated = p.pn == out.largestMarked && ev.rateSample().reliable(rtt.min)
			}
			cc.onAcked(ev)
		}
	}
	// Close the gap the freed packets leave by moving whichever side is
	// shorter. Moving the tail down — everything sent after the last packet
	// freed — cost a window's worth of copying per ACK frame, since an ACK
	// names packets a round trip old; the packets kept below the freed run
	// are the few still waiting on reordering, so they move up instead and
	// the slice advances from the front, as a queue does. The front slots
	// are cleared for the payloads they still reference; append reallocates
	// once the capacity behind the advanced front runs out, which amortises
	// as any append does.
	if tail := len(s.packets) - i; len(kept) < tail {
		dst := i - len(kept)
		copy(s.packets[dst:i], kept)
		clear(s.packets[:dst])
		s.packets = s.packets[dst:]
	} else {
		s.packets = append(kept, s.packets[i:]...)
	}
	if est != nil && out.newlyAckedCount > 0 {
		est.finish(now, rtt.min)
	}
}

// collectTimeLosses declares the packets whose loss timer has now matured,
// set by a previous ACK that left them too young to call.
//
// Only packets sent before the largest acknowledged one are candidates
// (RFC 9002 §6.1, A.10's DetectAndRemoveLostPackets): a packet sent after
// the ACK that armed the timer has nothing newer delivered to prove it
// missing, however old it grows. The sweep used to go by age alone, on the
// reasoning that an early loss only costs a duplicate; it also costs a
// congestion event, and on a clean loopback it halved the window one run
// in ten: a DPLPMTUD probe's hole armed the timer, and the transfer behind
// it — older than the 1 ms floor, acknowledged a moment later — was swept
// up as lost (TestLossTimerSparesPacketsPastLargestAcked).
//
// A DPLPMTUD probe that matured is reported apart (probeLost and the
// probe's bytes), never in lostBytes: its loss is no congestion event.
func (s *sentTracker) collectTimeLosses(now time.Time, rtt *rttEstimator, est *deliveryEstimator) (out ackOutcome) {
	if s.lossTime.IsZero() || now.Before(s.lossTime) {
		return out
	}
	threshold := max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
	// Lost once sent at or before this instant: A.10's time_sent <=
	// lost_send_time. Strictly before spared a packet at the instant its
	// timer matured and re-armed the timer on that same instant, which the
	// timer loop then waited out with a zero wait until the clock moved
	// (TestLossTimerDeclaresAtTheInstantItMatures).
	lostBefore := now.Add(-threshold)
	s.lossTime = time.Time{}
	prevLost := false // as in detectLost
	kept := s.packets[:0]
	for i := range s.packets {
		p := &s.packets[i]
		if p.pn <= s.largestAcked && !p.sentAt.After(lostBefore) {
			s.declareLost(&out, p, est, prevLost || p.afterLoss)
			prevLost = true
			continue
		}
		// Past the largest acknowledged, not a candidate and not a reason
		// to re-arm the timer: the next ACK that covers something newer
		// scans it (detectLost).
		if p.pn <= s.largestAcked && s.lossTime.IsZero() {
			s.lossTime = p.sentAt.Add(threshold)
		}
		kept = append(kept, *p)
		if prevLost {
			kept[len(kept)-1].afterLoss = true
			prevLost = false
		}
	}
	s.packets = kept
	if est != nil {
		est.lost += out.lostBytes
	}
	return out
}

// declareLost books one packet the ACK path or the loss timer declared lost.
// A DPLPMTUD probe is reported apart (probeLost and its bytes), never as a
// congestion event. Any other payload is queued for retransmission whatever
// path sent it; but only a packet sent on the current path releases flight
// bytes and dates the loss — the newest loss gates the congestion event
// (RFC 9002 B.8) — because a previous path's loss says nothing about this
// one (RFC 9000 §9.4, pathStart). It reports whether the loss counts for
// persistent congestion: ack-eliciting (§7.6.2) and on this path.
//
// A congestion loss also counts toward est's C.lost, packet by packet in
// number order as BBRHandleLostPacket runs (draft-ietf-ccwg-bbr-06
// §5.5.10.2), the first packet whose losses since it left exceed BBR's
// threshold kept as the batch's crossing; and it opens a lost range unless
// it extends one — the packet tracked before it was lost, in this batch or
// an earlier one (afterLoss): the packet threshold declares a hole's last
// packets an ACK or two after its first, and Startup counts discontiguous
// ranges (§5.3.1.3). A lost probe or a previous path's packet sits in its
// range without ever opening one.
func (s *sentTracker) declareLost(out *ackOutcome, p *sentPacket, est *deliveryEstimator, extends bool) bool {
	onPath := s.onPath(p)
	if p.pmtuProbe {
		out.probeLost, out.probeLostPN = true, p.pn
		if onPath {
			out.probeLostBytes += p.size
		}
		return false
	}
	out.lostCount++
	if len(p.payload) > 0 {
		out.lost = append(out.lost, p.payload)
		s.rememberLost(p.pn, p.payload)
	}
	if !onPath {
		return false
	}
	out.lostBytes += p.size
	if p.size > pmtuBaseSize {
		out.largeLost++
	}
	if p.sentAt.After(out.latestLossSent) {
		out.latestLossSent = p.sentAt
	}
	if est != nil && out.crossing.size == 0 {
		// C.lost as it stands after this packet: the scan adds the batch
		// to it when done.
		if lost := est.lost + out.lostBytes - p.lost; lossTooHigh(lost, p.txInFlight) {
			out.crossing = lostPacket{size: p.size, txInFlight: p.txInFlight, lost: lost, appLimited: p.appLimited}
		}
	}
	if !extends {
		out.lostRanges++
	}
	return p.ackEliciting
}

// lostRef is one entry of sentTracker.lost: a packet declared lost and the
// payload queued in its name.
type lostRef struct {
	pn      uint64
	payload []byte
}

// lostMemory bounds sentTracker.lost.
const lostMemory = 64

// rememberLost records a packet declared lost whose payload is queued for
// retransmission, forgetting the oldest entry past lostMemory.
func (s *sentTracker) rememberLost(pn uint64, payload []byte) {
	if len(s.lost) == lostMemory {
		copy(s.lost, s.lost[1:])
		s.lost = s.lost[:lostMemory-1]
	}
	s.lost = append(s.lost, lostRef{pn: pn, payload: payload})
}

// lateAcked moves the entries of s.lost that the frame's ranges cover to
// out.lateAcked: packets declared lost that arrived after all. Called only
// when s.lost holds something, which no connection without loss does.
func (s *sentTracker) lateAcked(out *ackOutcome, largest uint64, rangeData []byte) {
	_ = decodeAckRanges(largest, rangeData, func(lo, hi uint64) {
		kept := s.lost[:0]
		for _, l := range s.lost {
			if lo <= l.pn && l.pn <= hi {
				out.lateAcked = append(out.lateAcked, l.payload)
				continue
			}
			kept = append(kept, l)
		}
		s.lost = kept
	})
}

// oldestEliciting is what the probe timeout is armed on.
func (s *sentTracker) oldestEliciting() (sentPacket, bool) {
	for _, p := range s.packets {
		if p.ackEliciting {
			return p, true
		}
	}
	return sentPacket{}, false
}

// liftOldestResendable takes the payload of the oldest packet a probe can
// resend and leaves the packet itself in flight, now without a payload. The
// probe timeout sends that payload again as new packets, and RFC 9002 §7.5
// has them counted additionally in flight: the original stays in the
// record and in the count until its own acknowledgement or its loss retires
// it, like any other packet — a late acknowledgement of it is a sample, not
// a number nobody knows. Nothing is marked lost at the timeout, which RFC
// 9002 §6.2 forbids: a lost payload is resent, a lost packet is for §6.1 to
// declare. Its payload goes with the probe so that a loss
// declared later does not queue the same frames a third time, and so that
// the next timeout moves on: with the payload left in place a probe copy
// would be anonymous — acknowledged under its own number, it would tell the
// record nothing about the packet it copied, which would stay the oldest
// resendable, be copied again at every backoff step, and shadow every
// packet behind it. A server whose two-packet Initial flight is lost would
// then resend the first packet forever and never the second, and the
// client, holding half a ServerHello, would wait out the handshake.
//
// Until 2026-10-21 the packet was removed from the record instead, which
// kept the flight the same size when §7.5 says it must grow.
func (s *sentTracker) liftOldestResendable() ([]byte, bool) {
	for i := range s.packets {
		if p := &s.packets[i]; p.ackEliciting && len(p.payload) > 0 {
			payload := p.payload
			p.payload = nil
			return payload, true
		}
	}
	return nil, false
}

// takeAll empties the tracker, returning every retransmittable payload — what
// discarding a space's keys does to its in-flight state is drop it, and what
// a Retry does is queue it again under new keys.
func (s *sentTracker) takeAll() (payloads [][]byte, bytes int) {
	for _, p := range s.packets {
		bytes += p.size
		if len(p.payload) > 0 {
			payloads = append(payloads, p.payload)
		}
	}
	s.packets = nil
	s.lossTime = time.Time{}
	return payloads, bytes
}

// congestionController is what the connection asks of its congestion
// control, whatever the algorithm behind it (RFC 9002 §7 names NewReno as
// one choice among several). Every site that sends, acknowledges or loses a
// packet talks to it through these methods and nothing else, so a second
// algorithm is a second implementation and no change to the sending code.
// Events are passed by value — no allocation per packet. Callers hold the
// connection's lock.
type congestionController interface {
	// canSend reports whether bytes more may leave now; a refusal is also
	// the utilisation signal of §7.8 (see newReno.canSend).
	canSend(bytes int) bool
	// onSent adds a packet's bytes to the flight.
	onSent(now time.Time, bytes int)
	// onAcked is called once per newly acknowledged packet sent on the
	// current path, in packet-number order (RFC 9002 B.5 runs the growth
	// test per packet); the frame's newest, last, carries the frame's
	// sample (ackEvent.rated).
	onAcked(ev ackEvent)
	// onLost is called once per batch of losses declared together — one
	// ACK frame or one loss-timer expiry (§7.3.2: one congestion event).
	onLost(ev lossEvent)
	// onPersistentCongestion follows onLost in the same batch when the
	// losses span more than the persistent-congestion duration (§7.6).
	onPersistentCongestion()
	// onMarked follows the onAcked calls of an ACK frame whose ECN counts
	// passed validation and report more CE marks than before (RFC 9000
	// §13.4.2): the network is signalling congestion without dropping.
	// Controllers that read loss only ignore it.
	onMarked(ev markEvent)
	// onLifted releases bytes that leave the flight record without being
	// acknowledged or lost: a lost DPLPMTUD probe, a discarded space, a
	// Retry. (Not a probe timeout's: what it resends stays in flight,
	// RFC 9002 §7.5.)
	onLifted(bytes int)
	// pacingRate is how fast the pacer may release bytes, in bytes per
	// second, given the current smoothed round trip (§7.7).
	pacingRate(srtt time.Duration) float64
	// window and bytesInFlight are read by Stats and by the packet-number
	// skip (sentTracker.skipNumber sizes its gaps in windows).
	window() int
	bytesInFlight() int
	// setMaxDatagram follows a DPLPMTUD success: the per-datagram unit of
	// window growth changes, the window itself does not (pmtu.go).
	setMaxDatagram(bytes int)
}

// ackEvent is one newly acknowledged packet, as onAcked sees it.
type ackEvent struct {
	now    time.Time     // when the acknowledgement was processed
	sentAt time.Time     // when the packet left
	size   int           // bytes it occupied on the wire
	srtt   time.Duration // the smoothed round trip, this frame's sample included (A.7)
	// The delivery estimator's reading for this packet (deliveryEstimator):
	// delivered is how many bytes the network delivered between the
	// packet's send and this acknowledgement, itself included; sendElapsed
	// and sinceDelivered are the packet's snapshot (sentPacket). rateSample
	// turns them into the draft's sample.
	delivered      int
	sendElapsed    time.Duration
	sinceDelivered time.Duration
	appLimited     bool
	// txInFlight is the flight the packet joined, itself included
	// (RS.tx_in_flight, §2.3), and lost the bytes declared lost since it
	// left, this frame's losses included — A.7 declares them before it
	// releases the acknowledged (RS.lost = C.lost - P.lost): what
	// IsInflightTooHigh weighs
	// (§5.5.10.2).
	txInFlight int
	lost       int
	// rated marks the newest packet the frame delivered when the frame's
	// sample is reliable (rateSample.reliable): the one sample per
	// acknowledgement the draft defines (§4.1.2.3), and the only one BBR's
	// max filter reads (§5.5.5). The frame's older packets yield samples of
	// their own, noisier and more of them; the filter would keep their
	// highest.
	rated bool
}

// markEvent is one ACK frame's newly reported CE marks, as onMarked sees
// them: the peer counts marked packets, not bytes, and names no packet
// (RFC 9000 §19.3.2), so the event is a count at a time, not a packet.
// latestSent is when the frame's largest acknowledged packet left — what
// RFC 9002 B.7 dates the congestion event with.
type markEvent struct {
	now        time.Time
	latestSent time.Time
	packets    int
}

// rateSample is one acknowledgement's delivery-rate sample
// (draft-ietf-ccwg-bbr-06 §4.1.2.1.3): delivered bytes over interval, the
// longer of the flight's send interval and its acknowledgement interval
// (§4.1.1.2.4) — the longer one filters the implausible rates that ACK
// compression and aggregation produce. The rate itself is a
// division the consumer does (deliveryRate). Zero interval means nothing
// was delivered.
type rateSample struct {
	delivered  int
	interval   time.Duration
	appLimited bool // the newest delivered packet was sent application-limited (§4.1.1.3)
}

// reliable says the sample measures the path (§4.1.2.3): an interval
// shorter than the minimum round trip is a spuriously retransmitted
// packet's acknowledgement, and would read as a rate the path never had.
func (rs rateSample) reliable(minRTT time.Duration) bool {
	return rs.interval > 0 && rs.interval >= minRTT
}

// deliveryRate is the sample in bytes per second, zero when there is none.
func (rs rateSample) deliveryRate() float64 {
	if rs.interval <= 0 {
		return 0
	}
	return float64(rs.delivered) / rs.interval.Seconds()
}

// rateSample is the sample this packet yields (§4.1.2.3) — the frame's own
// sample when the packet is the newest the frame delivered (rated). Computed
// here and not on the ACK path, which computes it once per frame to rate
// the newest: a subtraction of instants per acknowledged packet was
// measured at a fifth of that path's cost, for a figure Reno and CUBIC
// never read.
func (ev ackEvent) rateSample() rateSample {
	return rateSample{
		delivered:  ev.delivered,
		interval:   max(ev.sendElapsed, ev.now.Sub(ev.sentAt)+ev.sinceDelivered),
		appLimited: ev.appLimited,
	}
}

// deliveryEstimator is the draft's per-connection state (§4.1.2.1.1): how
// much the network has delivered over the connection's lifetime, when that
// count last moved, and the send time the current flight is measured from.
// Every acknowledgement yields one rateSample, readable from each ackEvent
// the controller is handed and kept for Stats.DeliveryRate when it is
// reliable. Reno and CUBIC grow on acknowledgements alone; BBR lives on
// it. Only packets of the current path count: a previous path's
// deliveries are not evidence about this one (RFC 9000 §9.4, see
// sentTracker.pathStart).
type deliveryEstimator struct {
	delivered     int       // C.delivered: bytes acknowledged so far, pure ACKs excluded
	lost          int       // C.lost: bytes declared lost so far, on the current path
	deliveredTime time.Time // C.delivered_time: when delivered last moved
	firstSentTime time.Time // C.first_sent_time: the newest delivered packet's send, or the first send after idle
	// appLimited is C.app_limited: the delivered count the connection must
	// pass before the bubble of idle time has left the pipeline, zero when
	// the sender is not application-limited (§4.1.1.3, §4.1.2.4).
	appLimited int
	// The newest packet the acknowledgement in progress delivered (rs.prior_*
	// in §4.1.2.1.3), copied field by field — the record it came from is
	// compacted under it — and sampled says whether there is one yet.
	// finish turns them into cur, the frame's sample.
	sampled        bool
	priorDelivered int
	sendElapsed    time.Duration
	sinceDelivered time.Duration
	newestSentAt   time.Time
	newestLimited  bool
	cur            rateSample
	// rate is Stats.DeliveryRate: the last sample that was reliable and not
	// application-limited, in bytes per second; zero until there is one.
	rate float64
}

// onSent snapshots the connection's delivery state into a packet about to
// leave (§4.1.2.2, OnPacketSent); inFlight is the flight before it. An
// empty flight is idle: the flight starting now is measured from now, not
// from the last acknowledgement, which may be long gone.
func (e *deliveryEstimator) onSent(p *sentPacket, inFlight int) {
	if inFlight == 0 {
		e.firstSentTime, e.deliveredTime = p.sentAt, p.sentAt
	}
	p.delivered = e.delivered
	p.sendElapsed = p.sentAt.Sub(e.firstSentTime)
	p.sinceDelivered = p.sentAt.Sub(e.deliveredTime)
	p.txInFlight = inFlight + p.size
	p.lost = e.lost
	p.appLimited = e.appLimited != 0
}

// markAppLimited records that the sender ran out of data while the window
// had room (§4.1.2.4): every sample until the bytes now in flight are
// delivered is application-limited — the bubble of idle time this leaves in
// the pipeline takes a round trip to drain. Never zero, which means unmarked.
func (e *deliveryEstimator) markAppLimited(inFlight int) {
	e.appLimited = max(e.delivered+inFlight, 1)
}

// begin opens an acknowledgement's sample (§4.1.2.3, GenerateRateSample).
func (e *deliveryEstimator) begin() {
	e.cur, e.sampled = rateSample{}, false
}

// update counts one newly acknowledged packet of the current path as
// delivered and remembers it as the newest (§4.1.2.3, UpdateRateSample).
// Packets come in number order, which is send order, so each one is the
// most recently sent so far — the draft's IsNewestPacket, which orders by
// send time and then by packet number.
func (e *deliveryEstimator) update(p *sentPacket) {
	e.delivered += p.size
	e.sampled = true
	e.priorDelivered = p.delivered
	e.sendElapsed, e.sinceDelivered = p.sendElapsed, p.sinceDelivered
	e.newestSentAt = p.sentAt
	e.newestLimited = p.appLimited
}

// finish closes the acknowledgement's sample from its newest packet, lifts
// the application-limited mark once the bubble has been delivered, and
// keeps the sample for Stats when it is reliable and not
// application-limited.
func (e *deliveryEstimator) finish(now time.Time, minRTT time.Duration) {
	if !e.sampled {
		return
	}
	e.deliveredTime = now
	e.firstSentTime = e.newestSentAt
	e.cur = rateSample{
		delivered:  e.delivered - e.priorDelivered,
		interval:   max(e.sendElapsed, now.Sub(e.newestSentAt)+e.sinceDelivered),
		appLimited: e.newestLimited,
	}
	if e.appLimited != 0 && e.delivered > e.appLimited {
		e.appLimited = 0
	}
	if e.cur.appLimited || !e.cur.reliable(minRTT) {
		return
	}
	e.rate = e.cur.deliveryRate()
}

// writeBeginLocked opens a Stream write. The draft checks for an
// application-limited phase upon each write from the application, before
// its data is sent (§4.1.2.4, CheckIfApplicationLimited); here that check
// rides the write's first packet (sendPacketLocked), on the clock reading
// the pacer takes anyway, and only for a write that starts alone: writers
// here are synchronous, so unsent data lives in a blocked Write's own
// stack, and a second write in progress means the sender has data.
func (c *Conn) writeBeginLocked() {
	c.writers++
	c.limitCheck = c.writers == 1
}

// writeEndLocked closes what writeBeginLocked opened.
func (c *Conn) writeEndLocked() {
	c.writers--
	c.limitCheck = false
}

// checkAppLimitedLocked is the check of a write that starts alone, at now,
// its first packet cleared by the window — and, with no write in progress
// to judge for itself, of an ACK or a timer about to retransmit, judged on
// the flight before they change it (BBR §4.1.2.4's three sites; handleAck,
// timer.go). The two latter run only before a retransmission they decide:
// an idle ACK marks nothing a later write would not mark for itself, and
// the marks it would add depend on the peer's acknowledgement delay, which
// the tests counting marks per write would then read as noise. The sender
// had run out of data if
// a write last sent more than a pacing slot ago: a write that starts as the
// previous one ends finds the sender out of data for an instant no packet
// could have used, which is a bulk transfer's shape at every chunk, and
// marking it would discount nearly every sample of the transfer. The slot
// is one datagram at the pacing rate, floored at the timer granularity:
// the pacer releases shorter deficits at once (pacer.delay), so a gap
// shorter than that is one it would have closed with a burst. Without the
// floor, a bulk transfer was marked two and three times; with it, the flush
// of the previous write's batch, which follows its last pacing decision,
// stays inside the gap the floor forgives. The first write follows no
// other. With the window not full (RFC 9002 §7.8's underused window) and no
// lost payload waiting to be resent, the samples of the next round trip
// measure the application, not the path.
//
// BBR §4.1.2.1.1 tracks pending_transmissions, lost_out and retrans_out
// for this check (§4.1.2.4). Here "no lost payload waiting" is lost_out <=
// retrans_out, since a loss is resent through the retrans queue and nowhere
// else; pending_transmissions, the bytes below the transport (the GSO
// batch, the kernel), is not read: the batch is flushed before any wait on
// the window, and the kernel's queue would cost a system call per sample
// for a mark that is rarely wrong.
func (c *Conn) checkAppLimitedLocked(now time.Time) (marked bool) {
	if !c.writeSent.IsZero() {
		gap := now.Sub(c.writeSent)
		if gap <= granularity {
			return false
		}
		if rate := c.cc.pacingRate(c.rtt.current()); rate > 0 && gap <= time.Duration(float64(c.maxDatagram)/rate*float64(time.Second)) {
			return false
		}
	}
	for i := range c.spaces {
		if len(c.spaces[i].retrans) > 0 {
			return false
		}
	}
	c.delivery.markAppLimited(c.cc.bytesInFlight())
	return true
}

// lossEvent is one batch of losses, as onLost sees it. latestSent is the
// send time of the newest packet in the batch: B.8's gate for whether the
// batch is a new congestion event or belongs to a recovery period already
// paid for.
//
// ranges is how many discontiguous lost ranges the batch opens
// (draft-ietf-ccwg-bbr-06 §5.3.1.3). crossing is, of the packets BBR's
// loss response handles one by one in number order (§5.5.10.2,
// BBRHandleLostPacket), the first whose losses since it left exceed the
// 2% a probe may cause of the flight it was sent into (lossTooHigh); zero
// size if none did. The response reacts to that one packet, once per
// probe, and to none after it: the batch's other packets leave it nothing
// to read.
type lossEvent struct {
	now        time.Time
	latestSent time.Time
	bytes      int
	ranges     int
	crossing   lostPacket
}

// lossEvent is the congestion event of a batch of losses on the current
// path.
func (out *ackOutcome) lossEvent(now time.Time) lossEvent {
	return lossEvent{now: now, latestSent: out.latestLossSent, bytes: out.lostBytes, ranges: out.lostRanges, crossing: out.crossing}
}

// lostPacket is a lost packet as BBR's loss response reads it: its size,
// the flight it joined, itself included (P.tx_in_flight), and the bytes
// declared lost between its send and its own loss, itself included
// (RS.lost = C.lost - P.lost), and whether it left application-limited
// (RS.is_app_limited = P.is_app_limited).
type lostPacket struct {
	size       int
	txInFlight int
	lost       int
	appLimited bool
}

// newController builds the controller a connection's parameters name, at
// its initial window for the current datagram size. Two sites build one —
// newConn and a migration's resetPathStateLocked — and neither knows the
// algorithms by name. est is the connection's delivery estimator: BBR
// marks it application-limited during ProbeRTT, as the draft's
// MarkConnectionAppLimited does (C.app_limited has one owner).
//
// The default case is NewReno, and it is also what an unknown algorithm
// runs: newConn has already folded one into CongestionNewReno so that
// Stats.Algorithm names the controller built here.
//
// The default slow start is HyStart++ for CUBIC (RFC 9438 §4.10) and the
// standard one for NewReno. Reno with HyStart++ was measured against Reno
// alone on the eight gated path benchmarks (gate-path.sh, eight rounds,
// 2026-10-16): seven within 1.6%, BenchmarkTransferBottleneck 13% slower
// (868 to 985 ms), so NewReno keeps the standard one. The cost is not
// traced; the likely reading, unverified, is an exit below the window the
// loss would have reached, which Reno climbs back a datagram per round
// trip and CUBIC's curve in a few.
func newController(cfg CongestionConfig, maxDatagram int, est *deliveryEstimator) congestionController {
	switch cfg.Algorithm {
	case CongestionCubic:
		c := newCubic(maxDatagram)
		if cfg.SlowStart == SlowStartStandard {
			c.hs.disarm()
		}
		c.noFastConvergence = cfg.DisableFastConvergence
		return c
	case CongestionBBRv1:
		return newBBR(maxDatagram, est)
	case CongestionBBR:
		return newBBR3(maxDatagram, est)
	default:
		c := newNewReno(maxDatagram)
		if cfg.SlowStart == SlowStartHyStart {
			c.hs = newHystart()
		}
		return c
	}
}

// newReno is §7's congestion controller, the RFC's own default and the
// defensible minimum: nothing fancier belongs here without measurement.
type newReno struct {
	cwnd          int
	ssthresh      int
	inFlight      int
	recoveryStart time.Time // packets sent before this do not shrink the window again
	maxDatagram   int
	// credit counts the bytes acknowledged in avoidance since the window
	// last grew: a window's worth grows it by one datagram (§7.3.3's
	// ceiling, met exactly — RFC 3465's byte counting). B.5's per-packet
	// maxDatagram·size/cwnd falls short of it: the window grows within the
	// window it is divided by, a byte lost per window at 2 MB. The price is
	// the datagram arriving at the window's end rather than spread over it:
	// measured 2026-10-16 against B.5 (gate-path.sh, three runs of eight
	// rounds), BurstLoss 0.8–1% and Loss1pct 1.6–4.7% slower, the rest flat.
	credit int
	// hs is the initial slow start's HyStart++ state (hystart.go); the zero
	// value is disarmed, the standard slow start (see newController).
	hs hystart
	// Window utilisation, judged per round trip the way Linux's
	// tcp_is_cwnd_limited does (RFC 9002 §7.8 leaves the detection to the
	// implementation): limited records that a send was refused for lack of
	// window this round, maxInFlight the largest flight the round reached,
	// and roundMark when it began — the round is over once a packet sent
	// at or after the mark is acknowledged, and the next send starts a new
	// one. Zero while a round waits for its first send.
	limited     bool
	maxInFlight int
	roundMark   time.Time
}

var _ congestionController = (*newReno)(nil)

func newNewReno(maxDatagram int) *newReno {
	// §7.2: ten datagrams, floored the way the RFC floors it.
	w := max(min(10*maxDatagram, 14720), 2*maxDatagram)
	return &newReno{cwnd: w, ssthresh: 1 << 30, maxDatagram: maxDatagram}
}

func (c *newReno) window() int              { return c.cwnd }
func (c *newReno) bytesInFlight() int       { return c.inFlight }
func (c *newReno) setMaxDatagram(bytes int) { c.maxDatagram = bytes }

// windowLimited is the §7.8 utilisation test over the current round, Linux's
// tcp_is_cwnd_limited: in avoidance the window was the limit if a send was
// refused for lack of it; in slow start, also if the flight reached half
// the window — a paced sender doubling its window each round trip rarely
// hits the window itself, but its flight comes within a few datagrams of
// it, while an application-limited one sits far below.
func (c *newReno) windowLimited() bool {
	return c.limited || (c.cwnd < c.ssthresh && 2*c.maxInFlight >= c.cwnd)
}

// canSend is the congestion gate alone — bursts are not bounded here. The
// §7.7 burst bound was tried as a per-ACK allowance of one initial window
// and measured before it could land: on a clean 20ms-RTT path it collapsed
// goodput fifty-fold (BenchmarkTransferClean, 35.5 to 0.70 MB/s — one burst
// per round trip, because a burst's ACKs arrive as one cluster and the
// allowance starves between clusters). §7.7 is pacing's job — a
// timer-released send rate — or nothing.
//
// A refusal is also the utilisation signal §7.8 asks for: the window was
// what stopped the sender, and onAcked may grow it on what this flight
// earns. Every send path asks here, so no site has to remember to report.
func (c *newReno) canSend(bytes int) bool {
	if c.inFlight+bytes > c.cwnd {
		c.limited = true
		return false
	}
	return true
}

// onSent adds to the flight and, after a round-closing ACK, opens the next
// round on the sender's own evidence: the reset happens here and not when
// the ACK arrived so that the flight it measures is one the sender has
// had the chance to refill — a clump of acknowledgements drained by the
// read loop before the writer takes the lock would otherwise read as an
// application that stopped sending.
func (c *newReno) onSent(now time.Time, bytes int) {
	c.inFlight += bytes
	if c.roundMark.IsZero() {
		c.roundMark = now
		c.limited = false
		c.maxInFlight = 0
	}
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
}

// onLifted releases the bytes of a packet that leaves the flight record
// without an acknowledgement or a loss — a lost DPLPMTUD probe (RFC 9000
// §14.4), a discarded space, a Retry — and the window does not move: none
// of these is a congestion event. Floored as a guard only: a previous path's
// packets are filtered out before they reach here (sentTracker.pathStart).
func (c *newReno) onLifted(bytes int) { c.inFlight = max(c.inFlight-bytes, 0) }

// onMarked is a congestion event dated by the latest tracked packet the
// frame acknowledges (RFC 9002 §7.3.2, B.7 ProcessECN): the window halves
// and the recovery period opens, as on a loss, unless one is already
// running. handleAck calls it before the losses are detected and the acked
// packets released, which is A.7's order (ProcessECN, then
// DetectAndRemoveLostPackets, then OnPacketsAcked). The marked packets were
// delivered, so the flight does not move.
func (c *newReno) onMarked(ev markEvent) {
	c.onLost(lossEvent{now: ev.now, latestSent: ev.latestSent})
}

// pacingRate is 5/4 of the window per smoothed round trip — the RFC's
// "slightly higher than necessary" rate (§7.7), so pacing never becomes
// the throughput ceiling — and of twice the window in slow start, where
// the window itself doubles per round trip and pacing at the current cwnd
// would lag the growth it is spreading (measured: ~3% off a clean 20ms
// path's goodput; §7.7 permits a higher slow-start rate). The round trip
// is floored at the timer's granularity: below it the rate is meaningless
// and the division is unsafe.
//
// Conservative Slow Start is the exception: HyStart++ grows the window a
// quarter per round trip there, and five quarters cover it — pacing at
// twice the window would burst into the buffer it just saw filling.
func (c *newReno) pacingRate(srtt time.Duration) float64 {
	w := c.cwnd
	if c.cwnd < c.ssthresh && (!c.hs.armed || !c.hs.css) {
		w = 2 * c.cwnd
	}
	return 1.25 * float64(w) / max(srtt, granularity).Seconds()
}

// absorb is the part of an acknowledgement every loss-based controller
// shares, whatever curve it grows its window along: the packet leaves the
// flight, closes its round if it was sent after the round's mark, and the
// result says whether it earns growth at all — not if it predates the
// recovery start (B.5's per-packet test, see onAcked), not if the window
// was not what limited the sender this round (§7.8, see onAcked). CUBIC
// (cubic.go) calls it first and grows its own way on a yes.
func (c *newReno) absorb(ev ackEvent) bool {
	c.inFlight = max(c.inFlight-ev.size, 0)
	useful := c.windowLimited()
	if !c.roundMark.IsZero() && !ev.sentAt.Before(c.roundMark) {
		c.roundMark = time.Time{}
	}
	// RFC 9002 B.5: in recovery iff sent_time <= recovery_start_time, so
	// only a packet sent strictly after the period began earns growth.
	return useful && (c.recoveryStart.IsZero() || ev.sentAt.After(c.recoveryStart))
}

// onAcked grows the window: by the packet's bytes in slow start, or what
// HyStart++ grants while the initial one is armed, by one datagram per
// window's worth in avoidance (§7.3.1, §7.3.3). A packet sent
// before recovery began releases flight but earns nothing (B.5 applies that
// test per packet, which is why this is called per packet: one
// pre-recovery straggler in an ACK must not silence the growth of
// everything sent after the exit).
//
// A window the sender does not fill must not grow either (§7.8): a long
// connection of small exchanges would otherwise inflate it far past the
// path's capacity and burst into loss on its first large transfer. The
// test is Linux's tcp_is_cwnd_limited, over the current round: in
// avoidance the window grows only if a send was refused for lack of it;
// in slow start, also if the flight reached half the window — a paced
// sender doubling its window each round trip rarely hits the window
// itself, but its flight comes within a few datagrams of it, while an
// application-limited one sits far below. A packet sent at or after the
// round's mark closes the round, and the next send opens the next one.
func (c *newReno) onAcked(ev ackEvent) {
	inRound := !c.roundMark.IsZero()
	earns := c.absorb(ev)
	if c.cwnd < c.ssthresh {
		c.slowStart(ev, earns, inRound && c.roundMark.IsZero())
		return
	}
	if !earns {
		return
	}
	c.credit += ev.size
	if c.credit >= c.cwnd {
		c.credit -= c.cwnd
		c.cwnd += c.maxDatagram
	}
}

// slowStart is one acknowledgement in slow start, shared with CUBIC
// (RFC 9438 §4.10 keeps Reno's): the window grows by the packet's bytes,
// or by what HyStart++ grants while the initial slow start is armed — it
// reads every acknowledgement's round trip, earning or not; closed says
// this one ended a round (see absorb). When HyStart++ ends the slow start
// the threshold is the window, and nothing grows on this acknowledgement.
func (c *newReno) slowStart(ev ackEvent, earns, closed bool) {
	gain := ev.size
	if c.hs.armed {
		var exit bool
		if gain, exit = c.hs.grow(ev.size, ev.now.Sub(ev.sentAt), closed); exit {
			c.ssthresh = c.cwnd
			return
		}
	}
	if earns {
		c.cwnd += gain
	}
}

// onLost shrinks the window once per recovery period (§7.3.2): a whole
// window's losses are one congestion event, not one each. latestSent is the
// send time of the newest packet in the batch — B.8's gate: if even the
// newest predates the recovery start, the whole batch belongs to the epoch
// already paid for; if it does not, this is a new epoch, however many
// stragglers ride along.
//
// Nothing is sent from here. B.6's OnCongestionEvent ends with
// MaybeSendOnePacket — "a packet can be sent to speed up loss recovery",
// a may — and this end does not take it: the retransmissions the loss
// queued leave through retransmitLocked as the reduced window admits them,
// which costs at most one round trip of recovery when that window is
// already full, and a packet sent past the window right after a loss adds
// to the very load the loss reported. The PTO covers the case where no
// acknowledgement comes at all. The same holds for cubic, bbr and bbr3,
// whose onLost say so by reference.
func (c *newReno) onLost(ev lossEvent) {
	c.inFlight = max(c.inFlight-ev.bytes, 0)
	if !c.recoveryStart.IsZero() && !ev.latestSent.After(c.recoveryStart) {
		return // B.5/B.6: sent at or before the start, inside the period
	}
	c.recoveryStart = ev.now
	c.cwnd = max(c.cwnd/2, 2*c.maxDatagram)
	c.ssthresh = c.cwnd
	c.credit = 0
	c.hs.disarm()
}

// onPersistentCongestion collapses the window to its minimum (§7.6.2, B.8):
// an ACK just proved that everything sent over more than three probe
// intervals was lost — the path is not dropping a packet, it was gone. The
// slow-start threshold is not touched here: the loss event that precedes
// this call in the same batch (onLost, B.8's order) already halved it, and
// the window climbs back to it in slow start the way a TCP sender does
// after an RTO (RFC 5681 §3.1). The recovery period is cleared so that the
// first loss on the revived path is a new event.
func (c *newReno) onPersistentCongestion() {
	c.cwnd = 2 * c.maxDatagram
	c.recoveryStart = time.Time{}
	c.credit = 0
	c.hs.disarm()
}

// pacer spreads sends over time at the rate the congestion controller
// names (RFC 9002 §7.7) instead of letting a window leave as one line-rate
// burst. It is a token bucket: tokens refill continuously at that rate,
// and the bucket holds one initial window, which is the burst §7.7 permits
// a sender without finer timing. The rate is the controller's to choose —
// NewReno prices its window over the round trip, a model-based controller
// prices the path's bandwidth — and the pacer only converts it to a wait.
//
// The naive alternative — a per-ACK burst allowance — was measured before
// this existed and collapsed a clean 20ms path fifty-fold (see
// newReno.canSend); time, not the ack clock, has to be what refills the
// bucket.
//
// Two senders obey the pacer. The application writer's blocking path waits
// on it (sendPacketLocked's waitCwnd loop, woken by the timer loop at the
// pace deadline), going as soon as the bucket is not in deficit and pricing
// the dip for the next sender. The retransmission pass (retransmitLocked)
// does not park — the read and timer loops call it — so it asks whether the
// bucket holds the packet whole (until), stops when it does not, and the
// timer loop runs the next pass at the pace deadline: one pass sends at
// most the bucket, which is the burst §7.7 bounds. A retransmission never
// waits past its probe timeout for all that, which would answer loss with
// more loss: the probe lifts the head of the queue, pacer or not
// (sendProbeLocked). Everything else — probes, acknowledgements, grants —
// spends tokens, because it occupies the wire, but is never delayed by them.
type pacer struct {
	// tokens is how many bytes may leave now. One send may drive it
	// negative; the deficit is what the next sender waits out.
	tokens int64
	burst  int64
	last   time.Time // when tokens last refilled; zero until the first send
}

// refill credits the time elapsed since the last call at rate bytes per
// second. Callers hold the connection's lock, like every pacer method.
func (p *pacer) refill(now time.Time, rate float64) {
	if p.last.IsZero() {
		p.tokens = p.burst
		p.last = now
		return
	}
	elapsed := now.Sub(p.last)
	if elapsed <= 0 {
		return
	}
	p.last = now
	// Float, not integer: a benchmark-sized window (2^30) over a
	// millisecond is a rate that, times an elapsed in nanoseconds,
	// overflows int64 long before the arithmetic rounds wrong, and a
	// float64 carries both magnitudes exactly enough for a byte count.
	add := rate * elapsed.Seconds()
	if add >= float64(p.burst-p.tokens) {
		p.tokens = p.burst
	} else {
		p.tokens += int64(add)
	}
}

// spend takes bytes out of the bucket, below zero if they are not there:
// what was sent was sent, and the deficit prices it for whoever sends next.
func (p *pacer) spend(bytes int) { p.tokens -= int64(bytes) }

// delay is how long a paced sender must wait before its next send at rate
// bytes per second, zero when it may go now. Deficits smaller than one
// timer granularity are released immediately rather than armed: the timer
// cannot honour them, and rounding down bounds the overshoot to one
// granularity's worth of line rate — rate-proportional, unlike the
// unbounded burst pacing exists to prevent.
func (p *pacer) delay(rate float64) time.Duration {
	if p.tokens >= 0 || rate <= 0 {
		return 0
	}
	d := time.Duration(float64(-p.tokens) / rate * float64(time.Second))
	if d < granularity {
		return 0
	}
	return d
}

// until is how long until the bucket holds bytes whole at rate bytes per
// second, zero when it does now. No granularity rounding here, unlike
// delay: a retransmission released early would exceed the burst the bucket
// bounds, and the timer loop sleeps any duration. Rounded up to the
// microsecond so that the refill at the deadline, truncated to the byte,
// does not fall one byte short and cost a loop turn.
func (p *pacer) until(bytes int, rate float64) time.Duration {
	short := int64(bytes) - p.tokens
	if short <= 0 || rate <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(float64(short)/rate*1e6)) * time.Microsecond
}
