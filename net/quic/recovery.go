package quic

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
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
	// more numbers after it has been acknowledged (§6.1.1).
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
	pn           uint64
	sentAt       time.Time
	size         int
	ackEliciting bool
	// payload holds the packet's retransmittable frames, encoded. ACK and
	// PADDING are excluded when the packet is built: acknowledgements are
	// regenerated from current state, never replayed, and padding is a
	// property of the datagram that carried it.
	payload []byte
	// acked is processAck's working mark. It never survives a call: a marked
	// packet is removed by the same call that marked it.
	acked bool
	// pmtuProbe marks a DPLPMTUD probe (pmtu.go): its loss releases its
	// bytes from flight but is no congestion event (RFC 9000 §14.4), and
	// its acknowledgement or loss is reported by number to the search.
	pmtuProbe bool
	// ect0 marks a packet whose datagram left with the ECT(0) codepoint,
	// which the peer's ECN counts must then account for (§13.4.2.1).
	ect0 bool
	// The delivery estimator's snapshot of the connection when this packet
	// left (draft-ietf-ccwg-bbr-06 §4.1.2.2): bytes delivered so far, how
	// long before this send the count last moved and the flight's first
	// send happened — offsets from sentAt rather than the two instants,
	// which keeps the record 32 bytes smaller and the acknowledgement path
	// to one subtraction — and whether the sender was application-limited. Read once, when the packet is acknowledged
	// (deliveryEstimator.update).
	delivered      int
	sendElapsed    time.Duration // P.sent_time - P.first_sent_time
	sinceDelivered time.Duration // P.sent_time - P.delivered_time
	appLimited     bool
}

// sentTracker is one packet-number space's in-flight bookkeeping. Packets are
// appended in order — the packet number only grows — so the slice is sorted
// by construction and pruned from the front as acknowledgements arrive.
type sentTracker struct {
	packets  []sentPacket
	lossTime time.Time // when the earliest time-threshold candidate matures
	// largestAcked persists across ACK frames (RFC 9002 A.10): loss
	// thresholds compare against everything the peer has ever acknowledged,
	// not just what this one frame newly covered — an ACK that repeats old
	// news must still be able to mature a pending loss.
	largestAcked    uint64
	hasLargestAcked bool
	// skipped is a packet number this end deliberately never used (RFC
	// 9000 §21.4): a peer that acknowledges it acknowledged something it
	// cannot have received, and is closed with PROTOCOL_VIOLATION before
	// its invented ACKs open the congestion window. One number is kept —
	// the latest gap — which is enough: an attacker acknowledging every
	// number up to the largest it sees hits the current gap within a
	// window. nextSkip is where the next gap goes; skipArmed says whether
	// it has been drawn yet.
	skipped    uint64
	hasSkipped bool
	nextSkip   uint64
	skipArmed  bool
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

// errOptimisticAck is processAck's verdict on an ACK that covers the skipped
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
	s.skipped, s.hasSkipped = pn, true
	s.nextSkip = pn + 1 + skipGap(window)
	return true
}

func skipGap(window uint64) uint64 {
	window = max(window, 2)
	var b [8]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24
	return window/2 + binary.LittleEndian.Uint64(b[:])%window
}

// ackOutcome is what processing one ACK frame yields.
type ackOutcome struct {
	newlyAcked       int  // bytes released from flight (current path only)
	newlyAckedCount  int  // packets released, whichever path sent them
	largestWasNew    bool // the frame's largest was newly acknowledged…
	largestEliciting bool // …ack-eliciting and sent on this path: what a sample needs
	largestSentAt    time.Time
	lostBytes        int
	lostCount        uint64   // packets behind lostBytes, for Stats.LostPackets
	lost             [][]byte // payloads to queue for retransmission
	sawAckEliciting  bool     // any newly acked packet was ack-eliciting
	newlyAckedECT0   int      // newly acked packets that left marked ECT(0), for ECN validation
	raisedLargest    bool     // the frame increased the largest acknowledged: validation may fail on it (§13.4.2.1)
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
}

// processAck removes what the ranges cover and declares losses. largest is
// the frame's own largest field; visitRanges walks its encoded ranges. cc
// hears each newly acknowledged packet of the current path, in order, as
// it is released (RFC 9002 B.5 grows the window per packet); nil when a
// test drives the tracker alone. est, the connection's delivery estimator,
// sees the same packets and yields the frame's rate sample; nil likewise.
// Losses are reported in the outcome, not to cc: the caller queues their
// payloads and judges persistent congestion before it reports the batch as
// one event.
func (s *sentTracker) processAck(now time.Time, largest uint64, rangeData []byte, rtt *rttEstimator, cc congestionController, est *deliveryEstimator) (ackOutcome, error) {
	var out ackOutcome

	// Both sides are ordered — s.packets ascending by construction (the
	// packet number only grows), the frame's ranges strictly descending by
	// encoding (parseACK proved each one sits gap+2 below the previous) — so
	// one two-pointer merge marks every acknowledged packet without a map and
	// without rescanning the slice per range. An ACK may name billions of
	// packets; only the ones still in s.packets cost anything.
	i := len(s.packets) - 1
	marked := 0
	var largestMarked uint64 // the first mark is the highest: both walks descend
	optimistic := false
	err := decodeAckRanges(largest, rangeData, func(lo, hi uint64) {
		if s.hasSkipped && lo <= s.skipped && s.skipped <= hi {
			// The range names the number that was never sent (§21.4). The
			// marks already set stay set: the verdict closes the
			// connection, and this tracker with it.
			optimistic = true
		}
		if optimistic {
			return
		}
		for i >= 0 && s.packets[i].pn > hi {
			i--
		}
		for i >= 0 && s.packets[i].pn >= lo {
			if !s.packets[i].acked {
				if marked == 0 {
					largestMarked = s.packets[i].pn
				}
				s.packets[i].acked = true
				marked++
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
	if marked == 0 && len(s.packets) == 0 {
		return out, nil
	}

	// The largest ever acknowledged persists (A.10); the one before this
	// frame is remembered for the persistent-congestion span below.
	prevLargest, hadPrev := s.largestAcked, s.hasLargestAcked
	if marked > 0 && (!hadPrev || largestMarked > prevLargest) {
		s.largestAcked = largestMarked
		s.hasLargestAcked = true
		out.raisedLargest = true
	}

	// One pass in packet-number order: what the frame marked is released,
	// then anything packetThreshold below the largest acknowledged, or
	// older than the time threshold, is gone (§6.1), and whatever is too
	// young for either arms the loss timer instead. The loss scan runs on
	// every ACK, not just the ones with news: the timer was cleared here,
	// and an ACK that repeats old news used to disarm a pending loss
	// deadline for good — the loss then surfaced only on a later new ACK,
	// or on the PTO.
	//
	// The same pass measures the batch for RFC 9002 §7.6: the longest span
	// between the send times of two ack-eliciting packets declared lost
	// with nothing acknowledged between them — the only state it needs is
	// where the current run began, because the order here is the send
	// order. An acknowledged packet between two losses starts the run over
	// whether this frame acknowledged it or an earlier one did: the
	// earlier ones are all at or below the previous largest acknowledged,
	// so a run that straddles that number is cut there. (A run can also
	// straddle a packet acknowledged two frames ago and below that number;
	// the RFC allows per-space state that errs on the side of declaring.)
	s.lossTime = time.Time{}
	var threshold time.Duration
	var lostBefore time.Time
	if s.hasLargestAcked {
		threshold = max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
		lostBefore = now.Add(-threshold)
	}
	if est != nil {
		est.begin()
	}
	var runStart time.Time
	var runStartPN uint64
	kept := s.packets[:0]
	for i := range s.packets {
		p := &s.packets[i]
		if p.acked {
			runStart = time.Time{}
			out.newlyAckedCount++
			if p.ect0 {
				out.newlyAckedECT0++
			}
			onPath := s.onPath(p)
			if onPath {
				out.newlyAcked += p.size
				if est != nil {
					est.update(p)
				}
				if cc != nil {
					ev := ackEvent{now: now, sentAt: p.sentAt, size: p.size, srtt: rtt.current()}
					if est != nil {
						ev.delivered = est.delivered - p.delivered
						ev.sendElapsed, ev.sinceDelivered, ev.appLimited = p.sendElapsed, p.sinceDelivered, p.appLimited
						// Sent times rise with numbers: the highest newly
						// marked is the frame's newest, on this path if any
						// is, and its sample is the frame's (ackEvent.rated).
						ev.rated = p.pn == largestMarked && ev.rateSample().reliable(rtt.min)
					}
					cc.onAcked(ev)
				}
			}
			if p.ackEliciting {
				out.sawAckEliciting = true
			}
			if p.pmtuProbe {
				out.probeAcked, out.probeAckedPN = true, p.pn
			}
			if p.pn == largest {
				out.largestWasNew = true
				out.largestEliciting = p.ackEliciting && onPath
				out.largestSentAt = p.sentAt
			}
			continue
		}
		if s.hasLargestAcked {
			lostByCount := p.pn+packetThreshold <= s.largestAcked
			lostByTime := p.pn < s.largestAcked && p.sentAt.Before(lostBefore)
			if lostByCount || lostByTime {
				if s.declareLost(&out, p) {
					if hadPrev && runStartPN < prevLargest && prevLargest < p.pn {
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
		}
		kept = append(kept, *p)
	}
	s.packets = kept
	if est != nil {
		est.finish(now, rtt.min)
	}
	return out, nil
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
func (s *sentTracker) collectTimeLosses(now time.Time, rtt *rttEstimator) (out ackOutcome) {
	if s.lossTime.IsZero() || now.Before(s.lossTime) {
		return out
	}
	threshold := max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
	lostBefore := now.Add(-threshold)
	s.lossTime = time.Time{}
	kept := s.packets[:0]
	for _, p := range s.packets {
		if p.pn > s.largestAcked {
			// Not a candidate, and not a reason to re-arm the timer: the
			// next ACK that covers something newer scans it (processAck).
			kept = append(kept, p)
			continue
		}
		if p.sentAt.Before(lostBefore) {
			s.declareLost(&out, &p)
			continue
		}
		if s.lossTime.IsZero() {
			s.lossTime = p.sentAt.Add(threshold)
		}
		kept = append(kept, p)
	}
	s.packets = kept
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
func (s *sentTracker) declareLost(out *ackOutcome, p *sentPacket) bool {
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
	}
	if !onPath {
		return false
	}
	out.lostBytes += p.size
	if p.sentAt.After(out.latestLossSent) {
		out.latestLossSent = p.sentAt
	}
	return p.ackEliciting
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

// liftOldestResendable removes and returns the oldest packet whose payload
// a probe can resend. The probe timeout lifts a packet out of the flight
// record before sending its payload again as a new packet, so that the
// copy's acknowledgement retires the data: with the original left in place,
// a probe copy of the same payload is anonymous — acknowledged under its
// own number, it tells the record nothing about the packet it copied, which
// stays the oldest unacknowledged, is copied again on the next timeout, and
// shadows every packet behind it. A server whose two-packet Initial flight
// is lost then resends the first packet at every backoff step and never the
// second, and the client, holding half a ServerHello, waits out the
// handshake. RFC 9002 §6.2.4 permits either resending or marking in flight
// as lost on a timeout; this does the first with the bookkeeping of the
// second — minus the congestion event, which a timeout is not (§6.2,
// §7.6: only a later acknowledgement proves what was lost).
//
// An acknowledgement of the original that arrives after it was lifted
// names a number no longer recorded and is ignored; the copy then waits
// for its own, or for the time threshold. A probe interval is what bounds
// the acknowledgement's lateness, so that case is the rare one.
func (s *sentTracker) liftOldestResendable() (sentPacket, bool) {
	for i, p := range s.packets {
		if p.ackEliciting && len(p.payload) > 0 {
			s.packets = append(s.packets[:i], s.packets[i+1:]...)
			return p, true
		}
	}
	return sentPacket{}, false
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
	// windowLimited reports whether the window was what stopped the sender
	// this round (§7.8): what onAcked grows on, and what the delivery
	// estimator reads the other way — a writer that runs dry while the
	// window has room is application-limited (Conn.flowEmptyLocked).
	windowLimited() bool
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
	// acknowledged or lost: a probe timeout's lifted copy, a lost DPLPMTUD
	// probe, a discarded space, a Retry.
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
	srtt   time.Duration // the smoothed round trip as it stood before this frame
	// The delivery estimator's reading for this packet (deliveryEstimator):
	// delivered is how many bytes the network delivered between the
	// packet's send and this acknowledgement, itself included; sendElapsed
	// and sinceDelivered are the packet's snapshot (sentPacket). rateSample
	// turns them into the draft's sample.
	delivered      int
	sendElapsed    time.Duration
	sinceDelivered time.Duration
	appLimited     bool
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
// leave (§4.1.2.2, OnPacketSent). idle says nothing is in flight: the flight
// starting now is measured from now, not from the last acknowledgement,
// which may be long gone.
func (e *deliveryEstimator) onSent(p *sentPacket, idle bool) {
	if idle {
		e.firstSentTime, e.deliveredTime = p.sentAt, p.sentAt
	}
	p.delivered = e.delivered
	p.sendElapsed = p.sentAt.Sub(e.firstSentTime)
	p.sinceDelivered = p.sentAt.Sub(e.deliveredTime)
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

// flowEmptyLocked is told by a writer that the packet it is about to send
// carries the last of its data: the moment the application runs out, which
// is where the draft checks for an application-limited phase (§4.1.2.4). If
// the window still has room for that packet, was not what limited the sender
// this round (§7.8's signal, read the other way), the pacer is not what is
// holding the sender back either, and no lost payload waits to be resent,
// the samples of the next round trip measure the application, not the
// path. bytes is what the packet will occupy, the gate's own unit.
// Writers here are synchronous — unsent data lives in a blocked Write's own
// stack, not in a buffer the draft's first condition could inspect — so the
// end of a write is the only place the transport can see the flow run dry.
// A write that waited on the pacer is Linux's "packets still queued below
// the socket" (tcp_rate_check_app_limited): a writer that ends every 64 KiB
// chunk after waiting for its pace is paced, not idle — and a model-based
// controller is paced most of the time, so without this test it would
// discount nearly every sample of a bulk transfer.
func (c *Conn) flowEmptyLocked(bytes int) {
	paced := c.paceWaited
	c.paceWaited = false
	if paced || c.cc.windowLimited() || !c.cc.canSend(bytes) {
		return
	}
	for i := range c.spaces {
		if len(c.spaces[i].retrans) > 0 {
			return
		}
	}
	c.delivery.markAppLimited(c.cc.bytesInFlight())
}

// lossEvent is one batch of losses, as onLost sees it. latestSent is the
// send time of the newest packet in the batch: B.8's gate for whether the
// batch is a new congestion event or belongs to a recovery period already
// paid for.
type lossEvent struct {
	now        time.Time
	latestSent time.Time
	bytes      int
}

// newController builds the controller a connection's parameters name, at
// its initial window for the current datagram size. Two sites build one —
// newConn and a migration's resetPathStateLocked — and neither knows the
// algorithms by name.
func newController(cfg CongestionConfig, maxDatagram int) congestionController {
	switch cfg.Algorithm {
	case CongestionCubic:
		return newCubic(maxDatagram)
	case CongestionBBRv1:
		return newBBR(maxDatagram)
	case CongestionBBR:
		return newBBR3(maxDatagram)
	default:
		return newNewReno(maxDatagram)
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
	// credit is the avoidance growth earned and not yet spent, in bytes
	// times maxDatagram: each acknowledged byte earns maxDatagram/cwnd
	// bytes of window (§7.3.3), and carrying the remainder keeps that
	// exact whatever the window's size — an integer share per packet
	// would round to nothing once cwnd exceeds maxDatagram².
	credit int
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

// onLifted releases the bytes of a packet the probe timeout lifted from the
// flight record to resend: its copy re-enters through onSent, so the flight
// stays the same size, and the window does not move — a probe timeout is
// not a congestion event (RFC 9002 §6.2). A lost DPLPMTUD probe leaves
// the same way (RFC 9000 §14.4). Floored as a guard only: a previous path's
// packets are filtered out before they reach here (sentTracker.pathStart).
func (c *newReno) onLifted(bytes int) { c.inFlight = max(c.inFlight-bytes, 0) }

// onMarked is a congestion event dated by the frame's largest acknowledged
// packet (RFC 9002 §7.3.2, B.7 ProcessECN): the window halves and the
// recovery period opens, as on a loss, unless one is already running. The
// marked packets were delivered, so the flight does not move.
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
func (c *newReno) pacingRate(srtt time.Duration) float64 {
	w := c.cwnd
	if c.cwnd < c.ssthresh {
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
	return useful && !ev.sentAt.Before(c.recoveryStart)
}

// onAcked grows the window: by the packet's bytes in slow start, by one
// datagram per window's worth in avoidance (§7.3.1, §7.3.3). A packet sent
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
	if !c.absorb(ev) {
		return
	}
	if c.cwnd < c.ssthresh {
		c.cwnd += ev.size
		return
	}
	c.credit += c.maxDatagram * ev.size
	if grow := c.credit / c.cwnd; grow > 0 {
		c.credit -= grow * c.cwnd
		c.cwnd += grow
	}
}

// onLost shrinks the window once per recovery period (§7.3.2): a whole
// window's losses are one congestion event, not one each. latestSent is the
// send time of the newest packet in the batch — B.8's gate: if even the
// newest predates the recovery start, the whole batch belongs to the epoch
// already paid for; if it does not, this is a new epoch, however many
// stragglers ride along.
func (c *newReno) onLost(ev lossEvent) {
	c.inFlight = max(c.inFlight-ev.bytes, 0)
	if ev.latestSent.Before(c.recoveryStart) {
		return
	}
	c.recoveryStart = ev.now
	c.cwnd = max(c.cwnd/2, 2*c.maxDatagram)
	c.ssthresh = c.cwnd
	c.credit = 0
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
// Only the application writer's blocking path waits on the pacer
// (sendPacketLocked's waitCwnd loop, woken by the timer loop at the pace
// deadline). Everything else — probes, acknowledgements, grants,
// retransmissions driven by the read loop — spends tokens, because it
// occupies the wire, but is never delayed by them: the read and timer loops
// must not park on a send, and a retransmission delayed past its probe
// timeout would answer loss with more loss.
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
