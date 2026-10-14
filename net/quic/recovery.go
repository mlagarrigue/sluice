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
// test drives the tracker alone. Losses are reported in the outcome, not
// to cc: the caller queues their payloads and judges persistent
// congestion before it reports the batch as one event.
func (s *sentTracker) processAck(now time.Time, largest uint64, rangeData []byte, rtt *rttEstimator, cc congestionController) (ackOutcome, error) {
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
	var runStart time.Time
	var runStartPN uint64
	kept := s.packets[:0]
	for _, p := range s.packets {
		if p.acked {
			runStart = time.Time{}
			out.newlyAckedCount++
			onPath := s.onPath(&p)
			if onPath {
				out.newlyAcked += p.size
				if cc != nil {
					cc.onAcked(ackEvent{now: now, sentAt: p.sentAt, size: p.size, srtt: rtt.current()})
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
				if s.declareLost(&out, &p) {
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
		kept = append(kept, p)
	}
	s.packets = kept
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
// second — minus the congestion event, which a timeout is not (§6.2.4,
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
	// onAcked is called once per newly acknowledged packet sent on the
	// current path, in packet-number order (RFC 9002 B.5 runs the growth
	// test per packet).
	onAcked(ev ackEvent)
	// onLost is called once per batch of losses declared together — one
	// ACK frame or one loss-timer expiry (§7.3.2: one congestion event).
	onLost(ev lossEvent)
	// onPersistentCongestion follows onLost in the same batch when the
	// losses span more than the persistent-congestion duration (§7.6).
	onPersistentCongestion()
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
// not a congestion event (RFC 9002 §6.2.4). A lost DPLPMTUD probe leaves
// the same way (RFC 9000 §14.4). Floored as a guard only: a previous path's
// packets are filtered out before they reach here (sentTracker.pathStart).
func (c *newReno) onLifted(bytes int) { c.inFlight = max(c.inFlight-bytes, 0) }

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
	useful := c.limited || (c.cwnd < c.ssthresh && 2*c.maxInFlight >= c.cwnd)
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
