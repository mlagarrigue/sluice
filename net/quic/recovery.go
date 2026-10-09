package quic

import "time"

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
}

func (s *sentTracker) record(p sentPacket) {
	s.packets = append(s.packets, p)
}

// ackOutcome is what processing one ACK frame yields.
type ackOutcome struct {
	newlyAcked       int  // bytes released from flight
	newlyAckedCount  int  // packets released
	largestWasNew    bool // the frame's largest was newly acknowledged…
	largestEliciting bool // …and was ack-eliciting, which is what a sample needs
	largestSentAt    time.Time
	// growableBytes is the share of newlyAcked sent at or after the growAfter
	// instant the caller passed — the bytes the congestion window may grow
	// on (RFC 9002 B.5 runs this test per packet; an aggregate gated on the
	// oldest packet alone starves the ACK that straddles a recovery exit).
	growableBytes   int
	lostBytes       int
	lost            [][]byte // payloads to queue for retransmission
	sawAckEliciting bool     // any newly acked packet was ack-eliciting
	// latestLossSent is when the *newest* lost packet in this batch was
	// sent, which is what gates a congestion event (RFC 9002 B.8): one
	// pre-recovery straggler in a batch must not shield a genuinely new
	// loss epoch from the window reduction it deserves.
	latestLossSent time.Time
}

// processAck removes what the ranges cover and declares losses. largest is
// the frame's own largest field; visitRanges walks its encoded ranges.
// growAfter is the congestion controller's recovery start: acknowledged
// packets sent before it release flight bytes but earn no window growth.
func (s *sentTracker) processAck(now time.Time, largest uint64, rangeData []byte, rtt *rttEstimator, growAfter time.Time) (ackOutcome, error) {
	var out ackOutcome

	// Both sides are ordered — s.packets ascending by construction (the
	// packet number only grows), the frame's ranges strictly descending by
	// encoding (parseACK proved each one sits gap+2 below the previous) — so
	// one two-pointer merge marks every acknowledged packet without a map and
	// without rescanning the slice per range. An ACK may name billions of
	// packets; only the ones still in s.packets cost anything.
	i := len(s.packets) - 1
	marked := 0
	err := decodeAckRanges(largest, rangeData, func(lo, hi uint64) {
		for i >= 0 && s.packets[i].pn > hi {
			i--
		}
		for i >= 0 && s.packets[i].pn >= lo {
			if !s.packets[i].acked {
				s.packets[i].acked = true
				marked++
			}
			i--
		}
	})
	if err != nil {
		return out, err
	}
	if marked == 0 && len(s.packets) == 0 {
		return out, nil
	}

	kept := s.packets[:0]
	var largestAckedInFrame uint64
	for _, p := range s.packets {
		if !p.acked {
			kept = append(kept, p)
			continue
		}
		out.newlyAcked += p.size
		out.newlyAckedCount++
		if !p.sentAt.Before(growAfter) {
			out.growableBytes += p.size
		}
		if p.ackEliciting {
			out.sawAckEliciting = true
		}
		if p.pn == largest {
			out.largestWasNew = true
			out.largestEliciting = p.ackEliciting
			out.largestSentAt = p.sentAt
		}
		if p.pn > largestAckedInFrame {
			largestAckedInFrame = p.pn
		}
	}
	s.packets = kept
	if out.newlyAckedCount > 0 && (!s.hasLargestAcked || largestAckedInFrame > s.largestAcked) {
		s.largestAcked = largestAckedInFrame
		s.hasLargestAcked = true
	}

	// Loss: anything packetThreshold below the largest acknowledged, or older
	// than the time threshold, is gone (§6.1). Whatever is too young for
	// either arms the loss timer instead. The scan runs on every ACK, not
	// just the ones with news: the timer was cleared above, and an ACK that
	// repeats old news used to disarm a pending loss deadline for good —
	// the loss then surfaced only on a later new ACK, or on the PTO.
	s.lossTime = time.Time{}
	if s.hasLargestAcked {
		largest := s.largestAcked
		threshold := max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
		lostBefore := now.Add(-threshold)
		kept = s.packets[:0]
		for _, p := range s.packets {
			lostByCount := p.pn+packetThreshold <= largest
			lostByTime := p.pn < largest && p.sentAt.Before(lostBefore)
			if lostByCount || lostByTime {
				out.lostBytes += p.size
				if len(p.payload) > 0 {
					out.lost = append(out.lost, p.payload)
				}
				if p.sentAt.After(out.latestLossSent) {
					out.latestLossSent = p.sentAt
				}
				continue
			}
			if p.pn < largest && s.lossTime.IsZero() {
				// Not lost yet by time, but older than the largest: it will
				// be, if nothing acknowledges it by then.
				s.lossTime = p.sentAt.Add(threshold)
			}
			kept = append(kept, p)
		}
		s.packets = kept
	}
	return out, nil
}

// collectTimeLosses declares the packets whose loss timer has now matured,
// set by a previous ACK that left them too young to call.
//
// The sweep is by age alone: it does not re-check that a newer packet has
// been acknowledged, though the timer was armed by an ACK that had. A
// packet sent after that ACK can therefore be declared early. The error
// only ever runs toward resending — duplicates merge in reassembly and
// acknowledgements retire the copies — which is the cheap side to be wrong
// on.
func (s *sentTracker) collectTimeLosses(now time.Time, rtt *rttEstimator) (lost [][]byte, lostBytes int, latestSent time.Time) {
	if s.lossTime.IsZero() || now.Before(s.lossTime) {
		return nil, 0, time.Time{}
	}
	threshold := max(time.Duration(timeThresholdNum)*max(rtt.current(), rtt.latest)/timeThresholdDen, granularity)
	lostBefore := now.Add(-threshold)
	s.lossTime = time.Time{}
	kept := s.packets[:0]
	for _, p := range s.packets {
		if p.sentAt.Before(lostBefore) {
			lostBytes += p.size
			if len(p.payload) > 0 {
				lost = append(lost, p.payload)
			}
			if p.sentAt.After(latestSent) {
				// The newest loss gates the congestion event, same as the
				// ACK path (B.8).
				latestSent = p.sentAt
			}
			continue
		}
		if s.lossTime.IsZero() {
			s.lossTime = p.sentAt.Add(threshold)
		}
		kept = append(kept, p)
	}
	s.packets = kept
	return lost, lostBytes, latestSent
}

// oldestEliciting is what the probe timeout resends and what it is armed on.
func (s *sentTracker) oldestEliciting() (sentPacket, bool) {
	for _, p := range s.packets {
		if p.ackEliciting {
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

// newReno is §7's congestion controller, the RFC's own default and the
// defensible minimum: nothing fancier belongs here without measurement.
type newReno struct {
	cwnd          int
	ssthresh      int
	inFlight      int
	recoveryStart time.Time // packets sent before this do not shrink the window again
	maxDatagram   int
}

func newNewReno(maxDatagram int) newReno {
	// §7.2: ten datagrams, floored the way the RFC floors it.
	w := max(min(10*maxDatagram, 14720), 2*maxDatagram)
	return newReno{cwnd: w, ssthresh: 1 << 30, maxDatagram: maxDatagram}
}

// canSend is the congestion gate alone — bursts are not bounded here. The
// §7.7 burst bound was tried as a per-ACK allowance of one initial window
// and measured before it could land: on a clean 20ms-RTT path it collapsed
// goodput fifty-fold (BenchmarkTransferClean, 35.5 to 0.70 MB/s — one burst
// per round trip, because a burst's ACKs arrive as one cluster and the
// allowance starves between clusters). §7.7 is pacing's job — a
// timer-released send rate — or nothing.
func (c *newReno) canSend(bytes int) bool { return c.inFlight+bytes <= c.cwnd }

func (c *newReno) onSent(bytes int) { c.inFlight += bytes }

// paceWindow is the window the pacer prices sends against: doubled in slow
// start, where the window itself doubles per round trip and pacing at the
// current cwnd would lag the growth it is spreading (measured: ~3% off a
// clean 20ms path's goodput; §7.7 permits a higher slow-start rate).
func (c *newReno) paceWindow() int {
	if c.cwnd < c.ssthresh {
		return 2 * c.cwnd
	}
	return c.cwnd
}

// onAcked grows the window: by the growable bytes in slow start, by one
// datagram per window's worth in avoidance (§7.3.1, §7.3.3). bytes releases
// flight; growable is the share of it sent since recovery began — packets
// from before the loss release flight but earn nothing, and processAck
// applies that test per packet (B.5), so one pre-recovery straggler in an
// ACK no longer silences the growth of everything sent after the exit.
func (c *newReno) onAcked(bytes, growable int) {
	c.inFlight -= bytes
	if c.inFlight < 0 {
		c.inFlight = 0
	}
	if growable <= 0 {
		return
	}
	if c.cwnd < c.ssthresh {
		c.cwnd += growable
		return
	}
	c.cwnd += c.maxDatagram * growable / c.cwnd
}

// onLost shrinks the window once per recovery period (§7.3.2): a whole
// window's losses are one congestion event, not one each. latestSent is the
// send time of the newest packet in the batch — B.8's gate: if even the
// newest predates the recovery start, the whole batch belongs to the epoch
// already paid for; if it does not, this is a new epoch, however many
// stragglers ride along.
func (c *newReno) onLost(bytes int, latestSent, now time.Time) {
	c.inFlight -= bytes
	if c.inFlight < 0 {
		c.inFlight = 0
	}
	if latestSent.Before(c.recoveryStart) {
		return
	}
	c.recoveryStart = now
	c.cwnd /= 2
	if c.cwnd < 2*c.maxDatagram {
		c.cwnd = 2 * c.maxDatagram
	}
	c.ssthresh = c.cwnd
}

// pacer spreads the congestion window over the round trip (RFC 9002 §7.7)
// instead of letting it leave as one line-rate burst. It is a token bucket:
// tokens refill continuously at 5/4 of cwnd per smoothed round trip — the
// RFC's "slightly higher than necessary" rate, so pacing never becomes the
// throughput ceiling — and the bucket holds one initial window, which is
// the burst §7.7 permits a sender without finer timing.
//
// The naive alternative — a per-ACK burst allowance — was measured before
// this existed and collapsed a clean 20ms path fifty-fold (see canSend);
// time, not the ack clock, has to be what refills the bucket.
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

// refill credits the time elapsed since the last call at the current rate.
// Callers hold the connection's lock, like every pacer method.
func (p *pacer) refill(now time.Time, cwnd int, srtt time.Duration) {
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
	if srtt < granularity {
		srtt = granularity
	}
	// Float, not integer: a benchmark-sized cwnd (2^30) times an elapsed in
	// nanoseconds overflows int64 long before the arithmetic rounds wrong,
	// and a float64 carries both magnitudes exactly enough for a byte count.
	add := 1.25 * float64(cwnd) * (float64(elapsed) / float64(srtt))
	if add >= float64(p.burst-p.tokens) {
		p.tokens = p.burst
	} else {
		p.tokens += int64(add)
	}
}

// spend takes bytes out of the bucket, below zero if they are not there:
// what was sent was sent, and the deficit prices it for whoever sends next.
func (p *pacer) spend(bytes int) { p.tokens -= int64(bytes) }

// delay is how long a paced sender must wait before its next send, zero when
// it may go now. Deficits smaller than one timer granularity are released
// immediately rather than armed: the timer cannot honour them, and rounding
// down bounds the overshoot to one granularity's worth of line rate —
// rate-proportional, unlike the unbounded burst pacing exists to prevent.
func (p *pacer) delay(cwnd int, srtt time.Duration) time.Duration {
	if p.tokens >= 0 {
		return 0
	}
	if srtt < granularity {
		srtt = granularity
	}
	d := time.Duration(-p.tokens * 4 * int64(srtt) / (5 * int64(cwnd)))
	if d < granularity {
		return 0
	}
	return d
}

// onPersistentCongestion collapses the window to its minimum (§7.6.2), taken
// when a probe timeout has backed off repeatedly with nothing acknowledged:
// the path is not dropping a packet, it is gone.
func (c *newReno) onPersistentCongestion() {
	c.cwnd = 2 * c.maxDatagram
	c.recoveryStart = time.Time{}
}
