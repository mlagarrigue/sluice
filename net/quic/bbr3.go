package quic

import (
	"math"
	"math/rand/v2"
	"time"
)

// bbr3 is BBR as the IETF congestion control working group drafts it
// (draft-ietf-ccwg-bbr-06, the algorithm Linux ships as BBR version 3),
// chosen with CongestionBBR. It keeps version 1's model of the path — the
// highest delivery rate and the lowest round trip seen recently — and its
// states, and adds what version 1 lacked: loss as evidence about the
// volume of data the path can hold.
//
// Two models, one long-term and one short-term (§3.4.2). The long-term one
// is max_bw, the max filter over the last two ProbeBW cycles (§5.5.3), and
// inflight_longterm, the volume above which the loss rate crossed 2%
// (§5.5.10.2), learnt while probing and raised again slowly in ProbeBW_UP.
// The short-term one is bw_shortterm and inflight_shortterm, cut by 0.7 at
// every round that saw a loss while the flow was not probing (§5.5.10.3)
// and forgotten at the next probe. The controls read the minimum of the
// two: the pacing rate is pacing_gain × min(max_bw, bw_shortterm), and the
// window is bounded by the inflight bounds, with 15% of headroom left free
// in ProbeBW_CRUISE and ProbeRTT (§5.6.4.7).
//
// The state machine (§5.1): Startup at a pacing gain of 2.77 leaves on a
// bandwidth plateau or on a round with six loss events and 2% of loss
// (§5.3.1.3), Drain paces at half the estimate until the flight is down
// to the product, three rounds at most (§5.3.2), ProbeBW cycles through
// DOWN, CRUISE, REFILL and UP (§5.3.3) — probing every two to three
// seconds, or as often as a Reno flow of the same window would (§5.3.3.8)
// — and ProbeRTT halves the window for 200 ms every five seconds
// (§5.3.4). extra_acked (§5.5.9) adds the aggregation the path shows to
// the window. The send quantum is one datagram, the unit the pacer
// releases. The CE marks the peer reports (§3.7: the draft specifies no
// response, only that marks MUST be treated as congestion) are read as a
// loss is, less the retransmission: a marked round cuts the short-term
// model by 0.7 (onMarked), and marks count with losses against the 2% a
// probe may cause, so a probe that marks more than that bounds
// inflight_longterm and goes DOWN. Not the DCTCP-style response Linux
// gives L4S marks, which this end never asks for (it marks ECT(0)).
//
// One reading differs from the draft's pseudocode: Startup's loss rate
// over a round (§5.3.1.3) is the round's lost bytes over its lost plus
// delivered bytes, the loss round being the draft's (first loss to the
// acknowledgement of a packet sent after it). Every other loss test is the
// draft's, per packet: RS.lost and RS.tx_in_flight from the stamps each
// packet took when it left (lostPacket, ackEvent).
//
// A second: the precautionary exit from ProbeBW_UP (§5.3.3.7) compares
// inflight_longterm with the flight before the acknowledged packet left
// it, Linux's prior_in_flight. The window is capped at inflight_longterm
// in UP, so the flight after an acknowledgement is always under it and the
// draft's C.inflight read there would never stop a probe.
//
// Left out of draft -06 on purpose: the undo of a spurious loss recovery
// (§2.15, §5.2.5, §5.5.11). The draft saves the window and the short- and
// long-term model when a loss episode starts (SaveStateUponLoss, from
// BBRNoteLoss), the state a loss sends out of Startup or ProbeBW_UP
// (undo_state), and restores them if the transport later concludes that
// no packet of the episode was lost (HandleSpuriousLossDetection). The
// transport cannot conclude it: the ACK scan drops a packet from the
// flight record the moment it declares it lost (sentTracker), so a late
// acknowledgement of that number matches nothing and is never seen. A
// detector would keep lost packets in the record on the ACK path, and
// track each episode until all of its packets are acknowledged — a cost
// on every scan for a call RFC 9002 leaves optional (§6.1, A.1). Without
// it, the saved state would have no reader, so none is kept; a spurious
// loss costs what a real one does. Revisit when the transport keeps lost
// packets for reordering (an adaptive threshold, RFC 9002 §6.1.1): the
// detector then comes for free and the undo is wired to it.
type bbr3 struct {
	// est is the connection's delivery estimator, which ProbeRTT marks
	// application-limited (handleProbeRTT).
	est         *deliveryEstimator
	maxDatagram int
	initialCwnd int
	inFlight    int
	cwnd        int
	// limited says canSend refused a send this round;
	// limitedPrev is what it said over the previous round. The draft's
	// C.is_cwnd_limited, Linux's reading — whether the last send was
	// stopped by the window — is read as either (cwndLimited): the flag
	// of the round that just opened is always clear.
	limited     bool
	limitedPrev bool

	// The long-term model (§5.5): max_bw over the last two ProbeBW cycles,
	// the cycle count being the filter's clock (§5.5.6); min_rtt over ten
	// seconds, fed by probe_rtt_min_delay, the lowest sample of the last
	// five (§5.3.4.3). Zero durations mean no sample yet (the draft's
	// Infinity). bw is what the controls read, min(max_bw, bw_shortterm).
	maxBwFilter      windowedMax[float64]
	maxBw            float64
	cycleCount       uint64
	bw               float64
	minRTT           time.Duration
	minRTTStamp      time.Time
	probeRTTMinDelay time.Duration
	probeRTTMinStamp time.Time
	probeRTTExpired  bool
	// inflight_longterm is the volume the loss rate said was too much
	// (§5.5.10.2), bbr3Unbounded until a loss said so.
	inflightLongterm int

	// The short-term model (§5.5.10.3): the bounds, unset between probes,
	// and the latest delivery signals they adapt to — the best rate and
	// volume of the current loss round. A loss round runs from the first
	// loss or CE mark to the acknowledgement of a packet sent after it;
	// lostInRound counts its lost bytes, lostRangesInRound the discontiguous
	// ranges they form (§5.3.1.3), markedInRound its
	// marks in bytes (the peer counts packets: one datagram each),
	// roundDelivered holds what the round that just ended delivered.
	bwShortterm        float64
	inflightShortterm  int
	bwLatest           float64
	inflightLatest     int
	lossInRound        bool
	lossRoundStart     bool
	lossRoundDelivered int
	roundDelivered     int
	lostInRound        int
	markedInRound      int
	lostRangesInRound  int

	// Round counting (§5.5.1): delivered is the controller's own count of
	// acknowledged bytes, and a round ends when a packet sent after the
	// round's first delivery is acknowledged.
	delivered          int
	nextRoundDelivered int
	roundStart         bool
	roundCount         uint64
	drainStartRound    uint64
	roundsSinceBwProbe int

	// Aggregation (§5.5.9): extra_acked is the max over ten rounds of what
	// was acknowledged beyond bw × elapsed since the interval started.
	extraAcked              int
	extraAckedFilter        windowedMax[int]
	extraAckedIntervalStart time.Time
	extraAckedDelivered     int

	// The state machine (§5.3) and its control parameters (§5.6): rate is
	// the pacing rate in bytes per second, zero until the first sample.
	state      bbr3State
	pacingGain float64
	cwndGain   float64
	rate       float64
	// The full-pipe estimator (§5.3.1.2): reached is for the lifetime of
	// the connection, now for the current probe. fullBwPending carries a
	// round's check to the frame's own sample (checkFullBwReached).
	fullBwReached bool
	fullBwNow     bool
	fullBw        float64
	fullBwCount   int
	fullBwPending bool
	// ProbeBW (§5.3.3.9): when DOWN started and how long to wait before
	// REFILL, which samples the acknowledgements carry, whether a loss may
	// still speak for the last probe, and the slope inflight_longterm is
	// raised at in UP. prevProbeTooHigh says the last probe lost too much,
	// prevProbePrecautionary that the last UP stopped at inflight_longterm
	// for that reason (§2.14, §5.3.3.6); both start false.
	cycleStamp             time.Time
	bwProbeWait            time.Duration
	ackPhase               bbr3AckPhase
	bwProbeSamples         bool
	bwProbeUpRounds        int
	bwProbeUpAcks          int
	probeUpCnt             int
	prevProbeTooHigh       bool
	prevProbePrecautionary bool
	// ProbeRTT (§5.3.4.3): when the 200 ms are over and whether a round
	// has passed since.
	probeRTTDoneStamp time.Time
	probeRTTRoundDone bool
	idleRestart       bool

	// Loss recovery (§5.6.4.4): one period from a loss to the
	// acknowledgement of a packet sent after it (RFC 9002 §7.3.2); the
	// window is saved on entry and restored on exit, the model bounding it
	// right after.
	inRecovery    bool
	recoveryStart time.Time
	priorCwnd     int
}

type bbr3State uint8

const (
	bbr3Startup bbr3State = iota
	bbr3Drain
	bbr3ProbeBWDown
	bbr3ProbeBWCruise
	bbr3ProbeBWRefill
	bbr3ProbeBWUp
	bbr3ProbeRTT
)

// bbr3AckPhase says what the acknowledgements being processed are
// feedback about (§5.3.3.9): nothing in particular, the REFILL phase, the
// start of a probe, the probe itself, or its end.
type bbr3AckPhase uint8

const (
	bbr3AcksInit bbr3AckPhase = iota
	bbr3AcksRefilling
	bbr3AcksProbeStarting
	bbr3AcksProbeFeedback
	bbr3AcksProbeStopping
)

const (
	// bbr3StartupPacingGain is 4 ln 2, the smallest gain that doubles the
	// sending rate each round (§5.3.1.1); bbr3DrainPacingGain drains the
	// queue Startup built in under a round (§2.5, §5.3.2).
	bbr3StartupPacingGain = 2.77
	bbr3DrainPacingGain   = 0.5
	bbr3DefaultCwndGain   = 2.0
	// ProbeBW's gains (§5.3.3): DOWN sends at 0.9 of the estimate, UP at
	// 1.25 with the window at 2.25 so that the window never caps the probe.
	bbr3ProbeBWDownGain   = 0.9
	bbr3ProbeBWUpGain     = 1.25
	bbr3ProbeBWUpCwndGain = 2.25
	// bbr3ProbeRTTCwndGain halves the flight in ProbeRTT (§5.3.4.1).
	bbr3ProbeRTTCwndGain = 0.5
	// bbr3PacingMargin paces 1% under the estimate (§5.6.2).
	bbr3PacingMargin = 0.01
	// bbr3LossThresh is the loss rate a probe may cause; bbr3Beta is the
	// cut applied per round of loss; bbr3Headroom is the share of
	// inflight_longterm left free while cruising (§2.8).
	bbr3LossThresh = 0.02
	bbr3Beta       = 0.7
	bbr3Headroom   = 0.15
	// bbr3MaxBwFilterLen is the max filter's window in ProbeBW cycles;
	// bbr3ExtraAckedFilterLen the aggregation filter's in rounds.
	bbr3MaxBwFilterLen      = 2
	bbr3ExtraAckedFilterLen = 10
	// bbr3MinRTTFilterLen is the min filter's window, bbr3ProbeRTTInterval
	// how long without a new low before ProbeRTT, bbr3ProbeRTTDuration how
	// long ProbeRTT holds the flight down (§5.3.4).
	bbr3MinRTTFilterLen  = 10 * time.Second
	bbr3ProbeRTTInterval = 5 * time.Second
	bbr3ProbeRTTDuration = 200 * time.Millisecond
	// bbr3StartupFullLossCnt is how many discontiguous lost ranges a round
	// needs for Startup to end on loss (§5.3.1.3).
	bbr3StartupFullLossCnt = 6
	// bbr3FullBwThresh is the growth a round must show in the estimate to
	// count as still probing, bbr3FullBwRounds how many rounds without it
	// end the probe (§5.3.1.2).
	bbr3FullBwThresh = 1.25
	bbr3FullBwRounds = 3
	// bbr3RenoRoundsBound caps the rounds a Reno flow's window is read as
	// taking between probes (§5.3.3.8.3).
	bbr3RenoRoundsBound = 63
	// bbr3MinPipeCwnd is the window's floor, in datagrams (§5.6.4.3).
	bbr3MinPipeCwnd = 4
	// bbr3Unbounded is an inflight bound that is not set (the draft's
	// Infinity).
	bbr3Unbounded = math.MaxInt
)

var _ congestionController = (*bbr3)(nil)

func newBBR3(maxDatagram int, est *deliveryEstimator) *bbr3 {
	b := &bbr3{
		est:              est,
		maxDatagram:      maxDatagram,
		inflightLongterm: bbr3Unbounded,
	}
	// §5.6.4.1: the transport's initial window, RFC 9002 §7.2's.
	b.initialCwnd = newNewReno(maxDatagram).cwnd
	b.cwnd = b.initialCwnd
	b.resetShortTermModel()
	b.enterStartup()
	return b
}

func (b *bbr3) window() int              { return b.cwnd }
func (b *bbr3) bytesInFlight() int       { return b.inFlight }
func (b *bbr3) setMaxDatagram(bytes int) { b.maxDatagram = bytes }
func (b *bbr3) minPipeCwnd() int         { return bbr3MinPipeCwnd * b.maxDatagram }

func (b *bbr3) canSend(bytes int) bool {
	if b.inFlight+bytes > b.cwnd {
		b.limited = true
		return false
	}
	return true
}

// onSent adds to the flight and notices a restart from idle (§5.4.1): a
// send into an empty flight keeps the window and, in ProbeBW, paces at
// exactly bw until the next acknowledgement re-applies the gain; in
// ProbeRTT it checks whether the idle time met ProbeRTT's conditions. The
// draft also asks that the sender be application-limited; an empty flight
// is read as that here. The first send stamps the clocks the draft starts
// at initialisation.
func (b *bbr3) onSent(now time.Time, bytes int) {
	if b.inFlight == 0 {
		b.idleRestart = true
		b.extraAckedIntervalStart = now
		if b.inProbeBW() {
			b.setPacingRateWithGain(1)
		} else if b.state == bbr3ProbeRTT {
			b.checkProbeRTTDone(now)
		}
	}
	if b.minRTTStamp.IsZero() {
		b.minRTTStamp, b.probeRTTMinStamp = now, now
	}
	b.inFlight += bytes
}

func (b *bbr3) onLifted(bytes int) { b.inFlight = max(b.inFlight-bytes, 0) }

// pacingRate is the rate the model names, kept in rate by setPacingRate.
// Before the first sample it is the draft's initial rate (§5.6.2): the
// initial window over the round trip, at Startup's gain.
func (b *bbr3) pacingRate(srtt time.Duration) float64 {
	if b.rate > 0 {
		return b.rate
	}
	return bbr3StartupPacingGain * float64(b.initialCwnd) / max(srtt, granularity).Seconds()
}

// bdpMultiple is BBRBDPMultiple (§5.6.4.2): gain times the estimated
// bandwidth-delay product, or the initial window while there is no
// round-trip sample to size the product with.
func (b *bbr3) bdpMultiple(bw, gain float64) int {
	if b.minRTT == 0 {
		return b.initialCwnd
	}
	return int(gain * bw * b.minRTT.Seconds())
}

// quantizationBudget is BBRQuantizationBudget (§5.6.4.2): the offload
// budget, which for QUIC is one send quantum (§5.5.8.2) — a datagram here —
// and four datagrams at least, two more in UP. The segmentation offload
// writes what the pacer already let out, it schedules nothing ahead, so
// the budget has no pacing offload to add.
func (b *bbr3) quantizationBudget(volume int) int {
	volume = max(volume, b.maxDatagram, b.minPipeCwnd())
	if b.state == bbr3ProbeBWUp {
		volume += 2 * b.maxDatagram
	}
	return volume
}

func (b *bbr3) inflight(bw, gain float64) int {
	return b.quantizationBudget(b.bdpMultiple(bw, gain))
}

// maxInflight is BBRUpdateMaxInflight: the window's target, the product
// at the state's gain plus the aggregation seen.
func (b *bbr3) maxInflight() int {
	return b.quantizationBudget(b.bdpMultiple(b.bw, b.cwndGain) + b.extraAcked)
}

// targetInflight is BBRTargetInflight (§5.3.3.8.3): the product, unless
// congestion cut the window under it.
func (b *bbr3) targetInflight() int {
	return min(b.bdpMultiple(b.bw, 1), b.cwnd)
}

// inflightWithHeadroom is BBRInflightWithHeadroom (§5.3.3.9): the
// long-term bound less 15% of it, a datagram at least, for other flows.
func (b *bbr3) inflightWithHeadroom() int {
	if b.inflightLongterm == bbr3Unbounded {
		return bbr3Unbounded
	}
	headroom := max(b.maxDatagram, int(bbr3Headroom*float64(b.inflightLongterm)))
	return max(b.inflightLongterm-headroom, b.minPipeCwnd())
}

// onAcked is BBRUpdateOnACK (§5.2.3), run per packet in the draft's order:
// the packet leaves the flight, its sample feeds the model, the state
// machine reads the model, the recovery period ends if the packet was sent
// after the loss, and the control parameters follow. The max filter and
// the plateau's check read the frame's sample and nothing else, as the
// draft's once-per-ACK run does (§5.5.5, §5.3.1.2): an older packet of
// the frame passes them an empty one.
func (b *bbr3) onAcked(ev ackEvent) {
	b.inFlight = max(b.inFlight-ev.size, 0)
	b.delivered += ev.size
	priorDelivered := b.delivered - ev.delivered // the draft's P.delivered
	rs := ev.rateSample()
	rtt := ev.now.Sub(ev.sentAt)
	if b.rate == 0 {
		b.rate = bbr3StartupPacingGain * float64(b.initialCwnd) / max(rtt, granularity).Seconds()
	}

	var frame rateSample
	if ev.rated {
		frame = rs
	}
	b.updateLatestDeliverySignals(rs, priorDelivered)
	b.updateCongestionSignals(frame, priorDelivered)
	b.updateACKAggregation(ev.now, ev.size)
	b.checkFullBwReached(frame, ev.rated)
	b.checkStartupDone()
	b.checkDrainDone(ev.now)
	b.updateProbeBWCyclePhase(rs, ev)
	b.updateMinRTT(ev.now, rtt)
	b.checkProbeRTT(ev.now)
	b.advanceLatestDeliverySignals(rs)
	b.bw = min(b.maxBw, b.bwShortterm)

	if b.inRecovery && ev.sentAt.After(b.recoveryStart) { // RFC 9002 B.5: strictly after the start
		// BBRRestoreCwnd (§5.6.4.4): the period ends (RFC 9002 §7.3.2) and
		// the window the loss interrupted comes back, bounded by the model
		// in setCwnd right after.
		b.inRecovery = false
		b.cwnd = max(b.cwnd, b.priorCwnd)
	}
	b.setPacingRate()
	b.setCwnd(ev.size)
}

// updateLatestDeliverySignals is BBRUpdateLatestDeliverySignals
// (§5.5.10.3): the best rate and volume of the loss round, and whether this
// packet ends the loss round — it was sent after the round's mark.
func (b *bbr3) updateLatestDeliverySignals(rs rateSample, priorDelivered int) {
	b.lossRoundStart = false
	b.bwLatest = max(b.bwLatest, rs.deliveryRate())
	b.inflightLatest = max(b.inflightLatest, rs.delivered)
	if priorDelivered >= b.lossRoundDelivered {
		b.roundDelivered = b.delivered - b.lossRoundDelivered
		b.lossRoundDelivered = b.delivered
		b.lossRoundStart = true
	}
}

// updateCongestionSignals is BBRUpdateCongestionSignals (§5.5.10.3): the
// max filter on the frame's sample, and once per loss round the short-term
// bounds adapt to the losses, Startup reads them (§5.3.1.3), and the
// round's counts are cleared.
func (b *bbr3) updateCongestionSignals(rs rateSample, priorDelivered int) {
	b.updateMaxBw(rs, priorDelivered)
	if !b.lossRoundStart {
		return
	}
	b.adaptLowerBoundsFromCongestion()
	b.checkStartupHighLoss()
	b.lossInRound = false
	b.lostInRound, b.markedInRound, b.lostRangesInRound = 0, 0, 0
}

// updateMaxBw is BBRUpdateMaxBw (§5.5.5): no sample, or one that measured
// nothing, leaves the filter alone — a zero inserted as a new cycle opens
// would empty it; an application-limited sample is kept only when it beats
// the estimate (§5.5.4).
func (b *bbr3) updateMaxBw(rs rateSample, priorDelivered int) {
	b.updateRound(priorDelivered)
	if bw := rs.deliveryRate(); bw > 0 && (bw >= b.maxBw || !rs.appLimited) {
		b.maxBw = b.maxBwFilter.update(b.cycleCount, bw, bbr3MaxBwFilterLen)
	}
}

// updateRound is BBRUpdateRound (§5.5.1): the packet's own delivered count
// at its send reaching the mark means a packet sent after the round's
// first delivery came back. A round's start is also where the utilisation
// flag is cleared.
func (b *bbr3) updateRound(priorDelivered int) {
	if priorDelivered >= b.nextRoundDelivered {
		b.startRound()
		b.roundCount++
		b.roundsSinceBwProbe++
		b.roundStart = true
		b.limited, b.limitedPrev = false, b.limited
		return
	}
	b.roundStart = false
}

func (b *bbr3) cwndLimited() bool { return b.limited || b.limitedPrev }

func (b *bbr3) startRound() { b.nextRoundDelivered = b.delivered }

// adaptLowerBoundsFromCongestion is BBRAdaptLowerBoundsFromCongestion
// (§5.5.10.3): a round that saw a loss or a CE mark while the flow was not
// probing cuts the short-term bounds to 0.7 of what they were, never under
// what the round actually delivered.
func (b *bbr3) adaptLowerBoundsFromCongestion() {
	if b.probingBW() || !b.lossInRound {
		return
	}
	if math.IsInf(b.bwShortterm, 1) {
		b.bwShortterm = b.maxBw
	}
	if b.inflightShortterm == bbr3Unbounded {
		b.inflightShortterm = b.cwnd
	}
	b.bwShortterm = max(b.bwLatest, bbr3Beta*b.bwShortterm)
	b.inflightShortterm = max(b.inflightLatest, int(bbr3Beta*float64(b.inflightShortterm)))
}

func (b *bbr3) probingBW() bool {
	return b.state == bbr3Startup || b.state == bbr3ProbeBWRefill || b.state == bbr3ProbeBWUp
}

func (b *bbr3) inProbeBW() bool {
	return b.state >= bbr3ProbeBWDown && b.state <= bbr3ProbeBWUp
}

// lossTooHigh is IsInflightTooHigh (§5.5.10.2) on one packet: what was
// declared lost between its send and its acknowledgement or loss (RS.lost)
// exceeds the 2% a probe may cause of the flight it was sent into
// (RS.tx_in_flight).
func lossTooHigh(lost, txInFlight int) bool {
	return float64(lost) > bbr3LossThresh*float64(txInFlight)
}

// roundTooHigh is the same 2% at the loss round's scale: its losses and CE
// marks against a volume. The marks have no packet to stamp (markEvent),
// and Startup's exit reads a round's loss rate (§5.3.1.3).
func (b *bbr3) roundTooHigh(volume int) bool {
	return b.lossInRound && float64(b.lostInRound+b.markedInRound) > bbr3LossThresh*float64(volume)
}

// checkStartupHighLoss is BBRCheckStartupHighLoss (§5.3.1.3), read when a
// loss round ends: in recovery, six discontiguous ranges lost in the round
// and a loss rate over 2% of what the round carried mean the pipe is full
// — full_bw_now as well as full_bw_reached (test A.2) — and the bound is
// the product or the round's volume, whichever is larger.
func (b *bbr3) checkStartupHighLoss() {
	if b.state != bbr3Startup || !b.inRecovery || b.lostRangesInRound < bbr3StartupFullLossCnt {
		return
	}
	if !b.roundTooHigh(b.lostInRound + b.roundDelivered) {
		return
	}
	b.fullBwNow, b.fullBwReached = true, true
	b.inflightLongterm = max(b.bdpMultiple(b.bw, 1), b.inflightLatest)
}

// updateACKAggregation is BBRUpdateACKAggregation (§5.5.9): what was
// acknowledged beyond what bw predicts since the interval started, the
// interval restarting whenever acknowledgements fall behind the prediction.
func (b *bbr3) updateACKAggregation(now time.Time, acked int) {
	expected := int(b.bw * now.Sub(b.extraAckedIntervalStart).Seconds())
	if b.extraAckedDelivered <= expected {
		b.extraAckedDelivered = 0
		b.extraAckedIntervalStart = now
		expected = 0
	}
	b.extraAckedDelivered += acked
	extra := min(b.extraAckedDelivered-expected, b.cwnd)
	var window uint64 = 1 // in Startup, just remember one round
	if b.fullBwReached {
		window = bbr3ExtraAckedFilterLen
	}
	b.extraAcked = b.extraAckedFilter.update(b.roundCount, extra, window)
}

// checkFullBwReached is BBRCheckFullBWReached (§5.3.1.2): once per round,
// on the sample of the frame that started it, the probe is over when three
// rounds in a row have raised the delivery rate by less than a quarter. A
// round whose sample measures the application rather than the path is not
// counted. The round may start on an older packet of the frame, whose
// sample is the frame's newest (rated): fullBwPending carries the check
// there, or to the next frame whose sample is reliable.
func (b *bbr3) checkFullBwReached(frame rateSample, rated bool) {
	if b.roundStart {
		b.fullBwPending = true
	}
	if b.fullBwNow || !b.fullBwPending || !rated {
		return
	}
	b.fullBwPending = false
	if frame.appLimited {
		return
	}
	if rate := frame.deliveryRate(); rate >= b.fullBw*bbr3FullBwThresh {
		b.resetFullBw()
		b.fullBw = rate
		return
	}
	b.fullBwCount++
	b.fullBwNow = b.fullBwCount >= bbr3FullBwRounds
	if b.fullBwNow {
		b.fullBwReached = true
	}
}

func (b *bbr3) resetFullBw() {
	b.fullBw, b.fullBwCount, b.fullBwNow, b.fullBwPending = 0, 0, false, false
}

// checkStartupDone is BBRCheckStartupDone (§5.3.1.1); the loss half ran in
// updateCongestionSignals, where the loss round's counts still stand.
func (b *bbr3) checkStartupDone() {
	if b.state == bbr3Startup && b.fullBwReached {
		b.enterDrain()
	}
}

// checkDrainDone is BBRCheckDrainDone (§5.3.2): Drain ends when the flight
// is down to one bandwidth-delay product, or three rounds after it started
// if Startup overestimated the path and the flight stays above.
func (b *bbr3) checkDrainDone(now time.Time) {
	if b.state == bbr3Drain && (b.inFlight <= b.inflight(b.bw, 1) || b.roundCount > b.drainStartRound+3) {
		b.enterProbeBW(now)
	}
}

func (b *bbr3) enterStartup() {
	b.state = bbr3Startup
	b.pacingGain, b.cwndGain = bbr3StartupPacingGain, bbr3DefaultCwndGain
}

func (b *bbr3) enterDrain() {
	b.state = bbr3Drain
	b.pacingGain, b.cwndGain = bbr3DrainPacingGain, bbr3DefaultCwndGain
	b.drainStartRound = b.roundCount
}

// enterProbeBW is BBREnterProbeBW (§5.3.3.9): the cycle starts at DOWN.
func (b *bbr3) enterProbeBW(now time.Time) {
	b.cwndGain = bbr3DefaultCwndGain
	b.startProbeBWDown(now)
}

func (b *bbr3) startProbeBWDown(now time.Time) {
	b.resetCongestionSignals()
	b.probeUpCnt = bbr3Unbounded // not growing inflight_longterm
	b.pickProbeWait()
	b.cycleStamp = now
	b.ackPhase = bbr3AcksProbeStopping
	b.startRound()
	b.state = bbr3ProbeBWDown
	b.pacingGain, b.cwndGain = bbr3ProbeBWDownGain, bbr3DefaultCwndGain
}

func (b *bbr3) startProbeBWCruise() {
	b.state = bbr3ProbeBWCruise
	b.pacingGain, b.cwndGain = 1, bbr3DefaultCwndGain
}

func (b *bbr3) startProbeBWRefill() {
	b.resetShortTermModel()
	b.bwProbeUpRounds, b.bwProbeUpAcks = 0, 0
	b.prevProbePrecautionary = false
	b.ackPhase = bbr3AcksRefilling
	b.startRound()
	b.state = bbr3ProbeBWRefill
	b.pacingGain, b.cwndGain = 1, bbr3DefaultCwndGain
}

func (b *bbr3) startProbeBWUp() {
	b.ackPhase = bbr3AcksProbeStarting
	b.startRound()
	b.resetFullBw()
	b.fullBw = b.maxBw
	b.state = bbr3ProbeBWUp
	b.pacingGain, b.cwndGain = bbr3ProbeBWUpGain, bbr3ProbeBWUpCwndGain
	b.raiseInflightLongtermSlope()
}

func (b *bbr3) resetCongestionSignals() {
	b.lossInRound = false
	b.lostInRound, b.markedInRound, b.lostRangesInRound = 0, 0, 0
	b.bwLatest, b.inflightLatest = 0, 0
}

func (b *bbr3) resetShortTermModel() {
	b.bwShortterm = math.Inf(1)
	b.inflightShortterm = bbr3Unbounded
}

// pickProbeWait is BBRPickProbeWait (§5.3.3.8.3): two to three seconds of
// wall clock, and a round count that starts at zero or one, both drawn so
// that flows do not probe in step.
func (b *bbr3) pickProbeWait() {
	b.roundsSinceBwProbe = rand.IntN(2)                                                //nolint:gosec // G404: desynchronises probes, nothing a peer can predict to any effect
	b.bwProbeWait = 2*time.Second + time.Duration(rand.Float64()*float64(time.Second)) //nolint:gosec // G404: same
}

// updateProbeBWCyclePhase is BBRUpdateProbeBWCyclePhase (§5.3.3.9).
func (b *bbr3) updateProbeBWCyclePhase(rs rateSample, ev ackEvent) {
	if !b.fullBwReached {
		return // only handling steady-state behaviour here
	}
	now := ev.now
	if b.adaptLongTermModel(rs, ev) {
		return // already decided a state transition
	}
	if !b.inProbeBW() {
		return
	}
	switch b.state {
	case bbr3ProbeBWDown:
		if b.isTimeToProbeBW(now) {
			return
		}
		if b.isTimeToCruise() {
			b.startProbeBWCruise()
		}
	case bbr3ProbeBWCruise:
		b.isTimeToProbeBW(now)
	case bbr3ProbeBWRefill:
		// After one round of REFILL, start UP.
		if b.roundStart {
			b.bwProbeSamples = true
			b.startProbeBWUp()
		}
	case bbr3ProbeBWUp:
		if b.isTimeToGoDown(b.inFlight + ev.size) {
			b.prevProbeTooHigh = false // no high loss (yet)
			b.startProbeBWDown(now)
		}
	default:
	}
}

// isTimeToCruise is BBRIsTimeToCruise: the flight is under the headroom
// bound and down to one product of the long-term estimate.
func (b *bbr3) isTimeToCruise() bool {
	if b.inFlight > b.inflightWithHeadroom() {
		return false
	}
	return b.inFlight <= b.inflight(b.maxBw, 1)
}

// isTimeToProbeBW is BBRIsTimeToProbeBW (§5.3.3.8.3): the wall clock or
// the Reno-coexistence round count says so, and REFILL starts.
func (b *bbr3) isTimeToProbeBW(now time.Time) bool {
	if now.Sub(b.cycleStamp) > b.bwProbeWait || b.renoCoexistenceProbeTime() {
		b.startProbeBWRefill()
		return true
	}
	return false
}

// renoCoexistenceProbeTime is BBRIsRenoCoexistenceProbeTime: as many
// rounds as a Reno flow would take to grow back the window, 63 at most.
// The draft's pseudo-code (§5.3.3.8.3) writes reno_rounds = TargetInflight()
// and TargetInflight is in bytes, which the 63 bound would always win;
// the intent §5.3.3.8.2 states is "min(reno_bdp, T_reno_bound) round
// trips", a BDP counted in packets — Linux reads it that way too — so the
// target is divided by the datagram size first.
func (b *bbr3) renoCoexistenceProbeTime() bool {
	rounds := min(b.targetInflight()/b.maxDatagram, bbr3RenoRoundsBound)
	return b.roundsSinceBwProbe >= rounds
}

// isTimeToGoDown is BBRIsTimeToGoDown: after a probe that lost too much,
// the next stops as soon as the flight reaches the bound that probe set
// (§5.3.3.6); a probe capped by the long-term bound cannot find a
// plateau, so its estimator restarts; otherwise the plateau ends it.
// priorInFlight is the flight before the acknowledged packet left it.
func (b *bbr3) isTimeToGoDown(priorInFlight int) bool {
	if b.prevProbeTooHigh && priorInFlight >= b.inflightLongterm {
		b.prevProbePrecautionary = true
		return true
	}
	if b.cwndLimited() && b.cwnd >= b.inflightLongterm {
		b.resetFullBw()
		b.fullBw = b.maxBw
		return false
	}
	return b.fullBwNow
}

// adaptLongTermModel is BBRAdaptLongTermModel (§5.3.3.9): the
// acknowledgement phases advance on round starts — the probe's samples
// end one round after DOWN started, which also advances the max filter's
// clock, and a precautionary probe that lost nothing goes straight back
// to REFILL (§5.3.3.6) — and, when the loss rate is safe, the long-term
// bound rises to the flight the packet was sent into, and in UP along its
// slope. CE marks count against the rate too, at the loss round's scale
// (onMarked). It reports whether it changed the state.
func (b *bbr3) adaptLongTermModel(rs rateSample, ev ackEvent) bool {
	if b.ackPhase == bbr3AcksProbeStarting && b.roundStart {
		b.ackPhase = bbr3AcksProbeFeedback
	}
	if b.ackPhase == bbr3AcksProbeStopping && b.roundStart {
		b.bwProbeSamples = false
		b.ackPhase = bbr3AcksInit
		if b.inProbeBW() && !rs.appLimited {
			b.cycleCount++ // BBRAdvanceMaxBwFilter (§5.5.6)
		}
		if b.inProbeBW() && b.prevProbePrecautionary && !b.prevProbeTooHigh {
			b.startProbeBWRefill()
			return true
		}
	}
	if lossTooHigh(ev.lost, ev.txInFlight) || b.markedInRound > 0 && b.roundTooHigh(ev.txInFlight) || b.inflightLongterm == bbr3Unbounded {
		return false
	}
	if ev.txInFlight > b.inflightLongterm {
		b.inflightLongterm = ev.txInFlight
	}
	if b.state == bbr3ProbeBWUp {
		b.probeInflightLongtermUpward(ev.size)
	}
	return false
}

// raiseInflightLongtermSlope is BBRRaiseInflightLongtermSlope: the bound
// grows by one datagram this round, two the next, four, eight, spread over
// the round's acknowledgements — probeUpCnt bytes acknowledged per
// datagram added, a datagram at least (§5.3.3.9): past log2 of the window
// in rounds, a one-byte floor would add a datagram per byte acknowledged.
func (b *bbr3) raiseInflightLongtermSlope() {
	growth := b.maxDatagram << b.bwProbeUpRounds
	b.bwProbeUpRounds = min(b.bwProbeUpRounds+1, 30)
	b.probeUpCnt = max(b.cwnd*b.maxDatagram/growth, b.maxDatagram)
}

// probeInflightLongtermUpward is BBRProbeInflightLongtermUpward: only
// while the window is capped at the bound, and in use.
func (b *bbr3) probeInflightLongtermUpward(acked int) {
	if !b.cwndLimited() || b.cwnd < b.inflightLongterm {
		return
	}
	b.bwProbeUpAcks += acked
	if b.bwProbeUpAcks >= b.probeUpCnt {
		delta := b.bwProbeUpAcks / b.probeUpCnt
		b.bwProbeUpAcks -= delta * b.probeUpCnt
		b.inflightLongterm += delta * b.maxDatagram
	}
	if b.roundStart {
		b.raiseInflightLongtermSlope()
	}
}

// updateMinRTT is BBRUpdateMinRTT (§5.3.4.3): the sample is the packet's
// own round trip — the lowest in a frame, since every packet is offered.
// probe_rtt_min_delay takes it when lower or five seconds old; min_rtt
// takes probe_rtt_min_delay when lower or ten seconds old.
func (b *bbr3) updateMinRTT(now time.Time, rtt time.Duration) {
	b.probeRTTExpired = now.After(b.probeRTTMinStamp.Add(bbr3ProbeRTTInterval))
	if rtt > 0 && (b.probeRTTMinDelay == 0 || rtt < b.probeRTTMinDelay || b.probeRTTExpired) {
		b.probeRTTMinDelay = rtt
		b.probeRTTMinStamp = now
	}
	minRTTExpired := now.After(b.minRTTStamp.Add(bbr3MinRTTFilterLen))
	if b.probeRTTMinDelay != 0 && (b.minRTT == 0 || b.probeRTTMinDelay < b.minRTT || minRTTExpired) {
		b.minRTT = b.probeRTTMinDelay
		b.minRTTStamp = b.probeRTTMinStamp
	}
}

// checkProbeRTT is BBRCheckProbeRTT (§5.3.4.3): a probe_rtt_min_delay five
// seconds old sends every other state to ProbeRTT — not on the first
// acknowledgement after an idle period, whose age is the application's.
func (b *bbr3) checkProbeRTT(now time.Time) {
	if b.state != bbr3ProbeRTT && b.probeRTTExpired && !b.idleRestart {
		b.priorCwnd = b.saveCwnd()
		b.state = bbr3ProbeRTT
		b.pacingGain, b.cwndGain = 1, bbr3ProbeRTTCwndGain
		b.probeRTTDoneStamp = time.Time{}
		b.ackPhase = bbr3AcksProbeStopping
		b.startRound()
	}
	if b.state == bbr3ProbeRTT {
		b.handleProbeRTT(now)
	}
	b.idleRestart = false
}

// handleProbeRTT is BBRHandleProbeRTT: the connection is marked
// application-limited (MarkConnectionAppLimited), so the samples of what is
// sent meanwhile are discounted by this model and by Stats.DeliveryRate
// alike, the 200 ms start once the flight is down to half a product, and
// ProbeRTT ends after them and a round.
func (b *bbr3) handleProbeRTT(now time.Time) {
	b.est.markAppLimited(b.inFlight)
	switch {
	case b.probeRTTDoneStamp.IsZero() && b.inFlight <= b.probeRTTCwnd():
		b.probeRTTDoneStamp = now.Add(bbr3ProbeRTTDuration)
		b.probeRTTRoundDone = false
		b.startRound()
	case !b.probeRTTDoneStamp.IsZero():
		if b.roundStart {
			b.probeRTTRoundDone = true
		}
		if b.probeRTTRoundDone {
			b.checkProbeRTTDone(now)
		}
	}
}

// checkProbeRTTDone is BBRCheckProbeRTTDone: the next ProbeRTT is five
// seconds away, the window comes back, and the state machine resumes.
func (b *bbr3) checkProbeRTTDone(now time.Time) {
	if b.probeRTTDoneStamp.IsZero() || !now.After(b.probeRTTDoneStamp) {
		return
	}
	b.probeRTTMinStamp = now
	b.cwnd = max(b.cwnd, b.priorCwnd)
	b.exitProbeRTT(now)
}

// exitProbeRTT is BBRExitProbeRTT (§5.3.4.4): the short-term model is
// forgotten, and ProbeBW resumes at CRUISE — the flight is already under
// the product — or Startup if the pipe was never filled.
func (b *bbr3) exitProbeRTT(now time.Time) {
	b.resetShortTermModel()
	if b.fullBwReached {
		b.startProbeBWDown(now)
		b.startProbeBWCruise()
	} else {
		b.enterStartup()
	}
}

// probeRTTCwnd is BBRProbeRTTCwnd (§5.6.4.5): half a product, four
// datagrams at least.
func (b *bbr3) probeRTTCwnd() int {
	return max(b.bdpMultiple(b.bw, bbr3ProbeRTTCwndGain), b.minPipeCwnd())
}

// advanceLatestDeliverySignals is BBRAdvanceLatestDeliverySignals: a new
// loss round starts its signals from this sample.
func (b *bbr3) advanceLatestDeliverySignals(rs rateSample) {
	if b.lossRoundStart {
		b.bwLatest = rs.deliveryRate()
		b.inflightLatest = rs.delivered
	}
}

// saveCwnd is BBRSaveCwnd (§5.6.4.4): the last window not modulated by
// recovery or ProbeRTT, which is the current one outside both.
func (b *bbr3) saveCwnd() int {
	if !b.inRecovery && b.state != bbr3ProbeRTT {
		return b.cwnd
	}
	return max(b.priorCwnd, b.cwnd)
}

func (b *bbr3) setPacingRate() { b.setPacingRateWithGain(b.pacingGain) }

// setPacingRateWithGain is BBRSetPacingRateWithGain (§5.6.2): the rate
// follows the model, 1% under, once the pipe is known to be full, and
// before that only upward.
func (b *bbr3) setPacingRateWithGain(gain float64) {
	if rate := gain * b.bw * (1 - bbr3PacingMargin); b.fullBwReached || rate > b.rate {
		b.rate = rate
	}
}

// setCwnd is BBRSetCwnd (§5.6.4.6): growth by what was acknowledged,
// bounded by the target once the pipe is full, free while the window is
// below it or too little has been delivered to trust the model, above the
// floor of four datagrams, under ProbeRTT's cap, and under the model's
// bounds (§5.6.4.7).
func (b *bbr3) setCwnd(acked int) {
	target := b.maxInflight()
	if b.fullBwReached {
		b.cwnd = min(b.cwnd+acked, target)
	} else if b.cwnd < target || b.delivered < b.initialCwnd {
		b.cwnd += acked
	}
	b.cwnd = max(b.cwnd, b.minPipeCwnd())
	if b.state == bbr3ProbeRTT {
		b.cwnd = min(b.cwnd, b.probeRTTCwnd())
	}
	b.boundCwndForModel()
}

// boundCwndForModel is BBRBoundCwndForModel (§5.6.4.7): the long-term
// bound in the ProbeBW states, with the headroom taken off in CRUISE and
// ProbeRTT; the short-term bound everywhere.
func (b *bbr3) boundCwndForModel() {
	bound := bbr3Unbounded
	if b.inProbeBW() && b.state != bbr3ProbeBWCruise {
		bound = b.inflightLongterm
	} else if b.state == bbr3ProbeRTT || b.state == bbr3ProbeBWCruise {
		bound = b.inflightWithHeadroom()
	}
	bound = max(min(bound, b.inflightShortterm), b.minPipeCwnd())
	b.cwnd = min(b.cwnd, bound)
}

// onLost is BBRHandleLostPacket (§5.5.10.2) for a batch. The batch counts
// toward the loss round — opening one if none is running (BBRNoteLoss) —
// and toward the recovery period (RFC 9002 §7.3.2), whose entry saves the
// window (§5.6.4.4). Then, while the samples belong to a bandwidth probe,
// the first packet whose losses since it left exceed 2% of the flight it
// was sent into (lossEvent.crossing) sets the long-term bound to the
// volume at which they crossed it. That packet's own stamps, not the
// flight at the declaration: losses declared a round after an UP step, in
// DOWN, were sent into the larger flight (l.3891-3898).
func (b *bbr3) onLost(ev lossEvent) {
	b.inFlight = max(b.inFlight-ev.bytes, 0)
	b.noteCongestion()
	b.lostInRound += ev.bytes
	b.lostRangesInRound += ev.ranges
	if b.recoveryStart.IsZero() || ev.latestSent.After(b.recoveryStart) {
		// RFC 9002 B.5/B.6: a loss sent at or before the start is inside
		// the running period; strictly after it opens a new one.
		b.priorCwnd = b.saveCwnd()
		b.inRecovery = true
		b.recoveryStart = ev.now
	}
	if b.bwProbeSamples && ev.crossing.size > 0 {
		b.handleInflightTooHigh(ev.now, inflightAtLoss(ev.crossing), ev.crossing.appLimited)
	}
}

// noteCongestion is BBRNoteLoss: a loss or a mark opens a loss round if
// none is running — it ends when a packet sent from now on is acknowledged.
// The draft's SaveStateUponLoss is left out with the undo it feeds (bbr3).
func (b *bbr3) noteCongestion() {
	if !b.lossInRound {
		b.lossRoundDelivered = b.delivered
		b.lostInRound, b.markedInRound, b.lostRangesInRound = 0, 0, 0
	}
	b.lossInRound = true
}

// onMarked is the response to CE marks §3.7 leaves to the implementation,
// a loss's without the retransmission: the marks count toward the loss
// round, whose end cuts the short-term model by 0.7 if the flow was not
// probing (adaptLowerBoundsFromCongestion), and, while probing, toward the
// 2% that bounds inflight_longterm at the current flight — the volume the
// network marked at — and sends ProbeBW_UP DOWN. No recovery period: BBR
// cuts nothing on entering one, and the marked packets were delivered.
// The bound is set even application-limited: a mark is a count without
// packets (markEvent), so no P.is_app_limited to read, and the flight it
// names is the one the network marked now.
func (b *bbr3) onMarked(ev markEvent) {
	b.noteCongestion()
	b.markedInRound += ev.packets * b.maxDatagram
	if !b.bwProbeSamples || !b.roundTooHigh(b.inFlight) {
		return
	}
	b.handleInflightTooHigh(ev.now, b.inFlight, false)
}

// inflightAtLoss is InflightAtLoss (§5.5.10.2): the prefix of the packet
// at which the losses crossed the threshold, solved from lost / inflight =
// LossThresh — for a datagram, within a few bytes of the flight before it.
func inflightAtLoss(p lostPacket) int {
	inflightPrev := p.txInFlight - p.size
	lostPrev := p.lost - p.size
	lostPrefix := (bbr3LossThresh*float64(inflightPrev) - float64(lostPrev)) / (1 - bbr3LossThresh)
	return inflightPrev + int(lostPrefix)
}

// handleInflightTooHigh is BBRHandleInflightTooHigh: once per probe, the
// bound drops to the volume the losses name, never under 0.7 of the
// target (CUBIC's decrease), a probe in UP goes DOWN, and the next probe
// takes precaution (§5.3.3.6). The bound stays where it is when the
// losses were sent application-limited (l.3879): a flight the application
// did not fill measures nothing of the path. UP still goes DOWN — the
// "regardless of the application-limited state" of test case A.6
// (l.5226-5228), which reads as the transition, the pseudocode keeping
// the guard on the bound. The draft also records undo_state there, left
// out with the undo (bbr3).
func (b *bbr3) handleInflightTooHigh(now time.Time, txInFlight int, appLimited bool) {
	b.prevProbeTooHigh = true
	b.bwProbeSamples = false // only react once per bw probe
	if !appLimited {
		b.inflightLongterm = max(txInFlight, int(bbr3Beta*float64(b.targetInflight())))
	}
	if b.state == bbr3ProbeBWUp {
		b.startProbeBWDown(now)
	}
}

// onPersistentCongestion is BBROnEnterRTO (§5.6.4.4): the window drops to
// what lets one datagram out and climbs back through setCwnd until the
// period ends and the saved window is restored. The loss that precedes
// this call in the same batch opened the period.
func (b *bbr3) onPersistentCongestion() {
	b.priorCwnd = b.saveCwnd()
	b.cwnd = b.inFlight + b.maxDatagram
}

// windowedMax is the draft's windowed max filter (§5.5.3, §5.5.9) over a
// virtual clock: one slot per tick, the best of the last window ticks.
// Ticks that passed without a sample are cleared at the next insertion.
type windowedMax[T int | float64] struct {
	ring [bbr3ExtraAckedFilterLen]T
	tick uint64
}

func (f *windowedMax[T]) update(tick uint64, v T, window uint64) T {
	if tick > f.tick {
		for t := max(f.tick+1, tick-min(tick, bbr3ExtraAckedFilterLen-1)); t <= tick; t++ {
			f.ring[t%bbr3ExtraAckedFilterLen] = 0
		}
		f.tick = tick
	}
	slot := &f.ring[tick%bbr3ExtraAckedFilterLen]
	*slot = max(*slot, v)
	best := *slot
	for i := uint64(1); i < window && i <= tick; i++ {
		best = max(best, f.ring[(tick-i)%bbr3ExtraAckedFilterLen])
	}
	return best
}
