package quic

import (
	"math"
	"math/rand/v2"
	"time"
)

// bbr is BBR version 1 — draft-cardwell-iccrg-bbr-congestion-control-00,
// the algorithm as Linux shipped it in 2016 — chosen with CongestionBBRv1.
// Where Reno and CUBIC read loss and grow a window, BBR keeps a model of
// the path — the bottleneck bandwidth BtlBw, the highest delivery rate
// seen over the last ten round trips (§4.1.1), and the propagation delay
// RTprop, the lowest round trip seen over the last ten seconds (§4.1.2) —
// and sends at that bandwidth with about one bandwidth-delay product in
// flight. Loss is not a signal to it (§4.2.3.4 only makes it conservative
// for one round), which is what lets it hold a lossy path at the rate the
// path has rather than the rate a loss-based controller infers.
//
// The model is fed by the delivery estimator's samples (rateSample, one
// per acknowledged packet) and the state machine of §4.3 decides what to
// do with it: Startup doubles the rate each round until three rounds stop
// raising BtlBw, Drain empties the queue Startup built, ProbeBW cycles the
// pacing gain through eight phases to keep probing for more bandwidth and
// give back what it took, ProbeRTT drops the flight to four datagrams for
// 200 ms every ten seconds so that RTprop can be measured again. Pacing
// is the primary control (§4.2.1: the pacer already exists, this
// controller only names its rate) and the window the bound (§4.2.3).
//
// It shares nothing with newReno: no slow start, no §7.8 utilisation
// round, no B.8 epoch — the flight is kept here. The send quantum (§4.2.2)
// is one datagram, the unit the pacer releases, and the draft's
// application-limited marking of ProbeRTT (§4.3.5) is kept inside the
// controller rather than pushed to the estimator the connection owns.
type bbr struct {
	maxDatagram int
	initialCwnd int
	inFlight    int
	cwnd        int
	// limited says canSend refused a send this round (windowLimited): the
	// delivery estimator reads it the other way, as Reno's. Cleared when a
	// round starts.
	limited bool

	// The path model (§4.1). bwRing holds the best sample of each of the
	// last bbrBtlBwFilterLen rounds, indexed by round; btlBw is their
	// maximum, recomputed when a sample is inserted — a round that inserts
	// nothing expires nothing, the draft's filter being updated only on
	// insertion (§4.1.1.5). rtprop is zero until the first sample
	// (the draft's Inf), and rtpropStamp is when it was last lowered.
	btlBw         float64
	bwRing        [bbrBtlBwFilterLen]float64
	bwRound       uint64 // the round the last sample was inserted in
	rtprop        time.Duration
	rtpropStamp   time.Time
	rtpropExpired bool

	// Round counting (§4.1.1.3): delivered is the controller's own count
	// of acknowledged bytes — the estimator's count, which ackEvent carries
	// as the distance to the packet's own snapshot — and a round ends when a
	// packet sent after the round's first delivery is acknowledged.
	delivered          int
	nextRoundDelivered int
	roundStart         bool
	roundCount         uint64

	// The state machine (§4.3) and its control parameters (§4.2): rate is
	// the pacing rate in bytes per second, zero until the first sample.
	state      bbrState
	pacingGain float64
	cwndGain   float64
	rate       float64
	// Startup's full-pipe estimator (§4.3.2.2). fullBwPending holds the
	// round's check until a sample that measures the path arrives: the
	// packet that opens a round may carry an application-limited one, and
	// the draft's once-per-round check reads the frame's.
	filledPipe    bool
	fullBw        float64
	fullBwCount   int
	fullBwPending bool
	// ProbeBW's gain cycle (§4.3.4.3).
	cycleIndex int
	cycleStamp time.Time
	// ProbeRTT (§4.3.5): when the 200 ms are over, whether a round has
	// passed since, and the delivered count the samples sent during it are
	// discounted up to (the draft's C.app_limited, kept here).
	probeRTTDoneStamp time.Time
	probeRTTRoundDone bool
	appLimitedUntil   int
	idleRestart       bool

	// Loss recovery (§4.2.3.4): one period from a loss to the
	// acknowledgement of a packet sent after it (RFC 9002 §7.3.2), with
	// packet conservation for that round and the window restored on exit.
	inRecovery         bool
	recoveryStart      time.Time
	packetConservation bool
	priorCwnd          int
}

type bbrState uint8

const (
	bbrStartup bbrState = iota
	bbrDrain
	bbrProbeBW
	bbrProbeRTT
)

const (
	// bbrHighGain is Startup's gain, 2/ln 2: the smallest that doubles the
	// sending rate each round (§4.3.2.1).
	bbrHighGain = 2 / math.Ln2
	// bbrBtlBwFilterLen is the max filter's window in rounds (§4.1.1.2).
	bbrBtlBwFilterLen = 10
	// bbrRTpropFilterLen is the min filter's window, and the interval
	// between ProbeRTT states, which the draft requires to be the same ten
	// seconds (§4.3.5).
	bbrRTpropFilterLen = 10 * time.Second
	// bbrProbeRTTDuration is how long ProbeRTT holds the flight down.
	bbrProbeRTTDuration = 200 * time.Millisecond
	// bbrMinPipeCwnd is the window's floor, in datagrams (§4.2.3.3).
	bbrMinPipeCwnd = 4
	// bbrGainCycleLen is ProbeBW's number of phases (§4.3.4.1).
	bbrGainCycleLen = 8
	// bbrFullBwThresh is the growth a Startup round must show in BtlBw to
	// count as still filling the pipe (§4.3.2.2).
	bbrFullBwThresh = 1.25
	// bbrFullBwRounds is how many rounds without that growth end Startup.
	bbrFullBwRounds = 3
)

// bbrGainCycle is ProbeBW's pacing gain per phase (§4.3.4.1): probe,
// drain, then six rounds of cruising.
var bbrGainCycle = [bbrGainCycleLen]float64{1.25, 0.75, 1, 1, 1, 1, 1, 1}

var _ congestionController = (*bbr)(nil)

func newBBR(maxDatagram int) *bbr {
	b := &bbr{maxDatagram: maxDatagram}
	// §4.2.3.1: the transport's initial window, RFC 9002 §7.2's.
	b.initialCwnd = newNewReno(maxDatagram).cwnd
	b.cwnd = b.initialCwnd
	b.enterStartup()
	return b
}

func (b *bbr) window() int              { return b.cwnd }
func (b *bbr) bytesInFlight() int       { return b.inFlight }
func (b *bbr) setMaxDatagram(bytes int) { b.maxDatagram = bytes }
func (b *bbr) windowLimited() bool      { return b.limited }
func (b *bbr) minPipeCwnd() int         { return bbrMinPipeCwnd * b.maxDatagram }

func (b *bbr) canSend(bytes int) bool {
	if b.inFlight+bytes > b.cwnd {
		b.limited = true
		return false
	}
	return true
}

// onSent adds to the flight and notices a restart from idle (§4.3.4.4):
// a send into an empty flight keeps the window and, in ProbeBW, paces at
// exactly BtlBw until the next acknowledgement re-applies the gain. The
// draft also asks that the sender be application-limited; an empty flight
// is read as that here — the other way to empty it, losing everything, is
// persistent congestion, which has its own entry. The first send also
// stamps RTprop, the draft's "connection initialization" (§4.3.1).
func (b *bbr) onSent(now time.Time, bytes int) {
	if b.inFlight == 0 {
		b.idleRestart = true
		if b.state == bbrProbeBW {
			b.rate = b.btlBw
		}
	}
	if b.rtpropStamp.IsZero() {
		b.rtpropStamp = now
	}
	b.inFlight += bytes
	if b.state == bbrProbeRTT {
		// Sent at four datagrams a round trip: its sample measures ProbeRTT,
		// not the path (§4.3.5's marking, applied at the send as the
		// estimator would).
		b.appLimitedUntil = max(b.delivered+b.inFlight, 1)
	}
}

func (b *bbr) onLifted(bytes int) { b.inFlight = max(b.inFlight-bytes, 0) }

// onMarked is ignored: version 1 reads bandwidth and round trip only.
func (b *bbr) onMarked(markEvent) {}

// pacingRate is the rate the model names, pacing_gain × BtlBw (§4.2.1),
// kept in rate by setPacingRate. Before the first sample it is the
// draft's initial rate: the initial window over the round trip, at
// Startup's gain.
func (b *bbr) pacingRate(srtt time.Duration) float64 {
	if b.rate > 0 {
		return b.rate
	}
	return bbrHighGain * float64(b.initialCwnd) / max(srtt, granularity).Seconds()
}

// inflight is BBRInflight (§4.2.3.2): gain times the estimated
// bandwidth-delay product, plus three send quanta, or the initial window
// while there is no round-trip sample to size the product with.
func (b *bbr) inflight(gain float64) int {
	if b.rtprop == 0 {
		return b.initialCwnd
	}
	return int(gain*b.btlBw*b.rtprop.Seconds()) + 3*b.maxDatagram
}

// onAcked is the draft's per-ACK step (§3.5.2), run per packet: the
// packet leaves the flight, its sample feeds the model, the state machine
// reads the model, and the control parameters follow — in the draft's
// order. The max filter makes the per-packet feeding equivalent to the
// per-frame one: the frame's sample is the last packet's, and it is the
// highest of the frame's.
func (b *bbr) onAcked(ev ackEvent) {
	priorInFlight := b.inFlight
	b.inFlight = max(b.inFlight-ev.size, 0)
	b.delivered += ev.size
	rs := ev.rateSample()
	if b.appLimitedUntil != 0 {
		if b.delivered > b.appLimitedUntil {
			b.appLimitedUntil = 0
		} else {
			rs.appLimited = true
		}
	}
	if b.inRecovery && !ev.sentAt.Before(b.recoveryStart) {
		// A packet sent in the recovery period is acknowledged: the period
		// ends (RFC 9002 §7.3.2) and the window the loss interrupted comes
		// back (§4.2.3.4, BBRRestoreCwnd).
		b.inRecovery, b.packetConservation = false, false
		b.cwnd = max(b.cwnd, b.priorCwnd)
	}
	rtt := ev.now.Sub(ev.sentAt)
	if b.rate == 0 {
		// §4.2.1, BBRInitPacingRate: the initial window over the first
		// round trip measured, at Startup's gain.
		b.rate = bbrHighGain * float64(b.initialCwnd) / max(rtt, granularity).Seconds()
	}

	b.updateRound(ev)
	b.updateBtlBw(rs)
	b.checkCyclePhase(ev.now, priorInFlight, false)
	b.checkFullPipe(rs)
	b.checkDrain(ev.now)
	b.updateRTprop(ev.now, rtt)
	b.checkProbeRTT(ev.now)

	b.setPacingRate()
	b.setCwnd(ev.size)
}

// updateRound is BBRUpdateRound (§4.1.1.3): the packet's own delivered
// count at its send — the controller's count now less what the estimator
// says was delivered since — reaching the mark means a packet sent after
// the round's first delivery came back, so a round trip has passed. A
// round's start is also where the utilisation flag of §7.8 is cleared.
func (b *bbr) updateRound(ev ackEvent) {
	if b.delivered-ev.delivered >= b.nextRoundDelivered {
		b.nextRoundDelivered = b.delivered
		b.roundCount++
		b.roundStart = true
		b.limited = false
		return
	}
	b.roundStart = false
}

// updateBtlBw is BBRUpdateBtlBw (§4.1.1.5): an application-limited sample
// is kept only when it beats the estimate (§4.1.1.4). Inserting expires
// the rounds the window has passed since the last insertion.
func (b *bbr) updateBtlBw(rs rateSample) {
	bw := rs.deliveryRate()
	if bw < b.btlBw && rs.appLimited {
		return
	}
	if gap := b.roundCount - b.bwRound; gap > 0 {
		for r := b.bwRound + 1; r <= b.roundCount && r-b.bwRound <= bbrBtlBwFilterLen; r++ {
			b.bwRing[r%bbrBtlBwFilterLen] = 0
		}
		b.bwRound = b.roundCount
		b.btlBw = 0
		for _, v := range b.bwRing {
			b.btlBw = max(b.btlBw, v)
		}
	}
	slot := &b.bwRing[b.roundCount%bbrBtlBwFilterLen]
	*slot = max(*slot, bw)
	b.btlBw = max(b.btlBw, bw)
}

// updateRTprop is BBRUpdateRTprop (§4.1.2.3): the sample is the packet's
// own round trip — the lowest in a frame, since every packet is offered —
// and it replaces the estimate when lower, or when the estimate is ten
// seconds old, which is also what sends the state machine to ProbeRTT.
func (b *bbr) updateRTprop(now time.Time, rtt time.Duration) {
	b.rtpropExpired = !b.rtpropStamp.IsZero() && now.After(b.rtpropStamp.Add(bbrRTpropFilterLen))
	if rtt >= 0 && (b.rtprop == 0 || rtt <= b.rtprop || b.rtpropExpired) {
		b.rtprop = rtt
		b.rtpropStamp = now
	}
}

// checkFullPipe is BBRCheckFullPipe (§4.3.2.2): once per round, on a
// sample that measures the path and not the application, Startup is over
// when three rounds in a row have raised BtlBw by less than a quarter.
func (b *bbr) checkFullPipe(rs rateSample) {
	if b.roundStart {
		b.fullBwPending = true
	}
	if b.filledPipe || !b.fullBwPending || rs.appLimited {
		return
	}
	b.fullBwPending = false
	if b.btlBw >= b.fullBw*bbrFullBwThresh {
		b.fullBw = b.btlBw
		b.fullBwCount = 0
		return
	}
	b.fullBwCount++
	if b.fullBwCount >= bbrFullBwRounds {
		b.filledPipe = true
	}
}

// checkDrain is BBRCheckDrain (§4.3.3): a full pipe sends Startup to
// Drain, and Drain ends when the flight is down to one bandwidth-delay
// product — the queue Startup built is gone.
func (b *bbr) checkDrain(now time.Time) {
	if b.state == bbrStartup && b.filledPipe {
		b.enterDrain()
	}
	if b.state == bbrDrain && b.inFlight <= b.inflight(1) {
		b.enterProbeBW(now)
	}
}

func (b *bbr) enterStartup() {
	b.state = bbrStartup
	b.pacingGain, b.cwndGain = bbrHighGain, bbrHighGain
}

func (b *bbr) enterDrain() {
	b.state = bbrDrain
	b.pacingGain, b.cwndGain = 1/bbrHighGain, bbrHighGain
}

// enterProbeBW is BBREnterProbeBW (§4.3.4.3): the cycle starts at a random
// phase other than the draining one — there is no queue to drain on entry
// (§4.3.4.2) — and the window gain is two, for stretched and aggregated
// acknowledgements (§4.3.4.1).
func (b *bbr) enterProbeBW(now time.Time) {
	b.state = bbrProbeBW
	b.pacingGain, b.cwndGain = 1, 2
	b.cycleIndex = bbrGainCycleLen - 1 - rand.IntN(bbrGainCycleLen-1) //nolint:gosec // G404: a phase to start cycling from, nothing a peer can predict to any effect
	b.advanceCyclePhase(now)
}

func (b *bbr) advanceCyclePhase(now time.Time) {
	b.cycleStamp = now
	b.cycleIndex = (b.cycleIndex + 1) % bbrGainCycleLen
	b.pacingGain = bbrGainCycle[b.cycleIndex]
}

// checkCyclePhase is BBRCheckCyclePhase (§4.3.4.3). A phase lasts one
// RTprop; the probing phase also waits for the flight to reach what its
// gain aims at, or for a loss (lost, reported by onLost: the losses of a
// frame are declared after its acknowledgements), and the draining phase
// ends as soon as the flight is down to one bandwidth-delay product.
// priorInFlight is the flight before the packet being processed left it.
func (b *bbr) checkCyclePhase(now time.Time, priorInFlight int, lost bool) {
	if b.state != bbrProbeBW {
		return
	}
	full := now.Sub(b.cycleStamp) > b.rtprop
	var next bool
	switch {
	case b.pacingGain == 1:
		next = full
	case b.pacingGain > 1:
		next = full && (lost || priorInFlight >= b.inflight(b.pacingGain))
	default:
		next = full || priorInFlight <= b.inflight(1)
	}
	if next {
		b.advanceCyclePhase(now)
	}
}

// checkProbeRTT is BBRCheckProbeRTT (§4.3.5): an RTprop ten seconds old
// sends every other state to ProbeRTT — not on the first acknowledgement
// after an idle period, whose age is the application's — and ProbeRTT
// ends after 200 ms and one round with the flight at four datagrams or
// fewer, to ProbeBW if the pipe was ever filled and to Startup otherwise.
// The samples of what is sent meanwhile are discounted (the draft marks
// the estimator application-limited; appLimitedUntil does the same here):
// four datagrams per round trip is not the path's rate.
func (b *bbr) checkProbeRTT(now time.Time) {
	if b.state != bbrProbeRTT && b.rtpropExpired && !b.idleRestart {
		b.state = bbrProbeRTT
		b.pacingGain, b.cwndGain = 1, 1
		b.priorCwnd = b.saveCwnd()
		b.probeRTTDoneStamp = time.Time{}
	}
	if b.state == bbrProbeRTT {
		b.appLimitedUntil = max(b.delivered+b.inFlight, 1)
		switch {
		case b.probeRTTDoneStamp.IsZero() && b.inFlight <= b.minPipeCwnd():
			b.probeRTTDoneStamp = now.Add(bbrProbeRTTDuration)
			b.probeRTTRoundDone = false
			b.nextRoundDelivered = b.delivered
		case !b.probeRTTDoneStamp.IsZero():
			if b.roundStart {
				b.probeRTTRoundDone = true
			}
			if b.probeRTTRoundDone && now.After(b.probeRTTDoneStamp) {
				b.rtpropStamp = now
				b.cwnd = max(b.cwnd, b.priorCwnd)
				if b.filledPipe {
					b.enterProbeBW(now)
				} else {
					b.enterStartup()
				}
			}
		}
	}
	b.idleRestart = false
}

// saveCwnd is BBRSaveCwnd (§4.2.3.4): the last window not modulated by
// recovery or ProbeRTT, which is the current one outside both.
func (b *bbr) saveCwnd() int {
	if !b.inRecovery && b.state != bbrProbeRTT {
		return b.cwnd
	}
	return max(b.priorCwnd, b.cwnd)
}

// setPacingRate is BBRSetPacingRate (§4.2.1): the rate follows the model
// once the pipe is known to be full, and before that only upward — a
// Startup fed application-limited samples must not slow down on them.
func (b *bbr) setPacingRate() {
	if rate := b.pacingGain * b.btlBw; b.filledPipe || rate > b.rate {
		b.rate = rate
	}
}

// setCwnd is BBRSetCwnd (§4.2.3.6) with the recovery and ProbeRTT
// modulations in the draft's order: packet conservation for the first
// round of recovery, otherwise growth by what was acknowledged — bounded
// by the target once the pipe is full, free while the window is below it
// or too little has been delivered to trust the model — above the floor of
// four datagrams, and under ProbeRTT's cap.
func (b *bbr) setCwnd(acked int) {
	target := b.inflight(b.cwndGain)
	if b.packetConservation {
		b.cwnd = max(b.cwnd, b.inFlight+acked)
	} else {
		if b.filledPipe {
			b.cwnd = min(b.cwnd+acked, target)
		} else if b.cwnd < target || b.delivered < b.initialCwnd {
			b.cwnd += acked
		}
		b.cwnd = max(b.cwnd, b.minPipeCwnd())
	}
	if b.state == bbrProbeRTT {
		b.cwnd = min(b.cwnd, b.minPipeCwnd())
	}
}

// onLost is the draft's loss modulation (§4.2.3.4). A batch whose newest
// packet was sent after the current recovery started opens a new period:
// the window is saved and cut to the flight plus one datagram, and packet
// conservation holds for the round. A batch within the period takes its
// bytes off the window, never below one datagram. The model is not
// touched: loss is not evidence about the path's bandwidth. ProbeBW's
// probing phase ends on a loss once it has lasted its round trip.
func (b *bbr) onLost(ev lossEvent) {
	b.inFlight = max(b.inFlight-ev.bytes, 0)
	if ev.latestSent.Before(b.recoveryStart) {
		if b.inRecovery {
			b.cwnd = max(b.cwnd-ev.bytes, b.maxDatagram)
		}
	} else {
		b.priorCwnd = b.saveCwnd()
		b.inRecovery, b.packetConservation = true, true
		b.recoveryStart = ev.now
		b.cwnd = b.inFlight + b.maxDatagram
	}
	b.checkCyclePhase(ev.now, b.inFlight, true)
}

// onPersistentCongestion is the draft's retransmission timeout (§4.2.3.4):
// the window drops to one datagram — the floor of four returns at the next
// acknowledgement — and climbs back through the core mechanism until the
// period ends and the saved window is restored. The loss that precedes
// this call in the same batch saved it and opened the period.
func (b *bbr) onPersistentCongestion() {
	b.priorCwnd = b.saveCwnd()
	b.cwnd = b.maxDatagram
	b.packetConservation = false
}
