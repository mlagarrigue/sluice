package quic

import (
	"math"
	"net"
	"testing"
	"time"
)

// BBR's accessors at their edges: the pipe floor is four datagrams, a send
// that fills the window exactly goes, a lifted flight leaves the flight,
// the rate before a sample is the initial window at Startup's gain, and UP
// adds two datagrams to the budget while the other states do not.
func TestBBRAccessorsAtTheirEdges(t *testing.T) {
	b := newBBR3(1200, nil)
	if got := b.minPipeCwnd(); got != 4800 {
		t.Errorf("minPipeCwnd = %d, want 4800", got)
	}
	if !b.canSend(b.cwnd) || b.canSend(b.cwnd+1) {
		t.Errorf("canSend at the window %v, one past %v", b.canSend(b.cwnd), b.canSend(b.cwnd+1))
	}
	b.inFlight = 1000
	if b.canSend(b.cwnd - 999) {
		t.Error("a send past the window with a flight went")
	}
	b.onLifted(300)
	if b.inFlight != 700 {
		t.Errorf("inFlight after lifting 300 of 1000 = %d", b.inFlight)
	}
	initial := bbr3StartupPacingGain * float64(b.initialCwnd) / 0.1
	if got := b.pacingRate(100 * time.Millisecond); got != initial {
		t.Errorf("pacingRate before a sample = %v, want %v", got, initial)
	}
	b.rate = 5
	if got := b.pacingRate(100 * time.Millisecond); got != 5 {
		t.Errorf("pacingRate with a rate = %v, want 5", got)
	}
	b.state = bbr3ProbeBWUp
	if got := b.quantizationBudget(0); got != 4800+2400 {
		t.Errorf("UP's budget = %d, want 7200", got)
	}
	b.state = bbr3ProbeBWDown
	if got := b.quantizationBudget(0); got != 4800 {
		t.Errorf("DOWN's budget = %d, want 4800", got)
	}
}

// The first acknowledgement sets the initial pacing rate from its own
// round trip.
func TestBBRFirstAckSetsTheInitialRate(t *testing.T) {
	b := newBBR3(1200, nil)
	now := time.Now()
	b.onSent(now.Add(-100*time.Millisecond), 1200)
	b.onAcked(ackEvent{now: now, sentAt: now.Add(-100 * time.Millisecond), size: 1200})
	if want := bbr3StartupPacingGain * float64(b.initialCwnd) / 0.1; b.rate != want {
		t.Errorf("rate after the first ACK = %v, want %v", b.rate, want)
	}
}

// The model's signals at their edges.
func TestBBRSignalsAtTheirEdges(t *testing.T) {
	b := newBBR3(1200, nil)
	b.lossRoundDelivered, b.delivered = 1000, 1500
	b.updateLatestDeliverySignals(rateSample{}, 1000)
	if !b.lossRoundStart || b.roundDelivered != 500 {
		t.Errorf("a packet sent at the loss round's mark: start %v, delivered %d", b.lossRoundStart, b.roundDelivered)
	}

	b = newBBR3(1200, nil)
	b.updateRound(b.nextRoundDelivered)
	if b.roundsSinceBwProbe != 1 {
		t.Errorf("roundsSinceBwProbe after a round = %d", b.roundsSinceBwProbe)
	}

	// An application-limited sample equal to the estimate keeps it alive
	// into the current cycle; one above it raises it.
	b = newBBR3(1200, nil)
	b.nextRoundDelivered = 1 << 30
	b.maxBw = b.maxBwFilter.update(0, 100, bbr3MaxBwFilterLen)
	b.cycleCount = 1
	b.updateMaxBw(rateSample{delivered: 100, interval: time.Second, appLimited: true}, 0)
	if got := b.maxBwFilter.update(2, 0, bbr3MaxBwFilterLen); got != 100 {
		t.Errorf("an equal application-limited sample did not renew the estimate: %v", got)
	}
	b.updateMaxBw(rateSample{delivered: 200, interval: time.Second, appLimited: true}, 0)
	if b.maxBw != 200 {
		t.Errorf("a higher application-limited sample: maxBw %v, want 200", b.maxBw)
	}

	b = newBBR3(1200, nil)
	b.state, b.lossInRound = bbr3ProbeBWDown, true
	b.bwShortterm, b.bwLatest = 1000, 0
	b.adaptLowerBoundsFromCongestion()
	if b.bwShortterm != 700 {
		t.Errorf("bwShortterm after a lossy round = %v, want 700", b.bwShortterm)
	}

	if lossTooHigh(2, 100) || !lossTooHigh(3, 100) {
		t.Error("lossTooHigh at 2% exactly")
	}
	b = newBBR3(1200, nil)
	b.lossInRound, b.lostInRound = true, 2
	if b.roundTooHigh(100) {
		t.Error("roundTooHigh at 2% exactly")
	}
}

// The aggregation interval restarts when the acknowledgements fall to the
// prediction exactly, and the extra is what exceeds the prediction.
func TestBBRAggregationAtItsEdges(t *testing.T) {
	now := time.Now()
	b := newBBR3(1200, nil)
	b.bw, b.extraAckedIntervalStart, b.extraAckedDelivered = 1000, now.Add(-time.Second), 1000
	b.updateACKAggregation(now, 100)
	if !b.extraAckedIntervalStart.Equal(now) {
		t.Error("the interval did not restart at the prediction exactly")
	}

	b = newBBR3(1200, nil)
	b.bw, b.extraAckedIntervalStart, b.extraAckedDelivered = 1000, now.Add(-time.Second), 1500
	b.updateACKAggregation(now, 500)
	if b.extraAcked != 1000 {
		t.Errorf("extraAcked = %d, want 1500 + 500 - 1000", b.extraAcked)
	}
}

// A round whose rate is exactly a quarter above the plateau's starts a new
// plateau.
func TestBBRFullBandwidthAtTheThreshold(t *testing.T) {
	b := newBBR3(1200, nil)
	b.fullBw, b.roundStart = 800, true
	b.checkFullBwReached(rateSample{delivered: 1000, interval: time.Second}, true)
	if b.fullBw != 1000 || b.fullBwCount != 0 {
		t.Errorf("a rate 1.25 times the plateau: fullBw %v, count %d", b.fullBw, b.fullBwCount)
	}
}

// The probe wait is drawn in [2 s, 3 s), not all at one value.
func TestBBRProbeWaitSpread(t *testing.T) {
	b := newBBR3(1200, nil)
	spread := false
	for range 64 {
		b.pickProbeWait()
		if b.bwProbeWait < 2*time.Second || b.bwProbeWait >= 3*time.Second {
			t.Fatalf("bwProbeWait = %v", b.bwProbeWait)
		}
		if b.bwProbeWait > 2*time.Second+time.Millisecond {
			spread = true
		}
	}
	if !spread {
		t.Error("every wait within a millisecond of 2 s")
	}
}

// ProbeBW's decisions at their edges.
func TestBBRProbeBWDecisionsAtTheirEdges(t *testing.T) {
	b := newBBR3(1200, nil)
	b.inflightLongterm = 10000 // headroom 1500: 8500
	b.inFlight = 8500
	if !b.isTimeToCruise() {
		t.Error("a flight at the headroom bound is not time to cruise")
	}
	b = newBBR3(1200, nil)
	b.inFlight = b.inflight(b.maxBw, 1)
	if !b.isTimeToCruise() {
		t.Error("a flight at one product is not time to cruise")
	}

	now := time.Now()
	b = newBBR3(1200, nil)
	b.bwProbeWait, b.cycleStamp = 2*time.Second, now.Add(-2*time.Second)
	if b.isTimeToProbeBW(now) {
		t.Error("probing at exactly the wait")
	}
	b = newBBR3(1200, nil)
	b.cycleStamp, b.bwProbeWait = now, time.Hour
	b.roundsSinceBwProbe = b.targetInflight() / b.maxDatagram
	if !b.renoCoexistenceProbeTime() {
		t.Errorf("no Reno probe after %d rounds", b.roundsSinceBwProbe)
	}

	b = newBBR3(1200, nil)
	b.limited, b.inflightLongterm, b.fullBwNow = true, 10000, true
	if b.isTimeToGoDown(0) {
		t.Error("a window capped by the long-term bound went down")
	}
}

// The long-term bound rises with a safe sample, a round's losses counted
// only when the round carried marks.
func TestBBRLongTermBoundRises(t *testing.T) {
	b := newBBR3(1200, nil)
	b.inflightLongterm = 10000
	b.lossInRound, b.lostInRound = true, 1000
	b.adaptLongTermModel(rateSample{}, ackEvent{txInFlight: 20000})
	if b.inflightLongterm != 20000 {
		t.Errorf("inflightLongterm = %d, want 20000", b.inflightLongterm)
	}

	b = newBBR3(1200, nil)
	b.state, b.limited = bbr3ProbeBWUp, true
	b.inflightLongterm, b.probeUpCnt = b.cwnd, 1200
	b.probeInflightLongtermUpward(1200)
	if b.inflightLongterm != b.cwnd+1200 {
		t.Errorf("one probeUpCnt acknowledged: inflightLongterm %d, want %d", b.inflightLongterm, b.cwnd+1200)
	}
}

// The minimum round trip at its edges: a zero sample changes nothing, an
// equal one keeps its stamp, a lower one is taken, and min_rtt keeps its
// stamp for an equal probe_rtt_min_delay.
func TestBBRMinRTTAtItsEdges(t *testing.T) {
	const ms = time.Millisecond
	now := time.Now()
	old := now.Add(-time.Minute)
	b := newBBR3(1200, nil)
	b.probeRTTMinDelay, b.probeRTTMinStamp, b.minRTT, b.minRTTStamp = 50*ms, old, 50*ms, now
	b.updateMinRTT(now, 0)
	if b.probeRTTMinDelay != 50*ms {
		t.Errorf("a zero sample set probe_rtt_min_delay to %v", b.probeRTTMinDelay)
	}

	t0, t1 := now.Add(-2*time.Second), now.Add(-time.Second)
	b = newBBR3(1200, nil)
	b.probeRTTMinDelay, b.probeRTTMinStamp, b.minRTT, b.minRTTStamp = 50*ms, t1, 50*ms, t0
	b.updateMinRTT(now, 50*ms)
	if !b.probeRTTMinStamp.Equal(t1) || !b.minRTTStamp.Equal(t0) {
		t.Errorf("an equal sample moved the stamps: %v, %v", b.probeRTTMinStamp, b.minRTTStamp)
	}
	b.updateMinRTT(now, 40*ms)
	if b.probeRTTMinDelay != 40*ms || b.minRTT != 40*ms {
		t.Errorf("a lower sample: probe_rtt_min_delay %v, min_rtt %v", b.probeRTTMinDelay, b.minRTT)
	}
}

// The window and the rate at their edges.
func TestBBRWindowAndRateAtTheirEdges(t *testing.T) {
	b := newBBR3(1200, nil)
	b.cwnd, b.priorCwnd = 5000, 9000
	if got := b.saveCwnd(); got != 5000 {
		t.Errorf("saveCwnd outside recovery and ProbeRTT = %d, want 5000", got)
	}

	b = newBBR3(1200, nil)
	b.bw, b.fullBwReached = 1000, true
	b.setPacingRateWithGain(1)
	if b.rate != 990 {
		t.Errorf("rate at a full pipe = %v, want 990", b.rate)
	}
	b = newBBR3(1200, nil)
	b.bw, b.rate = 1000, 500
	b.setPacingRateWithGain(1)
	if b.rate != 990 {
		t.Errorf("rate raised before a full pipe = %v, want 990", b.rate)
	}

	// Startup's target is the initial window, 12000.
	for _, c := range []struct{ cwnd, delivered, want int }{
		{12000, 12000, 12000}, // at the target, enough delivered
		{13000, 12000, 13000}, // above it, exactly the initial window delivered
		{6000, 20000, 7200},
		{13000, 6000, 14200},
	} {
		b = newBBR3(1200, nil)
		b.cwnd, b.delivered = c.cwnd, c.delivered
		b.setCwnd(1200)
		if b.cwnd != c.want {
			t.Errorf("cwnd %d, %d delivered: %d after 1200 acknowledged, want %d", c.cwnd, c.delivered, b.cwnd, c.want)
		}
	}

	b = newBBR3(1200, nil)
	b.inflightLongterm = 10000
	b.boundCwndForModel()
	if b.cwnd != 12000 {
		t.Errorf("Startup took the headroom bound: cwnd %d", b.cwnd)
	}
}

// A loss batch with no crossing packet leaves the probe's bound alone.
func TestBBRLossWithoutCrossing(t *testing.T) {
	b := newBBR3(1200, nil)
	b.bwProbeSamples = true
	now := time.Now()
	b.onLost(lossEvent{now: now, latestSent: now, bytes: 1200})
	if b.prevProbeTooHigh || b.inflightLongterm != bbr3Unbounded {
		t.Errorf("no crossing: prevProbeTooHigh %v, inflightLongterm %d", b.prevProbeTooHigh, b.inflightLongterm)
	}
}

// The flight at loss solves lost / inflight = 2% from the packet before.
func TestInflightAtLoss(t *testing.T) {
	p := lostPacket{size: 1200, txInFlight: 101200, lost: 2200}
	lostPrefix := (0.02*100000 - 1000.0) / 0.98
	want := 100000 + int(lostPrefix)
	if got := inflightAtLoss(p); got != want {
		t.Errorf("inflightAtLoss = %d, want %d", got, want)
	}
}

// cubicStage is a CUBIC controller in congestion avoidance whose
// acknowledgements earn growth, with the stage set by hand.
func cubicStage(cwnd int, wMax, t float64, now time.Time) *cubic {
	c := newCubic(1200)
	c.cwnd, c.ssthresh, c.limited = cwnd, cwnd, true
	c.inEpoch, c.cwndEpoch, c.wMax = true, float64(cwnd), wMax
	c.k = math.Cbrt((wMax - float64(cwnd)) / (cubicC * 1200))
	c.t, c.lastAck = time.Duration(t*float64(time.Second)), now
	return c
}

// CUBIC's avoidance at its edges.
func TestCubicAvoidanceAtItsEdges(t *testing.T) {
	now := time.Now()
	ev := func(size int) ackEvent {
		return ackEvent{now: now, sentAt: now.Add(-50 * time.Millisecond), size: size, srtt: 100 * time.Millisecond}
	}

	// The Reno estimate grows at α = 1 once it reached the prior window.
	c := cubicStage(12000, 12000, 0, now)
	c.wEst, c.cwndPrior = 12000, 12000
	c.onAcked(ev(1200))
	if c.wEst != 12120 {
		t.Errorf("wEst = %v, want 12120", c.wEst)
	}

	// Estimate and curve equal: the curve's region.
	c = cubicStage(12000, 12000, 0, now)
	c.wEst = 11880
	c.onAcked(ev(1200))
	if c.frac <= 0 {
		t.Errorf("estimate equal to the curve took the Reno region (frac %v)", c.frac)
	}

	// The target is the curve one round trip ahead.
	c = cubicStage(20000, 24000, 1, now)
	want := 20000 + int(math.Floor((min(max(c.wCubic(1.1), 20000), 30000)-20000)*1200/20000))
	c.onAcked(ev(1200))
	if c.cwnd != want {
		t.Errorf("cwnd = %d, want %d", c.cwnd, want)
	}

	// A fraction reaching one byte exactly is a byte.
	c = cubicStage(12000, 24000, 2.9, now)
	c.frac = 0.5
	c.onAcked(ev(1))
	if c.cwnd != 12001 {
		t.Errorf("cwnd = %d after the fraction reached 1, want 12001", c.cwnd)
	}
}

// CUBIC's loss at its edges: a window at wMax keeps wMax, and the window
// floors at two datagrams.
func TestCubicLossAtItsEdges(t *testing.T) {
	now := time.Now()
	c := newCubic(1200)
	c.wMax = float64(c.cwnd)
	c.onLost(lossEvent{now: now, latestSent: now})
	if c.wMax != 12000 {
		t.Errorf("wMax after a loss at wMax = %v, want 12000", c.wMax)
	}
	c = newCubic(1200)
	c.cwnd = 2400
	c.onLost(lossEvent{now: now, latestSent: now})
	if c.cwnd != 2400 {
		t.Errorf("cwnd after a loss at two datagrams = %d, want 2400", c.cwnd)
	}
}

// ackWithECN is an ACK of largest carrying the three counts.
func ackWithECN(t *testing.T, largest byte, ect0, ect1, ce byte) Frame {
	t.Helper()
	f, _, err := parseFrame([]byte{frameACK | 1, largest, 0, 0, 0, ect0, ect1, ce})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// ECN validation reads a frame only when it raises the largest
// acknowledged, and fails a peer without counts only for packets that
// left marked.
func TestECNValidationAtItsEdges(t *testing.T) {
	c := &Conn{}
	c.spaces[spaceApplication].ecnSent = 10
	c.validateECNLocked(spaceApplication, ackWithECN(t, 5, 3, 0, 0), ackOutcome{newlyAckedECT0: 3})
	if c.ecn.phase != ecnCapable {
		t.Fatalf("phase = %v after a valid frame", c.ecn.phase)
	}
	c.validateECNLocked(spaceApplication, ackWithECN(t, 5, 1, 0, 0), ackOutcome{})
	if c.ecn.phase != ecnCapable {
		t.Error("a frame that did not raise the largest failed validation")
	}

	c = &Conn{}
	f, _, err := parseFrame(ackFrame(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.validateECNLocked(spaceApplication, f, ackOutcome{})
	if c.ecn.phase == ecnFailed {
		t.Error("a frame without counts failed validation with nothing marked")
	}

	c = &Conn{}
	c.spaces[spaceApplication].ecnSent = 10
	c.validateECNLocked(spaceApplication, ackWithECN(t, 5, 0, 0, 0), ackOutcome{})
	if c.ecn.phase != ecnTesting {
		t.Errorf("valid counts with no marked packet acknowledged: phase %v, want still testing", c.ecn.phase)
	}
}

// A sourced socket takes the ECN and Don't Fragment options as the plain
// one does.
func TestSourcedSocketTakesItsOptions(t *testing.T) {
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	plainECN, plainDF := ecnSocket(uc), dfSocket(uc)
	sc := &sourcedConn{UDPConn: uc}
	if ecnSocket(sc) != plainECN || dfSocket(sc) != plainDF {
		t.Errorf("sourced: ECN %v DF %v; plain: ECN %v DF %v", ecnSocket(sc), dfSocket(sc), plainECN, plainDF)
	}
	if !plainECN || !plainDF {
		t.Logf("this platform takes ECN %v, DF %v", plainECN, plainDF)
	}
}

// The search's target and its start at their edges.
func TestPMTUSearchAtItsEdges(t *testing.T) {
	c := &Conn{maxDatagram: pmtuBaseSize, hasPeerParams: true}
	c.params.MaxDatagramSize = 1500
	c.peerParams.MaxUDPPayloadSize = 65527
	ceiling := c.pmtuCeilingLocked()
	c.pmtu.lost = 10000
	if got, want := c.pmtuTargetLocked(), (pmtuBaseSize+ceiling+1)/2; got != want {
		t.Errorf("target with a loss above the ceiling = %d, want %d", got, want)
	}
	c.pmtu.lost = pmtuBaseSize + pmtuPrecision
	if got := c.pmtuTargetLocked(); got != 0 {
		t.Errorf("target with the range at the precision = %d, want 0", got)
	}

	if got := pmtuEarnedAfter(); got != 2*initialWindow(pmtuBaseSize) {
		t.Errorf("pmtuEarnedAfter = %d", got)
	}
	for _, acked := range []int{pmtuEarnedAfter() - 1, pmtuEarnedAfter()} {
		c = &Conn{maxDatagram: pmtuBaseSize, handshakeConfirmed: true}
		c.pmtu.acked = acked
		c.maybeProbePMTULocked()
		if want := acked == pmtuEarnedAfter(); c.pmtu.done != want {
			t.Errorf("%d bytes acknowledged: search done %v, want %v", acked, c.pmtu.done, want)
		}
	}

	c = &Conn{maxDatagram: pmtuBaseSize}
	c.pmtu.acked = 5
	c.blackHoleLocked()
	if c.pmtuBlackHoles != 0 || c.pmtu.acked != 5 {
		t.Errorf("a black hole at the base size: %d counted, acked %d", c.pmtuBlackHoles, c.pmtu.acked)
	}
}
