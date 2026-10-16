package quic

import (
	"math"
	"testing"
	"time"
)

// newBBR3Path is bbrPath over the draft's controller: a 4.8 MB/s link over
// 20 ms, a bandwidth-delay product of 80 datagrams.
func newBBR3Path() (*bbrPath, *bbr3) {
	b := newBBR3(1200)
	return &bbrPath{cc: b, b: nil, now: time.Unix(1000, 0), rtt: 20 * time.Millisecond, link: 4.8e6}, b
}

// bbr3AtLink is a controller four doubling rounds in, the last at the
// link's rate, Startup still on.
func bbr3AtLink(t *testing.T) (*bbrPath, *bbr3) {
	t.Helper()
	p, b := newBBR3Path()
	for _, n := range []int{10, 20, 40, 80} {
		p.round(n)
		if b.state != bbr3Startup || b.pacingGain != bbr3StartupPacingGain || b.cwndGain != 2 {
			t.Fatalf("after a round of %d growing datagrams, state %d gains %v/%v, want Startup at 2.77 and 2", n, b.state, b.pacingGain, b.cwndGain)
		}
	}
	if math.Abs(b.maxBw-4.8e6) > 1 {
		t.Fatalf("max_bw = %v after a round at the link's rate, want 4.8e6", b.maxBw)
	}
	return p, b
}

// bbr3Draining is a controller that has just left Startup on the plateau,
// 199 datagrams in flight: Drain, entered on the first acknowledgement of
// a round.
func bbr3Draining(t *testing.T) (*bbrPath, *bbr3) {
	t.Helper()
	p, b := bbr3AtLink(t)
	for range 3 {
		p.round(200)
	}
	// The next round's first acknowledgement counts the third flat round.
	p.send(200)
	p.now = p.now.Add(p.rtt)
	p.ack(1)
	if !b.fullBwReached || b.state != bbr3Drain {
		t.Fatalf("after three flat rounds: full %v, state %d, want Drain", b.fullBwReached, b.state)
	}
	if b.pacingGain != 0.5 || b.cwndGain != 2 {
		t.Fatalf("Drain paces at gain %v with window gain %v, want 0.5 and 2", b.pacingGain, b.cwndGain)
	}
	return p, b
}

// bbr3Cruising is a controller past Startup (on the plateau) and Drain,
// in ProbeBW_CRUISE with no loss ever seen.
func bbr3Cruising(t *testing.T) (*bbrPath, *bbr3) {
	t.Helper()
	p, b := bbr3Draining(t)
	// The flight drains under one product; DOWN then CRUISE at once, the
	// flight being under the product already.
	p.ack(199)
	if b.state != bbr3ProbeBWCruise || b.pacingGain != 1 || b.cwndGain != 2 {
		t.Fatalf("after the flight drained: state %d, gains %v/%v, want CRUISE at 1 and 2", b.state, b.pacingGain, b.cwndGain)
	}
	return p, b
}

// Drain ends when the flight is down to one bandwidth-delay product
// (§5.3.2, test A.3): not one acknowledgement before, at once after.
func TestBBR3DrainExitsOnFlight(t *testing.T) {
	p, b := bbr3Draining(t)
	for b.state == bbr3Drain {
		if b.inFlight <= b.inflight(b.bw, 1) {
			t.Fatalf("still in Drain with %d bytes in flight, the product being %d", b.inFlight, b.inflight(b.bw, 1))
		}
		p.ack(1)
	}
	if b.inFlight > b.inflight(b.bw, 1) {
		t.Fatalf("left Drain with %d bytes in flight, above the product %d", b.inFlight, b.inflight(b.bw, 1))
	}
	if b.state != bbr3ProbeBWDown && b.state != bbr3ProbeBWCruise {
		t.Fatalf("after Drain: state %d, want ProbeBW", b.state)
	}
}

// Drain also ends three rounds after it starts (§5.3.2, test A.4), when
// Startup overestimated the path and the flight never gets down to the
// product: here the link loses 10% as Drain starts and every round keeps
// 200 datagrams in flight.
func TestBBR3DrainExitsAfterThreeRounds(t *testing.T) {
	p, b := bbr3Draining(t)
	start := b.roundCount
	p.link *= 0.9
	// Each step sends a round of 200, then acknowledges what was in flight
	// before it: the first of these acknowledgements opens the next round.
	step := func() {
		p.send(200)
		p.now = p.now.Add(p.rtt)
		p.ack(len(p.queue) - 200)
		if b.inFlight <= b.inflight(b.bw, 1) {
			t.Fatalf("flight %d down to the product %d: the time exit is not what is read", b.inFlight, b.inflight(b.bw, 1))
		}
	}
	step()
	for b.roundCount <= start+3 {
		if b.state != bbr3Drain {
			t.Fatalf("left Drain in round %d, %d rounds after it started, want more than 3", b.roundCount, b.roundCount-start)
		}
		step()
	}
	if b.roundCount != start+4 || b.state != bbr3ProbeBWDown {
		t.Fatalf("round %d after Drain started: state %d, want ProbeBW_DOWN in round 4", b.roundCount-start, b.state)
	}
}

// Startup ends on loss (§5.3.1.3): a round in recovery with six loss events
// and more than 2% lost sets inflight_longterm to the product or the
// round's volume, and Drain follows. Fewer events, or a lower rate, do not.
func TestBBR3StartupExitsOnLoss(t *testing.T) {
	p, b := bbr3AtLink(t)
	// Rounds at the link's rate, with losses of one datagram among the
	// acknowledgements, closed by a round of one datagram sent after the
	// losses. Every round here is flat for the plateau estimator, which is
	// kept out of the way: the loss exit is what is read.
	lossRound := func(n, events, between int) {
		b.fullBwCount = 0
		p.send(n)
		p.now = p.now.Add(p.rtt)
		for range events {
			p.ack(between)
			p.lose(1)
		}
		p.ack(len(p.queue))
		p.round(1)
	}
	// Five events at 2.5%: not enough events.
	lossRound(200, 5, 20)
	if b.state != bbr3Startup || b.fullBwReached {
		t.Fatalf("five loss events ended Startup (state %d)", b.state)
	}
	// Six events at 1.2%: too few bytes.
	lossRound(600, 6, 99)
	if b.state != bbr3Startup {
		t.Fatalf("six loss events at 1%% ended Startup (state %d)", b.state)
	}
	// Six events at 3.5%.
	// The acknowledgement that closes the round also drains: one datagram
	// in flight is under the product, so ProbeBW is entered at once.
	lossRound(200, 6, 32)
	if !b.fullBwReached || b.state == bbr3Startup {
		t.Fatalf("six loss events at 3%%: full %v, state %d, want Startup over", b.fullBwReached, b.state)
	}
	if b.inflightLongterm == bbr3Unbounded || b.inflightLongterm < b.bdpMultiple(b.bw, 1) {
		t.Fatalf("inflight_longterm = %d after the exit, want at least the product %d", b.inflightLongterm, b.bdpMultiple(b.bw, 1))
	}
}

// ProbeBW cycles DOWN, CRUISE, REFILL, UP, DOWN (§5.3.3): REFILL after the
// probe wait, UP after one round of REFILL at gain 1.25 and window gain
// 2.25, DOWN at gain 0.9 when the estimate plateaus, CRUISE once the
// flight is back under the product.
func TestBBR3ProbeBWPhases(t *testing.T) {
	p, b := bbr3Cruising(t)
	// Rounds under the probe wait stay in CRUISE; the Reno-coexistence
	// clock is the product in datagrams, 80 rounds here.
	for range 3 {
		p.round(80)
	}
	if b.state != bbr3ProbeBWCruise {
		t.Fatalf("state %d within the probe wait, want CRUISE", b.state)
	}
	p.now = p.now.Add(3 * time.Second)
	p.round(80)
	if b.state != bbr3ProbeBWRefill || b.pacingGain != 1 {
		t.Fatalf("after the probe wait: state %d gain %v, want REFILL at 1", b.state, b.pacingGain)
	}
	if !math.IsInf(b.bwShortterm, 1) || b.inflightShortterm != bbr3Unbounded {
		t.Fatal("REFILL did not reset the short-term model")
	}
	p.round(80)
	if b.state != bbr3ProbeBWUp || b.pacingGain != 1.25 || b.cwndGain != 2.25 {
		t.Fatalf("after a round of REFILL: state %d gains %v/%v, want UP at 1.25 and 2.25", b.state, b.pacingGain, b.cwndGain)
	}
	if !b.bwProbeSamples {
		t.Fatal("UP did not mark its samples as a probe's")
	}
	// Three rounds at the link, no growth: the third's first
	// acknowledgement finds the plateau and sends the probe DOWN.
	for i := range 2 {
		p.round(100)
		if b.state != bbr3ProbeBWUp {
			t.Fatalf("UP ended after flat round %d, want three", i+1)
		}
	}
	p.send(100)
	p.now = p.now.Add(p.rtt)
	p.ack(1)
	if b.state != bbr3ProbeBWDown || b.pacingGain != 0.9 {
		t.Fatalf("after three flat rounds of UP: state %d gain %v, want DOWN at 0.9", b.state, b.pacingGain)
	}
	// DOWN ends as the flight falls back under the product.
	p.ack(99)
	if b.state != bbr3ProbeBWCruise {
		t.Fatalf("after a round under the product: state %d, want CRUISE", b.state)
	}
	// The max filter's clock advanced one round after DOWN started: the
	// probe's samples are in the previous slot.
	if b.cycleCount != 1 {
		t.Fatalf("cycle count %d after one probe, want 1", b.cycleCount)
	}
}

// A loss while probing bounds the volume in flight (§5.5.10.2): the round's
// losses over 2% of the flight set inflight_longterm, the probe goes DOWN,
// CRUISE caps the window at 85% of the bound (§5.6.4.7), and the next UP
// raises the bound by a datagram a round, then two (§5.3.3.4).
func TestBBR3InflightLongterm(t *testing.T) {
	p, b := bbr3Cruising(t)
	p.now = p.now.Add(3 * time.Second)
	p.round(80) // REFILL
	p.round(80) // UP
	if b.state != bbr3ProbeBWUp || b.inflightLongterm != bbr3Unbounded {
		t.Fatalf("state %d bound %d before the loss", b.state, b.inflightLongterm)
	}
	p.send(100)
	p.now = p.now.Add(p.rtt)
	p.ack(50)
	p.lose(5) // 5 of 50 in flight: 10%, over 2%
	if b.state != bbr3ProbeBWDown {
		t.Fatalf("state %d after a 10%% loss in UP, want DOWN", b.state)
	}
	// The prefix of the flight at which the losses crossed 2% is 46
	// datagrams, under the floor of 0.7 × the product of 80: the floor
	// holds (§5.5.10.2).
	bound := b.inflightLongterm
	if want := int(bbr3Beta * 80 * 1200); bound != want {
		t.Fatalf("inflight_longterm = %d after the loss, want 0.7 × the product, %d", bound, want)
	}
	if b.bwProbeSamples {
		t.Fatal("the probe's samples still count after the reaction")
	}
	p.ack(45)
	// Down to CRUISE: the window is capped at the bound less 15%.
	for range 3 {
		p.round(40)
	}
	if b.state != bbr3ProbeBWCruise {
		t.Fatalf("state %d after the loss and three short rounds, want CRUISE", b.state)
	}
	if want := bound - int(0.15*float64(bound)); b.window() > want || b.inflightShortterm != bbr3Unbounded {
		t.Fatalf("CRUISE window %d, want at most the bound less headroom %d (short-term bound %d)", b.window(), want, b.inflightShortterm)
	}
	// The next probe: a window capped at the bound raises it only while
	// the window is what stops the sender — a datagram over the first
	// round, two over the second (§5.3.3.4). A round's start clears the
	// flag, so it is raised after the first acknowledgement of a round.
	p.now = p.now.Add(3 * time.Second)
	p.round(40) // REFILL
	p.round(40) // UP
	if b.state != bbr3ProbeBWUp || b.window() != bound {
		t.Fatalf("state %d window %d, want UP capped at the bound %d", b.state, b.window(), bound)
	}
	cappedRound := func(limited bool) {
		p.send(b.window() / 1200)
		p.now = p.now.Add(p.rtt)
		p.ack(1)
		b.limited = limited
		p.ack(len(p.queue))
	}
	cappedRound(false)
	if b.inflightLongterm != bound {
		t.Fatalf("a round that the window did not limit raised the bound to %d", b.inflightLongterm)
	}
	cappedRound(true)
	cappedRound(true)
	if got := (b.inflightLongterm - bound) / 1200; got < 2 || got > 4 {
		t.Fatalf("two limited rounds of UP raised the bound by %d datagrams, want two to four", got)
	}
}

// A loss between probes cuts the short-term model (§5.5.10.3): at the end
// of the round that lost, bw_shortterm and inflight_shortterm drop to 0.7
// of what they were, never under what the round delivered, and the window
// follows; REFILL forgets them.
func TestBBR3ShortTermModel(t *testing.T) {
	p, b := bbr3Cruising(t)
	p.send(80)
	p.now = p.now.Add(p.rtt)
	p.ack(40)
	p.lose(1)
	p.ack(39)
	if b.inflightShortterm != bbr3Unbounded {
		t.Fatal("the short-term bound moved before the loss round ended")
	}
	// The first acknowledgement of a packet sent after the loss ends the
	// round: the bounds start from the window and the estimate as they
	// stand and take 0.7 of them — the rate's floor, what the round
	// delivered, is the link's here, so the rate is not cut.
	cwnd := b.window()
	p.round(80)
	if want := int(bbr3Beta * float64(cwnd)); b.inflightShortterm != want {
		t.Fatalf("inflight_shortterm = %d after a loss round, want 0.7 × %d = %d", b.inflightShortterm, cwnd, want)
	}
	if b.bwShortterm < 0.98*b.maxBw || b.bwShortterm > b.maxBw || b.bw != b.bwShortterm {
		t.Fatalf("bw_shortterm = %v, max %v, bw %v: want the floor of what the round delivered, under the estimate by the round trip's share of a datagram", b.bwShortterm, b.maxBw, b.bw)
	}
	if b.window() > b.inflightShortterm {
		t.Fatalf("window %d above the short-term bound %d", b.window(), b.inflightShortterm)
	}
	p.now = p.now.Add(3 * time.Second)
	p.round(40)
	if b.state != bbr3ProbeBWRefill || b.bw != b.maxBw {
		t.Fatalf("after REFILL: state %d, bw %v, want max_bw %v back", b.state, b.bw, b.maxBw)
	}
}

// A probe_rtt_min_delay five seconds old sends CRUISE to ProbeRTT
// (§5.3.4.3): the window drops to half a product, and after 200 ms and a
// round there the window comes back and CRUISE resumes directly.
func TestBBR3ProbeRTT(t *testing.T) {
	p, b := bbr3Cruising(t)
	before := b.window()
	p.send(80)
	for range 2 {
		p.now = p.now.Add(p.rtt)
		p.send(80)
		p.ack(80)
	}
	p.now = p.now.Add(p.rtt)
	p.send(80)
	b.probeRTTMinStamp = p.now.Add(-bbr3ProbeRTTInterval - time.Second)
	p.ack(80)
	half := b.probeRTTCwnd()
	if b.state != bbr3ProbeRTT || b.cwndGain != 0.5 || b.window() != half || half != 40*1200 {
		t.Fatalf("after an expired probe_rtt_min_delay: state %d, cwnd gain %v, window %d, want ProbeRTT at half the product %d", b.state, b.cwndGain, b.window(), half)
	}
	if !b.probeRTTDoneStamp.IsZero() {
		t.Fatal("the 200 ms started with 80 datagrams still in flight")
	}
	p.now = p.now.Add(p.rtt)
	p.ack(80)
	entered := p.now
	if b.probeRTTDoneStamp != entered.Add(bbr3ProbeRTTDuration) {
		t.Fatalf("the 200 ms started at %v, want when the flight was down", b.probeRTTDoneStamp.Sub(entered))
	}
	for p.now.Before(entered.Add(bbr3ProbeRTTDuration)) {
		p.round(40)
		if b.state != bbr3ProbeRTT {
			t.Fatalf("left ProbeRTT after %v", p.now.Sub(entered))
		}
	}
	p.now = p.now.Add(time.Millisecond)
	p.round(40)
	if b.state != bbr3ProbeBWCruise {
		t.Fatalf("state %d after the 200 ms and a round, want CRUISE", b.state)
	}
	if b.window() < before {
		t.Fatalf("window %d after ProbeRTT, want at least the %d it had", b.window(), before)
	}
	// The exit is read at the round's send (§5.4.1, a restart from idle in
	// ProbeRTT) or at its acknowledgement: either way the stamp moved.
	if !b.probeRTTMinStamp.After(entered) {
		t.Fatal("leaving ProbeRTT did not restamp probe_rtt_min_delay")
	}
}

// The windowed max filter keeps the best of the last window ticks and
// forgets what fell out, ticks without a sample included.
func TestWindowedMax(t *testing.T) {
	var f windowedMax[int]
	if got := f.update(0, 5, 2); got != 5 {
		t.Fatalf("tick 0: %d", got)
	}
	if got := f.update(1, 3, 2); got != 5 {
		t.Fatalf("tick 1: %d, want 5 still in the window", got)
	}
	if got := f.update(2, 4, 2); got != 4 {
		t.Fatalf("tick 2: %d, want 5 gone", got)
	}
	if got := f.update(2, 1, 10); got != 5 {
		t.Fatalf("tick 2 over ten: %d, want 5 back", got)
	}
	if got := f.update(30, 1, 10); got != 1 {
		t.Fatalf("tick 30: %d, want everything expired", got)
	}
}

// A CE mark between probes is a loss without the retransmission (§3.7 leaves
// the response open; this one reads marks as congestion the way §5.5.10.3
// reads loss): the round that saw it ends on the acknowledgement of a packet
// sent after it, and the short-term bounds drop to 0.7 of the window and of
// max_bw, never under what the round delivered. The window follows.
func TestBBR3MarksCutTheShortTermBound(t *testing.T) {
	p, b := bbr3Cruising(t)
	p.round(80)
	if b.inflightShortterm != bbr3Unbounded || !math.IsInf(b.bwShortterm, 1) {
		t.Fatalf("short-term bounds %d / %v set before any signal", b.inflightShortterm, b.bwShortterm)
	}
	p.send(80)
	p.now = p.now.Add(p.rtt)
	p.ack(40)
	p.mark(2)
	if !b.lossInRound || b.markedInRound != 2*1200 {
		t.Fatalf("after two marks: loss round %v, marked %d bytes, want open and 2400", b.lossInRound, b.markedInRound)
	}
	// The rest of the flight was sent before the marks: the round stays open.
	p.ack(40)
	if b.inflightShortterm != bbr3Unbounded {
		t.Fatalf("the round closed on a packet sent before the marks: bound %d", b.inflightShortterm)
	}
	p.send(80)
	p.now = p.now.Add(p.rtt)
	cwnd, maxBw := b.cwnd, b.maxBw
	p.ack(1)
	if b.lossInRound {
		t.Fatal("the acknowledgement of a packet sent after the marks did not close the round")
	}
	wantInflight := max(b.inflightLatest, int(bbr3Beta*float64(cwnd)))
	if b.inflightShortterm != wantInflight || b.inflightShortterm >= cwnd {
		t.Fatalf("inflight_shortterm = %d after a marked round, want %d (under the window %d)", b.inflightShortterm, wantInflight, cwnd)
	}
	// The rate bound is set too, at the delivered rate here: the round ran
	// at the link's rate, which is what "never under what it delivered"
	// protects.
	if math.IsInf(b.bwShortterm, 1) || b.bwShortterm > maxBw || b.bwShortterm < bbr3Beta*maxBw {
		t.Fatalf("bw_shortterm = %v after a marked round, want within 0.7 of %v", b.bwShortterm, maxBw)
	}
	if b.window() > b.inflightShortterm {
		t.Fatalf("window %d above inflight_shortterm %d", b.window(), b.inflightShortterm)
	}
	// Reno, told the same, does not read it the same way: this test is the
	// draft's controller's; recovery_test.go has Reno's.
}

// Marks while probing count with losses against the 2% a probe may cause
// (§5.5.10.2): five marked datagrams on fifty in flight bound
// inflight_longterm at the flight — never under 0.7 of the target — and
// send the probe DOWN.
func TestBBR3MarksWhileProbingBoundTheFlight(t *testing.T) {
	p, b := bbr3Cruising(t)
	p.now = p.now.Add(3 * time.Second)
	p.round(80) // REFILL
	p.round(80) // UP
	if b.state != bbr3ProbeBWUp || b.inflightLongterm != bbr3Unbounded {
		t.Fatalf("state %d bound %d before the marks", b.state, b.inflightLongterm)
	}
	p.send(100)
	p.now = p.now.Add(p.rtt)
	p.ack(50)
	want := max(50*1200, int(bbr3Beta*float64(b.targetInflight())))
	p.mark(5) // 5 of 50 in flight: 10%, over 2%
	if b.state != bbr3ProbeBWDown {
		t.Fatalf("state %d after 10%% of marks in UP, want DOWN", b.state)
	}
	if b.inflightLongterm != want {
		t.Fatalf("inflight_longterm = %d, want %d", b.inflightLongterm, want)
	}
	if b.bwProbeSamples {
		t.Fatal("the probe reacted but still counts its samples: it would react again")
	}
}
