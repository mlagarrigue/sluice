package quic

import (
	"math"
	"testing"
	"time"
)

// bbrPath drives a BBR controller by hand over a path of one round trip and
// one link rate, doing the delivery estimator's bookkeeping the way
// processAck does: each packet remembers the delivered count at its send,
// and its acknowledgement carries what was delivered since, over the longer
// of the round trip and the link's serialisation of those bytes — so a
// round's last acknowledgement samples the round's rate, capped at the
// link's (draft-ietf-ccwg-bbr-06 §4.1.1.2.4).
type bbrPath struct {
	cc        congestionController // the controller driven: b, or bbr3_test.go's
	b         *bbr
	now       time.Time
	rtt       time.Duration
	link      float64 // bytes per second
	delivered int
	lost      int  // C.lost
	afterLoss bool // the last datagram off the queue was lost: a loss extends its range
	queue     []bbrSent
	// acks holds the delivered count after each acknowledgement since the
	// last send: the path is ACK-clocked, its k-th datagram sent as the k-th
	// of them arrived, so its sample spans what was delivered since — a
	// round-start sample reads the previous round's flight, as it does on
	// a paced path, not the one datagram a burst sent after a drained pipe
	// would. A loss or a mark clears it: what is sent next was sent after
	// the congestion signal.
	acks []int
	// est is the controller's estimator, for its application-limited mark
	// alone: delivered is kept equal to the path's, the mark is snapshot
	// into each datagram sent, and settle lifts it as finish does.
	est *deliveryEstimator
}

type bbrSent struct {
	sentAt     time.Time
	prior      int
	txInFlight int
	lost       int
	appLimited bool
}

func newBBRPath(rtt time.Duration, link float64) *bbrPath {
	est := &deliveryEstimator{}
	b := newBBR(1200, est)
	return &bbrPath{cc: b, b: b, now: time.Unix(1000, 0), rtt: rtt, link: link, est: est}
}

// settle is the previous frame's finish, for the application-limited mark:
// lifted once the bubble has been delivered. Every frame here is one
// datagram, so it runs before the next acknowledgement or send.
func (p *bbrPath) settle() {
	if p.est.appLimited != 0 && p.est.delivered > p.est.appLimited {
		p.est.appLimited = 0
	}
}

// send puts n datagrams on the path now.
func (p *bbrPath) send(n int) {
	p.settle()
	for i := range n {
		prior := p.delivered
		if i < len(p.acks) {
			prior = p.acks[i]
		}
		p.cc.onSent(p.now, 1200)
		p.queue = append(p.queue, bbrSent{sentAt: p.now, prior: prior, txInFlight: p.cc.bytesInFlight(), lost: p.lost, appLimited: p.est.appLimited != 0})
	}
	p.acks = p.acks[:0]
}

// ack acknowledges the oldest n datagrams now, in order, each by a frame
// of its own: each carries its frame's sample.
func (p *bbrPath) ack(n int) {
	for range n {
		p.cc.onAcked(p.acked(true))
	}
}

// acked takes the oldest datagram off the queue as delivered now: its
// sample reads the link's rate. rated says it is its frame's newest.
func (p *bbrPath) acked(rated bool) ackEvent {
	p.settle()
	s := p.queue[0]
	p.queue = p.queue[1:]
	p.delivered += 1200
	p.est.delivered = p.delivered
	p.acks = append(p.acks, p.delivered)
	p.afterLoss = false
	ev := ackEvent{now: p.now, sentAt: s.sentAt, size: 1200, srtt: p.rtt, delivered: p.delivered - s.prior, appLimited: s.appLimited, rated: rated}
	ev.txInFlight, ev.lost = s.txInFlight, p.lost-s.lost
	ev.sendElapsed = time.Duration(float64(ev.delivered) / p.link * float64(time.Second))
	return ev
}

// lose declares the oldest n datagrams lost now, as one batch: one lost
// range, or none if the datagram before them was lost too, and the
// crossing as declareLost finds it.
func (p *bbrPath) lose(n int) {
	ev := lossEvent{now: p.now, latestSent: p.queue[n-1].sentAt, bytes: n * 1200}
	if !p.afterLoss {
		ev.ranges = 1
	}
	for _, s := range p.queue[:n] {
		p.lost += 1200
		if lost := p.lost - s.lost; ev.crossing.size == 0 && lossTooHigh(lost, s.txInFlight) {
			ev.crossing = lostPacket{size: 1200, txInFlight: s.txInFlight, lost: lost, appLimited: s.appLimited}
		}
	}
	p.queue = p.queue[n:]
	p.afterLoss = true
	p.acks = p.acks[:0]
	p.cc.onLost(ev)
}

// mark reports n CE-marked datagrams now, as one validated ACK frame does.
func (p *bbrPath) mark(n int) {
	p.acks = p.acks[:0]
	p.cc.onMarked(markEvent{now: p.now, packets: n})
}

// round sends n datagrams and acknowledges them a round trip later.
func (p *bbrPath) round(n int) {
	p.send(n)
	p.now = p.now.Add(p.rtt)
	p.ack(n)
}

// bbrAtLink is a controller that has just found a 4.8 MB/s link over 20 ms
// (a bandwidth-delay product of 80 datagrams): four doubling rounds, the
// last at the link's rate. Startup is still on.
func bbrAtLink(t *testing.T) *bbrPath {
	t.Helper()
	p := newBBRPath(20*time.Millisecond, 4.8e6)
	for _, n := range []int{10, 20, 40, 80} {
		p.round(n)
		if p.b.state != bbrStartup || p.b.pacingGain != bbrHighGain {
			t.Fatalf("after a round of %d growing datagrams, state %d gain %v, want Startup at 2/ln 2", n, p.b.state, p.b.pacingGain)
		}
	}
	if math.Abs(p.b.btlBw-4.8e6) > 1 {
		t.Fatalf("BtlBw = %v after a round at the link's rate, want 4.8e6", p.b.btlBw)
	}
	return p
}

// bbrInProbeBW is a controller past Startup and Drain, cruising at phase
// 2 of the gain cycle (the first of the six at gain 1).
func bbrInProbeBW(t *testing.T) *bbrPath {
	t.Helper()
	p := bbrAtLink(t)
	for range 4 {
		p.round(200)
	}
	if p.b.state != bbrProbeBW {
		t.Fatalf("state %d after three flat rounds and a drain, want ProbeBW", p.b.state)
	}
	p.b.cycleIndex = 1
	p.b.advanceCyclePhase(p.now)
	return p
}

// Startup ends on three rounds that fail to raise BtlBw by a quarter
// (§4.3.2.2), Drain paces at the inverse of Startup's gain until the flight
// is down to one bandwidth-delay product (§4.3.3), and ProbeBW starts at
// any phase but the draining one (§4.3.4.2).
func TestBBRStartupDrainProbeBW(t *testing.T) {
	p := bbrAtLink(t)
	// Rounds that overfill the link: BtlBw is the link's, flat. A round's
	// check reads the estimate as the previous round left it, so the first
	// of them still sees the round that reached the link as growth.
	for i := range 3 {
		p.round(200)
		if p.b.state != bbrStartup || p.b.fullBwCount != i {
			t.Fatalf("after flat round %d: state %d, flat rounds counted %d", i+1, p.b.state, p.b.fullBwCount)
		}
	}
	// The next round's first acknowledgement counts the third and fills
	// the pipe; the flight is still 199 datagrams, well above the 80 of
	// the product.
	p.send(200)
	p.now = p.now.Add(p.rtt)
	p.ack(1)
	if !p.b.filledPipe || p.b.state != bbrDrain {
		t.Fatalf("after the third flat round: filled %v, state %d, want Drain", p.b.filledPipe, p.b.state)
	}
	if want := 4.8e6 / bbrHighGain; math.Abs(p.b.pacingRate(p.rtt)-want) > 1 {
		t.Fatalf("Drain paces at %v, want BtlBw / (2/ln 2) = %v", p.b.pacingRate(p.rtt), want)
	}
	if p.b.cwndGain != bbrHighGain {
		t.Fatalf("Drain's cwnd gain is %v, want Startup's kept", p.b.cwndGain)
	}
	p.ack(199)
	if p.b.state != bbrProbeBW || p.b.cwndGain != 2 {
		t.Fatalf("after the flight drained: state %d, cwnd gain %v, want ProbeBW at 2", p.b.state, p.b.cwndGain)
	}
	if p.b.pacingGain == 0.75 {
		t.Fatal("ProbeBW entered at the draining phase")
	}
	if want := 2*4.8e6*0.02 + 3*1200; p.b.window() != int(want) {
		t.Fatalf("ProbeBW window = %d, want 2 × BDP + 3 datagrams = %v", p.b.window(), want)
	}
}

// ProbeBW cycles its pacing gain through 5/4, 3/4 and six rounds of 1
// (§4.3.4.3): the probing phase lasts a round trip and until the flight
// reaches what it aims at, the draining one ends as soon as the flight is
// back to one product, the rest last a round trip each.
func TestBBRProbeBWGainCycle(t *testing.T) {
	p := bbrInProbeBW(t)
	p.b.cycleIndex = bbrGainCycleLen - 1
	p.b.advanceCyclePhase(p.now)
	if p.b.pacingGain != 1.25 {
		t.Fatalf("phase 0: gain %v", p.b.pacingGain)
	}
	// A round at the product does not end the probe: the flight never
	// reached 5/4 of it. The pacing rate follows the gain at the next
	// acknowledgement.
	p.round(80)
	p.now = p.now.Add(time.Millisecond)
	p.round(80)
	if p.b.cycleIndex != 0 || p.b.pacingRate(p.rtt) != 1.25*4.8e6 {
		t.Fatalf("the probing phase ended with the flight at the product (index %d, rate %v)", p.b.cycleIndex, p.b.pacingRate(p.rtt))
	}
	// A round above 5/4 of the product ends it on its first acknowledgement,
	// and the draining phase ends as the flight falls back to the product.
	p.now = p.now.Add(time.Millisecond)
	p.send(110)
	p.now = p.now.Add(p.rtt)
	p.ack(1)
	if p.b.cycleIndex != 1 || p.b.pacingGain != 0.75 {
		t.Fatalf("after a flight of 110: index %d, gain %v, want the draining phase", p.b.cycleIndex, p.b.pacingGain)
	}
	p.ack(109)
	if p.b.cycleIndex != 2 || p.b.pacingGain != 1 {
		t.Fatalf("after the flight drained: index %d, gain %v, want phase 2 at gain 1", p.b.cycleIndex, p.b.pacingGain)
	}
	// Six cruising rounds, one phase each, then back to the probe.
	for want := 3; want <= 7; want++ {
		p.now = p.now.Add(time.Millisecond)
		p.round(80)
		if p.b.cycleIndex != want || p.b.pacingGain != 1 {
			t.Fatalf("index %d gain %v, want %d at 1", p.b.cycleIndex, p.b.pacingGain, want)
		}
	}
	p.now = p.now.Add(time.Millisecond)
	p.round(80)
	if p.b.cycleIndex != 0 || p.b.pacingGain != 1.25 {
		t.Fatalf("after eight phases: index %d gain %v, want the probe again", p.b.cycleIndex, p.b.pacingGain)
	}
}

// An RTprop ten seconds old sends ProbeBW to ProbeRTT (§4.3.5): the window
// drops to four datagrams, and after 200 ms and one round with the flight
// at that size the window comes back and ProbeBW resumes.
func TestBBRProbeRTT(t *testing.T) {
	p := bbrInProbeBW(t)
	before := p.b.window()
	// A standing flight: a send into an empty one is a restart from idle,
	// whose first acknowledgement refreshes RTprop instead (§4.3.4.4).
	p.send(80)
	for range 2 {
		p.now = p.now.Add(p.rtt)
		p.send(80)
		p.ack(80)
	}
	p.now = p.now.Add(p.rtt)
	p.send(80)
	p.b.rtpropStamp = p.now.Add(-bbrRTpropFilterLen - time.Second)
	p.ack(80)
	if p.b.state != bbrProbeRTT || p.b.window() != 4*1200 || p.b.pacingGain != 1 {
		t.Fatalf("after an expired RTprop: state %d, window %d, gain %v", p.b.state, p.b.window(), p.b.pacingGain)
	}
	if !p.b.probeRTTDoneStamp.IsZero() {
		t.Fatal("the 200 ms started with 80 datagrams still in flight")
	}
	p.now = p.now.Add(p.rtt)
	p.ack(80)
	entered := p.now
	if p.b.probeRTTDoneStamp != entered.Add(bbrProbeRTTDuration) {
		t.Fatalf("the 200 ms started at %v, want when the flight was down", p.b.probeRTTDoneStamp.Sub(entered))
	}
	// Rounds of four datagrams: not out before the 200 ms are over.
	for p.now.Before(entered.Add(bbrProbeRTTDuration)) {
		p.round(4)
		if p.b.state != bbrProbeRTT {
			t.Fatalf("left ProbeRTT after %v", p.now.Sub(entered))
		}
	}
	p.now = p.now.Add(time.Millisecond)
	p.round(4)
	if p.b.state != bbrProbeBW {
		t.Fatalf("state %d after the 200 ms and a round, want ProbeBW", p.b.state)
	}
	if p.b.window() != before {
		t.Fatalf("window %d after ProbeRTT, want the %d it had", p.b.window(), before)
	}
	if p.b.rtpropStamp != p.now {
		t.Fatal("leaving ProbeRTT did not restamp RTprop")
	}
}

// Loss makes BBR conservative for one round (§4.2.3.4): the window drops to
// the flight plus a datagram, a later batch in the period takes its bytes
// off it, and the acknowledgement of a packet sent after the loss restores
// the window. The model does not move. Persistent congestion is the
// draft's timeout: one datagram, then the floor of four.
func TestBBRLossRecovery(t *testing.T) {
	p := bbrInProbeBW(t)
	before := p.b.window()
	btlBw := p.b.btlBw
	p.send(100)
	p.now = p.now.Add(p.rtt)
	p.ack(50)
	p.b.onLost(lossEvent{now: p.now, latestSent: p.now.Add(-p.rtt), bytes: 2 * 1200})
	p.queue = p.queue[2:]
	if !p.b.inRecovery || p.b.priorCwnd != before || p.b.window() != 48*1200+1200 {
		t.Fatalf("after a loss: recovery %v, saved %d, window %d, want %d saved and the flight plus one", p.b.inRecovery, p.b.priorCwnd, p.b.window(), before)
	}
	p.b.onLost(lossEvent{now: p.now, latestSent: p.now.Add(-p.rtt), bytes: 1200})
	p.queue = p.queue[1:]
	if p.b.window() != 48*1200 {
		t.Fatalf("a second batch in the period left the window at %d, want one datagram off", p.b.window())
	}
	if p.b.btlBw != btlBw {
		t.Fatal("loss moved BtlBw")
	}
	p.send(10)
	p.now = p.now.Add(p.rtt)
	p.ack(57)
	if p.b.inRecovery || p.b.window() != before {
		t.Fatalf("after a post-loss packet was acknowledged: recovery %v, window %d, want %d restored", p.b.inRecovery, p.b.window(), before)
	}

	// Persistent congestion: the loss that declares it opens a period, the
	// collapse follows in the same batch. A straggler from before the
	// collapse brings the floor back; a packet sent after it ends the
	// period and restores the window the draft saved.
	sentAt := p.now
	p.send(5)
	p.now = p.now.Add(p.rtt)
	p.b.onLost(lossEvent{now: p.now, latestSent: sentAt, bytes: 1200})
	p.queue = p.queue[1:]
	p.b.onPersistentCongestion()
	if p.b.window() != 1200 || !p.b.inRecovery {
		t.Fatalf("window %d after persistent congestion, want one datagram in a recovery period", p.b.window())
	}
	p.ack(1)
	if p.b.window() != 4*1200 || !p.b.inRecovery {
		t.Fatalf("window %d after a straggler, want the floor of four", p.b.window())
	}
	p.send(1)
	p.now = p.now.Add(p.rtt)
	p.ack(4)
	if p.b.inRecovery || p.b.window() != before {
		t.Fatalf("after a post-collapse packet was acknowledged: recovery %v, window %d, want %d restored", p.b.inRecovery, p.b.window(), before)
	}
}

// An application-limited sample feeds the max filter only when it beats
// the estimate (§4.1.1.4), and never the full-pipe check (§4.3.2.2).
func TestBBRAppLimitedSamples(t *testing.T) {
	p := bbrAtLink(t)
	p.send(40)
	p.now = p.now.Add(p.rtt)
	for range 40 {
		q := p.queue[0]
		p.queue = p.queue[1:]
		p.delivered += 1200
		p.b.onAcked(ackEvent{now: p.now, sentAt: q.sentAt, size: 1200, srtt: p.rtt, delivered: p.delivered - q.prior, appLimited: true})
	}
	if p.b.btlBw != 4.8e6 || p.b.fullBwCount != 0 {
		t.Fatalf("a limited round at half the link: BtlBw %v, flat rounds %d, want the estimate kept and no round counted", p.b.btlBw, p.b.fullBwCount)
	}
	p.send(160)
	p.now = p.now.Add(p.rtt)
	for range 160 {
		q := p.queue[0]
		p.queue = p.queue[1:]
		p.delivered += 1200
		p.b.onAcked(ackEvent{now: p.now, sentAt: q.sentAt, size: 1200, srtt: p.rtt, delivered: p.delivered - q.prior, appLimited: true})
	}
	if p.b.btlBw != 9.6e6 {
		t.Fatalf("a limited round above the estimate left BtlBw at %v, want 9.6e6", p.b.btlBw)
	}
}
