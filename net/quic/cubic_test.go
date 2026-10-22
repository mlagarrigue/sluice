package quic

import (
	"math"
	"testing"
	"time"
)

// cubicInAvoidance is a CUBIC controller just after its first loss at a
// window of 100 datagrams: wMax 120 000 bytes, the window at 84 000 (β),
// K = cbrt(30 / 0.4) s. The numbers below are RFC 9438 Figures 1 and 2 by
// hand, in bytes with a 1200-byte segment.
func cubicInAvoidance(now time.Time) *cubic {
	cc := newCubic(1200)
	cc.cwnd = 120000
	cc.ssthresh = cc.cwnd
	cc.onSent(now, 120000)
	// The period opens an instant before now: RFC 9002 B.5 counts a packet
	// sent at the start's very instant as inside it, and the tests below
	// send from now on.
	cc.onLost(lossEvent{now: now.Add(-time.Nanosecond), latestSent: now.Add(-time.Nanosecond), bytes: 1200})
	return cc
}

// limitedAck acknowledges one datagram sent rtt ago, with the window the
// limit this round: the send opened the round, a send of a whole window on
// top of it was refused, and the acknowledgement closes it. Callers keep
// now − rtt strictly after the recovery start, or B.5 gates the packet.
func limitedAck(cc *cubic, now time.Time, rtt time.Duration) {
	cc.onSent(now.Add(-rtt), 1200)
	cc.canSend(cc.cwnd)
	cc.onAcked(ackEvent{now: now, sentAt: now.Add(-rtt), size: 1200, srtt: rtt})
}

// unlimitedAck is the same acknowledgement with nothing refused this round:
// the sender had room and did not use it.
func unlimitedAck(cc *cubic, now time.Time, rtt time.Duration) {
	cc.onSent(now.Add(-rtt), 1200)
	cc.onAcked(ackEvent{now: now, sentAt: now.Add(-rtt), size: 1200, srtt: rtt})
}

func TestCubicCurve(t *testing.T) {
	now := time.Now()
	cc := cubicInAvoidance(now)
	if cc.cwnd != 84000 || cc.ssthresh != 84000 {
		t.Fatalf("after a loss at 120000, cwnd = %d, ssthresh = %d, want 84000 (β = 0.7)", cc.cwnd, cc.ssthresh)
	}
	if cc.wMax != 120000 {
		t.Fatalf("wMax = %v, want the window the loss happened at", cc.wMax)
	}
	// Figure 2: K = cbrt((W_max − cwnd_epoch) / C) = cbrt(30 / 0.4) s.
	if want := math.Cbrt(75); math.Abs(cc.k-want) > 1e-9 {
		t.Fatalf("K = %v s, want %v", cc.k, want)
	}
	// Figure 1 at t = 0 is the window the stage started with, at t = K
	// the plateau, and C · 1 segment past K one segment above it.
	for _, tc := range []struct{ t, want float64 }{
		{0, 84000},
		{cc.k, 120000},
		{cc.k + 1, 120000 + 0.4*1200},
		{cc.k - 1, 120000 - 0.4*1200},
	} {
		if got := cc.wCubic(tc.t); math.Abs(got-tc.want) > 1e-6 {
			t.Errorf("W_cubic(%v) = %v, want %v", tc.t, got, tc.want)
		}
	}
}

// On a short round trip the Reno estimate climbs α segments per round trip
// faster than the curve climbs per second: the window follows the estimate
// (§4.3). Far past K the curve is steep and the window follows it, capped
// at one and a half times itself per round trip (§4.2, §4.5).
func TestCubicRegions(t *testing.T) {
	now := time.Now()
	cc := cubicInAvoidance(now)
	const rtt = 5 * time.Millisecond
	start := cc.cwnd
	// One window of acknowledgements, 70 datagrams spread over one round
	// trip: the estimate grows α · segment ≈ 635 bytes; the curve, whose
	// slope at the start of this stage is 3 · C · K² ≈ 25.6 kB/s, grows
	// about 250 bytes over the 10 ms the clock advances.
	for i := range 70 {
		limitedAck(cc, now.Add(rtt+time.Duration(i)*rtt/70), rtt)
	}
	grew := cc.cwnd - start
	if want := int(math.Round(cubicAlpha * 1200)); grew < want-40 || grew > want+40 {
		t.Fatalf("Reno-friendly round grew the window by %d, want about α · 1200 = %d", grew, want)
	}
	if cc.wCubic(cc.t.Seconds()) >= cc.wEst {
		t.Fatalf("W_cubic(%v) = %v is not below W_est = %v: the window should have been the estimate's", cc.t, cc.wCubic(cc.t.Seconds()), cc.wEst)
	}

	// Jump the clock to twice K: the curve names a window far above the
	// current one, and one acknowledgement earns the capped share — half
	// a window spread over a window's worth of bytes.
	cc.t = time.Duration(2 * cc.k * float64(time.Second))
	before := cc.cwnd
	limitedAck(cc, now.Add(time.Second), rtt)
	if got, want := cc.cwnd-before, 600; got < want-1 || got > want+1 {
		t.Fatalf("convex region grew the window by %d on one datagram, want 1.5 × cwnd capped: %d", got, want)
	}
}

// β applies once per recovery period (RFC 9002 B.6's gate, as Reno); a
// loss below the previous wMax lowers wMax further (§4.7, fast
// convergence) before the reduction.
func TestCubicDecrease(t *testing.T) {
	now := time.Now()
	cc := cubicInAvoidance(now)
	cc.onSent(now, 1200)
	cc.onLost(lossEvent{now: now, latestSent: now.Add(-time.Second), bytes: 1200})
	if cc.cwnd != 84000 {
		t.Fatalf("a loss sent before the recovery start shrank the window to %d", cc.cwnd)
	}
	later := now.Add(time.Second)
	cc.onSent(later, 1200)
	cc.onLost(lossEvent{now: later, latestSent: later, bytes: 1200})
	if want := 84000 * (1 + cubicBeta) / 2; cc.wMax != want {
		t.Errorf("fast convergence: wMax = %v after a loss at 84000 below wMax 120000, want %v", cc.wMax, want)
	}
	if want := int(84000 * cubicBeta); cc.cwnd != want {
		t.Errorf("second loss: cwnd = %d, want %d", cc.cwnd, want)
	}
	if cc.cwndPrior != 84000 {
		t.Errorf("cwndPrior = %v, want the window before the reduction", cc.cwndPrior)
	}
}

// TestCubicFastConvergenceCanBeDisabled: §4.7 wants fast convergence off
// for a flow alone on its path; DisableFastConvergence keeps wMax at the
// window of each loss, the one below the previous wMax included.
func TestCubicFastConvergenceCanBeDisabled(t *testing.T) {
	cc, ok := newController(CongestionConfig{Algorithm: CongestionCubic, DisableFastConvergence: true}, 1200, &deliveryEstimator{}).(*cubic)
	if !ok {
		t.Fatal("CongestionCubic did not build a cubic controller")
	}
	now := time.Unix(1000, 0)
	cc.cwnd = 120000
	cc.ssthresh = cc.cwnd
	for i, at := range []float64{120000, 84000, 58800} {
		if float64(cc.cwnd) != at {
			t.Fatalf("loss %d: cwnd %d, want %v", i, cc.cwnd, at)
		}
		now = now.Add(time.Second)
		cc.onSent(now, 1200)
		cc.onLost(lossEvent{now: now, latestSent: now, bytes: 1200})
		if cc.wMax != at {
			t.Errorf("loss %d at %v: wMax = %v, want the window", i, at, cc.wMax)
		}
	}
	if def := newController(CongestionConfig{Algorithm: CongestionCubic}, 1200, &deliveryEstimator{}).(*cubic); def.noFastConvergence {
		t.Error("the zero configuration disables fast convergence")
	}
}

// The curve's clock runs only while the window is what limits the sender
// (§4.2, §5.8): a round the sender did not fill, and the gap of an idle
// period, do not count.
func TestCubicClockFreezesWhenApplicationLimited(t *testing.T) {
	now := time.Now()
	cc := cubicInAvoidance(now)
	const rtt = 80 * time.Millisecond
	// The first acknowledgement after the loss arrives one round trip
	// later, the next 10 ms after it.
	limitedAck(cc, now.Add(rtt), rtt)
	limitedAck(cc, now.Add(rtt+10*time.Millisecond), rtt)
	if cc.t != rtt+10*time.Millisecond {
		t.Fatalf("t = %v after window-limited acknowledgements at +%v and +%v, want %v", cc.t, rtt, rtt+10*time.Millisecond, rtt+10*time.Millisecond)
	}
	// Rounds the window did not limit: nothing is refused, and 300 ms of
	// acknowledgements leave t where it was.
	for i := range 30 {
		unlimitedAck(cc, now.Add(rtt+time.Duration(i+2)*10*time.Millisecond), rtt)
	}
	if cc.t != rtt+10*time.Millisecond {
		t.Fatalf("t = %v after application-limited rounds, want %v unchanged", cc.t, rtt+10*time.Millisecond)
	}
	// Ten seconds idle, then a window-limited round: the first
	// acknowledgement advances t by its own round trip, not by the gap.
	limitedAck(cc, now.Add(10*time.Second), rtt)
	if want := 2*rtt + 10*time.Millisecond; cc.t != want {
		t.Fatalf("t = %v after a 10 s idle gap, want %v (the gap bounded by one round trip)", cc.t, want)
	}
}

// After persistent congestion the window collapses and slow-starts back to
// the threshold the loss set; the avoidance stage that follows starts with
// K = 0 and wMax at its own first window (§4.8), not at the old plateau.
func TestCubicPersistentCongestion(t *testing.T) {
	now := time.Now()
	cc := cubicInAvoidance(now)
	cc.onPersistentCongestion()
	if cc.cwnd != 2400 || cc.ssthresh != 84000 {
		t.Fatalf("collapsed: cwnd = %d, ssthresh = %d, want 2400 and 84000", cc.cwnd, cc.ssthresh)
	}
	// Slow start, window-limited, up to the threshold.
	for cc.cwnd < cc.ssthresh {
		limitedAck(cc, now, 80*time.Millisecond)
	}
	exit := cc.cwnd
	limitedAck(cc, now.Add(time.Millisecond), 80*time.Millisecond)
	if !cc.inEpoch || cc.k != 0 || cc.wMax != float64(exit) {
		t.Fatalf("first stage after a timeout: inEpoch %v, K = %v, wMax = %v, want K = 0 and wMax = %d", cc.inEpoch, cc.k, cc.wMax, exit)
	}
}

func TestNewControllerByAlgorithm(t *testing.T) {
	if _, ok := newController(CongestionConfig{}, 1200, &deliveryEstimator{}).(*newReno); !ok {
		t.Error("the zero configuration is not NewReno")
	}
	cc, ok := newController(CongestionConfig{Algorithm: CongestionCubic}, 1200, &deliveryEstimator{}).(*cubic)
	if !ok {
		t.Fatal("CongestionCubic did not build a cubic controller")
	}
	if cc.window() != newNewReno(1200).cwnd {
		t.Errorf("CUBIC's initial window is %d, want Reno's %d (RFC 9002 §7.2)", cc.window(), newNewReno(1200).cwnd)
	}
	if !cc.hs.armed {
		t.Error("CUBIC's initial slow start has HyStart++ disarmed (RFC 9438 §4.10)")
	}
	b3, ok := newController(CongestionConfig{Algorithm: CongestionBBR}, 1200, &deliveryEstimator{}).(*bbr3)
	if !ok {
		t.Fatal("CongestionBBR did not build a draft-ietf-ccwg-bbr controller")
	}
	if b3.window() != newNewReno(1200).cwnd || b3.state != bbr3Startup || b3.pacingGain != bbr3StartupPacingGain || b3.cwndGain != 2 {
		t.Errorf("BBR starts with window %d, state %d, gains %v/%v: want Reno's window, Startup, 2.77 and 2", b3.window(), b3.state, b3.pacingGain, b3.cwndGain)
	}
	if CongestionBBR.String() != "bbr" {
		t.Errorf("CongestionBBR prints as %q", CongestionBBR)
	}
	// An algorithm this build does not know falls to the default, and the
	// connection's Stats say which one runs.
	pcA, pcB := newLoopbackPair(t)
	params := DefaultParameters()
	params.Congestion.Algorithm = CongestionAlgorithm(200)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, params)
	defer c.Close()
	if got := c.Stats().Algorithm; got != CongestionNewReno {
		t.Errorf("Stats.Algorithm = %v for an unknown choice, want newreno", got)
	}
	if _, ok := c.cc.(*newReno); !ok {
		t.Errorf("an unknown choice built a %T", c.cc)
	}
}
