package quic

import (
	"testing"
	"time"
)

// feedRound acks n packets of 1200 bytes at round trip rtt into h, the
// first one closing the previous round, and returns the window growth they
// earned and whether the slow start ended.
func feedRound(h *hystart, n int, rtt time.Duration) (gain int, exit bool) {
	for i := range n {
		g, e := h.grow(1200, rtt, i == 0)
		if e {
			return gain, true
		}
		gain += g
	}
	return gain, false
}

// TestHystartEntersCSSOnDelayIncrease is RFC 9406 §4.2's trigger: a round
// whose minimum round trip sits a threshold above the previous round's —
// max(4 ms, min(last/8, 16 ms)), 4 ms on a 20 ms path — ends standard
// slow start once N_RTT_SAMPLE samples are in, and the acknowledgements
// after that grow the window a quarter as much.
func TestHystartEntersCSSOnDelayIncrease(t *testing.T) {
	h := newHystart()
	base := 20 * time.Millisecond
	// Two rounds at the base round trip: the first has no previous round
	// to compare with, the second compares equal.
	feedRound(&h, 10, base)
	if gain, _ := feedRound(&h, 10, base); gain != 12000 || h.css {
		t.Fatalf("steady round trip: gain %d, css %v; want 12000, false", gain, h.css)
	}
	// A rise below the threshold is not a signal.
	if gain, _ := feedRound(&h, 10, base+3*time.Millisecond); gain != 12000 || h.css {
		t.Fatalf("3 ms rise: gain %d, css %v; want 12000, false", gain, h.css)
	}
	// At the threshold, the eighth sample enters CSS and the two
	// acknowledgements after it earn 300 bytes each instead of 1200.
	h = newHystart()
	feedRound(&h, 10, base)
	feedRound(&h, 10, base)
	gain, exit := feedRound(&h, 10, base+4*time.Millisecond)
	if exit || !h.css || gain != 8*1200+2*300 {
		t.Fatalf("4 ms rise: gain %d, css %v, exit %v; want %d, true, false", gain, h.css, exit, 8*1200+2*300)
	}
	if h.cssBaseline != base+4*time.Millisecond || h.cssRounds != 1 {
		t.Fatalf("CSS entered with baseline %v after %d rounds; want %v and 1 (the partial round counts)", h.cssBaseline, h.cssRounds, base+4*time.Millisecond)
	}
	// Seven samples are not enough, however high.
	h = newHystart()
	feedRound(&h, 10, base)
	feedRound(&h, 10, base)
	if gain, _ := feedRound(&h, 7, 2*base); gain != 7*1200 || h.css {
		t.Fatalf("seven samples: gain %d, css %v; want 8400, false", gain, h.css)
	}
}

// TestHystartResumesSlowStartWhenDelayDrops is §4.2's spurious-exit test:
// a CSS round whose minimum drops below the baseline the exit was taken at
// returns to standard slow start, HyStart++ still armed.
func TestHystartResumesSlowStartWhenDelayDrops(t *testing.T) {
	h := newHystart()
	base := 20 * time.Millisecond
	feedRound(&h, 10, base)
	feedRound(&h, 10, base)
	feedRound(&h, 10, base+10*time.Millisecond)
	if !h.css {
		t.Fatal("not in CSS after a 10 ms rise")
	}
	gain, exit := feedRound(&h, 10, base)
	if exit || h.css || !h.armed || gain != 8*300+2*1200 {
		t.Fatalf("round trip back at base: gain %d, css %v, armed %v, exit %v; want %d, false, true, false", gain, h.css, h.armed, exit, 8*300+2*1200)
	}
	if h.cssBaseline != noRTT {
		t.Fatalf("baseline %v after resuming; want infinity", h.cssBaseline)
	}
}

// TestHystartExitsAfterCSSRounds: a rise that persists through CSS_ROUNDS
// rounds of CSS ends the slow start at the first acknowledgement of the
// round after, and HyStart++ is disarmed for the connection (§4.3: only
// the initial slow start).
func TestHystartExitsAfterCSSRounds(t *testing.T) {
	h := newHystart()
	base := 20 * time.Millisecond
	feedRound(&h, 10, base)
	feedRound(&h, 10, base)
	feedRound(&h, 10, base+10*time.Millisecond) // CSS round 1, partial
	for r := 2; r <= hystartCSSRounds; r++ {
		if gain, exit := feedRound(&h, 10, base+10*time.Millisecond); exit || gain != 10*300 {
			t.Fatalf("CSS round %d: gain %d, exit %v; want 3000, false", r, gain, exit)
		}
	}
	gain, exit := feedRound(&h, 10, base+10*time.Millisecond)
	if !exit || gain != 0 || h.armed {
		t.Fatalf("round after CSS: gain %d, exit %v, armed %v; want 0, true, false", gain, exit, h.armed)
	}
}

// TestCubicHystartSetsTheThreshold drives the controller itself: with a
// limited window, HyStart++ ending the slow start leaves ssthresh at the
// window and the next acknowledgement opens an avoidance stage with the
// plateau there (K = 0, RFC 9438 §4.10); a loss during slow start disarms
// it, and persistent congestion too.
func TestCubicHystartSetsTheThreshold(t *testing.T) {
	cc := newCubic(1200)
	if !cc.hs.armed {
		t.Fatal("a new CUBIC controller has HyStart++ disarmed")
	}
	now := time.Unix(0, 0)
	rtt := 20 * time.Millisecond
	// Each round: one send opens it, ten acknowledgements of packets
	// sent at the mark close it on the first; the window is the limit.
	round := func(sample time.Duration) {
		cc.onSent(now, 1200)
		cc.limited = true
		sentAt := now
		now = now.Add(sample)
		for range 10 {
			cc.onAcked(ackEvent{now: now, sentAt: sentAt, size: 1200, srtt: rtt})
		}
	}
	round(rtt)
	round(rtt)
	for r := 0; r <= hystartCSSRounds; r++ {
		round(rtt + 10*time.Millisecond)
	}
	// The first acknowledgement of the sixth round of rise ends the slow
	// start at the window of the moment; the nine after it are avoidance,
	// on a stage whose plateau is that window.
	if cc.hs.armed || cc.ssthresh == 1<<30 || cc.cwnd < cc.ssthresh {
		t.Fatalf("after %d rounds of rise: armed %v, cwnd %d, ssthresh %d; want disarmed and avoidance", hystartCSSRounds+1, cc.hs.armed, cc.cwnd, cc.ssthresh)
	}
	if !cc.inEpoch || cc.wMax != float64(cc.ssthresh) || cc.k != 0 {
		t.Fatalf("first avoidance acknowledgements: inEpoch %v, wMax %v, k %v; want true, %d, 0", cc.inEpoch, cc.wMax, cc.k, cc.ssthresh)
	}

	cc = newCubic(1200)
	cc.onLost(lossEvent{now: now, latestSent: now, bytes: 1200})
	if cc.hs.armed {
		t.Fatal("HyStart++ still armed after a loss in slow start")
	}
	cc = newCubic(1200)
	cc.onPersistentCongestion()
	if cc.hs.armed {
		t.Fatal("HyStart++ still armed after persistent congestion")
	}
}

// TestCubicPacesCSSAtFiveQuarters: in CSS the window grows a quarter per
// round trip and the pacing rate is avoidance's five quarters, not slow
// start's two windows.
func TestCubicPacesCSSAtFiveQuarters(t *testing.T) {
	cc := newCubic(1200)
	rtt := 20 * time.Millisecond
	cc.cwnd = 120_000
	if got, want := cc.pacingRate(rtt), 2*1.25*120_000/rtt.Seconds(); got != want {
		t.Fatalf("slow start pacing %v; want %v", got, want)
	}
	cc.hs.css = true
	if got, want := cc.pacingRate(rtt), 1.25*120_000/rtt.Seconds(); got != want {
		t.Fatalf("CSS pacing %v; want %v", got, want)
	}
}
