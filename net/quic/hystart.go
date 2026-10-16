package quic

import (
	"math"
	"time"
)

// hystart is RFC 9406's HyStart++: a slow start that ends on the round
// trip rising instead of on the loss that ends standard slow start. Slow
// start doubles the window every round trip until the bottleneck's buffer
// overflows, and the round trip before that overflow shows the buffer
// filling — a rise of a few milliseconds over the previous round's
// minimum. HyStart++ reads that rise (§4.2, the Delay Increase algorithm)
// and switches to Conservative Slow Start, CSS, which grows a quarter as
// fast for a few rounds: if the rise persists, the slow start is over and
// the window is the threshold; if the round trip drops back, the rise was
// jitter and slow start resumes.
//
// The round trip it reads is the acknowledged packet's own, as the
// controller sees it: now minus the send time, with whatever delay the
// receiver's acknowledgement added. The minimum over a round discards that
// delay the way it discards queueing jitter, and the controller does not
// depend on the connection's RTT estimator, which samples once per
// acknowledgement frame, from its largest packet only.
//
// Only the initial slow start is armed (§4.3: later slow starts have a
// threshold the path taught them). The constants are §4.3's, and L is
// infinity because the sender is paced: every acknowledged byte counts.
//
// What it buys, measured on the path profiles (docs/benchmarks.md,
// 2026-10-14): the detector compares one round's minimum with the
// previous round's, so it sees a queue one round after the queue begins,
// and a slow start that doubles every round fills a buffer no deeper than
// the path's bandwidth-delay product within that round — on the 20 ms and
// 5 ms buffers of the 40 and 80 ms profiles the loss comes first and the
// exit is the loss, as before. On a buffer ten times the product it fires
// at under three times the product and the five CSS rounds at a quarter
// growth triple the window from there: no loss at all where the
// loss-based exit lost a buffer's worth, but a fuller queue than β left
// behind. Transfer times move by nothing on either.
type hystart struct {
	// armed is true during the initial slow start, false once a loss or
	// HyStart++ itself has set the threshold.
	armed bool
	// css is true in Conservative Slow Start; cssRounds counts the rounds
	// spent there, the partial round of entry included (§4.2).
	css       bool
	cssRounds int
	// lastMinRTT and curMinRTT are the previous and the current round's
	// smallest round trip, noRTT until a sample arrives; samples counts
	// the current round's. cssBaseline is the round-trip minimum CSS was
	// entered at: a round below it means the exit was spurious.
	lastMinRTT, curMinRTT, cssBaseline time.Duration
	samples                            int
}

const (
	hystartMinRTTThresh     = 4 * time.Millisecond
	hystartMaxRTTThresh     = 16 * time.Millisecond
	hystartMinRTTDivisor    = 8
	hystartRTTSamples       = 8
	hystartCSSGrowthDivisor = 4
	hystartCSSRounds        = 5
	// noRTT is §4.2's infinity.
	noRTT = time.Duration(math.MaxInt64)
)

func newHystart() hystart {
	return hystart{armed: true, lastMinRTT: noRTT, curMinRTT: noRTT, cssBaseline: noRTT}
}

// disarm ends HyStart++ for the connection: the threshold is set, later
// slow starts are standard (§4.3).
func (h *hystart) disarm() { h.armed = false }

// grow is one acknowledged packet of size bytes with round trip rtt, in
// slow start or CSS; roundClosed says this acknowledgement ended a round
// (the first of a flight sent after the previous round's mark, see
// newReno.absorb). It returns the bytes the window may grow by — all of
// them in slow start, a quarter in CSS — and whether the slow start is
// over, in which case the caller sets the threshold to the window and
// grows nothing.
func (h *hystart) grow(size int, rtt time.Duration, roundClosed bool) (gain int, exit bool) {
	if roundClosed {
		h.lastMinRTT, h.curMinRTT, h.samples = h.curMinRTT, noRTT, 0
		if h.css {
			h.cssRounds++
			if h.cssRounds > hystartCSSRounds {
				h.disarm()
				return 0, true
			}
		}
	}
	gain = size
	if h.css {
		gain /= hystartCSSGrowthDivisor
	}
	h.curMinRTT = min(h.curMinRTT, rtt)
	h.samples++
	if h.samples < hystartRTTSamples {
		return gain, false
	}
	if h.css {
		if h.curMinRTT < h.cssBaseline {
			h.css = false
			h.cssBaseline = noRTT
		}
		return gain, false
	}
	if h.lastMinRTT == noRTT {
		return gain, false
	}
	thresh := max(hystartMinRTTThresh, min(h.lastMinRTT/hystartMinRTTDivisor, hystartMaxRTTThresh))
	if h.curMinRTT >= h.lastMinRTT+thresh {
		h.cssBaseline = h.curMinRTT
		h.css = true
		h.cssRounds = 1
	}
	return gain, false
}
