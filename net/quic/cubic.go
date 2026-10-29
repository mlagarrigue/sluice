package quic

import (
	"math"
	"time"
)

// cubic is RFC 9438's congestion controller, chosen with
// CongestionCubic. CUBIC keeps everything of Reno but the shape of the
// window: it shrinks less on a loss (β = 0.7 instead of a half, §4.6) and
// grows back along a cubic curve of time rather than a datagram per round
// trip — steep at first, flat around the window the loss happened at, steep
// again past it (§4.2). On a long round trip that is the difference between
// refilling a lost window in seconds and in minutes; on a short one the
// Reno-friendly region (§4.3) keeps it from doing worse than Reno.
//
// So cubic embeds newReno and overrides the three methods that read loss
// and growth. The flight, the §7.8 utilisation test, the B.8 recovery gate
// and the pacing rate are Reno's, unchanged. The slow start is Reno's too
// (§4.10), HyStart++ included (RFC 9406, hystart.go): it ends the initial
// one on the round trip rising where the path's buffer lets it rise —
// §4.10 recommends it, and a slow start that ends on loss overshoots the
// path by up to a window. Windows are bytes, as RFC 9438 §4.1
// allows, with the datagram size as the segment wherever the RFC counts
// segments; the curve is computed in floating point and the window held
// as an integer, the fraction carried between acknowledgements.
type cubic struct {
	newReno

	// An epoch is one congestion-avoidance stage: from a loss (or the end
	// of slow start) to the next loss. inEpoch is false until the first
	// acknowledgement in avoidance opens one.
	inEpoch bool
	// wMax is the window the last loss happened at, or less when fast
	// convergence (§4.7) decided the saturation point is falling; the
	// curve's plateau. noFastConvergence is
	// CongestionConfig.DisableFastConvergence. cwndEpoch is the window the stage started with,
	// and k the time in seconds the curve takes from one to the other
	// (Figure 2).
	wMax, cwndEpoch, k float64
	noFastConvergence  bool
	// wEst is what a Reno window would be by now (§4.3, Figure 4): the
	// window follows it when the curve is below it. cwndPrior is the
	// window when ssthresh was last set; wEst grows at α_cubic until it
	// reaches it and at Reno's one datagram per round trip after.
	wEst, cwndPrior float64
	// t is the stage's elapsed time as the curve sees it: wall clock while
	// the window was what limited the sender, frozen while it was not
	// (§4.2 and §5.8: a window computed on time the sender did not use
	// would open far past the path on its return). lastAck is when the
	// last acknowledgement arrived, whichever kind.
	t       time.Duration
	lastAck time.Time
	// frac is the growth below one byte, carried: each acknowledgement
	// earns (target − cwnd) · acked / cwnd bytes, usually a fraction.
	frac float64
}

const (
	// cubicC scales the curve, in segments per second cubed (§4.1, §5:
	// the value every deployment has run for a decade).
	cubicC = 0.4
	// cubicBeta is the multiplicative decrease (§4.6).
	cubicBeta = 0.7
	// cubicAlpha is the Reno-friendly region's increase per round trip
	// (§4.3, Figure 4): the α that gives AIMD(α, β) Reno's average window.
	cubicAlpha = 3 * (1 - cubicBeta) / (1 + cubicBeta)
)

var _ congestionController = (*cubic)(nil)

func newCubic(maxDatagram int) *cubic {
	c := &cubic{newReno: *newNewReno(maxDatagram)}
	c.hs = newHystart()
	c.ssPace = 2.5 // newReno.pacingRate
	return c
}

// wCubic is Figure 1: the window the curve names t seconds into the stage.
func (c *cubic) wCubic(t float64) float64 {
	d := t - c.k
	return cubicC*float64(c.maxDatagram)*d*d*d + c.wMax
}

// startEpoch opens an avoidance stage at the current window (§4.2). K is
// how long the curve takes to climb back to wMax from here, zero when there
// is nothing to climb back to: after persistent congestion, §4.8 says the
// stage starts with K = 0 and wMax at its own first window, and
// onPersistentCongestion forgets wMax for that. The Reno estimate starts at
// the same window (§4.3) and the clock at zero.
func (c *cubic) startEpoch(now time.Time) {
	c.inEpoch = true
	c.cwndEpoch = float64(c.cwnd)
	if c.wMax < c.cwndEpoch {
		c.wMax = c.cwndEpoch
	}
	c.k = math.Cbrt((c.wMax - c.cwndEpoch) / (cubicC * float64(c.maxDatagram)))
	c.wEst = c.cwndEpoch
	c.t = 0
	c.lastAck = now
	c.frac = 0
}

// onAcked grows the window: Reno's way in slow start, HyStart++ included
// (newReno.slowStart; the round it reads is Reno's §7.8 round, closed by
// absorb), and in avoidance by what the curve or the Reno
// estimate says, whichever is higher (§4.2). When HyStart++ ends the slow
// start the threshold is the window and the stage opens at the next
// acknowledgement with wMax there and K = 0 (§4.10 of RFC 9438).
// The clock first: an acknowledgement that earns growth advances t by the
// time since the previous acknowledgement, and one that does not — the
// window was not the limit this round — moves lastAck without advancing t,
// so the round is dropped from the curve. The advance is also bounded by
// the packet's own round trip: after an idle period the first
// acknowledgement arrives a long gap after the previous one, and the
// sender was not using the window during it either.
func (c *cubic) onAcked(ev ackEvent) {
	inRound := !c.roundMark.IsZero()
	earns := c.absorb(ev)
	// Strict, as RFC 9002 §7.3.1 and §7.3.3 draw the line (slow start
	// below ssthresh, avoidance at or above it). RFC 9438 §4.10 writes "no
	// more than", which read to the letter would put a window just reduced
	// to ssthresh (§4.6) back into slow start, against §4.2 and §4.6 of
	// the same text; the difference is one growth step at equality.
	if c.cwnd < c.ssthresh {
		c.slowStart(ev, earns, inRound && c.roundMark.IsZero())
		return
	}
	if !c.inEpoch {
		c.startEpoch(ev.now)
	}
	if earns {
		c.t += min(ev.now.Sub(c.lastAck), ev.now.Sub(ev.sentAt))
	}
	c.lastAck = ev.now
	if !earns {
		return
	}

	cwnd := float64(c.cwnd)
	acked := float64(ev.size)
	// Figure 4, in bytes: segments_acked / cwnd is acked / cwnd whatever
	// the unit, and α is in segments.
	alpha := 1.0
	if c.wEst < c.cwndPrior {
		alpha = cubicAlpha
	}
	c.wEst += alpha * float64(c.maxDatagram) * acked / cwnd
	t := c.t.Seconds()
	if c.wCubic(t) < c.wEst {
		// Reno-friendly region (§4.3): the window is the estimate, and
		// never less than it already is — the estimate lags the curve the
		// window just left.
		if w := int(c.wEst); w > c.cwnd {
			c.cwnd = w
		}
		return
	}
	// Concave or convex (§4.4, §4.5): the window reaches target, the
	// curve's value one round trip ahead, over one round trip's worth of
	// acknowledgements, never less than itself and never faster than
	// slow start would.
	target := min(max(c.wCubic(t+ev.srtt.Seconds()), cwnd), 1.5*cwnd)
	c.frac += (target - cwnd) * acked / cwnd
	if g := math.Floor(c.frac); g >= 1 {
		c.cwnd += int(g)
		c.frac -= g
	}
}

// onLost shrinks the window once per recovery period, under Reno's B.8
// gate, to β of itself (§4.6, Figure 5, with the window in place of the
// flight: the §7.8 utilisation test in absorb is the "other measure" the
// section requires of a sender that does so). Before that, wMax remembers
// the window the loss happened at — or less, when the loss came below the
// previous wMax: the path's saturation point is falling, and giving back
// more leaves room for whoever is taking it (§4.7, fast convergence). A
// flow alone on its path has no one to leave room for, and §4.7 wants the
// heuristic off there: DisableFastConvergence keeps wMax at the window.
// The new stage starts at the reduced window. Nothing is sent from here:
// see newReno.onLost on B.6's MaybeSendOnePacket.
func (c *cubic) onLost(ev lossEvent) {
	c.inFlight = max(c.inFlight-ev.bytes, 0)
	if !c.recoveryStart.IsZero() && !ev.latestSent.After(c.recoveryStart) {
		return // RFC 9002 B.5: sent at or before the start, inside the period
	}
	c.recoveryStart = ev.now
	cwnd := float64(c.cwnd)
	if cwnd < c.wMax && !c.noFastConvergence {
		c.wMax = cwnd * (1 + cubicBeta) / 2
	} else {
		c.wMax = cwnd
	}
	c.cwndPrior = cwnd
	// The floor is RFC 9002's minimum window of two packets (§4.8, and
	// §7.2 names it for a loss and for a CE mark alike). RFC 9438 §4.6 has
	// CUBIC reduce on ECN-Echo down to 1 SMSS and then back off a timer:
	// that is TCP's floor (RFC 3168). A QUIC window of one packet waits a
	// PTO on its first loss, and §4.8 calls two safe because the probe
	// backoff of §6.2 still throttles persistent congestion.
	c.ssthresh = max(int(math.Round(cwnd*cubicBeta)), 2*c.maxDatagram)
	c.cwnd = c.ssthresh
	c.hs.disarm()
	c.startEpoch(ev.now)
}

// onMarked is the multiplicative decrease of a loss (RFC 9438 §3.1 reads a
// CE mark as a congestion event) with nothing leaving the flight: the
// marked packets were delivered. Dated by the latest tracked packet the
// frame acknowledges (RFC 9002 B.7), once per recovery period.
func (c *cubic) onMarked(ev markEvent) {
	c.onLost(lossEvent{now: ev.now, latestSent: ev.latestSent})
}

// onPersistentCongestion collapses the window the way Reno does (RFC 9002
// §7.6.2); the slow-start threshold stays what the loss in the same batch
// set it to, β of the window. The stage is closed and wMax forgotten: a
// persistent-congestion verdict is QUIC's timeout, and §4.8 has the first
// avoidance stage after one start with K = 0 and wMax at its own first
// window — the curve is convex from there, the plateau the old path had
// is not evidence about the path that comes back.
func (c *cubic) onPersistentCongestion() {
	c.cwnd = 2 * c.maxDatagram
	c.recoveryStart = time.Time{}
	c.inEpoch = false
	c.wMax = 0
	c.hs.disarm()
}
