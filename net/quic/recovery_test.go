package quic

import (
	"errors"
	"math"
	"testing"
	"time"
)

func ackFrameFor(t *testing.T, tracker *ackTracker) Frame {
	t.Helper()
	raw := tracker.appendAck(nil, time.Now())
	frames, err := parseFrames(nil, raw)
	if err != nil || len(frames) != 1 {
		t.Fatalf("building an ACK: %v", err)
	}
	return frames[0]
}

func TestRTTEstimator(t *testing.T) {
	var r rttEstimator
	if r.current() != initialRTT {
		t.Errorf("before a sample, current = %v", r.current())
	}
	r.sample(100*time.Millisecond, 0)
	if r.smoothed != 100*time.Millisecond || r.min != 100*time.Millisecond {
		t.Errorf("first sample: smoothed %v min %v", r.smoothed, r.min)
	}
	// The peer's declared delay is subtracted so its ACK pacing does not
	// read as path latency…
	r.sample(200*time.Millisecond, 100*time.Millisecond)
	if r.smoothed >= 200*time.Millisecond {
		t.Errorf("ack delay was not subtracted: smoothed %v", r.smoothed)
	}
	// …but never below the smallest round trip seen.
	r.sample(100*time.Millisecond, 90*time.Millisecond)
	if r.min < 100*time.Millisecond {
		t.Errorf("the adjustment undercut the minimum: %v", r.min)
	}
}

// Packet-threshold loss: acknowledging packet 3 declares packet 0 lost, and
// its payload comes back for retransmission.
func TestPacketThresholdLoss(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	for pn := range uint64(4) {
		s.record(sentPacket{
			pn: pn, sentAt: now.Add(-time.Duration(4-pn) * time.Millisecond),
			size: 100, ackEliciting: true, payload: []byte{byte(pn)},
		})
	}
	var peer ackTracker
	peer.record(3, now)
	f := ackFrameFor(t, &peer)

	out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAckedCount != 1 || out.newlyAcked != 100 {
		t.Fatalf("acked %d packets, %d bytes", out.newlyAckedCount, out.newlyAcked)
	}
	if len(out.lost) != 1 || out.lost[0][0] != 0 {
		t.Fatalf("lost payloads = %v, want packet 0's", out.lost)
	}
	// Packets 1 and 2 are too new by count and not past the time threshold.
	if len(s.packets) != 2 {
		t.Fatalf("%d packets still tracked, want 2", len(s.packets))
	}
}

// An ACK straddling a recovery exit: packets sent before recovery began
// release flight bytes without earning growth, packets sent after earn
// theirs. processAck hands the controller each packet on its own, so one
// pre-recovery straggler cannot silence the whole frame's growth (RFC 9002
// B.5 runs the test per packet); an aggregate gated on the oldest packet
// used to.
func TestProcessAckGrowableStraddlesRecovery(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	recovery := now.Add(-10 * time.Millisecond)
	s.record(sentPacket{pn: 0, sentAt: recovery.Add(-time.Millisecond), size: 100, ackEliciting: true})
	s.record(sentPacket{pn: 1, sentAt: recovery.Add(time.Millisecond), size: 100, ackEliciting: true})
	cc := newNewReno(1200)
	cc.recoveryStart = recovery
	cc.onSent(recovery.Add(-time.Millisecond), 100)
	cc.onSent(recovery.Add(time.Millisecond), 100)
	cc.limited = true // the window was the limit this round: growth is earned
	start := cc.cwnd

	var peer ackTracker
	peer.record(0, now)
	peer.record(1, now)
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, cc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAcked != 200 {
		t.Fatalf("acked %d bytes, want 200", out.newlyAcked)
	}
	if cc.cwnd != start+100 {
		t.Fatalf("the window grew by %d, want only the post-recovery packet's 100", cc.cwnd-start)
	}
}

// Time-threshold loss: a packet older than 9/8 of the round trip is gone
// even when the packet distance is small.
func TestTimeThresholdLoss(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(10*time.Millisecond, 0)
	now := time.Now()
	s.record(sentPacket{pn: 0, sentAt: now.Add(-time.Second), size: 50, ackEliciting: true, payload: []byte{0}})
	s.record(sentPacket{pn: 1, sentAt: now, size: 50, ackEliciting: true, payload: []byte{1}})

	var peer ackTracker
	peer.record(1, now)
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.lost) != 1 || out.lost[0][0] != 0 {
		t.Fatalf("lost = %v, want the second-old packet", out.lost)
	}
}

// RFC 9002 §6.1, A.10: the loss timer only matures packets sent before the
// largest acknowledged one. A packet sent after the ACK that armed the
// timer is not a candidate however old it grows — nothing newer was
// delivered to prove it missing — and it must not re-arm the timer either.
// Swept by age alone, it was a congestion event on a clean path: a DPLPMTUD
// probe's hole armed the timer and the transfer behind it, older than the
// 1 ms floor but not yet acknowledged, halved the window.
func TestLossTimerSparesPacketsPastLargestAcked(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(10*time.Millisecond, 0)
	now := time.Now()
	s.record(sentPacket{pn: 0, sentAt: now.Add(-time.Millisecond), size: 50, ackEliciting: true, payload: []byte{0}})
	s.record(sentPacket{pn: 1, sentAt: now, size: 50, ackEliciting: true, payload: []byte{1}})

	// The ACK of 1 leaves 0 unacknowledged and 1 ms old: too young for a
	// 11.25 ms threshold, so the timer is armed on it.
	var peer ackTracker
	peer.record(1, now)
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.lost) != 0 || s.lossTime.IsZero() {
		t.Fatalf("after the ACK: lost = %v, lossTime = %v, want nothing lost and the timer armed", out.lost, s.lossTime)
	}
	// Packet 2 leaves after that ACK; nothing newer than 1 is acknowledged.
	s.record(sentPacket{pn: 2, sentAt: now, size: 50, ackEliciting: true, payload: []byte{2}})

	// Long after the timer matured, both 0 and 2 are older than the threshold.
	out = s.collectTimeLosses(s.lossTime.Add(time.Second), &rtt)
	if len(out.lost) != 1 || out.lost[0][0] != 0 {
		t.Fatalf("lost = %v, want packet 0 alone: packet 2 was sent after the largest acknowledged", out.lost)
	}
	if len(s.packets) != 1 || s.packets[0].pn != 2 {
		t.Errorf("packets kept = %v, want packet 2 alone", s.packets)
	}
	if !s.lossTime.IsZero() {
		t.Errorf("lossTime = %v, want the timer disarmed: packet 2 is no candidate", s.lossTime)
	}
}

// reno is the connection's controller as the concrete NewReno, for tests
// that read or set its fields.
func (c *Conn) reno() *newReno { return c.cc.(*newReno) }

func TestNewRenoWindow(t *testing.T) {
	cc := newNewReno(1200)
	start := cc.cwnd
	if start != 12000 {
		t.Fatalf("initial window = %d", start)
	}
	now := time.Now()

	// Slow start: acknowledged bytes grow the window byte for byte — the
	// flight fills the window, so the window is what limits the sender.
	cc.onSent(now, 12000)
	cc.onAcked(ackEvent{now: now, sentAt: now, size: 2400})
	if cc.cwnd != start+2400 {
		t.Errorf("slow start grew %d, want %d", cc.cwnd-start, 2400)
	}

	// A packet sent before a recovery start releases flight but earns
	// nothing: the window must not move on it.
	cc.recoveryStart = now
	cc.onSent(now, 1200)
	cc.onAcked(ackEvent{now: now, sentAt: now.Add(-time.Second), size: 1200})
	if cc.cwnd != start+2400 {
		t.Errorf("a pre-recovery packet moved the window to %d", cc.cwnd)
	}

	// Loss halves it, once per recovery period.
	cc.onSent(now, 1200)
	cc.onLost(lossEvent{now: now, latestSent: now, bytes: 1200})
	half := cc.cwnd
	if half != (start+2400)/2 {
		t.Errorf("after loss, cwnd = %d", half)
	}
	cc.onLost(lossEvent{now: now, latestSent: now.Add(-time.Second)}) // sent before recovery began
	if cc.cwnd != half {
		t.Error("a loss inside the same recovery period shrank the window again")
	}

	// Persistent congestion collapses to the minimum.
	cc.onPersistentCongestion()
	if cc.cwnd != 2*1200 {
		t.Errorf("collapsed window = %d", cc.cwnd)
	}
}

// A window the sender never fills must not grow (RFC 9002 §7.8): a thousand
// acknowledgements of a flight far below the window leave it where it was,
// while the same acknowledgements after the window refused a send grow it
// as slow start always did. The flight at half the window is the slow-start
// boundary; avoidance grows on a refusal alone; a refusal counts for its
// round, which the acknowledgement of a packet sent after it closes.
func TestNewRenoUnderutilizedWindow(t *testing.T) {
	const datagram = 1200
	now := time.Now()
	t.Run("slow start, flight far below the window", func(t *testing.T) {
		cc := newNewReno(datagram)
		start := cc.cwnd
		for range 1000 {
			cc.onSent(now, datagram)
			cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram})
		}
		if cc.cwnd != start {
			t.Fatalf("cwnd = %d after 1000 application-limited ACKs, want %d unchanged", cc.cwnd, start)
		}
	})
	t.Run("slow start, the window refused a send", func(t *testing.T) {
		cc := newNewReno(datagram)
		start := cc.cwnd
		for range 1000 {
			cc.onSent(now, datagram)
			if cc.canSend(cc.cwnd) {
				t.Fatal("a send of a whole window on top of a flight was not refused")
			}
			cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram})
		}
		if want := start + 1000*datagram; cc.cwnd != want {
			t.Fatalf("cwnd = %d after 1000 window-limited ACKs, want %d", cc.cwnd, want)
		}
	})
	t.Run("slow start, flight at half the window", func(t *testing.T) {
		cc := newNewReno(datagram)
		start := cc.cwnd
		cc.onSent(now, start/2)
		cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram})
		if cc.cwnd != start+datagram {
			t.Fatalf("cwnd = %d with the flight at half the window, want %d", cc.cwnd, start+datagram)
		}
		// The window grew; the same flight is now below half of it.
		cc.onSent(now, datagram)
		cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram})
		if cc.cwnd != start+datagram {
			t.Fatalf("cwnd = %d with the flight under half the window, want %d unchanged", cc.cwnd, start+datagram)
		}
	})
	t.Run("avoidance grows on a refusal alone", func(t *testing.T) {
		cc := newNewReno(datagram)
		cc.ssthresh = cc.cwnd
		start := cc.cwnd
		for range 1000 {
			cc.onSent(now, start-datagram) // nearly full, never refused
			cc.onAcked(ackEvent{now: now, sentAt: now, size: start - datagram})
		}
		if cc.cwnd != start {
			t.Fatalf("cwnd = %d after 1000 unrefused ACKs in avoidance, want %d unchanged", cc.cwnd, start)
		}
		cc.onSent(now, start)
		if cc.canSend(datagram) {
			t.Fatal("a datagram on top of a full window was not refused")
		}
		cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram})
		if want := start + datagram*datagram/start; cc.cwnd != want {
			t.Fatalf("cwnd = %d after a window-limited ACK in avoidance, want %d", cc.cwnd, want)
		}
	})
	t.Run("a refusal is spent with its round", func(t *testing.T) {
		cc := newNewReno(datagram)
		start := cc.cwnd
		cc.onSent(now, start)
		if cc.canSend(datagram) {
			t.Fatal("a datagram on top of a full window was not refused")
		}
		cc.onAcked(ackEvent{now: now, sentAt: now, size: start}) // the whole flight: the window grows once…
		if cc.cwnd != 2*start {
			t.Fatalf("cwnd = %d after the full flight was acknowledged, want %d", cc.cwnd, 2*start)
		}
		cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram}) // …still within the round, so again…
		if cc.cwnd != 2*start+datagram {
			t.Fatalf("cwnd = %d after a second ACK of the refused round, want %d", cc.cwnd, 2*start+datagram)
		}
		cc.onSent(now, datagram)                                    // the first send after the round opens the next one
		cc.onAcked(ackEvent{now: now, sentAt: now, size: datagram}) // …and not on the next small exchange
		if cc.cwnd != 2*start+datagram {
			t.Fatalf("cwnd = %d after the refusal was spent, want %d unchanged", cc.cwnd, 2*start+datagram)
		}
	})
}

// The probe timeout grows with variance and doubles per unanswered probe —
// checked through the estimator since the connection composes them.
func TestProbeTimeoutComposition(t *testing.T) {
	var r rttEstimator
	r.sample(100*time.Millisecond, 0)
	base := r.pto(25 * time.Millisecond)
	if base <= 100*time.Millisecond {
		t.Errorf("pto = %v, must exceed the round trip", base)
	}
	if r.pto(0) >= base {
		t.Error("max_ack_delay was not added for the application space")
	}
}

// BenchmarkProcessAck measures the ACK-processing hot path: an authenticated
// peer sends one ACK per received datagram, and each used to cost a
// map[uint64]bool allocation plus a full rescan of the in-flight slice per
// declared range. The two-pointer merge is expected to hold 0 allocs/op.
func BenchmarkProcessAck(b *testing.B) {
	var rtt rttEstimator
	rtt.sample(100*time.Millisecond, 0)
	now := time.Now()

	const inFlight = 128
	template := make([]sentPacket, inFlight)
	for pn := range uint64(inFlight) {
		template[pn] = sentPacket{pn: pn, sentAt: now, size: 1200, ackEliciting: true, sendElapsed: 10 * time.Millisecond, sinceDelivered: 20 * time.Millisecond}
	}

	// Eight ranges acknowledging every other run of eight packets — the
	// reordered-but-mostly-delivered shape loss recovery actually sees.
	var peer ackTracker
	for pn := range uint64(inFlight) {
		if pn/8%2 == 0 {
			peer.record(pn, now)
		}
	}
	frame := appendAckFrame(nil, peer.ranges, 0, [3]uint64{})
	f, _, err := parseFrame(frame)
	if err != nil {
		b.Fatal(err)
	}

	s := sentTracker{packets: make([]sentPacket, inFlight)}
	est := &deliveryEstimator{}
	b.ReportAllocs()
	for b.Loop() {
		s.packets = s.packets[:inFlight]
		copy(s.packets, template)
		s.hasLargestAcked = false
		if _, err := s.processAck(now, f.largest, f.Data, &rtt, nil, est); err != nil {
			b.Fatal(err)
		}
	}
}

// The pacer's bucket: starts full at one initial window, spends below zero,
// refills at the controller's rate — NewReno's is 5/4 of its window per
// smoothed round trip, doubled in slow start — and prices a deficit in time,
// with sub-granularity deficits released immediately, since the timer could
// not honour them anyway.
func TestPacerBucket(t *testing.T) {
	now := time.Now()
	p := pacer{burst: 12000}
	cc := newNewReno(1200)
	cc.ssthresh = cc.cwnd // avoidance: the rate is the window itself, 12000
	const srtt = 100 * time.Millisecond
	rate := cc.pacingRate(srtt)
	if rate != 150000 {
		t.Fatalf("NewReno paces at %v B/s, want 5/4 × 12000 per 100ms = 150000", rate)
	}
	cc.ssthresh = 1 << 30
	if r := cc.pacingRate(srtt); r != 2*rate {
		t.Fatalf("NewReno paces slow start at %v B/s, want twice the avoidance rate", r)
	}

	p.refill(now, rate)
	if p.tokens != 12000 {
		t.Fatalf("first refill filled to %d, want the burst 12000", p.tokens)
	}
	if d := p.delay(rate); d != 0 {
		t.Fatalf("a full bucket priced a %v wait", d)
	}

	// Spend the bucket and one packet beyond: the deficit prices the wait.
	p.spend(12000)
	p.spend(1200)
	// 1200 bytes at 150000 B/s take 8ms.
	if d := p.delay(rate); d != 8*time.Millisecond {
		t.Fatalf("deficit of 1200 priced at %v, want 8ms", d)
	}

	// Half the deficit's time passes: the wait halves.
	p.refill(now.Add(4*time.Millisecond), rate)
	if d := p.delay(rate); d != 4*time.Millisecond {
		t.Fatalf("after 4ms the deficit prices at %v, want 4ms", d)
	}

	// A sub-granularity deficit is released immediately.
	p.refill(now.Add(8*time.Millisecond), rate)
	p.spend(100)
	if d := p.delay(rate); d != 0 {
		t.Fatalf("a deficit under one granularity priced a %v wait", d)
	}

	// A long idle refills to the burst, never beyond.
	p.refill(now.Add(10*time.Second), rate)
	if p.tokens != 12000 {
		t.Fatalf("after idling, tokens = %d, want the burst", p.tokens)
	}
}

// A skipped packet number (RFC 9000 §21.4): an ACK whose ranges go around
// it is ordinary — nothing is declared lost for a number never sent — and
// an ACK whose range covers it is the optimistic-ACK verdict.
func TestSkippedPacketNumberCatchesOptimisticAck(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	s.skipArmed, s.nextSkip = true, 3 // the draw, pinned
	skipped := -1
	for pn := range uint64(6) {
		if s.skipNumber(pn, 10) {
			skipped = int(pn)
			continue
		}
		s.record(sentPacket{pn: pn, sentAt: now, size: 100, ackEliciting: true, payload: []byte{byte(pn)}})
	}
	if skipped != 3 || !s.hasSkipped || s.skipped != 3 {
		t.Fatalf("skipped %d, tracker says %d (%v)", skipped, s.skipped, s.hasSkipped)
	}

	var peer ackTracker
	for _, pn := range []uint64{0, 1, 2, 4, 5} {
		peer.record(pn, now)
	}
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
	if err != nil {
		t.Fatalf("an ACK around the gap was refused: %v", err)
	}
	if out.newlyAckedCount != 5 || out.lostCount != 0 || len(s.packets) != 0 {
		t.Fatalf("around the gap: acked %d, lost %d, kept %d", out.newlyAckedCount, out.lostCount, len(s.packets))
	}

	peer.record(3, now)
	f = ackFrameFor(t, &peer)
	if _, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil); !errors.Is(err, errOptimisticAck) {
		t.Fatalf("an ACK covering the gap: err = %v, want errOptimisticAck", err)
	}
}

// The gaps land about one window apart, never two in a row, and the first
// one only after a window has been sent.
func TestSkippedPacketNumberOncePerWindow(t *testing.T) {
	var s sentTracker
	const window, sent = 10, 10_000
	skips, prev := 0, -2
	for pn := range uint64(sent) {
		if !s.skipNumber(pn, window) {
			continue
		}
		if int(pn) == prev+1 {
			t.Fatalf("numbers %d and %d both skipped", prev, pn)
		}
		if skips == 0 && pn < window/2 {
			t.Fatalf("first gap at %d, before half a window", pn)
		}
		prev, skips = int(pn), skips+1
	}
	// The gap is uniform in [½w, 1½w): one skip per w+1 numbers on average.
	if lo, hi := sent/(2*window), sent/(window/2); skips < lo || skips > hi {
		t.Fatalf("%d gaps in %d numbers with a window of %d, want between %d and %d", skips, sent, window, lo, hi)
	}
}

// RFC 9002 §7.6.2: the span an ACK's loss batch reports is measured between
// ack-eliciting losses only, and an acknowledged packet between two losses
// starts it over — whether this frame acknowledged it or an earlier one did.
func TestProcessAckMeasuresTheLostSpan(t *testing.T) {
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(-time.Duration(ms) * time.Millisecond) }
	sent := func(pn uint64, ms int, eliciting bool) sentPacket {
		return sentPacket{pn: pn, sentAt: at(ms), size: 50, ackEliciting: eliciting, payload: []byte{byte(pn)}}
	}
	ack := func(t *testing.T, s *sentTracker, now time.Time, pns ...uint64) ackOutcome {
		t.Helper()
		var peer ackTracker
		for _, pn := range pns {
			peer.record(pn, now)
		}
		f := ackFrameFor(t, &peer)
		var rtt rttEstimator
		out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	t.Run("between the oldest and newest ack-eliciting losses", func(t *testing.T) {
		var s sentTracker
		s.record(sent(0, 1000, true))
		s.record(sent(1, 900, true))
		s.record(sent(2, 800, false)) // an ACK-only packet: no bound, no break
		s.record(sent(3, 700, true))
		s.record(sent(6, 0, true))
		out := ack(t, &s, now, 6)
		if out.lostCount != 4 || out.lostSpan != 300*time.Millisecond {
			t.Errorf("%d losses spanning %v, want 4 spanning 300ms (packets 0 to 3)", out.lostCount, out.lostSpan)
		}
	})
	t.Run("an acknowledgement in the same frame splits the run", func(t *testing.T) {
		var s sentTracker
		for pn, ms := range []int{1000, 900, 800, 700, 600} {
			s.record(sent(uint64(pn), ms, true)) //nolint:gosec // G115: a loop index
		}
		s.record(sent(6, 0, true))
		out := ack(t, &s, now, 2, 6) // packet 2 sits between the losses
		if out.lostCount != 4 || out.lostSpan != 100*time.Millisecond {
			t.Errorf("%d losses spanning %v, want 4 spanning 100ms (runs 0–1 and 3–4)", out.lostCount, out.lostSpan)
		}
	})
	t.Run("an earlier acknowledgement splits it too", func(t *testing.T) {
		var s sentTracker
		for pn, ms := range []int{1000, 900, 800, 700, 600} {
			s.record(sent(uint64(pn), ms, true)) //nolint:gosec // G115: a loop index
		}
		s.record(sent(6, 0, true))
		if out := ack(t, &s, at(650), 2); out.lostCount != 0 {
			t.Fatalf("%d losses on the first ACK, want none yet", out.lostCount)
		}
		out := ack(t, &s, now, 6)
		if out.lostCount != 4 || out.lostSpan != 100*time.Millisecond {
			t.Errorf("%d losses spanning %v, want 4 spanning 100ms (packet 2 was acknowledged before)", out.lostCount, out.lostSpan)
		}
	})
}

// spyController counts what the connection tells its congestion controller
// and lets a real NewReno answer, so the connection behaves as usual while
// every site that must report is checked for reporting.
type spyController struct {
	congestionController
	sent, acked, lost, collapsed, lifted, marked int
}

func (s *spyController) onSent(now time.Time, bytes int) {
	s.sent++
	s.congestionController.onSent(now, bytes)
}

func (s *spyController) onAcked(ev ackEvent) {
	s.acked++
	s.congestionController.onAcked(ev)
}

func (s *spyController) onLost(ev lossEvent) {
	s.lost++
	s.congestionController.onLost(ev)
}

func (s *spyController) onPersistentCongestion() {
	s.collapsed++
	s.congestionController.onPersistentCongestion()
}

func (s *spyController) onLifted(bytes int) {
	s.lifted++
	s.congestionController.onLifted(bytes)
}

func (s *spyController) onMarked(ev markEvent) {
	s.marked++
	s.congestionController.onMarked(ev)
}

// Every event the connection owes its congestion controller reaches it
// through the interface: a send, each acknowledged packet, a batch of
// losses, the persistent-congestion collapse that follows one, and the
// lift of a packet a probe timeout resends. A second algorithm relies on
// this; a site that bypassed the interface would starve it.
func TestEverySiteReportsToTheController(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.mu.Lock()
	spy := &spyController{congestionController: c.cc}
	c.cc = spy
	// A round-trip sample, so that persistent congestion can be judged:
	// pto = 100ms + 1ms + 25ms, the duration is three of those, 378ms.
	c.rtt = rttEstimator{latest: 100 * time.Millisecond, min: 100 * time.Millisecond, smoothed: 100 * time.Millisecond, sampled: true}
	sp := &c.spaces[spaceInitial]
	// Five ack-eliciting packets spread over 400ms, all about to be lost,
	// and a sixth the peer acknowledges.
	for pn := range uint64(5) {
		sp.sent.record(sentPacket{pn: pn, sentAt: now.Add(-time.Second + time.Duration(pn)*100*time.Millisecond), size: 1200, ackEliciting: true, payload: []byte{framePing, byte(pn)}})
	}
	sp.sent.record(sentPacket{pn: 5, sentAt: now, size: 100, ackEliciting: true})
	sp.nextPN = 6
	c.mu.Unlock()

	var peer ackTracker
	peer.record(5, now)
	if err := c.handleAck(spaceInitial, ackFrameFor(t, &peer), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if spy.acked != 1 {
		t.Errorf("onAcked called %d times for one acknowledged packet, want 1", spy.acked)
	}
	if spy.lost != 1 {
		t.Errorf("onLost called %d times for one batch of losses, want 1", spy.lost)
	}
	if spy.collapsed != 1 {
		t.Errorf("onPersistentCongestion called %d times for losses spanning 400ms, want 1", spy.collapsed)
	}

	// A send through the sending path.
	sp.retrans = nil // what the losses queued; the probe below must lift, not drain
	if err := c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{}); err != nil {
		t.Fatal(err)
	}
	if spy.sent != 1 {
		t.Errorf("onSent called %d times for one packet, want 1", spy.sent)
	}

	// A probe timeout with nothing queued lifts that packet out of the
	// record and resends its payload as new packets — two of them, the
	// probe's fixed count (sendProbeLocked).
	c.sendProbeLocked(spaceInitial)
	if spy.lifted != 1 {
		t.Errorf("onLifted called %d times for one lifted packet, want 1", spy.lifted)
	}
	if spy.sent != 3 {
		t.Errorf("onSent called %d times after the probe resent the lifted packet twice, want 3", spy.sent)
	}
}

// ackCovering builds and parses an ACK frame covering exactly pns.
func ackCovering(t *testing.T, now time.Time, pns ...uint64) Frame {
	t.Helper()
	var peer ackTracker
	for _, pn := range pns {
		peer.record(pn, now)
	}
	f, _, err := parseFrame(appendAckFrame(nil, peer.ranges, 0, [3]uint64{}))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// The delivery-rate sample, by hand (draft-ietf-ccwg-bbr-06 §4.1.2.3).
// Three packets of 1000 bytes leave an idle connection 10 ms apart and
// are acknowledged together 120 ms after the first: the sample is re-based on
// the newest, whose send interval is 20 ms and whose acknowledgement
// interval is 120 ms, so 3000 bytes over 120 ms. A fourth packet sent 5 ms
// after that ACK and acknowledged 5 ms later has it the other way round —
// 10 ms since the last delivery, but 105 ms since the first send of its
// flight — and the longer interval wins: 1000 bytes over 105 ms, not over
// 10 ms, which is the filter against ACK compression (§2.2.4). A fifth
// packet sent after the writer ran dry is marked application-limited, its
// sample leaves Stats alone, and the mark lifts once it is delivered: the
// sixth is not marked.
func TestDeliveryRateSampleByHand(t *testing.T) {
	var est deliveryEstimator
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(100*time.Millisecond, 0)
	cc := &rateSpy{congestionController: newNewReno(1200)}
	t0 := time.Now()
	send := func(pn uint64, at time.Duration, idle bool) {
		p := sentPacket{pn: pn, sentAt: t0.Add(at), size: 1000, ackEliciting: true}
		est.onSent(&p, idle)
		s.record(p)
	}
	ack := func(at time.Duration, pns ...uint64) rateSample {
		f := ackCovering(t, t0.Add(at), pns...)
		if _, err := s.processAck(t0.Add(at), f.largest, f.Data, &rtt, cc, &est); err != nil {
			t.Fatal(err)
		}
		return est.cur
	}

	send(0, 0, true)
	send(1, 10*time.Millisecond, false)
	send(2, 20*time.Millisecond, false)
	rs := ack(120*time.Millisecond, 0, 1, 2)
	if rs.delivered != 3000 || rs.interval != 120*time.Millisecond || rs.appLimited {
		t.Fatalf("three packets acknowledged together: %+v, want 3000 bytes over 120ms", rs)
	}
	if got, want := est.rate, 3000/0.120; math.Abs(got-want) > 1e-6 {
		t.Fatalf("Stats rate %v, want %v", got, want)
	}
	// The controller reads the same sample off the frame's newest packet.
	if got := cc.last.rateSample(); got != rs {
		t.Fatalf("ackEvent.rateSample() = %+v, the estimator says %+v", got, rs)
	}

	send(3, 125*time.Millisecond, false)
	rs = ack(130*time.Millisecond, 3)
	if rs.delivered != 1000 || rs.interval != 105*time.Millisecond {
		t.Fatalf("send interval 105ms against ack interval 10ms: %+v, want the longer", rs)
	}
	if got, want := est.rate, 1000/0.105; math.Abs(got-want) > 1e-6 {
		t.Fatalf("Stats rate %v, want %v", got, want)
	}

	// The writer runs dry with nothing in flight: the next packet is
	// application-limited, and so is its sample.
	est.markAppLimited(0)
	before := est.rate
	send(4, 240*time.Millisecond, true)
	rs = ack(360*time.Millisecond, 4)
	if !rs.appLimited {
		t.Fatalf("packet sent after the writer ran dry: %+v, want application-limited", rs)
	}
	if est.rate != before {
		t.Fatalf("an application-limited sample moved Stats: %v, was %v", est.rate, before)
	}
	if est.appLimited != 0 {
		t.Fatalf("the bubble is delivered (%d > mark %d) and the mark stands", est.delivered, est.appLimited)
	}
	send(5, 370*time.Millisecond, true)
	if rs = ack(490*time.Millisecond, 5); rs.appLimited {
		t.Fatalf("packet sent after the mark lifted: %+v, want unmarked", rs)
	}
	if est.rate == before {
		t.Fatal("a reliable, unmarked sample left Stats where it was")
	}
}

// rateSpy counts the application-limited samples a controller is handed.
type rateSpy struct {
	congestionController
	appLimited, samples int
	last                ackEvent
}

func (s *rateSpy) onAcked(ev ackEvent) {
	s.samples++
	s.last = ev
	if ev.rateSample().appLimited {
		s.appLimited++
	}
	s.congestionController.onAcked(ev)
}

// An ACK frame hands the controller its packets in order, the newest last
// and alone rated: its sample is the frame's (§4.1.2.3), and the max filter
// reads no other. A sample shorter than the minimum round trip is no
// sample (§4.1.2.3): the newest still arrives, unrated.
func TestProcessAckRatesTheNewestPacket(t *testing.T) {
	for _, tc := range []struct {
		minRTT time.Duration
		rated  bool
	}{{20 * time.Millisecond, true}, {40 * time.Millisecond, false}} {
		var s sentTracker
		rtt := rttEstimator{min: tc.minRTT}
		now := time.Now()
		var peer ackTracker
		for pn := range uint64(3) {
			s.record(sentPacket{pn: pn, sentAt: now.Add(-30 * time.Millisecond), size: 1200, ackEliciting: true})
			peer.record(pn, now)
		}
		spy := &eventSpy{congestionController: newNewReno(1200)}
		f := ackFrameFor(t, &peer)
		if _, err := s.processAck(now, f.largest, f.Data, &rtt, spy, &deliveryEstimator{}); err != nil {
			t.Fatal(err)
		}
		if len(spy.events) != 3 {
			t.Fatalf("%d events for a frame of three packets", len(spy.events))
		}
		for i, ev := range spy.events {
			if want := tc.rated && i == 2; ev.rated != want || ev.delivered != (i+1)*1200 {
				t.Fatalf("min_rtt %v, event %d: rated %v delivered %d, want %v and %d", tc.minRTT, i, ev.rated, ev.delivered, want, (i+1)*1200)
			}
		}
	}
}

// eventSpy records the acknowledgements a controller is handed.
type eventSpy struct {
	congestionController
	events []ackEvent
}

func (s *eventSpy) onAcked(ev ackEvent) {
	s.events = append(s.events, ev)
	s.congestionController.onAcked(ev)
}

// A short write on a connection with nothing in flight is the
// application-limited case the estimator exists to tell apart (draft §2.3):
// the window had room, the writer ran dry, and the sample its acknowledgement
// yields says so — and Stats.DeliveryRate, which skips such samples, does
// not move.
func TestShortWriteIsApplicationLimited(t *testing.T) {
	client, _ := plainPair(t)
	spy := &rateSpy{}
	client.mu.Lock()
	spy.congestionController = client.cc
	client.cc = spy
	client.mu.Unlock()
	drained := func() {
		deadline := time.Now().Add(5 * time.Second)
		for client.Stats().BytesInFlight > 0 {
			if time.Now().After(deadline) {
				t.Fatal("the flight never drained")
			}
			time.Sleep(time.Millisecond)
		}
	}
	drained()
	before := client.Stats().DeliveryRate

	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	drained()

	client.mu.Lock()
	limited, samples := spy.appLimited, spy.samples
	client.mu.Unlock()
	if samples == 0 || limited == 0 {
		t.Fatalf("%d samples after a 100-byte write, %d application-limited: the writer running dry was not seen", samples, limited)
	}
	if after := client.Stats().DeliveryRate; after != before {
		t.Fatalf("Stats.DeliveryRate moved on an application-limited sample: %v, was %v", after, before)
	}
}

// plainPair is a client and a server connection over loopback, handshake
// done, nothing else said yet.
func plainPair(t *testing.T) (client, server *Conn) {
	t.Helper()
	serverPC := loopbackSocket(t)
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
			close(accepted)
			return
		}
		accepted <- c
	}()
	client = dialInternal(t, loopbackSocket(t), serverPC.LocalAddr())
	server = <-accepted
	if server == nil {
		t.Fatal("no server connection")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

// A CE increase is a congestion event for Reno and CUBIC (RFC 9002 §7.3.2,
// B.7; RFC 9438 §3.1): the window takes its loss decrease and the recovery
// period opens, once — a second report dated before the period's start is
// the same event — and the flight does not move, the marked packets having
// arrived.
func TestMarksAreACongestionEvent(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		cc   congestionController
		want int // the window after one event, from 12000
	}{
		{"reno", newNewReno(1200), 6000},
		{"cubic", newCubic(1200), 8400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := tc.cc
			cc.onSent(now, 12000)
			cc.onMarked(markEvent{now: now, latestSent: now.Add(-time.Millisecond), packets: 1})
			if cc.window() != tc.want || cc.bytesInFlight() != 12000 {
				t.Fatalf("after one mark: window %d flight %d, want %d and 12000", cc.window(), cc.bytesInFlight(), tc.want)
			}
			cc.onMarked(markEvent{now: now, latestSent: now.Add(-time.Millisecond), packets: 3})
			if cc.window() != tc.want {
				t.Fatalf("a second report of the same event moved the window to %d", cc.window())
			}
			later := now.Add(time.Second)
			cc.onMarked(markEvent{now: later, latestSent: now.Add(time.Millisecond), packets: 1})
			if cc.window() >= tc.want {
				t.Fatalf("a mark on a packet sent after the period began left the window at %d", cc.window())
			}
		})
	}
}
