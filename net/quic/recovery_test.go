package quic

import (
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

	out, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{})
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

// An ACK straddling a recovery exit: packets sent before growAfter release
// flight bytes without earning growth, packets sent after earn theirs. The
// aggregate this replaced gated everything on the oldest packet, so one
// pre-recovery straggler silenced the whole frame's growth (RFC 9002 B.5
// runs the test per packet).
func TestProcessAckGrowableStraddlesRecovery(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	recovery := now.Add(-10 * time.Millisecond)
	s.record(sentPacket{pn: 0, sentAt: recovery.Add(-time.Millisecond), size: 100, ackEliciting: true})
	s.record(sentPacket{pn: 1, sentAt: recovery.Add(time.Millisecond), size: 100, ackEliciting: true})

	var peer ackTracker
	peer.record(0, now)
	peer.record(1, now)
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, recovery)
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAcked != 200 {
		t.Fatalf("acked %d bytes, want 200", out.newlyAcked)
	}
	if out.growableBytes != 100 {
		t.Fatalf("growable = %d, want only the post-recovery packet's 100", out.growableBytes)
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
	out, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.lost) != 1 || out.lost[0][0] != 0 {
		t.Fatalf("lost = %v, want the second-old packet", out.lost)
	}
}

func TestNewRenoWindow(t *testing.T) {
	cc := newNewReno(1200)
	start := cc.cwnd
	if start != 12000 {
		t.Fatalf("initial window = %d", start)
	}
	now := time.Now()

	// Slow start: acknowledged bytes grow the window byte for byte.
	cc.onSent(2400)
	cc.onAcked(2400, 2400)
	if cc.cwnd != start+2400 {
		t.Errorf("slow start grew %d, want %d", cc.cwnd-start, 2400)
	}

	// Bytes from before a recovery start release flight but earn nothing:
	// processAck reports them as acked and not growable, and the window
	// must not move on them.
	cc.onSent(1200)
	cc.onAcked(1200, 0)
	if cc.cwnd != start+2400 {
		t.Errorf("non-growable bytes moved the window to %d", cc.cwnd)
	}

	// Loss halves it, once per recovery period.
	cc.onSent(1200)
	cc.onLost(1200, now, now)
	half := cc.cwnd
	if half != (start+2400)/2 {
		t.Errorf("after loss, cwnd = %d", half)
	}
	cc.onLost(0, now.Add(-time.Second), now) // sent before recovery began
	if cc.cwnd != half {
		t.Error("a loss inside the same recovery period shrank the window again")
	}

	// Persistent congestion collapses to the minimum.
	cc.onPersistentCongestion()
	if cc.cwnd != 2*1200 {
		t.Errorf("collapsed window = %d", cc.cwnd)
	}
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
		template[pn] = sentPacket{pn: pn, sentAt: now, size: 1200, ackEliciting: true}
	}

	// Eight ranges acknowledging every other run of eight packets — the
	// reordered-but-mostly-delivered shape loss recovery actually sees.
	var peer ackTracker
	for pn := range uint64(inFlight) {
		if pn/8%2 == 0 {
			peer.record(pn, now)
		}
	}
	frame := appendAckFrame(nil, peer.ranges, 0)
	f, _, err := parseFrame(frame)
	if err != nil {
		b.Fatal(err)
	}

	s := sentTracker{packets: make([]sentPacket, inFlight)}
	b.ReportAllocs()
	for b.Loop() {
		s.packets = s.packets[:inFlight]
		copy(s.packets, template)
		s.hasLargestAcked = false
		if _, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The pacer's bucket: starts full at one initial window, spends below zero,
// refills at 5/4 cwnd per smoothed round trip, and prices a deficit in time
// — with sub-granularity deficits released immediately, since the timer
// could not honour them anyway.
func TestPacerBucket(t *testing.T) {
	now := time.Now()
	p := pacer{burst: 12000}
	const cwnd, srtt = 12000, 100 * time.Millisecond

	p.refill(now, cwnd, srtt)
	if p.tokens != 12000 {
		t.Fatalf("first refill filled to %d, want the burst 12000", p.tokens)
	}
	if d := p.delay(cwnd, srtt); d != 0 {
		t.Fatalf("a full bucket priced a %v wait", d)
	}

	// Spend the bucket and one packet beyond: the deficit prices the wait.
	p.spend(12000)
	p.spend(1200)
	// 1200 bytes at 5/4 × 12000 bytes per 100ms (150000 B/s) take 8ms:
	// deficit × 4 × srtt / (5 × cwnd).
	if d := p.delay(cwnd, srtt); d != 8*time.Millisecond {
		t.Fatalf("deficit of 1200 priced at %v, want 8ms", d)
	}

	// Half the deficit's time passes: the wait halves.
	p.refill(now.Add(4*time.Millisecond), cwnd, srtt)
	if d := p.delay(cwnd, srtt); d != 4*time.Millisecond {
		t.Fatalf("after 4ms the deficit prices at %v, want 4ms", d)
	}

	// A sub-granularity deficit is released immediately.
	p.refill(now.Add(8*time.Millisecond), cwnd, srtt)
	p.spend(100)
	if d := p.delay(cwnd, srtt); d != 0 {
		t.Fatalf("a deficit under one granularity priced a %v wait", d)
	}

	// A long idle refills to the burst, never beyond.
	p.refill(now.Add(10*time.Second), cwnd, srtt)
	if p.tokens != 12000 {
		t.Fatalf("after idling, tokens = %d, want the burst", p.tokens)
	}
}
