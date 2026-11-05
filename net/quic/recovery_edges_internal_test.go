package quic

import (
	"errors"
	"testing"
	"time"
)

// The estimator follows RFC 9002 §5.3 to the nanosecond: the peer's delay
// comes off a sample that stays at or above the minimum, the variance takes
// the absolute difference, and a persistent-congestion reset starts over
// from the newest sample.
func TestRTTSampleByTheFormulas(t *testing.T) {
	const ms = time.Millisecond
	for _, c := range []struct {
		rtt, ackDelay            time.Duration
		smoothed, variance, pto0 time.Duration
	}{
		{130 * ms, 10 * ms, 102500 * time.Microsecond, 42500 * time.Microsecond, 272500 * time.Microsecond},
		{130 * ms, 30 * ms, 100 * ms, 37500 * time.Microsecond, 250 * ms}, // exactly at the minimum
		{130 * ms, 40 * ms, 103750 * time.Microsecond, 45 * ms, 283750 * time.Microsecond},
	} {
		var r rttEstimator
		r.sample(100*ms, 0)
		r.sample(c.rtt, c.ackDelay)
		if r.smoothed != c.smoothed || r.variance != c.variance || r.pto(0) != c.pto0 {
			t.Errorf("100 ms then %v with %v of delay: smoothed %v variance %v pto %v, want %v %v %v",
				c.rtt, c.ackDelay, r.smoothed, r.variance, r.pto(0), c.smoothed, c.variance, c.pto0)
		}
		r.resetAfterPersistentCongestion()
		if r.min != c.rtt || r.smoothed != c.rtt || r.variance != c.rtt/2 {
			t.Errorf("after a reset: min %v smoothed %v variance %v, want %v, %v, %v", r.min, r.smoothed, r.variance, c.rtt, c.rtt, c.rtt/2)
		}
	}
	var r rttEstimator
	if got, want := r.pto(25*ms), 3*initialRTT+25*ms; got != want {
		t.Errorf("pto before a sample = %v, want %v", got, want)
	}
}

// Two numbers skipped in a row would be one gap a window wide in name only:
// the next skip is drawn from past the one just made.
func TestSkippedNumbersAreNeverAdjacent(t *testing.T) {
	var s sentTracker
	last := -2
	for pn := range 400 {
		if s.skipNumber(uint64(pn), 2) {
			if pn == last+1 {
				t.Fatalf("numbers %d and %d both skipped", last, pn)
			}
			last = pn
		}
	}
	if last < 0 {
		t.Fatal("nothing skipped")
	}
}

// Only skips between a packet and the largest acknowledged count.
func TestSkippedAboveCountsGapsBelowTheLargest(t *testing.T) {
	s := sentTracker{largestAcked: 10, nSkips: 3}
	s.skips[0], s.skips[1], s.skips[2] = 20, 7, 5
	if got := s.skippedAbove(2); got != 2 {
		t.Errorf("skippedAbove(2) = %d, want 2 (5 and 7, not 20)", got)
	}
}

// trackerWithSkip holds packets 0 to 9 with 5 skipped.
func trackerWithSkip() *sentTracker {
	s := &sentTracker{nSkips: 1}
	s.skips[0] = 5
	for pn := range uint64(10) {
		if pn != 5 {
			s.record(sentPacket{pn: pn, size: 100, ackEliciting: true})
		}
	}
	return s
}

// A range that starts or ends on the skipped number names it.
func TestOptimisticAckAtARangeEdge(t *testing.T) {
	for _, b := range [][]byte{ackFrame(8, 3), ackFrame(5, 3)} {
		f, _, err := parseFrame(b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := trackerWithSkip().markAcked(f.largest, f.Data); !errors.Is(err, errOptimisticAck) {
			t.Errorf("ACK of [%d, %d] around skipped 5: %v", f.largest-3, f.largest, err)
		}
	}
}

// An ACK that marks nothing names no largest acknowledged.
func TestAnAckThatMarksNothingSetsNoLargest(t *testing.T) {
	var s sentTracker
	f, _, err := parseFrame(ackFrame(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.markAcked(f.largest, f.Data); err != nil || s.hasLargestAcked {
		t.Errorf("markAcked on nothing tracked: %v, hasLargestAcked %v", err, s.hasLargestAcked)
	}
}

// A packet counts as large above the 1200-byte base, acknowledged or lost;
// a lost one without payload queues nothing.
func TestLargeAboveTheBase(t *testing.T) {
	var out ackOutcome
	out.mark(&sentPacket{size: pmtuBaseSize}, true, 99)
	if out.largeAcked {
		t.Error("a 1200-byte packet acknowledged as large")
	}
	out.mark(&sentPacket{size: pmtuBaseSize + 1}, true, 99)
	if !out.largeAcked {
		t.Error("a 1201-byte packet not acknowledged as large")
	}

	var s sentTracker
	out = ackOutcome{}
	s.declareLost(&out, &sentPacket{pn: 1, size: pmtuBaseSize}, nil, false)
	if out.largeLost != 0 || len(out.lost) != 0 || len(s.lost) != 0 {
		t.Errorf("a 1200-byte packet without payload lost: large %d, %d payloads, %d remembered", out.largeLost, len(out.lost), len(s.lost))
	}
	s.declareLost(&out, &sentPacket{pn: 2, size: pmtuBaseSize + 1, payload: []byte{framePing}}, nil, false)
	if out.largeLost != 1 || len(out.lost) != 1 || len(s.lost) != 1 {
		t.Errorf("a 1201-byte packet lost: large %d, %d payloads, %d remembered", out.largeLost, len(out.lost), len(s.lost))
	}
}

// The crossing carries the losses since the packet left: the estimator's
// count and the batch's, less what was lost before it.
func TestLossCrossingCountsSinceThePacketLeft(t *testing.T) {
	var s sentTracker
	var out ackOutcome
	est := &deliveryEstimator{lost: 1000}
	s.declareLost(&out, &sentPacket{pn: 1, size: 100, lost: 900, txInFlight: 5000}, est, false)
	if out.crossing.lost != 200 {
		t.Errorf("crossing.lost = %d, want 1000 + 100 - 900", out.crossing.lost)
	}
}

// The memory of lost packets keeps the newest lostMemory.
func TestLostMemoryKeepsTheNewest(t *testing.T) {
	var s sentTracker
	for pn := range uint64(lostMemory + 6) {
		s.rememberLost(pn, []byte{framePing})
	}
	if len(s.lost) != lostMemory || s.lost[0].pn != 6 {
		t.Errorf("%d remembered from %d, want %d from 6", len(s.lost), s.lost[0].pn, lostMemory)
	}
}

// takeAll counts every packet's bytes and returns only real payloads.
func TestTakeAllSkipsEmptyPayloads(t *testing.T) {
	var s sentTracker
	s.record(sentPacket{pn: 0, size: 100})
	s.record(sentPacket{pn: 1, size: 100, payload: []byte{framePing}})
	if payloads, n := s.takeAll(); len(payloads) != 1 || n != 200 {
		t.Errorf("takeAll = %d payloads, %d bytes; want 1 and 200", len(payloads), n)
	}
}

// A sample with no interval is neither reliable nor a rate.
func TestRateSampleWithoutInterval(t *testing.T) {
	rs := rateSample{delivered: 100}
	if rs.reliable(0) || rs.deliveryRate() != 0 {
		t.Errorf("zero interval: reliable %v, rate %v", rs.reliable(0), rs.deliveryRate())
	}
}

// The controller hears the losses since each packet left.
func TestAckEventCarriesTheLossesSinceThePacket(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	s.record(sentPacket{pn: 0, sentAt: now, size: 100, ackEliciting: true, lost: 200})
	est := &deliveryEstimator{lost: 1000}
	spy := &eventSpy{congestionController: newNewReno(1200)}
	f, _, err := parseFrame(ackFrame(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.processAck(now, f.largest, f.Data, &rtt, spy, est); err != nil {
		t.Fatal(err)
	}
	if len(spy.events) != 1 || spy.events[0].lost != 800 {
		t.Errorf("events = %+v, want one with 800 lost", spy.events)
	}
}

// A frame that only declares losses, and one that only acknowledges a
// previous path's packets, leave the delivery estimate where the last
// sample put it.
func TestDeliveryEstimateMovesOnlyOnThisPathsAcks(t *testing.T) {
	ack := func(t *testing.T, s *sentTracker, now time.Time, pn byte, est *deliveryEstimator) ackOutcome {
		t.Helper()
		var rtt rttEstimator
		f, _, err := parseFrame(ackFrame(pn, 0))
		if err != nil {
			t.Fatal(err)
		}
		out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, est)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	t0 := time.Now()
	t1 := t0.Add(10 * time.Millisecond)

	var s sentTracker
	est := &deliveryEstimator{}
	for pn := range uint64(3) {
		s.record(sentPacket{pn: pn, sentAt: t0, size: 100, ackEliciting: true})
	}
	ack(t, &s, t1, 1, est)
	if !est.deliveredTime.Equal(t1) {
		t.Fatalf("deliveredTime = %v after the first ACK, want %v", est.deliveredTime, t1)
	}
	if out := ack(t, &s, t1.Add(time.Second), 1, est); out.declared != 1 || out.newlyAckedCount != 0 {
		t.Fatalf("the repeat declared %d and acknowledged %d, want 1 and 0", out.declared, out.newlyAckedCount)
	}
	if !est.deliveredTime.Equal(t1) {
		t.Errorf("a loss-only frame moved deliveredTime to %v", est.deliveredTime)
	}

	s.pathStart = t1.Add(time.Millisecond) // the packets left on a previous path
	if out := ack(t, &s, t1.Add(2*time.Second), 2, est); out.newlyAckedCount != 1 {
		t.Fatalf("the old path's packet: %d acknowledged", out.newlyAckedCount)
	}
	if !est.deliveredTime.Equal(t1) {
		t.Errorf("an ACK of a previous path's packet moved deliveredTime to %v", est.deliveredTime)
	}
}

// The time threshold scales the round trip: a packet younger than 9/8 of
// it waits for the loss timer.
func TestTimeThresholdScalesTheRoundTrip(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(100*time.Millisecond, 0)
	now := time.Now()
	s.record(sentPacket{pn: 0, sentAt: now.Add(-50 * time.Millisecond), size: 100, ackEliciting: true})
	s.record(sentPacket{pn: 1, sentAt: now, size: 100, ackEliciting: true})
	f, _, err := parseFrame(ackFrame(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.processAck(now, f.largest, f.Data, &rtt, nil, nil)
	if err != nil || out.declared != 0 || s.lossTime.IsZero() {
		t.Errorf("a packet 50 ms old under a 100 ms round trip: %v, %d declared, loss timer %v", err, out.declared, s.lossTime)
	}
}

// NewReno at its edges: the initial window floors at two datagrams, a send
// that exactly fills the window goes, and a loss halves no lower than two
// datagrams.
func TestNewRenoAtItsEdges(t *testing.T) {
	if got := initialWindow(10000); got != 20000 {
		t.Errorf("initialWindow(10000) = %d, want 20000", got)
	}
	c := newNewReno(1200)
	if !c.canSend(c.cwnd) || c.canSend(c.cwnd+1) {
		t.Errorf("canSend at the window %v, one past %v", c.canSend(c.cwnd), c.canSend(c.cwnd+1))
	}
	c = newNewReno(1200)
	c.cwnd = 2400
	now := time.Now()
	c.onLost(lossEvent{now: now, latestSent: now})
	if c.cwnd != 2400 {
		t.Errorf("cwnd after a loss at two datagrams = %d, want 2400", c.cwnd)
	}
}

// The pacer's bucket refills a deficit before filling up, releases a
// deficit of one granularity on its timer, and prices a wait to the
// microsecond.
func TestPacerAtItsEdges(t *testing.T) {
	t0 := time.Now()
	p := pacer{burst: 1000, tokens: -500, last: t0}
	p.refill(t0.Add(time.Second), 600)
	if p.tokens != 100 {
		t.Errorf("600 bytes into a 500-byte deficit: %d tokens, want 100", p.tokens)
	}
	p = pacer{burst: 1000, tokens: -1000}
	if got := p.delay(1e6); got != time.Millisecond {
		t.Errorf("a deficit of exactly one granularity: delay %v, want 1ms", got)
	}
	if got := p.delay(0); got != 0 {
		t.Errorf("no rate: delay %v", got)
	}
	p = pacer{burst: 1000}
	if got := p.until(1000, 1e6); got != time.Millisecond {
		t.Errorf("until 1000 bytes at 1 MB/s = %v, want 1ms", got)
	}
	if got := p.until(1000, 0); got != 0 {
		t.Errorf("no rate: until %v", got)
	}
}

// A write is application-limited after a gap past both the granularity and
// one datagram at the pacing rate, and not at either.
func TestAppLimitedGapAtItsEdges(t *testing.T) {
	for _, c := range []struct {
		cwnd int
		gap  time.Duration
		want bool
	}{
		{1 << 30, granularity, false},        // the rate's datagram is far below the granularity
		{12000, 5 * time.Millisecond, false}, // a datagram at 72 kB/s is about 17 ms
		{12000, 50 * time.Millisecond, true},
	} {
		conn := newIdleServerConn(t)
		conn.mu.Lock()
		cc := newNewReno(1200)
		cc.cwnd = c.cwnd
		conn.cc = cc
		now := time.Now()
		conn.writeSent = now.Add(-c.gap)
		got := conn.checkAppLimitedLocked(now)
		conn.mu.Unlock()
		if got != c.want {
			t.Errorf("cwnd %d, gap %v: application-limited %v, want %v", c.cwnd, c.gap, got, c.want)
		}
	}
}

// A connection with no Initial or Handshake keys has no space for an
// anti-deadlock probe.
func TestAntiDeadlockSpaceWithoutKeys(t *testing.T) {
	if got := (&Conn{}).antiDeadlockSpaceLocked(); got != -1 {
		t.Errorf("antiDeadlockSpaceLocked = %d, want -1", got)
	}
}

// fullWindow gives conn a NewReno controller whose flight fills its window
// when full is set.
func fullWindow(conn *Conn, full bool) {
	cc := newNewReno(1200)
	if full {
		cc.inFlight = cc.cwnd
	}
	conn.cc = cc
}

// The probe timeout counts itself, takes a path that carried large
// datagrams for a black hole on its third expiry, and counts a resend from
// an idle connection as application-limited — not from one whose window is
// full.
func TestPTOAtItsCounts(t *testing.T) {
	for _, full := range []bool{false, true} {
		c := newIdleServerConn(t)
		now := time.Now()
		c.mu.Lock()
		fullWindow(c, full)
		c.maxDatagram, c.ptoCount = 1400, 2
		sp := &c.spaces[spaceInitial]
		sp.lastElicit = now.Add(-time.Minute)
		sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: []byte{framePing}})
		sp.nextPN = 1
		c.mu.Unlock()

		c.actOnDeadlines(now)

		c.mu.Lock()
		resends, fired, holes := c.appLimitedResends, c.ptoFired, c.pmtuBlackHoles
		c.mu.Unlock()
		if want := map[bool]int{false: 1, true: 0}[full]; resends != want {
			t.Errorf("window full %v: %d application-limited resends, want %d", full, resends, want)
		}
		if fired != 1 || holes != 1 {
			t.Errorf("window full %v: %d probe timeouts, %d black holes; want 1 and 1", full, fired, holes)
		}
	}
}

// The loss timer counts an application-limited resend only when it
// declared something, from an idle connection; a loss on a previous path
// is no congestion event.
func TestLossTimerAtItsCounts(t *testing.T) {
	run := func(t *testing.T, full bool, sentAt time.Time, pathStart time.Time) (*Conn, int) {
		t.Helper()
		c := newIdleServerConn(t)
		now := time.Now()
		c.mu.Lock()
		fullWindow(c, full)
		sp := &c.spaces[spaceInitial]
		sp.sent.pathStart = pathStart
		sp.sent.record(sentPacket{pn: 0, sentAt: sentAt, size: 100, ackEliciting: true, payload: []byte{framePing, 0xaa}})
		sp.sent.record(sentPacket{pn: 1, sentAt: now, size: 100, ackEliciting: true, payload: []byte{framePing, 0xbb}})
		sp.sent.largestAcked, sp.sent.hasLargestAcked = 2, true
		sp.nextPN = 3
		sp.lastElicit = now
		sp.sent.lossTime = now.Add(-time.Millisecond)
		c.mu.Unlock()
		c.actOnDeadlines(now)
		c.mu.Lock()
		defer c.mu.Unlock()
		return c, c.appLimitedResends
	}
	now := time.Now()
	if _, n := run(t, false, now.Add(-time.Minute), time.Time{}); n != 1 {
		t.Errorf("idle, one loss: %d application-limited resends, want 1", n)
	}
	if _, n := run(t, true, now.Add(-time.Minute), time.Time{}); n != 0 {
		t.Errorf("window full, one loss: %d application-limited resends, want 0", n)
	}
	if _, n := run(t, false, now, time.Time{}); n != 0 {
		t.Errorf("idle, nothing old enough: %d application-limited resends, want 0", n)
	}
	c, _ := run(t, false, now.Add(-time.Minute), now.Add(-30*time.Second))
	c.mu.Lock()
	defer c.mu.Unlock()
	if w := c.cc.window(); w != initialWindow(1200) {
		t.Errorf("a previous path's loss moved the window to %d", w)
	}
}
