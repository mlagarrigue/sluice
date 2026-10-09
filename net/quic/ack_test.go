package quic

import (
	"testing"
	"time"
)

func TestAckTrackerRangesAndDuplicates(t *testing.T) {
	var a ackTracker
	now := time.Now()
	for _, pn := range []uint64{0, 1, 2, 5, 6, 4, 10} {
		if !a.record(pn, now) {
			t.Fatalf("packet %d reported as a duplicate on first sight", pn)
		}
	}
	if a.record(5, now) {
		t.Fatal("a duplicate was not recognised")
	}
	// 0-2, 4-6, 10 → after 4 filled the gap's edge: [10,10] [4,6] [0,2].
	want := []pnRange{{10, 10}, {4, 6}, {0, 2}}
	if len(a.ranges) != len(want) {
		t.Fatalf("ranges = %+v, want %+v", a.ranges, want)
	}
	for i, r := range want {
		if a.ranges[i] != r {
			t.Fatalf("ranges = %+v, want %+v", a.ranges, want)
		}
	}
	// 3 joins 0-2 and 4-6 into one run.
	a.record(3, now)
	if len(a.ranges) != 2 || a.ranges[1] != (pnRange{0, 6}) {
		t.Fatalf("after filling the gap: %+v", a.ranges)
	}
}

// The frame round-trips: what the tracker writes, the parser reads back and
// the range decoder walks to the same set.
func TestAckFrameRoundTrip(t *testing.T) {
	var a ackTracker
	now := time.Now()
	for _, pn := range []uint64{2, 3, 4, 9, 11, 12} {
		a.record(pn, now)
	}
	raw := a.appendAck(nil, now.Add(50*time.Millisecond))

	frames, err := parseFrames(nil, raw)
	if err != nil {
		t.Fatalf("the tracker wrote an ACK the parser refuses: %v", err)
	}
	if len(frames) != 1 || frames[0].Type != frameACK {
		t.Fatalf("frames = %+v", frames)
	}
	f := frames[0]
	if f.largest != 12 {
		t.Errorf("largest = %d, want 12", f.largest)
	}
	var got []pnRange
	if err := decodeAckRanges(f.largest, f.Data, func(lo, hi uint64) {
		got = append(got, pnRange{lo, hi})
	}); err != nil {
		t.Fatal(err)
	}
	want := []pnRange{{11, 12}, {9, 9}, {2, 4}}
	if len(got) != len(want) {
		t.Fatalf("ranges = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ranges = %+v, want %+v", got, want)
		}
	}
}

// Initial and handshake packets are acknowledged at once; the application
// space answers the second ack-eliciting packet at once and the first on a
// timer, so one ACK can cover a burst.
func TestAckUrgencyPolicy(t *testing.T) {
	now := time.Now()

	var initial ackTracker
	initial.record(0, now)
	initial.onAckEliciting(spaceInitial, now)
	if !initial.owes(now) {
		t.Error("an Initial packet was not acknowledged immediately")
	}

	var app ackTracker
	app.record(0, now)
	app.onAckEliciting(spaceApplication, now)
	if app.owes(now) {
		t.Error("the first application packet forced an immediate ACK")
	}
	if app.due.IsZero() {
		t.Error("no delayed ACK was armed")
	}
	if !app.owes(now.Add(localMaxAckDelay + time.Millisecond)) {
		t.Error("the delayed ACK never matured")
	}
	app.record(1, now)
	app.onAckEliciting(spaceApplication, now)
	if !app.owes(now) {
		t.Error("the second ack-eliciting packet did not force an ACK")
	}
}

// Forgetting the lowest ranges under the bound costs the peer a
// retransmission, never this endpoint its memory.
func TestAckTrackerBoundsRanges(t *testing.T) {
	var a ackTracker
	now := time.Now()
	for i := range uint64(200) {
		a.record(i*2, now) // every other packet: one range each
	}
	if len(a.ranges) > maxAckRanges {
		t.Fatalf("%d ranges held, bound is %d", len(a.ranges), maxAckRanges)
	}
	top := a.ranges[0]
	if top.hi != 398 {
		t.Errorf("the newest range was evicted: %+v", top)
	}
}
