package quic

import (
	"testing"
	"time"
)

// An ACK whose space saw marked packets is type 0x03 and carries the three
// counts after its ranges (RFC 9000 §19.3.2); one whose space saw none is
// the plain type 0x02. Both parse, and the counts read back.
func TestAckFrameCarriesECNCounts(t *testing.T) {
	ranges := []pnRange{{10, 12}, {3, 5}, {0, 0}}
	plain := appendAckFrame(nil, ranges, 0, [3]uint64{})
	f, rest, err := parseFrame(plain)
	if err != nil || len(rest) != 0 || f.Type != frameACK {
		t.Fatalf("plain ACK: type %#x rest %d err %v", f.Type, len(rest), err)
	}
	if _, ok := ackECNCounts(f); ok {
		t.Fatal("a plain ACK reported ECN counts")
	}

	want := [3]uint64{70000, 1, 3} // ECT(0) past one varint byte, ECT(1), CE
	withECN := appendAckFrame(nil, ranges, 0, want)
	f, rest, err = parseFrame(withECN)
	if err != nil || len(rest) != 0 || f.Type != frameACK|1 {
		t.Fatalf("ECN ACK: type %#x rest %d err %v", f.Type, len(rest), err)
	}
	got, ok := ackECNCounts(f)
	if !ok || got != want {
		t.Fatalf("counts %v ok %v, want %v", got, ok, want)
	}
	// The ranges still decode as before: the counts sit after them.
	var seen []pnRange
	if err := decodeAckRanges(f.largest, f.Data, func(lo, hi uint64) { seen = append(seen, pnRange{lo, hi}) }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(ranges) || seen[0] != ranges[0] || seen[2] != ranges[2] {
		t.Fatalf("ranges %v, want %v", seen, ranges)
	}
	// Truncated counts are a malformed frame, not a plain ACK.
	if _, _, err := parseFrame(withECN[:len(withECN)-1]); err == nil {
		t.Fatal("a truncated ECN ACK parsed")
	}
}

// The tracker counts each recorded packet under its datagram's codepoint
// and reports the counts in the ACK it encodes; a duplicate counts nothing.
func TestAckTrackerReportsECNCounts(t *testing.T) {
	var a ackTracker
	now := time.Now()
	for pn, cp := range []uint8{ecnECT0, ecnECT0, ecnCE, ecnNotECT, ecnECT0} {
		if a.record(uint64(pn), now) && cp != ecnNotECT {
			a.ecn[cp]++
		}
	}
	if a.record(1, now) {
		t.Fatal("a duplicate was recorded")
	}
	f, _, err := parseFrame(a.encodeAck(nil, now))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ackECNCounts(f); !ok || got != [3]uint64{3, 0, 1} {
		t.Fatalf("counts %v ok %v, want [3 0 1]", got, ok)
	}
}

// The path marks its first ten packets, then nothing until an ACK validates
// the marks, then everything; a socket that cannot mark never marks.
func TestECNStateMarksTestPacketsThenWaits(t *testing.T) {
	var e ecnState
	e.reset(true)
	for i := range ecnTestPackets {
		if e.mark() != ecnECT0 {
			t.Fatalf("test packet %d left unmarked", i)
		}
	}
	if e.phase != ecnUnknown || e.mark() != ecnNotECT {
		t.Fatalf("after the test packets: phase %d, mark %d", e.phase, e.mark())
	}
	e.phase = ecnCapable
	if e.mark() != ecnECT0 {
		t.Fatal("a capable path left a packet unmarked")
	}
	e.reset(false)
	if e.phase != ecnFailed || e.mark() != ecnNotECT {
		t.Fatal("a socket that cannot mark marked")
	}
}

// §13.4.2.1: counts that grow by at least the newly acknowledged marked
// packets validate the path; a frame without counts for marked packets, a
// count that decreases, a codepoint never applied, or more marked arrivals
// than departures fail it; and a frame that does not raise the largest
// acknowledged number never fails it, whatever it carries.
func TestECNValidationOutcomes(t *testing.T) {
	ack := func(counts [3]uint64, withCounts bool) Frame {
		c := [3]uint64{}
		if withCounts {
			c = counts
		}
		f, _, err := parseFrame(appendAckFrame(nil, []pnRange{{5, 9}}, 0, c))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	fresh := func(sent uint64) *Conn {
		c := &Conn{}
		c.ecn.reset(true)
		c.spaces[spaceApplication].ecnSent = sent
		return c
	}
	acked := ackOutcome{raisedLargest: true, newlyAckedECT0: 5}

	c := fresh(5)
	if newCE := c.validateECNLocked(spaceApplication, ack([3]uint64{5, 0, 0}, true), acked); newCE != 0 {
		t.Fatalf("matching counts reported %d new CE marks, want 0", newCE)
	}
	if c.ecn.phase != ecnCapable || c.spaces[spaceApplication].ecnLast != [3]uint64{5, 0, 0} {
		t.Fatalf("matching counts: phase %d last %v", c.ecn.phase, c.spaces[spaceApplication].ecnLast)
	}
	// CE marks stand in for ECT(0) ones: the network remarked, not cleared,
	// and the two marks are the congestion signal the controller hears.
	c = fresh(5)
	if newCE := c.validateECNLocked(spaceApplication, ack([3]uint64{3, 0, 2}, true), acked); newCE != 2 {
		t.Fatalf("ECT(0)+CE covering the packets reported %d new CE marks, want 2", newCE)
	}
	if c.ecn.phase != ecnCapable {
		t.Fatalf("ECT(0)+CE covering the packets: phase %d", c.ecn.phase)
	}
	// The same counts again, under an unchanged largest: old news, no
	// second signal. Then one more mark in a frame that raises it: one.
	if newCE := c.validateECNLocked(spaceApplication, ack([3]uint64{3, 0, 2}, true), ackOutcome{}); newCE != 0 {
		t.Fatalf("a frame that does not raise the largest reported %d new CE marks, want 0", newCE)
	}
	c.spaces[spaceApplication].ecnSent = 10
	if newCE := c.validateECNLocked(spaceApplication, ack([3]uint64{7, 0, 3}, true), acked); newCE != 1 {
		t.Fatalf("one more CE mark reported as %d, want 1", newCE)
	}
	// Decreasing counts, on a path already validated: failed, no signal.
	c = fresh(20)
	c.validateECNLocked(spaceApplication, ack([3]uint64{5, 0, 0}, true), acked)
	c.spaces[spaceApplication].ecnSent = 20
	if newCE := c.validateECNLocked(spaceApplication, ack([3]uint64{4, 0, 1}, true), acked); newCE != 0 || c.ecn.phase != ecnFailed {
		t.Fatalf("decreasing counts: phase %d, %d new CE marks, want failed and 0", c.ecn.phase, newCE)
	}
	c = fresh(5)
	c.validateECNLocked(spaceApplication, ack([3]uint64{}, false), acked)
	if c.ecn.phase != ecnFailed {
		t.Fatalf("no counts for marked packets: phase %d", c.ecn.phase)
	}
	c = fresh(5)
	c.validateECNLocked(spaceApplication, ack([3]uint64{4, 0, 0}, true), acked)
	if c.ecn.phase != ecnFailed {
		t.Fatalf("one mark cleared: phase %d", c.ecn.phase)
	}
	c = fresh(5)
	c.validateECNLocked(spaceApplication, ack([3]uint64{5, 1, 0}, true), acked)
	if c.ecn.phase != ecnFailed {
		t.Fatalf("ECT(1) never applied: phase %d", c.ecn.phase)
	}
	c = fresh(5)
	c.validateECNLocked(spaceApplication, ack([3]uint64{6, 0, 0}, true), acked)
	if c.ecn.phase != ecnFailed {
		t.Fatalf("more arrivals than departures: phase %d", c.ecn.phase)
	}
	// Reordered news: the same bad frames change nothing.
	for _, f := range []Frame{ack([3]uint64{}, false), ack([3]uint64{1, 1, 1}, true)} {
		c = fresh(5)
		c.validateECNLocked(spaceApplication, f, ackOutcome{newlyAckedECT0: 5})
		if c.ecn.phase != ecnTesting {
			t.Fatalf("a frame that did not raise the largest acknowledged changed the phase to %d", c.ecn.phase)
		}
	}
	// A plain ACK of unmarked packets — before any test packet — is fine.
	c = fresh(0)
	c.validateECNLocked(spaceApplication, ack([3]uint64{}, false), ackOutcome{raisedLargest: true})
	if c.ecn.phase != ecnTesting {
		t.Fatalf("a plain ACK of unmarked packets changed the phase to %d", c.ecn.phase)
	}
}
