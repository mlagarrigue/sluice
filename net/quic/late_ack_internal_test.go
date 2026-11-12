package quic

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// RFC 9000 §13.3: what a packet carried is not retransmitted once the
// packet is acknowledged, "including after being declared lost". A packet
// declared lost by the packet threshold (§6.1.1) whose acknowledgement then
// arrives — it was reordered, not lost — has its payload taken off the
// retransmission queue, and the next pass resends nothing. It used to be
// resent anyway, the late acknowledgement finding nothing in the record.
func TestALateAcknowledgementTakesThePayloadOffTheResendQueue(t *testing.T) {
	c := newResetReplayConn(t)
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	s.sendQ.max, c.connSend.max = 1<<20, 1<<20 // the peer's credit, as if granted
	// A round trip of a second already measured, so the ACK below, whose own
	// sample is a few microseconds, leaves the time threshold (§6.1.2) far
	// above the gaps between the four sends. Without it the threshold was a
	// millisecond, and a Windows clock that ticked 15 ms between two writes
	// declared packet 1 lost by time as well (2026-10-22, twice).
	c.rtt.sample(time.Second, 0)
	c.mu.Unlock()
	for i := range 4 { // packets 0 to 3, one byte each
		if _, err := s.Write([]byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.Lock()
	tracked := len(sp.sent.packets)
	c.mu.Unlock()
	if tracked != 4 {
		t.Fatalf("%d packets tracked after four one-byte writes, want 4", tracked)
	}

	// 3 acknowledged alone: 0 is three packets behind, declared lost, its
	// payload queued.
	var peer ackTracker
	now := time.Now()
	peer.record(3, now)
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	queued := len(sp.retrans)
	c.mu.Unlock()
	if queued != 1 {
		t.Fatalf("%d payloads queued after packet 3 was acknowledged alone, want packet 0's", queued)
	}

	// Then 0 after all — reordered, not lost.
	peer.record(0, now)
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	queued = len(sp.retrans)
	before := len(sp.sent.packets)
	c.retransmitLocked()
	after := len(sp.sent.packets)
	c.mu.Unlock()
	if queued != 0 || after != before {
		t.Fatalf("after the late acknowledgement of packet 0: %d payloads queued, %d packets resent, want none of either (§13.3)", queued, after-before)
	}
}

// ackRangesOneApart encodes the ranges of an ACK frame naming n single
// packets below its largest, one unacknowledged number between two: the
// most ranges per byte a peer can send.
func ackRangesOneApart(n int) []byte {
	data := varint.Append(nil, uint64(n-1)) // ACK Range Count
	data = varint.Append(data, 0)           // First ACK Range
	for range n - 1 {
		data = varint.Append(data, 0) // Gap: one packet not acknowledged
		data = varint.Append(data, 0) // ACK Range Length: one packet
	}
	return data
}

// benchLateAcked runs lateAcked on an ACK of 700 single-packet ranges with
// s.lost holding lost entries, each at one of the unacknowledged numbers
// between the ranges near the top: nothing is taken, so every iteration
// sees the same table.
func benchLateAcked(b *testing.B, lost int) {
	b.Helper()
	const ranges, largest = 700, 1_000_000
	data := ackRangesOneApart(ranges)
	var s sentTracker
	for k := lost; k > 0; k-- {
		s.rememberLost(largest-uint64(2*k-1), []byte{framePing})
	}
	var out ackOutcome
	b.ReportAllocs()
	for b.Loop() {
		s.lateAcked(&out, largest, data)
	}
	if len(s.lost) != lost || len(out.lateAcked) != 0 {
		b.Fatalf("%d remembered, %d taken; want %d and none", len(s.lost), len(out.lateAcked), lost)
	}
}

func BenchmarkLateAcked700RangesLost64(b *testing.B) { benchLateAcked(b, lostMemory) }
func BenchmarkLateAcked700RangesLost1(b *testing.B)  { benchLateAcked(b, 1) }

// encodeAckRanges encodes strictly descending, non-adjacent ranges as an
// ACK frame's largest and range fields.
func encodeAckRanges(ranges [][2]uint64) (largest uint64, data []byte) {
	data = varint.Append(nil, uint64(len(ranges)-1))
	data = varint.Append(data, ranges[0][1]-ranges[0][0])
	for i := 1; i < len(ranges); i++ {
		data = varint.Append(data, ranges[i-1][0]-ranges[i][1]-2) // Gap
		data = varint.Append(data, ranges[i][1]-ranges[i][0])     // ACK Range Length
	}
	return ranges[0][1], data
}

// lateAcked takes exactly the remembered losses some range covers, whatever
// the order losses were declared in and however many were forgotten, and
// leaves the rest ascending: checked against a direct scan.
func TestLateAckedTakesWhatTheRangesCover(t *testing.T) {
	rng := rand.New(rand.NewPCG(19, 13)) //nolint:gosec // G404: a seeded table, reproducible on purpose
	for round := range 2000 {
		var s sentTracker
		var all []uint64
		for range rng.IntN(2 * lostMemory) {
			pn := rng.Uint64N(400)
			if slices.Contains(all, pn) {
				continue
			}
			all = append(all, pn)
			s.rememberLost(pn, []byte{byte(pn), byte(pn >> 8)})
		}
		if !slices.IsSortedFunc(s.lost, func(a, b lostRef) int { return cmp.Compare(a.pn, b.pn) }) {
			t.Fatalf("round %d: remembered losses out of order", round)
		}
		var ranges [][2]uint64
		for hi := 400 + rng.Uint64N(10); ; {
			lo := hi - min(hi, rng.Uint64N(8))
			ranges = append(ranges, [2]uint64{lo, hi})
			if lo < 2+12 { // room for the next gap
				break
			}
			hi = lo - 2 - rng.Uint64N(12)
		}
		var want, kept []uint64
		for _, l := range s.lost {
			covered := slices.ContainsFunc(ranges, func(r [2]uint64) bool { return r[0] <= l.pn && l.pn <= r[1] })
			if covered {
				want = append(want, l.pn)
			} else {
				kept = append(kept, l.pn)
			}
		}
		largest, data := encodeAckRanges(ranges)
		var out ackOutcome
		s.lateAcked(&out, largest, data)
		var got, left []uint64
		for _, p := range out.lateAcked {
			got = append(got, uint64(p[0])|uint64(p[1])<<8)
		}
		for _, l := range s.lost {
			left = append(left, l.pn)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) || !slices.Equal(left, kept) {
			t.Fatalf("round %d: took %v, want %v; kept %v, want %v", round, got, want, left, kept)
		}
	}
}
