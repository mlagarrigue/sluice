package quic

import (
	"testing"
	"time"
)

// FuzzDecodePacketNumber: RFC 9000 §12.3 bounds a packet number to
// 0..2^62-1, because it has to fit the Largest Acknowledged field of an
// ACK frame. Whatever the wire and the receiver's state say, the decoder
// either returns a number inside that range or refuses.
func FuzzDecodePacketNumber(f *testing.F) {
	f.Add(uint64(0xa82f30ea), uint64(0x9b32), 2) // the RFC's own example
	f.Add(uint64(255), uint64(0), 1)
	f.Add(uint64(1<<62-1), uint64(0), 4) // expected is 2^62: the bare candidate overflows
	f.Add(uint64(1<<62-1), uint64(1), 1)
	f.Add(uint64(1<<62-2), uint64(0xffffffff), 4)
	f.Fuzz(func(t *testing.T, largest, truncated uint64, pnLen int) {
		if pnLen < 1 || pnLen > 4 || largest > MaxVarint || truncated >= 1<<(8*pnLen) {
			return
		}
		got, ok := decodePacketNumber(largest, truncated, pnLen)
		if !ok {
			return
		}
		if got > MaxVarint {
			t.Fatalf("decode(largest=%d, truncated=%#x, len=%d) = %d, past 2^62-1",
				largest, truncated, pnLen, got)
		}
		if mask := uint64(1)<<(8*pnLen) - 1; got&mask != truncated {
			t.Fatalf("decode(largest=%d, truncated=%#x, len=%d) = %d: the low bytes are not the wire's",
				largest, truncated, pnLen, got)
		}
	})
}

// The ACK tracker never holds a number the ACK frame cannot carry: a
// number past 2^62-1 is refused at the door, so appendAckFrame cannot meet
// one whatever the receive path lets through.
func TestAckTrackerRefusesPacketNumberPastCeiling(t *testing.T) {
	var a ackTracker
	now := time.Now()
	if !a.record(MaxVarint, now) {
		t.Fatal("2^62-1 is a valid packet number and was refused")
	}
	if a.record(1<<62, now) {
		t.Fatal("2^62 was recorded")
	}
	if want := []pnRange{{MaxVarint, MaxVarint}}; len(a.ranges) != 1 || a.ranges[0] != want[0] {
		t.Fatalf("ranges = %v, want %v", a.ranges, want)
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("encoding the ACK panicked: %v", r)
			}
		}()
		_ = a.encodeAck(nil, now)
	}()
}

// RFC 9000 §12.3: once the largest received number is 2^62-1, the next
// expected one is 2^62 and the bare candidate of §A.3 decodes to it. The
// packet is sealed with that nonce, so the AEAD would accept it; the
// receiver refuses it before the AEAD runs, counts it as discarded, records
// nothing and owes no ACK for it. The process must not die on the ACK
// encoder.
func TestReceiveRefusesPacketNumberPastCeiling(t *testing.T) {
	c, peer := newKeyedConn(t)
	dcid := []byte("scid0000")
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	sp.largest = MaxVarint
	sp.ack.record(MaxVarint, time.Now())
	c.mu.Unlock()

	pkt := shortPacket(t, peer, 0, dcid, 1<<62, []byte{framePing})
	if err := c.receive(pkt, c.peer); err != nil {
		t.Fatalf("receive returned %v; a packet past the ceiling is dropped, not a connection error", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if sp.largest != MaxVarint {
		t.Errorf("largest received packet number is %d: the packet was accepted", sp.largest)
	}
	if c.discarded != 1 {
		t.Errorf("discarded = %d, want 1", c.discarded)
	}
	if len(sp.ack.ranges) != 1 || sp.ack.ranges[0].hi != MaxVarint {
		t.Errorf("ack ranges = %v: the packet past the ceiling is owed an ACK", sp.ack.ranges)
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("encoding the ACK panicked: %v", r)
			}
		}()
		_ = sp.ack.encodeAck(nil, time.Now())
	}()
}
