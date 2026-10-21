package quic

import (
	"testing"
	"time"
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
