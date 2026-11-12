package quic

import (
	"net"
	"testing"
	"time"
)

// A packet sent off the path in use — here a PATH_RESPONSE toward the
// address a challenge came from — is recorded for its acknowledgement but
// counts in none of that path's estimates: RFC 9002 §7, "packets sent on
// other paths do not alter the current path's congestion controller", and
// §5.3, they "do not contribute RTT samples to the current path" (RFC 9000
// §9.4 too). The only notion of path the record had was a date, which sets
// apart the packets of a previous path, not those sent elsewhere while the
// path in use stays the same.

// offPathAnswer has an application-ready server answer one PATH_CHALLENGE
// that came from an address other than the path in use, and returns the
// answer's packet number. Callers hold mu.
func offPathAnswer(t *testing.T, c *Conn) uint64 {
	t.Helper()
	c.dcid = nil // zero-length identifiers: the answer may leave off-path
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c.rxFrom = from
	c.offPath.addr, c.offPath.recv = from, 1000
	sp := &c.spaces[spaceApplication]
	pn := sp.nextPN
	c.answerPathChallengesLocked([]byte{framePathResponse, 1, 2, 3, 4, 5, 6, 7, 8}, time.Now())
	if c.offPath.sent == 0 || sp.nextPN != pn+1 {
		t.Fatalf("no single off-path answer (offPath.sent=%d, packets=%d)", c.offPath.sent, sp.nextPN-pn)
	}
	c.rxFrom = nil
	return pn
}

// Lost while the path in use keeps working, the off-path answer leaves its
// window as it was.
func TestALostOffPathPacketLeavesTheWindowAlone(t *testing.T) {
	c, cc := appConn(t)
	c.mu.Lock()
	initial := cc.cwnd
	// A round trip of a second already measured keeps the time threshold
	// (RFC 9002 §6.1.2) far above the gaps between the sends below: with a
	// millisecond, a Windows clock that ticked 15 ms between two of them
	// declared the on-path packets lost by time as well (CI, 2026-11-12).
	c.rtt.sample(time.Second, 0)
	pn := offPathAnswer(t, c)
	for range 3 {
		sent(t, c, []byte{framePing}) // on the path in use
	}
	c.mu.Unlock()

	var peer ackTracker
	at := time.Now().Add(10 * time.Millisecond)
	peer.record(pn+3, at) // the answer is packetThreshold below: lost
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), at); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	cwnd := cc.cwnd
	// Against a false green: the two on-path packets below the acknowledged
	// one are still tracked, so the answer was the only loss.
	left := len(c.spaces[spaceApplication].sent.packets)
	c.mu.Unlock()
	if left != 2 {
		t.Fatalf("%d packets still tracked, want the 2 on-path ones: the loss is not the answer's alone", left)
	}
	if cwnd < initial {
		t.Errorf("cwnd = %d after losing only the off-path answer, want at least the initial %d", cwnd, initial)
	}
}

// Acknowledged as the frame's largest, the off-path answer gives the path in
// use no round-trip sample.
func TestAnAcknowledgedOffPathPacketGivesNoRTTSample(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	if c.rtt.sampled {
		c.mu.Unlock()
		t.Fatal("precondition lost: appConn already sampled")
	}
	pn := offPathAnswer(t, c)
	c.mu.Unlock()

	var peer ackTracker
	at := time.Now().Add(10 * time.Millisecond)
	peer.record(pn, at)
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), at); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	sampled := c.rtt.sampled
	c.mu.Unlock()
	if sampled {
		t.Error("the off-path answer's acknowledgement gave the path in use a round-trip sample")
	}
}
