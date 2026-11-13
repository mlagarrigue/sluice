package quic

import (
	"net"
	"testing"
	"time"
)

// RFC 9000 §9.4: "Packets sent on the old path MUST NOT contribute to
// congestion control or RTT estimation for the new path." The first
// PATH_CHALLENGE toward the path being left goes out from migrateLocked
// before the peer address changes, so neither its destination nor its
// socket tells it apart from the path in use: its record must still count
// as the old path's, and its loss — certain when the old binding is dead,
// a NAT rebinding's case — must leave the new path's window alone.
func TestOldPathChallengeIsNotTheNewPaths(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.peer
	c.peerCIDs = []peerCID{{seq: 1, cid: []byte("cid1")}, {seq: 0, cid: []byte("cid0"), used: true}}
	c.currentPeerSeq = 0
	sp := &c.spaces[spaceApplication]
	first := sp.nextPN
	c.migrateLocked(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, c.pc, time.Now())
	if sameAddr(c.peer, old) {
		t.Fatal("the connection did not move")
	}
	var challenge *sentPacket
	for i := range sp.sent.packets {
		if sp.sent.packets[i].pn == first {
			challenge = &sp.sent.packets[i]
		}
	}
	if challenge == nil {
		t.Fatalf("no record for packet %d, the old path's challenge", first)
	}
	if sp.sent.onPath(challenge) {
		t.Errorf("the old path's first PATH_CHALLENGE (pn %d) counts as a packet of the new path", first)
	}

	// Only the old path's challenge is lost, by the time threshold: the
	// loss timer runs past it with a later number acknowledged.
	before := c.cc.window()
	later := time.Now().Add(2 * time.Second)
	sp.sent.largestAcked, sp.sent.hasLargestAcked = first+1, true
	sp.sent.lossTime = later.Add(-time.Millisecond)
	c.mu.Unlock()
	c.actOnDeadlines(later)
	c.mu.Lock()
	for i := range sp.sent.packets {
		if sp.sent.packets[i].pn == first {
			t.Fatal("the old path's challenge was not declared lost")
		}
	}
	if w := c.cc.window(); w < before {
		t.Errorf("the old path's challenge lost: the new path's window went from %d to %d", before, w)
	}
}
