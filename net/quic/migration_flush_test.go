package quic

import (
	"testing"
	"time"
)

// What the control queue holds when a migration begins drains on the path
// being left, ahead of that path's PATH_CHALLENGE, and like the challenge it
// stays off the new path's books (RFC 9000 §9.4): its loss — certain when the
// old binding is dead, a NAT rebinding — opens no congestion event there, and
// its acknowledgement gives that path no RTT sample. The flush went out with
// the new path's start time and the old path's address and socket, which
// nothing then told apart from the path in use, and counted as the new
// path's. The setup is a credit waiting in the queue and a datagram from a
// new address through the real entry point, maybeMigrate.
func TestTheFlushBeforeAMigrationIsTheOldPathsTraffic(t *testing.T) {
	main := &recordingPC{addr: fakeFuzzAddr("flush:self")}
	peerA := fakeFuzzAddr("flush:peerA")
	peerB := fakeFuzzAddr("flush:peerB")
	c := newConn(main, peerA, nil, []byte("selfcid0"), false, DefaultConfig())
	installAppSealer(t, c)
	t.Cleanup(func() { _ = c.Close() })

	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = []peerCID{{seq: 0, cid: []byte("peercid0"), used: true}, {seq: 1, cid: []byte("peercid1")}}
	c.currentPeerSeq = 0
	c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = peerB, main, 1200, true
	c.grants = appendFrame(c.grants, frameMaxData, uint64(1<<20))
	sp := &c.spaces[spaceApplication]
	first := sp.nextPN
	startWrites := len(main.recorded())
	c.mu.Unlock()

	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())

	c.mu.Lock()
	defer c.mu.Unlock()
	if sameAddr(c.peer, peerA) {
		t.Fatal("setup: no migration happened")
	}
	toOld := 0
	for _, w := range main.recorded()[startWrites:] {
		if sameAddr(w.to, peerA) {
			toOld++
		}
	}
	if toOld < 2 {
		t.Fatalf("setup: %d datagrams to the old address; want the flush and the challenge", toOld)
	}
	if len(c.grants) != 0 {
		t.Fatalf("setup: the credit is still queued (%d bytes)", len(c.grants))
	}
	for i := range sp.sent.packets {
		p := &sp.sent.packets[i]
		if p.pn >= first && p.pn < first+uint64(toOld) && sp.sent.onPath(p) {
			t.Errorf("packet %d left for the old address during the migration and counts as the new path's (§9.4)", p.pn)
		}
	}
}
