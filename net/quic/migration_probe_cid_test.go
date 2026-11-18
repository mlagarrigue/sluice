package quic

import (
	"bytes"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// The identifier a probe given up went under is retired (RFC 9000 §5.1.2):
// a RETIRE_CONNECTION_ID is queued for it and it leaves the pool. Marked
// used and kept, it was out of every pick for the life of the connection,
// and the peer, which saw nothing retired, had no reason to issue a
// replacement: against a server that issued two identifiers, Rebind
// returned ErrNoFreshConnectionID for good.

// seqOf is the pool sequence of cid, and whether it is pooled. Callers hold
// mu.
func seqOf(c *Conn, cid []byte) (uint64, bool) {
	for _, e := range c.peerCIDs {
		if bytes.Equal(e.cid, cid) {
			return e.seq, true
		}
	}
	return 0, false
}

// retired reports whether seq left the pool with a RETIRE_CONNECTION_ID
// queued. Callers hold mu.
func retired(c *Conn, seq uint64) bool {
	for _, e := range c.peerCIDs {
		if e.seq == seq {
			return false
		}
	}
	return slices.Contains(c.controlRetires, seq)
}

// A client's probe of the server's preferred address that the server never
// answers.
func TestAPreferredAddressProbeGivenUpRetiresItsIdentifier(t *testing.T) {
	pref := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 4433}
	c, _ := newPreferredClient(t, "probe-cid-pref", pref)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmLocked()
	seq, ok := seqOf(c, c.pathProbe.targetCID)
	if c.pathProbe.target == nil || !ok || seq == c.currentPeerSeq {
		t.Fatal("setup: no probe toward the preferred address under an identifier of its own")
	}
	now := c.pathProbe.sentAt
	for range pathProbeMaxRetransmits + 2 {
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
	}
	if c.pathProbe.pending || c.pathProbe.target != nil {
		t.Fatal("setup: the probe is still alive")
	}
	if !retired(c, seq) {
		t.Errorf("sequence %d of the probe given up: not retired (pool %v, retires %v)", seq, c.peerCIDs, c.controlRetires)
	}
}

// A server's probe from its preferred socket that the client never answers.
func TestAServersProbeGivenUpRetiresItsIdentifier(t *testing.T) {
	c, _ := newMigratableServer(t, "probe-cid-via")
	pref := &recordingPC{addr: fakeFuzzAddr("probe-cid-via:preferred")}
	tok := make([]byte, 16)
	for _, f := range []struct {
		seq uint64
		cid string
	}{{1, "cli-cid1"}, {2, "cli-cid2"}} {
		if err := c.frames(spaceApplication, ncidFrame(f.seq, 0, []byte(f.cid), tok), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.Lock()
	c.amplActive = false
	c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = c.peer, pref, 1200, true
	c.mu.Unlock()
	if err := c.frames(spaceApplication, append(varint.Append(nil, framePathChallenge), 8, 7, 6, 5, 4, 3, 2, 1), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	seq, ok := seqOf(c, c.pathProbe.targetCID)
	if c.pathProbe.via == nil || !ok || seq == c.currentPeerSeq {
		t.Fatal("setup: no probe from the preferred socket under an identifier of its own")
	}
	now := c.pathProbe.sentAt
	for range pathProbeMaxRetransmits + 2 {
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
	}
	if c.pathProbe.pending || c.pathProbe.via != nil {
		t.Fatal("setup: the probe is still alive")
	}
	if !retired(c, seq) {
		t.Errorf("sequence %d of the probe given up: not retired (pool %v, retires %v)", seq, c.peerCIDs, c.controlRetires)
	}
}

// A Rebind while the preferred address is probed: the probe from the old
// socket is given up, and so, at a second Rebind, is the first one's
// candidate (§9.6.3) — each one's identifier with it.
func TestARebindRetiresTheIdentifiersOfThePreferredProbesItReplaces(t *testing.T) {
	first := &recordingPC{addr: fakeFuzzAddr("probe-cid-cand:first")}
	c := newConn(first, fakeFuzzAddr("probe-cid-cand:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultConfig())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	for i := 1; i <= 7; i++ {
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: uint64(i), cid: fmt.Appendf(nil, "peercid%d", i)})
	}
	c.preferred = fakeFuzzAddr("probe-cid-cand:preferred")
	c.probePreferredAddressLocked(time.Now())
	initial, ok := seqOf(c, c.pathProbe.targetCID)
	c.mu.Unlock()
	if !ok {
		t.Fatal("setup: no probe toward the preferred address")
	}
	rebind := func(n string) {
		t.Helper()
		if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("probe-cid-cand:" + n)}); err != nil {
			t.Fatal(err)
		}
	}
	rebind("a")
	c.mu.Lock()
	candidate, ok := seqOf(c, c.preferredCandidate.cid)
	if !ok {
		c.mu.Unlock()
		t.Fatal("setup: no candidate after the first Rebind")
	}
	if !retired(c, initial) {
		t.Errorf("sequence %d of the probe the first Rebind gave up: not retired (retires %v)", initial, c.controlRetires)
	}
	c.mu.Unlock()
	rebind("b")
	c.mu.Lock()
	defer c.mu.Unlock()
	if next, ok := seqOf(c, c.preferredCandidate.cid); !ok || next == candidate {
		t.Fatal("setup: the candidate was not replaced")
	}
	if !retired(c, candidate) {
		t.Errorf("sequence %d of the candidate the second Rebind replaced: not retired (retires %v)", candidate, c.controlRetires)
	}
}
