package quic

import (
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// An identifier of the peer spent answering a probe from a new address is
// retired once that address is forgotten (RFC 9000 §5.1.2: "Endpoints
// SHOULD retire connection IDs when they are no longer actively using either
// the local or destination address for which the connection ID was used").
// Otherwise a peer that probes three addresses it never moves to, under the
// default active_connection_id_limit of 4, leaves this end with no spare
// identifier and itself at its limit: no probe answered, no migration
// followed, no identifier it may issue.

// abandonedSetup is a server whose peer issued the three spare identifiers
// the default limit allows, sequences 1 to 3.
func abandonedSetup(t *testing.T, name string) *Conn {
	t.Helper()
	c, _ := newMigratableServer(t, name)
	for seq := uint64(1); seq <= 3; seq++ {
		issuePeerCID(t, c, seq)
	}
	return c
}

func issuePeerCID(t *testing.T, c *Conn, seq uint64) {
	t.Helper()
	tok := make([]byte, 16)
	tok[0] = byte(seq)
	cid := []byte{'c', 'l', 'i', '-', 'c', 'i', 'd', '0' + byte(seq)}
	if err := c.frames(spaceApplication, ncidFrame(seq, 0, cid, tok), time.Now()); err != nil {
		t.Fatal(err)
	}
}

// probeFrom is a PATH_CHALLENGE from addr under this end's identifier named,
// answered or not; what it sent is taken off the books.
func probeFrom(t *testing.T, c *Conn, addr fakeFuzzAddr, named uint64) {
	t.Helper()
	rxFromNewAddress(c, addr, 1200, true)
	c.mu.Lock()
	c.rxVia, c.rxLocalCIDSeq = nil, named
	c.mu.Unlock()
	ch := append(varint.Append(nil, framePathChallenge), byte(named), 9, 9, 9, 9, 9, 9, 9)
	if err := c.frames(spaceApplication, ch, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	_, inFlight := c.spaces[spaceApplication].sent.takeAll()
	c.cc.onLifted(inFlight)
	c.mu.Unlock()
}

// moveFrom is a non-probing packet from addr under this end's identifier
// named; it reports whether the connection followed.
func moveFrom(t *testing.T, c *Conn, addr fakeFuzzAddr, named uint64) bool {
	t.Helper()
	rxFromNewAddress(c, addr, 1200, true)
	c.mu.Lock()
	c.rxVia, c.rxLocalCIDSeq = nil, named
	c.mu.Unlock()
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return sameAddr(c.peer, addr)
}

// retiredSeqs is every RETIRE_CONNECTION_ID queued or already sent.
func retiredSeqs(t *testing.T, c *Conn) []uint64 {
	t.Helper()
	frames := controlFrames(t, c)
	c.mu.Lock()
	for _, r := range c.spaces[spaceApplication].sent.packets {
		sent, err := parseFrames(nil, r.payload)
		if err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
		frames = append(frames, sent...)
	}
	c.mu.Unlock()
	var out []uint64
	for _, f := range frames {
		if f.Type == frameRetireConnectionID {
			out = append(out, f.value)
		}
	}
	slices.Sort(out)
	return out
}

func TestAbandonedProbesGiveTheirIdentifiersBack(t *testing.T) {
	ctl := abandonedSetup(t, "ctl")
	if !moveFrom(t, ctl, "ctl:F", 4) {
		t.Fatal("control: with spare identifiers, a move to a new address is not followed")
	}

	c := abandonedSetup(t, "spent")
	for i, a := range []fakeFuzzAddr{"spent:B", "spent:C", "spent:D"} {
		probeFrom(t, c, a, uint64(i+1))
	}
	// No spare identifier left: this move is not followed, and the oldest
	// address answered gives its identifier back so the peer can issue one.
	if moveFrom(t, c, "spent:F", 4) {
		t.Fatal("a move was followed with no identifier left to send under")
	}
	if got := retiredSeqs(t, c); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after three abandoned probes and a move, RETIRE_CONNECTION_ID for %v, want [1] (the oldest probe's)", got)
	}
	// The peer replaces it; its next packet from the new address is followed.
	issuePeerCID(t, c, 4)
	if !moveFrom(t, c, "spent:F", 4) {
		t.Fatal("with the replacement issued, the move is still not followed")
	}
}

func TestAMoveRetiresTheOtherProbedAddresses(t *testing.T) {
	c := abandonedSetup(t, "moved")
	probeFrom(t, c, "moved:B", 1) // answered under the peer's sequence 1
	probeFrom(t, c, "moved:C", 2) // and 2
	if !moveFrom(t, c, "moved:B", 1) {
		t.Fatal("the move to a probed address is not followed")
	}
	c.mu.Lock()
	current := c.currentPeerSeq
	c.mu.Unlock()
	if current != 1 {
		t.Fatalf("the move sends under the peer's sequence %d, want 1, the one B was answered under", current)
	}
	// C is forgotten with the move: its identifier goes back to the peer.
	// The one left behind (0) waits for the new path's validation
	// (retireLeftCIDLocked).
	if got := retiredSeqs(t, c); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("after moving to B, RETIRE_CONNECTION_ID for %v, want [2] (C's)", got)
	}
}
