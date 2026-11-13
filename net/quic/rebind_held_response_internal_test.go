package quic

import (
	"bytes"
	"testing"
)

// A PATH_RESPONSE held for a full window answers a challenge that came to
// the local address in use (RFC 9000 §8.2.2: the response goes on the path
// the challenge arrived by). Rebind leaves that address even when it keeps
// the socket — "the address behind this socket changed" — so the held
// answer is dropped, not sent from the new address under a fresh window,
// where the peer would take it as validating the path it left.
func TestRebindOnTheSameSocketDropsTheHeldResponse(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("held-rebind:self")}
	c := newConn(pc, fakeFuzzAddr("held-rebind:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultConfig())
	defer c.Close()
	installAppSealer(t, c)
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")})
	h := &c.heldResponse
	h.frame[0] = framePathResponse
	copy(h.frame[1:], data)
	h.pc, h.peer, h.owed = c.pc, c.peer, true
	c.mu.Unlock()

	if err := c.Rebind(pc); err != nil {
		t.Fatal(err)
	}
	for _, w := range pc.recorded() {
		for _, f := range openShort(t, w.data, len("peercid1")) {
			if f.Type == framePathResponse && bytes.Equal(f.Data, data) {
				t.Fatal("the PATH_RESPONSE held for the address left went out after Rebind")
			}
		}
	}
	c.mu.Lock()
	owed := c.heldResponse.owed
	c.mu.Unlock()
	if owed {
		t.Error("Rebind kept the held PATH_RESPONSE owed")
	}
}
