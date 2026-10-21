package quic

import (
	"testing"
)

// RFC 9001 §6.6: the packets that fail authentication are counted "during
// the lifetime of a connection", "across all keys" — not per level. A
// forgery at the Initial level and one under 1-RTT keys add up, and the
// one that crosses the integrity limit of the AEAD it was tried under
// closes the connection with AEAD_LIMIT_REACHED, whichever level it came
// at. Both levels here use AES-128-GCM, whose limit is 2^52.
func TestFailedOpensAddUpAcrossLevels(t *testing.T) {
	c, _ := newKeyedConn(t)
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	limit := c.spaces[spaceInitial].opener.k.intLimit
	c.failedOpens = limit - 2
	c.mu.Unlock()

	// One forgery at the Initial level: the count is one short of the limit.
	if err := c.receive(syntheticInitial(t, []byte("scid0000"), []byte("attacker")), c.peer); err != nil {
		t.Fatalf("the Initial forgery one short of the limit ended the connection: %v", err)
	}
	// One under 1-RTT keys the peer does not hold: the limit is reached.
	forger, err := newSealerSuite([]byte("not the peer's secret, thirty-two"), aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	err = c.receive(shortPacket(t, forger, 0, []byte("scid0000"), 7, []byte{framePing}), c.peer)
	if err == nil {
		t.Fatal("the 1-RTT forgery that crossed the connection-wide limit was tolerated: the count is per level")
	}
	if code := closeCodeFor(c.Err()); code != transportAEADLimitReached {
		t.Errorf("the close carries code %#x, want AEAD_LIMIT_REACHED (0x0f)", code)
	}
}
