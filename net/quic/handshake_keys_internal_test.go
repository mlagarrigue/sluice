package quic

import "testing"

// RFC 9001 §4.1.4: packets received before their keys are buffered and
// "can be processed once keys are provided by TLS". crypto/tls provides the
// 1-RTT read secret right behind QUICHandshakeDone, in the same queue; until
// 2026-10-27 pump returned at the first and left the second queued, so a
// connection came out of its handshake — Dial returned, Accept returned —
// with no opener in the application space, and the 1-RTT packets already
// deferred waited for the peer's next datagram to run the pump again. Here
// the opener is in place when the handshake completes, on both ends, and an
// extra pump finds nothing left to install.
func TestTheOneRTTReadKeyIsInPlaceWhenTheHandshakeCompletes(t *testing.T) {
	client, server := plainPair(t)
	for name, c := range map[string]*Conn{"server": server, "client": client} {
		c.mu.Lock()
		before := c.spaces[spaceApplication].opener != nil
		c.mu.Unlock()
		if !before {
			t.Errorf("%s: the handshake is complete and the application space has no opener: the 1-RTT read secret is still queued in crypto/tls", name)
		}
		if _, err := c.pump(); err != nil {
			t.Fatalf("%s: pump: %v", name, err)
		}
		c.mu.Lock()
		after := c.spaces[spaceApplication].opener != nil
		c.mu.Unlock()
		if !before && after {
			t.Errorf("%s: one more pump installed the 1-RTT read key: it was queued behind QUICHandshakeDone", name)
		}
	}
}
