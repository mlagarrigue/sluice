package quic

import "testing"

// RFC 9001 §4.1.4: packets received before their keys are buffered and
// "can be processed once keys are provided by TLS". crypto/tls provides the
// 1-RTT read secret right behind QUICHandshakeDone, in the same queue; until
// 2026-10-27 pump returned at the first and left the second queued, so a
// connection came out of its handshake — Dial returned, Accept returned —
// with no opener in the application space, and the 1-RTT packets already
// deferred waited for the peer's next datagram to run the pump again. Here
// the opener is in place when the handshake completes, on both ends. The
// connections are live, their read loops running: nothing here pumps or
// injects, which would race with them.
func TestTheOneRTTReadKeyIsInPlaceWhenTheHandshakeCompletes(t *testing.T) {
	client, server := plainPair(t)
	for name, c := range map[string]*Conn{"server": server, "client": client} {
		c.mu.Lock()
		opener := c.spaces[spaceApplication].opener != nil
		c.mu.Unlock()
		if !opener {
			t.Errorf("%s: the handshake is complete and the application space has no opener: the 1-RTT read secret is still queued in crypto/tls", name)
		}
	}
}
