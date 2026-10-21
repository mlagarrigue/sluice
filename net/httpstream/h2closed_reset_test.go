package httpstream

import (
	"testing"
	"time"
)

// RFC 9113 §5.1: "An endpoint MUST NOT send frames other than PRIORITY on a
// closed stream". A stream the peer reset is closed; a stream error found on
// it afterwards — a WINDOW_UPDATE of zero, §6.9 — is noted, not answered
// with a RST_STREAM of this end's.
func TestH2NoResetOnAStreamThePeerClosed(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	withReset := func(reset bool) (resets int, goAway bool) {
		c := dialH2(t, addr)
		c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/open")})
		if reset {
			var cancel [4]byte
			be32put(cancel[:], errCancel)
			c.frame(h2Frame{Type: frameRSTStream, StreamID: 1, Payload: cancel[:]})
		}
		c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 1, Payload: []byte{0, 0, 0, 0}})
		c.frame(h2Frame{Type: framePing, Payload: []byte("roundtrp")})
		c.flush()
		deadline := time.Now().Add(3 * time.Second)
		c.pump(func() bool { return c.pings > 0 || c.sawGoAway || time.Now().After(deadline) })
		return c.resetCount[1], c.sawGoAway
	}
	if n, ga := withReset(true); n != 0 || ga {
		t.Errorf("after the peer's RST_STREAM: %d RST_STREAM from the server (GOAWAY %v), want none on a closed stream", n, ga)
	}
	if n, ga := withReset(false); n != 1 || ga {
		t.Errorf("control, no peer reset: %d RST_STREAM (GOAWAY %v), want exactly one for the zero WINDOW_UPDATE", n, ga)
	}
}
