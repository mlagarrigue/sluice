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

// §5.1 again: a stream enters "closed" once an endpoint "both sends and
// receives a frame with an END_STREAM flag set". A request ended by its
// HEADERS and answered to its END_STREAM is closed on both sides, and a
// late HEADERS or DATA on it — the peer's fault — is answered with nothing,
// where a stream still being answered ("half-closed (remote)") gets the
// STREAM_CLOSED reset the same paragraph asks for.
func TestH2NoResetOnAStreamClosedByBothEndStreams(t *testing.T) {
	for _, late := range []struct {
		name  string
		frame func() h2Frame
	}{
		{"HEADERS", func() h2Frame {
			return h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: headBlock("GET", "/y")}
		}},
		{"DATA", func() h2Frame {
			return h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1, Payload: []byte("late")}
		}},
	} {
		t.Run(late.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)
			c.request(1, "GET", "/x")
			c.flush()
			c.bodies(1)
			if !c.ended[1] {
				t.Fatal("the answer on stream 1 did not end; the case under test is not reached")
			}
			c.frame(late.frame())
			// A second request after it: its answer is the sign the late
			// frame was read, and a RST_STREAM for it would have come first.
			c.request(3, "GET", "/z")
			c.flush()
			c.bodies(3)
			if code, seen := c.resets[1]; seen {
				t.Fatalf("the server sent RST_STREAM(%#x) on stream 1, closed by both END_STREAM flags: §5.1 forbids any frame but PRIORITY there", code)
			}
			if c.sawGoAway {
				t.Fatal("the late frame on a closed stream ended the connection")
			}
		})
	}
}

// A stream closed by both END_STREAM flags stays closed however many answers
// follow it: whether its answer ended is known for as long as this end
// remembers the stream at all. It used to be read from a ring of the last 256
// answers, and a late frame on a stream answered 300 requests earlier drew
// the RST_STREAM that §5.1 forbids on a closed stream. MaxConcurrentStreams
// is raised so the reader still remembers stream 1 after 300 retirements.
func TestH2NoResetOnAClosedStreamAnsweredLongAgo(t *testing.T) {
	addr, _ := serveH2(t, Config{MaxConcurrentStreams: 512}, echoTarget)
	c := dialH2(t, addr)
	c.request(1, "GET", "/first")
	c.flush()
	c.bodies(1)
	if !c.ended[1] {
		t.Fatal("the answer on stream 1 did not end; the case under test is not reached")
	}
	const between = 300
	for i := range between {
		id := uint32(3 + 2*i) //nolint:gosec // G115: a small loop index
		c.request(id, "GET", "/next")
		c.flush()
		c.bodies(2 + i)
	}
	c.frame(h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1, Payload: []byte("late")})
	last := uint32(3 + 2*between)
	c.request(last, "GET", "/last")
	c.flush()
	c.bodies(2 + between)
	if !c.ended[last] {
		t.Fatal("the last request was not answered; the late frame's effect is unread")
	}
	if code, seen := c.resets[1]; seen {
		t.Fatalf("the server sent RST_STREAM(%#x) on stream 1, closed by both END_STREAM flags %d answers earlier", code, between)
	}
	if c.sawGoAway {
		t.Fatal("the late frame on a closed stream ended the connection")
	}
}
