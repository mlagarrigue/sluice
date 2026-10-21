package httpstream

import "testing"

// rawH2 dials and sends nothing: the preface and the frames are the test's.
func rawH2(t *testing.T, addr string) *h2client {
	t.Helper()
	return &h2client{
		t: t, conn: dial(t, addr),
		got:        map[uint32]string{},
		heads:      map[uint32][]byte{},
		ended:      map[uint32]bool{},
		windows:    map[uint32]int{},
		resets:     map[uint32]uint32{},
		resetCount: map[uint32]int{},
		settings:   map[uint16]uint32{},
	}
}

// RFC 9113 §3.4: the client preface "MUST be followed by a SETTINGS frame".
// A PING there is an invalid preface, a connection error of type
// PROTOCOL_ERROR — not a PING to acknowledge.
func TestH2PrefaceMustBeFollowedBySettings(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := rawH2(t, addr)
	if _, err := c.conn.Write([]byte(Preface)); err != nil {
		t.Fatal(err)
	}
	c.frame(h2Frame{Type: framePing, Payload: []byte("no-setts")})
	c.flush()
	c.pump(func() bool { return c.sawGoAway || c.pings > 0 })
	if c.pings > 0 {
		t.Fatal("a PING sent in place of the preface's SETTINGS was acknowledged")
	}
	if !c.sawGoAway {
		t.Fatal("no GOAWAY for a preface not followed by SETTINGS")
	}
	if code := goAwayCode(c.goAway); code != errProtocolError {
		t.Errorf("GOAWAY code %#x, want PROTOCOL_ERROR", code)
	}

	// The control: SETTINGS first, then the PING, is acknowledged.
	c = rawH2(t, addr)
	if _, err := c.conn.Write([]byte(Preface)); err != nil {
		t.Fatal(err)
	}
	c.frame(h2Frame{Type: frameSettings})
	c.frame(h2Frame{Type: framePing, Payload: []byte("settsfst")})
	c.flush()
	c.pump(func() bool { return c.pings > 0 || c.sawGoAway })
	if c.pings == 0 || c.sawGoAway {
		t.Errorf("after a proper preface: pings %d, GOAWAY %v; want the PING acknowledged", c.pings, c.sawGoAway)
	}
}
