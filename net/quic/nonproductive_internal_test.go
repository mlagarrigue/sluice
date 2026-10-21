package quic

import "testing"

// RFC 9000 §21.9: the packets that cost an AEAD open and a parse and move
// nothing are counted, for the operator to read (Stats.NonProductivePackets
// says why nothing is closed on them). A PING alone counts; bytes already
// received count; new bytes, and a packet that elicits no acknowledgement,
// do not.
func TestNonProductivePacketsAreCounted(t *testing.T) {
	c, peer := newKeyedConn(t)
	dcid := []byte("scid0000")
	var pn uint64
	send := func(payload []byte) {
		t.Helper()
		if err := c.receive(shortPacket(t, peer, 0, dcid, pn, payload), c.peer); err != nil {
			t.Fatal(err)
		}
		pn++
	}
	count := func() uint64 { return c.Stats().NonProductivePackets }

	for range 3 {
		send([]byte{framePing})
	}
	if n := count(); n != 3 {
		t.Fatalf("NonProductivePackets = %d after three PING-only packets, want 3", n)
	}
	send(streamFrame(nil, 0, 0, "hello", false))
	if n := count(); n != 3 {
		t.Errorf("NonProductivePackets = %d after new stream bytes, want 3 still: new bytes are progress", n)
	}
	send(streamFrame(nil, 0, 0, "hello", false))
	if n := count(); n != 4 {
		t.Errorf("NonProductivePackets = %d after the same bytes again, want 4: nothing moved", n)
	}
	send([]byte{framePadding, framePadding})
	if n := count(); n != 4 {
		t.Errorf("NonProductivePackets = %d after a PADDING-only packet, want 4: it elicits nothing", n)
	}
	send(streamFrame(nil, 0, 5, " world", true))
	if n := count(); n != 4 {
		t.Errorf("NonProductivePackets = %d after more new bytes and a FIN, want 4", n)
	}
}
