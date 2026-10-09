package quic

import "testing"

// A datagram may carry several packets end to end (RFC 9000 §12.2), and the
// declared length of each is what separates them. Both must be processed —
// and zeros after the last packet are padding, not a parse error.
func TestReceiveCoalescedPackets(t *testing.T) {
	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	scid := []byte{9, 10, 11, 12, 13, 14, 15, 16}
	c := newConn(&fuzzSink{addr: "coalesced:self"}, fakeFuzzAddr("coalesced:peer"), dcid, scid, false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}

	// Seal two Initial packets with the client's keys, as a client would.
	secrets, err := initialSecrets(dcid)
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		t.Fatal(err)
	}
	seal := func(pn uint64, payload []byte) []byte {
		var header []byte
		header = append(header, 0xc3) // Initial, 4-byte packet number
		header = append(header, 0, 0, 0, 1)
		header = append(header, byte(len(scid))) // the server's SCID is the DCID here
		header = append(header, scid...)
		header = append(header, byte(len(dcid)))
		header = append(header, dcid...)
		header = AppendVarint(header, 0) // no token
		header = AppendVarint(header, uint64(4+len(payload)+16))
		pnOffset := len(header)
		for i := 3; i >= 0; i-- {
			header = append(header, byte(pn>>(8*uint(i))))
		}
		pkt, err := sealer.Seal(nil, header, payload, pn, pnOffset, 4)
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}

	// Two PING packets coalesced, then datagram padding.
	datagram := seal(0, []byte{framePing, 0, 0, 0})
	datagram = append(datagram, seal(1, []byte{framePing, 0, 0, 0})...)
	datagram = append(datagram, make([]byte, 40)...)

	if err := c.receive(datagram, c.peer); err != nil {
		t.Fatalf("receive: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.spaces[spaceInitial].ack.ranges; len(got) != 1 || got[0] != (pnRange{0, 1}) {
		t.Fatalf("received packet numbers %+v, want both packets of the datagram", got)
	}
	if c.discarded != 0 {
		t.Fatalf("%d packets discarded out of a well-formed coalesced datagram", c.discarded)
	}
}
