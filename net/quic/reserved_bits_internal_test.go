package quic

import (
	"testing"
)

// RFC 9000 §17.2 and §17.3.1: a packet whose reserved bits are set once
// both protections are off is PROTOCOL_VIOLATION — and only once both are
// off (RFC 9001 §9.5), so the same bits on a packet that fails to
// authenticate are nobody's business and end nothing.
func TestReservedBitsAreCheckedAfterTheAEADOpens(t *testing.T) {
	t.Run("long header", func(t *testing.T) {
		pcA, pcB := newLoopbackPair(t)
		// The server's own identifier is what the client's Initial names,
		// and what both ends derive the Initial keys from.
		scid := []byte("scid0000")
		c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), scid, false, DefaultParameters())
		defer c.Close()
		if err := c.installInitial(scid); err != nil {
			t.Fatal(err)
		}
		pkt := sealedClientInitialFirstByte(t, 0xcc, scid, []byte("cscid000"), nil)
		if err := c.receive(pkt, c.peer); err == nil {
			t.Fatal("an Initial with its reserved bits set was accepted")
		}
		if code := closeCodeFor(c.Err()); code != transportProtocolViolation {
			t.Errorf("the close carries %#x, want PROTOCOL_VIOLATION (0xa); err %v", code, c.Err())
		}
	})

	t.Run("short header", func(t *testing.T) {
		c, peerSealer := newBenchConn(t)
		defer c.Close()
		pkt := sealShortPacket(t, peerSealer, 0x40|0x18, c.scid, []byte{framePing})
		if err := c.receive(pkt, c.peer); err == nil {
			t.Fatal("a 1-RTT packet with its reserved bits set was accepted")
		}
		if code := closeCodeFor(c.Err()); code != transportProtocolViolation {
			t.Errorf("the close carries %#x, want PROTOCOL_VIOLATION (0xa); err %v", code, c.Err())
		}
	})

	t.Run("short header that does not authenticate", func(t *testing.T) {
		c, _ := newBenchConn(t)
		defer c.Close()
		secrets, err := initialSecrets([]byte("someone else's keys"))
		if err != nil {
			t.Fatal(err)
		}
		forger, err := newPacketSealer(secrets.Client)
		if err != nil {
			t.Fatal(err)
		}
		pkt := sealShortPacket(t, forger, 0x40|0x18, c.scid, []byte{framePing})
		if err := c.receive(pkt, c.peer); err != nil {
			t.Fatalf("a forgery ended the connection: %v", err)
		}
		if err := c.Err(); err != nil {
			t.Errorf("a forgery with reserved bits set closed the connection: %v", err)
		}
		c.mu.Lock()
		n := c.discarded
		c.mu.Unlock()
		if n != 1 {
			t.Errorf("discarded = %d, want 1: the forgery is dropped like any other", n)
		}
	})
}

// sealShortPacket seals one 1-RTT packet (packet number 1) with the first
// byte the caller names, so a test can set the bits a real sender never
// would.
func sealShortPacket(t *testing.T, sealer *packetSealer, first byte, dcid, payload []byte) []byte {
	t.Helper()
	const pnLen = 4
	header := []byte{first | byte(pnLen-1)}
	header = append(header, dcid...)
	pnOffset := len(header)
	header = append(header, 0, 0, 0, 1)
	pkt, err := sealer.Seal(nil, header, payload, 1, pnOffset, pnLen)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}
