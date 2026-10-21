package quic

import (
	"testing"
	"time"
)

// RFC 9000 §14: a datagram larger than the path carries, or than the peer's
// max_udp_payload_size, may be discarded by the peer (§14.1). The ACK rides
// every packet that owes one, and its size is decided by the peer: 32
// ranges of received packets (maxAckRanges) make it a few hundred bytes,
// which a full stream chunk leaves no room for — the datagram used to leave
// oversized, on a data flight, whenever the peer had opened gaps.
func TestLongAckBesideFullChunkStaysUnderMaxDatagram(t *testing.T) {
	const ranges = maxAckRanges
	for _, tc := range []struct {
		name string
		gaps bool
	}{
		{"with gaps", true},
		{"without gaps", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tap := sealedAppConn(t)
			now := time.Now()
			c.mu.Lock()
			sp := &c.spaces[spaceApplication]
			if tc.gaps {
				// Every other packet number, spaced so each gap takes a
				// four-byte varint: the widest ACK one space can owe.
				for i := range uint64(ranges) {
					sp.ack.record(1+i<<20, now)
				}
				sp.ack.onAckEliciting(spaceApplication, now, false)
			} else {
				sp.ack.record(1, now)
				sp.ack.onAckEliciting(spaceApplication, now, false)
			}
			chunk := c.maxStreamChunk()
			limit := c.maxDatagram
			c.peerMaxStreamsBidi = 1 // the grant the handshake would have brought
			c.mu.Unlock()

			s, err := c.Stream(0)
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			s.sendQ.max = 1 << 20
			c.mu.Unlock()
			if _, err := s.Write(make([]byte, chunk)); err != nil {
				t.Fatal(err)
			}
			writes := tap.recorded()
			for _, w := range writes {
				if len(w.data) > limit {
					t.Errorf("a datagram of %d bytes left; maxDatagram is %d", len(w.data), limit)
				}
			}
			c.mu.Lock()
			owed := sp.ack.owes(time.Now())
			c.mu.Unlock()
			if owed {
				t.Error("the ACK is still owed after the chunk left")
			}
			// A short ACK rides the chunk's packet; the long one leaves
			// ahead on its own, and the chunk is not cut for it.
			want := 1
			if tc.gaps {
				want = 2
			}
			if len(writes) != want {
				t.Errorf("%d datagrams left, want %d", len(writes), want)
			}
		})
	}
}

// sealedAppConn is a client with application keys and send credit, with a
// tap on its socket — enough for Stream.Write to produce packets without a
// peer at the other end.
func sealedAppConn(t *testing.T) (*Conn, *tapPC) {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	tap := newTap(pcA)
	c := newConn(tap, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.connSend.max = 1 << 20
	c.mu.Unlock()
	return c, tap
}
