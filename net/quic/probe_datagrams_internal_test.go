package quic

import (
	"bytes"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// cryptoPayload is one CRYPTO frame of n bytes at off, the bytes all fill.
func cryptoPayload(off uint64, n int, fill byte) []byte {
	p := varint.Append(nil, frameCrypto)
	p = varint.Append(p, off)
	p = varint.Append(p, uint64(n)) //nolint:gosec // G115: a test's small positive count
	return append(p, bytes.Repeat([]byte{fill}, n)...)
}

// RFC 9002 §6.2.4: on a probe timeout "a sender MAY send up to two
// full-sized datagrams containing ack-eliciting packets [...] to transmit
// data from multiple packet number spaces" — two in all, not two a space.
// A server whose Initial flight (a ServerHello with a post-quantum key
// share spans two packets) and Handshake flight both wait unacknowledged
// probes the two spaces in one expiry: at most two datagrams, the Initial
// first, since a client reads nothing else without the ServerHello. Under
// the §8.1 limit of an unvalidated client, four datagrams a probe — two
// padded Initial ones, two Handshake ones — spent the budget after two
// probes and left the server waiting on a client whose own probes back
// off. Nothing a probe has no room
// for is lost: what is not sent stays resendable.
func TestAProbeOfTwoSpacesSendsTwoDatagrams(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("probe2:self")}
	c := assembleConn(pc, fakeFuzzAddr("probe2:peer"), []byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	hs, err := initialSecrets([]byte("hskeys00")) // stand-in handshake keys
	if err != nil {
		t.Fatal(err)
	}
	hsSealer, err := newPacketSealer(hs.Server)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	sent := now.Add(-time.Minute)
	initial := [][]byte{cryptoPayload(0, 1000, 'S'), cryptoPayload(1000, 200, 's')}
	handshake := [][]byte{cryptoPayload(0, 700, 'C'), cryptoPayload(700, 700, 'c')}
	c.mu.Lock()
	c.amplActive = false
	c.spaces[spaceHandshake].sealer = hsSealer
	for space, flight := range map[int][][]byte{spaceInitial: initial, spaceHandshake: handshake} {
		sp := &c.spaces[space]
		sp.lastElicit = sent
		for pn, payload := range flight {
			sp.sent.record(sentPacket{pn: uint64(pn), sentAt: sent, size: 1200, ackEliciting: true, payload: payload}) //nolint:gosec // G115: 0 or 1
		}
		sp.nextPN = uint64(len(flight))
	}
	c.mu.Unlock()

	c.actOnDeadlines(now)

	writes := pc.recorded()
	if len(writes) == 0 || len(writes) > 2 {
		t.Fatalf("one probe timeout over two spaces wrote %d datagrams, want one or two", len(writes))
	}
	if types := packetTypes(t, writes[0].data); types[0] != packetInitial {
		t.Errorf("the first datagram opens with packet type %d, want the Initial (%d)", types[0], packetInitial)
	}
	for i, w := range writes {
		t.Logf("datagram %d: %d bytes, packet types %v", i, len(w.data), packetTypes(t, w.data))
		if len(w.data) > 1200 {
			t.Errorf("a %d-byte datagram, over the path's 1200", len(w.data))
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Both Initial payloads left: they are what the client needs first.
	isp := &c.spaces[spaceInitial]
	if isp.nextPN < 4 {
		t.Errorf("Initial nextPN = %d, want both Initial payloads probed (4)", isp.nextPN)
	}
	// Whatever of the Handshake flight did not go is still resendable:
	// queued, or still on its original packet.
	hsp := &c.spaces[spaceHandshake]
	resendable := len(hsp.retrans)
	for _, p := range hsp.sent.packets {
		if p.pn < 2 && len(p.payload) > 0 {
			resendable++
		}
	}
	if hsp.nextPN < 4 && resendable == 0 {
		t.Errorf("the Handshake flight was cut and nothing of it is left to resend: nextPN %d, retrans %d", hsp.nextPN, len(hsp.retrans))
	}
}

// A probe timeout over two spaces whose flights fit in one datagram sends
// that datagram twice: one lost probe must not cost a doubled interval
// (§6.2.4), the reason a one-space probe sends two.
func TestAProbeOfTwoSmallSpacesSendsItsDatagramTwice(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("probe2s:self")}
	c := assembleConn(pc, fakeFuzzAddr("probe2s:peer"), []byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.mu.Lock()
	c.amplActive = false
	c.spaces[spaceHandshake].sealer = c.spaces[spaceInitial].sealer
	for _, space := range []int{spaceInitial, spaceHandshake} {
		sp := &c.spaces[space]
		sp.lastElicit = now.Add(-time.Minute)
		sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: cryptoPayload(0, 100, 'x')})
		sp.nextPN = 1
	}
	c.mu.Unlock()

	c.actOnDeadlines(now)

	writes := pc.recorded()
	if len(writes) != 2 {
		t.Fatalf("%d datagrams, want the one datagram twice", len(writes))
	}
	for i, w := range writes {
		if types := packetTypes(t, w.data); len(types) != 2 || types[0] != packetInitial || types[1] != packetHandshake {
			t.Errorf("datagram %d carries packet types %v, want an Initial then a Handshake", i, types)
		}
	}
}
