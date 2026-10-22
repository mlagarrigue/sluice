package quic

import (
	"context"
	"crypto/tls"
	"testing"
)

// controlCeiling is what the control queue may hold whatever the peer
// sends: each kind of frame a peer can make this end queue has its own
// counter (controlRetires, controlNewCIDs, the blocked flags), and their
// sum at the defaults stays far under this. A queue past it is one a peer
// grows by sending more.
const controlCeiling = 4 << 10

// FuzzConnFramesBudget: authenticated frames, of any content, against a
// confirmed server connection. What the peer controls is the frames; what
// this end must keep whatever they say is its own budget:
//
//   - every datagram it sends fits maxDatagram (RFC 9000 §14);
//   - an address other than the validated peer's receives at most three
//     times what came from it (§8, §21.1.1.1), whether the packet only
//     probed (§9.1, the offPath books) or moved the path (§9.3, amplRecv);
//   - the control queue stays under controlCeiling.
//
// The frames are sealed with the keys the connection reads, so they reach
// the frame parser rather than stopping at authentication, and delivered
// one to four times so that what accumulates across packets is reached.
func FuzzConnFramesBudget(f *testing.F) {
	ping := []byte{framePing}
	challenge := append(AppendVarint(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	ncid := AppendVarint(nil, frameNewConnectionID)
	ncid = append(ncid, 1, 1, 8, 's', 'r', 'v', '-', 'c', 'i', 'd', '1')
	ncid = append(ncid, make([]byte, 16)...)
	retire := append(AppendVarint(nil, frameRetireConnectionID), 0)
	stream := append(AppendVarint(nil, FrameStream|0x02), 0, 4, 'a', 'b', 'c', 'd')
	for _, frames := range [][]byte{ping, challenge, ncid, retire, stream, append(challenge, challenge...)} {
		f.Add(frames, false, uint8(1), uint16(0))
		f.Add(frames, true, uint8(1), uint16(0))
		f.Add(frames, true, uint8(4), uint16(1100))
	}

	peer := fakeFuzzAddr("budget:peerA")
	other := fakeFuzzAddr("budget:peerB")
	scid := []byte{9, 10, 11, 12, 13, 14, 15, 16}
	secrets, err := initialSecrets([]byte("budget secrets"))
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, frames []byte, offPath bool, rounds uint8, pad uint16) {
		if len(frames) == 0 || len(frames) > 1400 {
			return
		}
		// A fresh connection per input, as in FuzzReceive: a failure must
		// reproduce from the failing input alone.
		pc := &recordingPC{addr: "budget:self"}
		c := newConn(pc, peer, []byte("peercid0"), scid, false, DefaultParameters())
		defer c.Close()
		opener, err := newPacketOpener(secrets.Client)
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := newPacketSealer(secrets.Server)
		if err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.spaces[spaceApplication].opener = opener
		c.spaces[spaceApplication].sealer = sealer
		c.hasPeerParams = true
		c.peerParams = DefaultParameters()
		c.handshakeConfirmed = true
		// A started TLS server, as every server connection has: a CRYPTO
		// frame reaches it like any other, and its refusal is the peer's
		// error to earn.
		c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}})
		c.mu.Unlock()
		if err := c.tls.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer c.endTLS()
		peerSealer, err := newPacketSealer(secrets.Client)
		if err != nil {
			t.Fatal(err)
		}

		payload := append([]byte(nil), frames...)
		if extra := int(pad%1200) - len(payload); extra > 0 {
			payload = append(payload, make([]byte, extra)...) // PADDING frames
		}
		from := peer
		if offPath {
			from = other
		}
		received := 0
		for pn := range uint64(rounds%4) + 1 {
			dg := shortPacket(t, peerSealer, 0, scid, pn, payload)
			received += len(dg)
			_ = c.receive(dg, from) // a frame the parser refuses closes the connection; that is allowed
		}

		c.mu.Lock()
		limit, ceiling, control := c.maxDatagram, c.pmtuCeilingLocked(), len(c.control)
		c.mu.Unlock()
		sentOff := 0
		for _, w := range pc.recorded() {
			// Past maxDatagram only a DPLPMTUD probe, which goes to the
			// validated peer and never past MAX_PLPMTU (pmtu.go).
			if len(w.data) > limit && (len(w.data) > ceiling || !sameAddr(w.to, peer)) {
				t.Fatalf("a %d-byte datagram left for %v, over maxDatagram %d (probe ceiling %d)",
					len(w.data), w.to, limit, ceiling)
			}
			if !sameAddr(w.to, peer) {
				sentOff += len(w.data)
			}
		}
		if offPath && sentOff > 3*received {
			t.Fatalf("%d bytes to an unvalidated address that sent %d, over three times", sentOff, received)
		}
		if !offPath && sentOff > 0 {
			t.Fatalf("%d bytes to an address that sent nothing", sentOff)
		}
		if control > controlCeiling {
			t.Fatalf("the control queue holds %d bytes, over %d", control, controlCeiling)
		}
	})
}
