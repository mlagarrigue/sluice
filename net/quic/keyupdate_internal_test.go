package quic

import (
	"bytes"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// RFC 9001 Appendix A.5 publishes the secret that follows the ChaCha20
// example's under "quic ku". A wrong label, hash or length derives a key
// the peer does not hold, and the first rotation would end the connection
// with every packet discarded.
func TestNextSecretMatchesRFC9001AppendixA5(t *testing.T) {
	secret := unhex(t, "9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b")
	s, err := suiteFor(tls.TLS_CHACHA20_POLY1305_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	k, err := newSuiteKeys(secret, s)
	if err != nil {
		t.Fatal(err)
	}
	next, err := k.next()
	if err != nil {
		t.Fatal(err)
	}
	want := unhex(t, "1223504755036d556342ee9361d253421a826c9ecdf3c7148684b36b714881f9")
	if !bytes.Equal(next.secret, want) {
		t.Errorf("next secret = %x, want %x", next.secret, want)
	}
	sample := unhex(t, "5e5cd55c41f69080575d7999c25a5bfb")
	if next.hp.mask(sample) != k.hp.mask(sample) {
		t.Error("the header-protection key changed across the update; §6.1 keeps it")
	}
}

// shortPacket seals one 1-RTT packet the way writePacketLocked does: fixed
// bit, the key phase bit, a four-byte packet number.
func shortPacket(t *testing.T, s *packetSealer, phase byte, dcid []byte, pn uint64, payload []byte) []byte {
	t.Helper()
	header := append([]byte{0x40 | phase | 3}, dcid...)
	header = append(header, byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn))
	pkt, err := s.Seal(nil, header, payload, pn, 1+len(dcid), 4)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

// RFC 9001 §6.5: after the peer rotates, a packet it sent under the old
// keys and the network delayed still authenticates, chosen by its packet
// number sitting below everything seen under the new keys.
func TestOpenerReadsAcrossAKeyUpdate(t *testing.T) {
	sec, err := initialSecrets([]byte("phase-test"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newSealerSuite(sec.Client, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPhasedOpener(sec.Client, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	dcid := []byte("dcid")
	old := shortPacket(t, sealer, 0, dcid, 5, []byte{framePing})
	next, err := sealer.k.next()
	if err != nil {
		t.Fatal(err)
	}
	sealer = &packetSealer{k: next}
	fresh := shortPacket(t, sealer, keyPhaseBit, dcid, 6, []byte{framePing})

	if _, pn, used, err := opener.openPhased(fresh, 1+len(dcid), 0); err != nil || pn != 6 || used != usedNext {
		t.Fatalf("the first packet of the new phase: pn %d, used %d, err %v; want 6, usedNext, nil", pn, used, err)
	}
	opener, err = opener.rotated(6)
	if err != nil {
		t.Fatal(err)
	}
	if _, pn, used, err := opener.openPhased(old, 1+len(dcid), 6); err != nil || pn != 5 || used != usedPrevious {
		t.Fatalf("the delayed old-phase packet: pn %d, used %d, err %v; want 5, usedPrevious, nil", pn, used, err)
	}
	// Once dropped, the old keys are gone: the same packet is a forgery.
	if _, _, _, err := opener.withoutPrevious().openPhased(old, 1+len(dcid), 6); err == nil {
		t.Error("an old-phase packet authenticated after the previous keys were dropped")
	}
}

// newKeyedConn is a bare server Conn with 1-RTT keys installed both ways
// and the handshake confirmed, plus the sealer a peer would use to write to
// it. The peer's read secret is this end's write secret and vice versa.
func newKeyedConn(t *testing.T) (c *Conn, peer *packetSealer) {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	c = newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	sec, err := initialSecrets([]byte("app-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelApplication, tls.TLS_AES_128_GCM_SHA256, sec.Server, true); err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelApplication, tls.TLS_AES_128_GCM_SHA256, sec.Client, false); err != nil {
		t.Fatal(err)
	}
	peer, err = newSealerSuite(sec.Client, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	return c, peer
}

// RFC 9001 §6.2: a packet under the next phase moves this end's read and
// send keys; a delayed packet of the old phase is still read; and a second
// rotation before this end has acknowledged the first under the new keys
// is KEY_UPDATE_ERROR. The packets carry only PADDING so no acknowledgement
// is owed, which keeps the response pending for as long as the test needs.
func TestPeerKeyUpdateIsFollowedAndADoubleOneRefused(t *testing.T) {
	c, peer := newKeyedConn(t)
	dcid := []byte("scid0000")
	pad := []byte{framePadding}

	early := shortPacket(t, peer, 0, dcid, 1, pad)
	if err := c.receive(shortPacket(t, peer, 0, dcid, 0, pad), c.peer); err != nil {
		t.Fatal(err)
	}
	next, err := peer.k.next()
	if err != nil {
		t.Fatal(err)
	}
	peer = &packetSealer{k: next}
	if err := c.receive(shortPacket(t, peer, keyPhaseBit, dcid, 2, pad), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	phase, due, updates := c.ku.phase, c.ku.responseDue, c.ku.updates
	readBit := c.spaces[spaceApplication].opener.phase.bit
	c.mu.Unlock()
	if phase != keyPhaseBit || readBit != keyPhaseBit || updates != 1 {
		t.Fatalf("after the peer's rotation: send phase %#x, read phase %#x, %d updates; want %#x, %#x, 1", phase, readBit, updates, keyPhaseBit, keyPhaseBit)
	}
	if !due {
		t.Fatal("the answer to the rotation was not recorded as pending")
	}

	// The old-phase packet the network held back.
	if err := c.receive(early, c.peer); err != nil {
		t.Fatal(err)
	}
	if n := c.Stats().DiscardedPackets; n != 0 {
		t.Fatalf("%d packets discarded; the delayed old-phase packet should have been read", n)
	}

	// The peer rotates again without waiting for this end's acknowledgement.
	again, err := peer.k.next()
	if err != nil {
		t.Fatal(err)
	}
	peer = &packetSealer{k: again}
	err = c.receive(shortPacket(t, peer, 0, dcid, 3, pad), c.peer)
	wantViolation(t, err, transportKeyUpdateError, "a second rotation before the first was acknowledged")
}

// RFC 9001 §6.6 and §6: at the confidentiality limit a 1-RTT sender rotates
// instead of closing — once a packet under the current keys was
// acknowledged (§6.1). Before that, the limit still closes the connection.
func TestAEADLimitRotatesTheApplicationKeys(t *testing.T) {
	c, _ := newKeyedConn(t)

	c.mu.Lock()
	c.spaces[spaceApplication].sealer.sealed = aes128Suite.confLimit - 1
	err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{})
	c.mu.Unlock()
	if code := closeCodeFor(err); err == nil || code != transportAEADLimitReached {
		t.Fatalf("before any acknowledgement, the limit gave %v (code %#x); want AEAD_LIMIT_REACHED", err, code)
	}

	c, _ = newKeyedConn(t)
	c.mu.Lock()
	c.ku.acked = true
	c.spaces[spaceApplication].sealer.sealed = aes128Suite.confLimit - 1
	err = c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{})
	phase, sealed := c.ku.phase, c.spaces[spaceApplication].sealer.sealed
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("the send at the limit failed: %v", err)
	}
	if phase != keyPhaseBit || sealed != 1 {
		t.Errorf("after the limit: phase %#x, %d sealed under the new key; want %#x, 1", phase, sealed, keyPhaseBit)
	}
	if n := c.Stats().KeyUpdates; n != 1 {
		t.Errorf("Stats().KeyUpdates = %d, want 1", n)
	}
}

// echoOnce writes payload on a fresh stream and reads it back, the server
// side echoing whatever it accepts.
func echoRoundTrip(t *testing.T, client *Conn, payload string) {
	t.Helper()
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(payload))
	for got := 0; got < len(payload); {
		n, err := st.Read(buf[got:])
		got += n
		if err != nil && got < len(payload) {
			t.Fatalf("reading the echo: %v", err)
		}
	}
	if string(buf) != payload {
		t.Fatalf("echo = %q, want %q", buf, payload)
	}
}

// A real handshake over loopback, then a key update initiated by each side
// in turn, with data flowing across both: RFC 9001 §6 end to end, the
// sender's §6.1 permission, the responder's §6.2 switch and the
// acknowledgement that completes it.
func TestKeyUpdateInitiatedByEachSide(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
		for {
			st, err := c.AcceptStream()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := st.Read(buf)
					if n > 0 {
						if _, werr := st.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						_ = st.CloseWrite()
						return
					}
				}
			}()
		}
	}()

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(clientPC, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}

	initiate := func(who string, c *Conn) {
		t.Helper()
		waitFor(t, who+" has a packet of the current phase acknowledged", func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.canInitiateKeyUpdateLocked()
		})
		c.mu.Lock()
		ok, err := c.initiateKeyUpdateLocked()
		c.mu.Unlock()
		if err != nil || !ok {
			t.Fatalf("%s could not initiate: ok=%v err=%v", who, ok, err)
		}
	}
	wantUpdates := func(n uint64) {
		t.Helper()
		waitFor(t, "both sides counted the rotation", func() bool {
			return client.Stats().KeyUpdates == n && server.Stats().KeyUpdates == n
		})
	}

	echoRoundTrip(t, client, "before any rotation")
	initiate("the client", client)
	echoRoundTrip(t, client, "under the client's rotation")
	wantUpdates(1)
	initiate("the server", server)
	echoRoundTrip(t, client, "under the server's rotation")
	wantUpdates(2)
	if err := client.Err(); err != nil {
		t.Errorf("the client ended with %v", err)
	}
	if err := server.Err(); err != nil {
		t.Errorf("the server ended with %v", err)
	}
}
