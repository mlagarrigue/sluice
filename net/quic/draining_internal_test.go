package quic

import (
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// closeFrameBytes is a transport CONNECTION_CLOSE with NO_ERROR, no
// offending frame type and no reason phrase.
func closeFrameBytes() []byte {
	f := varint.Append(nil, frameConnectionClose)
	f = varint.Append(f, 0)
	f = varint.Append(f, 0)
	return varint.Append(f, 0)
}

// drainingConn is a keyed connection whose datagrams a recordingPC keeps,
// with the peer's sealer and its own identifier as the peer names it.
func drainingConn(t *testing.T, name string) (c *Conn, peer *packetSealer, pc *recordingPC, dcid []byte) {
	t.Helper()
	pc = &recordingPC{addr: fakeFuzzAddr(name + ":self")}
	peerPC := &recordingPC{addr: fakeFuzzAddr(name + ":peer")}
	c, peer = newKeyedConnOn(t, pc, peerPC)
	return c, peer, pc, []byte("scid0000")
}

func assertDraining(t *testing.T, c *Conn) {
	t.Helper()
	c.mu.Lock()
	dr := c.draining
	c.mu.Unlock()
	if !dr {
		t.Fatal("the peer's CONNECTION_CLOSE did not put the connection in the draining state")
	}
}

// RFC 9000 §10.2.2: a peer's CONNECTION_CLOSE puts this end in the
// draining state, where it "MUST NOT send any packets". A PING first makes
// an acknowledgement owed; the close that follows must not be what flushes
// it.
func TestDrainingSendsNothingAfterPeerClose(t *testing.T) {
	c, peer, pc, dcid := drainingConn(t, "drain-ping")
	if err := c.receive(shortPacket(t, peer, 0, dcid, 0, []byte{framePing}), c.peer); err != nil {
		t.Fatal(err)
	}
	before := len(pc.recorded())
	_ = c.receive(shortPacket(t, peer, 0, dcid, 1, closeFrameBytes()), c.peer)
	assertDraining(t, c)
	if n := len(pc.recorded()) - before; n > 0 {
		t.Errorf("%d datagram(s) sent while draining", n)
	}
}

// RFC 9000 §13.2 (Table 3): CONNECTION_CLOSE is not ack-eliciting, so a
// packet carrying nothing else owes no acknowledgement — and the draining
// state would forbid sending it anyway.
func TestDrainingLoneCloseElicitsNothing(t *testing.T) {
	c, peer, pc, dcid := drainingConn(t, "drain-lone")
	before := len(pc.recorded())
	_ = c.receive(shortPacket(t, peer, 0, dcid, 0, closeFrameBytes()), c.peer)
	assertDraining(t, c)
	c.mu.Lock()
	owed := c.spaces[spaceApplication].ack.owes(time.Now().Add(time.Second))
	c.mu.Unlock()
	if owed {
		t.Error("a packet carrying only CONNECTION_CLOSE was counted ack-eliciting")
	}
	if n := len(pc.recorded()) - before; n > 0 {
		t.Errorf("%d datagram(s) sent after a lone CONNECTION_CLOSE", n)
	}
}

// The same at the Handshake level: the draining state and the frame's
// classification do not depend on the packet number space.
func TestDrainingLoneHandshakeCloseElicitsNothing(t *testing.T) {
	c, _, pc, _ := drainingConn(t, "drain-hs")
	sec, err := initialSecrets([]byte("handshake-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelHandshake, tls.TLS_AES_128_GCM_SHA256, sec.Server, true); err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelHandshake, tls.TLS_AES_128_GCM_SHA256, sec.Client, false); err != nil {
		t.Fatal(err)
	}
	peer, err := newSealerSuite(sec.Client, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	before := len(pc.recorded())
	_ = c.receive(handshakePacket(t, peer, []byte("scid0000"), []byte("dcid0000"), 0, closeFrameBytes()), c.peer)
	assertDraining(t, c)
	if n := len(pc.recorded()) - before; n > 0 {
		t.Errorf("%d datagram(s) sent after a lone Handshake CONNECTION_CLOSE", n)
	}
}

// handshakePacket seals one Handshake packet the way writePacketLocked
// does: long header, version 1, a four-byte packet number.
func handshakePacket(t *testing.T, s *packetSealer, dcid, scid []byte, pn uint64, payload []byte) []byte {
	t.Helper()
	const pnLen = 4
	header := []byte{0xc0 | packetHandshake<<4 | (pnLen - 1), 0, 0, 0, 1, byte(len(dcid))}
	header = append(header, dcid...)
	header = append(header, byte(len(scid)))
	header = append(header, scid...)
	header = varint.Append(header, uint64(pnLen+len(payload)+16))
	pnOffset := len(header)
	header = append(header, byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn))
	pkt, err := s.Seal(nil, header, payload, pn, pnOffset, pnLen)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

// A packet that carries STREAM bytes and a CONNECTION_CLOSE: the bytes the
// peer sent before closing reach Read, exactly as they do when the same two
// frames arrive in two packets.
//
// Until 2026-10-26 the CONNECTION_CLOSE case returned from framesIn, which
// skipped the packet's fourth pass — the one that moves its STREAM bytes to
// the read buffers or the batch callback (dispatchDeltas). The reassembly had
// already advanced and the packet was acknowledged, so the peer counted the
// bytes received while the application never saw them; the same two frames
// in two packets were delivered normally, so readability depended on how the
// peer had filled its packets. The control case below is what makes that
// comparison the test rather than a claim.
func TestStreamBytesSurviveACloseInTheSamePacket(t *testing.T) {
	read := func(t *testing.T, c *Conn) (string, error) {
		t.Helper()
		s := c.streamIfOpen(0)
		if s == nil {
			t.Fatal("stream 0 was never opened by the peer's STREAM frame")
		}
		p := make([]byte, 32)
		n, err := s.Read(p)
		return string(p[:n]), err
	}

	// The control: the same two frames in two packets, which always worked.
	t.Run("two packets", func(t *testing.T) {
		c := boundsConn(t, false)
		now := time.Now()
		if err := c.frames(spaceApplication, streamFrame(nil, 0, 0, "hello", true), now); err != nil {
			t.Fatal(err)
		}
		if err := c.frames(spaceApplication, closeFrameBytes(), now); err != nil {
			t.Fatal(err)
		}
		if got, err := read(t, c); got != "hello" {
			t.Fatalf("Read returned %q, %v; want %q", got, err, "hello")
		}
	})

	t.Run("one packet", func(t *testing.T) {
		c := boundsConn(t, false)
		payload := streamFrame(nil, 0, 0, "hello", true)
		payload = append(payload, closeFrameBytes()...)
		if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
			t.Fatal(err)
		}
		if got, err := read(t, c); got != "hello" {
			t.Fatalf("Read returned %q, %v; want %q then the close", got, err, "hello")
		}
		// The end follows, on the next Read: the bytes first, then the
		// close, which is the order Read's contract promises.
		if s := c.streamIfOpen(0); s != nil {
			if _, err := s.Read(make([]byte, 1)); err != nil &&
				!errors.Is(err, io.EOF) && !errors.Is(err, errClosed) {
				t.Errorf("the read after the bytes returned %v", err)
			}
		}
	})
}

// RFC 9000 §10.2.2: "While otherwise identical to the closing state, an
// endpoint in the draining state MUST NOT send any packets." A peer may
// coalesce its handshake-time close behind an ack-eliciting packet: the
// first packet's immediate acknowledgement is gathered into the datagram
// this end is building, the second packet's CONNECTION_CLOSE puts the
// connection in draining, and what was gathered is dropped, not written
// when the gathering ends.
func TestDrainingDropsWhatTheSameDatagramGathered(t *testing.T) {
	c, _, pc, _ := drainingConn(t, "drain-gathered")
	sec, err := initialSecrets([]byte("handshake-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelHandshake, tls.TLS_AES_128_GCM_SHA256, sec.Server, true); err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelHandshake, tls.TLS_AES_128_GCM_SHA256, sec.Client, false); err != nil {
		t.Fatal(err)
	}
	peer, err := newSealerSuite(sec.Client, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	// One datagram: a Handshake PING, then a Handshake CONNECTION_CLOSE.
	d := append(handshakePacket(t, peer, []byte("scid0000"), []byte("dcid0000"), 0, []byte{framePing}),
		handshakePacket(t, peer, []byte("scid0000"), []byte("dcid0000"), 1, closeFrameBytes())...)
	before := len(pc.recorded())
	// processDatagram's gather window around receive, without its pump:
	// the test connection has no TLS state to drive.
	c.mu.Lock()
	c.gatherLocked(true)
	c.mu.Unlock()
	_ = c.receive(d, c.peer)
	c.mu.Lock()
	c.gatherLocked(false)
	c.mu.Unlock()
	assertDraining(t, c)
	if n := len(pc.recorded()) - before; n > 0 {
		t.Errorf("%d datagram(s) written after the datagram that put the connection in draining: the acknowledgement gathered before the close left anyway", n)
	}
}
