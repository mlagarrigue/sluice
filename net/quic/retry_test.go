package quic

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

func TestRetryTokenRoundTrips(t *testing.T) {
	key, err := newRetryTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}
	odcid := []byte("original-dcid00")
	newSCID := []byte("server-scid")

	token := appendRetryToken(nil, key, peer, odcid, newSCID)
	got, err := verifyRetryToken(token, key, peer)
	if err != nil {
		t.Fatalf("VerifyRetryToken: %v", err)
	}
	if string(got.odcid) != string(odcid) {
		t.Errorf("odcid = %q, want %q", got.odcid, odcid)
	}
	if string(got.newSCID) != string(newSCID) {
		t.Errorf("newSCID = %q, want %q", got.newSCID, newSCID)
	}
}

func TestRetryTokenRejectsAWrongKey(t *testing.T) {
	key, _ := newRetryTokenKey()
	other, _ := newRetryTokenKey()
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}

	token := appendRetryToken(nil, key, peer, []byte("odcid"), []byte("scid"))
	if _, err := verifyRetryToken(token, other, peer); !errors.Is(err, ErrQUIC) {
		t.Fatalf("a token signed with a different key verified: %v", err)
	}
}

func TestRetryTokenRejectsAWrongAddress(t *testing.T) {
	key, _ := newRetryTokenKey()
	issued := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}
	other := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}

	token := appendRetryToken(nil, key, issued, []byte("odcid"), []byte("scid"))
	if _, err := verifyRetryToken(token, key, other); !errors.Is(err, ErrQUIC) {
		t.Fatalf("a token replayed from a different address verified: %v", err)
	}
}

func TestRetryTokenExpires(t *testing.T) {
	key, _ := newRetryTokenKey()
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}

	// The real encoder, handed an issue time already past the TTL, rather
	// than sleeping past the real TTL in a test.
	stale := appendRetryTokenAt(nil, key, time.Now().Add(-2*retryTokenTTL), peer, []byte("odcid"), []byte("scid"))
	if _, err := verifyRetryToken(stale, key, peer); !errors.Is(err, ErrQUIC) {
		t.Fatalf("an expired token verified: %v", err)
	}

	// The same token freshly issued verifies, so the rejection above is the
	// timestamp's doing and not an encoding drift.
	fresh := appendRetryTokenAt(nil, key, time.Now(), peer, []byte("odcid"), []byte("scid"))
	if _, err := verifyRetryToken(fresh, key, peer); err != nil {
		t.Fatalf("a fresh token built the same way did not verify: %v", err)
	}
}

// RFC 9001 Appendix A.4's Retry packet, pinned byte for byte: the sample
// Retry answering an Initial with destination identifier 0x8394c8f03e515708
// under version 1, whose last sixteen bytes are the integrity tag §5.8
// computes with the published key and nonce. The self-consistency test below
// proves sealing and verifying agree with each other; this one proves they
// agree with every other implementation.
func TestRetryIntegrityTagMatchesRFC9001A4(t *testing.T) {
	odcid := []byte{0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08}
	packet := []byte{
		0xff,                   // long header, Retry, unused bits as the RFC sampled them
		0x00, 0x00, 0x00, 0x01, // version 1
		0x00,                                           // empty DCID
		0x08,                                           // SCID length
		0xf0, 0x67, 0xa5, 0x50, 0x2a, 0x42, 0x62, 0xb5, // SCID
		0x74, 0x6f, 0x6b, 0x65, 0x6e, // the token, literally "token"
		0x04, 0xa2, 0x65, 0xba, 0x2e, 0xff, 0x4d, 0x82, // the integrity tag
		0x90, 0x58, 0xfb, 0x3f, 0x0f, 0x24, 0x96, 0xba,
	}
	tag := retryTag(odcid, packet[:len(packet)-16])
	if !bytes.Equal(tag[:], packet[len(packet)-16:]) {
		t.Errorf("retryTag = %x, want %x — the RFC 9001 A.4 tag", tag, packet[len(packet)-16:])
	}
	if !verifyRetryTag(odcid, packet) {
		t.Error("the RFC 9001 A.4 Retry packet did not verify against its original DCID")
	}
	wrong := []byte{0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x09}
	if verifyRetryTag(wrong, packet) {
		t.Error("the A.4 packet verified against a different original DCID")
	}
}

func TestRetryPacketIntegrityTagVerifies(t *testing.T) {
	dcid := []byte("client-scid-abc")
	newSCID := []byte("server-scid-xyz")
	odcid := []byte("client-first-dcid")

	pkt := appendRetryPacket(nil, dcid, newSCID, odcid, []byte("a-token"))
	h, rest, err := parseLongHeader(pkt)
	if err != nil {
		t.Fatalf("ParseLongHeader: %v", err)
	}
	if len(rest) != 0 {
		t.Errorf("%d bytes left over, want none — a Retry cannot be coalesced", len(rest))
	}
	if h.Type != packetRetry {
		t.Fatalf("type = %d, want PacketRetry", h.Type)
	}
	if string(h.DCID) != string(dcid) || string(h.SCID) != string(newSCID) {
		t.Fatalf("DCID/SCID = %q/%q, want %q/%q", h.DCID, h.SCID, dcid, newSCID)
	}
	if string(h.Token) != "a-token" {
		t.Errorf("token = %q, want %q", h.Token, "a-token")
	}
	if !verifyRetryTag(odcid, h.Raw) {
		t.Error("the Retry's own integrity tag did not verify against the odcid it was built for")
	}
	if verifyRetryTag([]byte("wrong-odcid"), h.Raw) {
		t.Error("the Retry's tag verified against the wrong odcid")
	}
}
