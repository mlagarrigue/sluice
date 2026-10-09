package quic

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"testing"
)

// RFC 9001 Appendix A's test vectors. Everything else in this package checks
// that the pieces agree with each other; this file checks that they agree
// with the specification. A wrong label in the key schedule round-trips
// exactly like a right one, and these vectors are the only thing that would
// notice.
//
// What is pinned is the derivation chain of A.1 — both directions' Initial
// secrets and the packet key, IV and header-protection key expanded from
// them — plus the nonce construction. The full packets of A.2 and A.3 are not
// transcribed: their plaintext runs to a kilobyte, and a vector reproduced
// wrongly is worse than one not used, because it fails the code that is
// right. The derivation is where every label lives, and the seal/open path
// above it is [crypto/cipher] driven by these keys, covered by the round-trip
// tests.

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex in the test itself: %v", err)
	}
	return b
}

// Appendix A.1: the keys derived from the Destination Connection ID of the
// client's first Initial packet, 0x8394c8f03e515708.
func TestInitialKeysMatchRFC9001AppendixA(t *testing.T) {
	s, err := initialSecrets(unhex(t, "8394c8f03e515708"))
	if err != nil {
		t.Fatal(err)
	}
	if want := unhex(t, "c00cf151ca5be075ed0ebfb5c80323c42d6b7db67881289af4008f1f6c357aea"); !bytes.Equal(s.Client, want) {
		t.Errorf("client_initial_secret\n got %x\nwant %x", s.Client, want)
	}
	if want := unhex(t, "3c199828fd139efd216c155ad844cc81fb82fa8d7446fa7d78be803acdda951b"); !bytes.Equal(s.Server, want) {
		t.Errorf("server_initial_secret\n got %x\nwant %x", s.Server, want)
	}

	expansions := []struct {
		dir    string
		secret []byte
		label  string
		want   string
	}{
		{"client", s.Client, "quic key", "1f369613dd76d5467730efcbe3b1a22d"},
		{"client", s.Client, "quic iv", "fa044b2f42a3fd3b46fb255c"},
		{"client", s.Client, "quic hp", "9f50449e04a0e810283a1e9933adedd2"},
		{"server", s.Server, "quic key", "cf3a5331653c364c88f0f379b6067e37"},
		{"server", s.Server, "quic iv", "0ac1493ca1905853b0bba03e"},
		{"server", s.Server, "quic hp", "c206b8d9b9f0f37644430b490eeaa314"},
	}
	for _, e := range expansions {
		want := unhex(t, e.want)
		got, err := expandLabel(e.secret, e.label, len(want))
		if err != nil {
			t.Fatalf("%s %q: %v", e.dir, e.label, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s %q\n got %x\nwant %x", e.dir, e.label, got, want)
		}
	}
}

// The nonce is the IV exclusive-ored with the packet number, right-aligned
// (RFC 9001 §5.3). A.2's packet is number 2, so its nonce is the client IV
// with the last byte flipped from 0x5c to 0x5e.
func TestNonceConstruction(t *testing.T) {
	k, err := newKeys(unhex(t, "c00cf151ca5be075ed0ebfb5c80323c42d6b7db67881289af4008f1f6c357aea"))
	if err != nil {
		t.Fatal(err)
	}
	if want := unhex(t, "fa044b2f42a3fd3b46fb255e"); !bytes.Equal(k.nonce(2), want) {
		t.Errorf("nonce for packet 2\n got %x\nwant %x", k.nonce(2), want)
	}
}

// Appendix A.5: the ChaCha20-Poly1305 short-header packet. Unlike A.2/A.3
// this one is transcribed whole — it is twenty-one bytes — and it is the only
// vector that exercises the transplanted cipher and the §5.4.4 header
// protection end to end: a wrong counter/nonce split of the sample, or a
// wrong keystream offset, round-trips happily against itself and only this
// fails.
func TestChaChaPacketMatchesRFC9001AppendixA5(t *testing.T) {
	secret := unhex(t, "9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b")
	s, err := suiteFor(tls.TLS_CHACHA20_POLY1305_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	k, err := newSuiteKeys(secret, s)
	if err != nil {
		t.Fatal(err)
	}
	if want := unhex(t, "e0459b3474bdd0e44a41c144"); !bytes.Equal(k.iv, want) {
		t.Fatalf("iv\n got %x\nwant %x", k.iv, want)
	}

	// A short header with a zero-length connection ID: first byte 0x42
	// (pnLen 3), packet number 654360564.
	const pn = 654360564
	header := unhex(t, "4200bff4")
	payload := []byte{0x01} // one PING frame
	want := unhex(t, "4cfe4189655e5cd55c41f69080575d7999c25a5bfb")

	sealer := &packetSealer{k: k}
	got, err := sealer.Seal(nil, header, payload, pn, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("protected packet\n got %x\nwant %x", got, want)
	}

	opener := &packetOpener{k: k}
	plain, gotPN, err := opener.Open(append([]byte(nil), want...), 1, pn-1)
	if err != nil {
		t.Fatal(err)
	}
	if gotPN != pn {
		t.Fatalf("packet number = %d, want %d", gotPN, pn)
	}
	if !bytes.Equal(plain, payload) {
		t.Fatalf("payload\n got %x\nwant %x", plain, payload)
	}
}
