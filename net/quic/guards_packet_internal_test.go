package quic

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
)

// versionNegotiationFor builds a Version Negotiation packet (RFC 8999 §6)
// echoing dcid then scid, followed by the given supported versions.
func versionNegotiationFor(dcid, scid string, versions ...uint32) []byte {
	vn := []byte{0x80, 0, 0, 0, 0}
	vn = append(vn, byte(len(dcid)))
	vn = append(vn, dcid...)
	vn = append(vn, byte(len(scid)))
	vn = append(vn, scid...)
	for _, v := range versions {
		vn = append(vn, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	return vn
}

// guardClient is a client connection whose first flight used the
// identifiers dcid0000 (destination) and scid0000 (source), as Dial records
// them for the echo check.
func guardClient(t *testing.T) *Conn {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	c.initialDCID = []byte("dcid0000")
	c.mu.Unlock()
	return c
}

// RFC 9368 §4 and RFC 9000 §6.2: a client "MUST ignore a Version Negotiation
// packet that lists the version it attempted". A real server only lists what
// it would have answered, so version 1 in the list is a forgery or a bug;
// acting on it would let one packet that echoes the first flight's
// identifiers (which every observer of the path saw) end a handshake the
// server is willing to complete. The list also carries another version, so
// the packet is not ignored for being unreadable, and the control shows the
// same packet without version 1 is still acted on.
func TestAVersionNegotiationListingVersionOneIsIgnored(t *testing.T) {
	const otherVersion = 0x6b3343cf // RFC 9368's version 2, which this package does not speak
	for name, versions := range map[string][]uint32{
		"version 1 alone":          {version1},
		"version 1 first":          {version1, otherVersion},
		"version 1 last":           {otherVersion, version1},
		"version 1 between others": {otherVersion, version1, 0x0a0a0a0a},
	} {
		t.Run(name, func(t *testing.T) {
			c := guardClient(t)
			_ = c.receive(versionNegotiationFor("scid0000", "dcid0000", versions...), c.peer)
			if err := c.Err(); err != nil {
				t.Errorf("the connection closed with %v on a Version Negotiation listing version 1", err)
			}
			if n := discardedCount(c); n != 1 {
				t.Errorf("discarded = %d, want 1", n)
			}
		})
	}

	t.Run("control without version 1", func(t *testing.T) {
		c := guardClient(t)
		_ = c.receive(versionNegotiationFor("scid0000", "dcid0000", otherVersion, 0x0a0a0a0a), c.peer)
		if err := c.Err(); !errors.Is(err, ErrVersionNegotiation) {
			t.Errorf("a Version Negotiation without version 1 left the connection with %v, want ErrVersionNegotiation", err)
		}
	})
}

// RFC 9000 §6.2 and RFC 8999 §6: a Version Negotiation packet echoes the
// client's identifiers swapped (its Destination is the client's Source, its
// Source the client's Destination). One that does not is not an answer to
// this connection — it is a blind forgery from an off-path sender who never
// saw the first flight — and is ignored. The list has no version 1 in any
// case, so only the echo check keeps the connection open; the control shows
// the correctly echoed packet ends the dial.
func TestAVersionNegotiationNotEchoingOurIdentifiersIsIgnored(t *testing.T) {
	const otherVersion = 0x0a0a0a0a
	for _, tc := range []struct {
		name       string
		dcid, scid string
	}{
		{"wrong destination", "other000", "dcid0000"},
		{"wrong source", "scid0000", "other000"},
		{"both wrong", "other000", "other111"},
		{"echoed unswapped", "dcid0000", "scid0000"},
		{"destination truncated", "scid000", "dcid0000"},
		{"source extended", "scid0000", "dcid00000"},
		{"empty identifiers", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := guardClient(t)
			_ = c.receive(versionNegotiationFor(tc.dcid, tc.scid, otherVersion), c.peer)
			if err := c.Err(); err != nil {
				t.Errorf("the connection closed with %v on a Version Negotiation that does not echo its identifiers", err)
			}
			if n := discardedCount(c); n != 1 {
				t.Errorf("discarded = %d, want 1", n)
			}
		})
	}

	t.Run("control echoing them", func(t *testing.T) {
		c := guardClient(t)
		_ = c.receive(versionNegotiationFor("scid0000", "dcid0000", otherVersion), c.peer)
		if err := c.Err(); !errors.Is(err, ErrVersionNegotiation) {
			t.Errorf("a correctly echoed Version Negotiation left the connection with %v, want ErrVersionNegotiation", err)
		}
	})
}

// RFC 8999 §5.1: under a version this package does not speak, a long
// header's connection IDs run from 0 to 255 bytes; QUIC version 1's 20-byte
// cap (RFC 9000 §17.2) belongs to version 1 alone. The listener's Version
// Negotiation answer must therefore read identifiers of 21..255 bytes and
// echo them whole, swapped, or a client of a future version with long
// identifiers never learns that version 1 is on offer and waits out its
// timeout instead.
func TestAVersionNegotiationEchoesForeignIdentifiersLongerThanTwenty(t *testing.T) {
	serverPC, clientPC := newLoopbackPair(t)
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	for _, tc := range []struct{ dcidLen, scidLen int }{
		{21, 21},
		{255, 255},
		{255, 8},
		{8, 255},
		{20, 20}, // control: within version 1's cap
	} {
		t.Run(fmt.Sprintf("dcid %d scid %d", tc.dcidLen, tc.scidLen), func(t *testing.T) {
			dcid := bytes.Repeat([]byte{0xd1}, tc.dcidLen)
			scid := bytes.Repeat([]byte{0x5c}, tc.scidLen)
			dg := []byte{0xc3, 0x0a, 0x0a, 0x0a, 0x0a} // a version this package does not speak
			dg = append(dg, byte(len(dcid)))
			dg = append(dg, dcid...)
			dg = append(dg, byte(len(scid)))
			dg = append(dg, scid...)
			dg = append(dg, make([]byte, 1200-len(dg))...)

			before := l.DiscardedPackets()
			l.dispatch(dg, clientPC.LocalAddr(), l.pc, 0)
			if err := clientPC.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 2048)
			n, _, err := clientPC.ReadFrom(buf)
			if err != nil {
				t.Fatalf("no Version Negotiation packet arrived (discarded %d more): %v", l.DiscardedPackets()-before, err)
			}
			pkt := buf[:n]
			if len(pkt) < 7 || pkt[0]&0x80 == 0 || pkt[1]|pkt[2]|pkt[3]|pkt[4] != 0 {
				t.Fatalf("the answer is not a Version Negotiation packet: % x", pkt)
			}
			// Read the identifiers by hand: connectionID would refuse
			// anything over 20 bytes, which is the cap under test.
			rest := pkt[5:]
			gotDCIDLen := int(rest[0])
			if len(rest) < 1+gotDCIDLen+1 {
				t.Fatalf("the answer is truncated inside its destination identifier: % x", pkt)
			}
			gotDCID, rest := rest[1:1+gotDCIDLen], rest[1+gotDCIDLen:]
			gotSCIDLen := int(rest[0])
			if len(rest) < 1+gotSCIDLen {
				t.Fatalf("the answer is truncated inside its source identifier: % x", pkt)
			}
			gotSCID, rest := rest[1:1+gotSCIDLen], rest[1+gotSCIDLen:]
			if !bytes.Equal(gotDCID, scid) || !bytes.Equal(gotSCID, dcid) {
				t.Errorf("identifiers echoed as dcid of %d bytes and scid of %d bytes, want the client's %d-byte source and %d-byte destination, whole and swapped",
					len(gotDCID), len(gotSCID), len(scid), len(dcid))
			}
			if len(rest) != 4 || rest[0]|rest[1]|rest[2] != 0 || rest[3] != 1 {
				t.Errorf("supported versions % x, want version 1 alone", rest)
			}
		})
	}
}

// RFC 9000 §17.3.1: the Fixed Bit of a short header "MUST be set to 1"
// and a packet where it is clear is not a valid 1-RTT packet. It has to be
// refused at the parse, before header protection is removed, so that
// anything else the demultiplexer (RFC 7983) sees as non-QUIC never reaches
// the decryption path. The control shows the same bytes with the bit set
// parse, so the refusal is the bit and not the length or the form bit.
func TestAShortHeaderWithTheFixedBitClearIsRefused(t *testing.T) {
	const dcidLen = 8
	pkt := func(first byte) []byte {
		b := []byte{first}
		b = append(b, bytes.Repeat([]byte{0xab}, dcidLen)...)
		return append(b, make([]byte, 32)...)
	}

	h, err := parseShortHeader(pkt(0x40), dcidLen)
	if err != nil {
		t.Fatalf("control: a short header with the fixed bit set was refused: %v", err)
	}
	if !bytes.Equal(h.DCID, bytes.Repeat([]byte{0xab}, dcidLen)) || h.PNOffset != 1+dcidLen {
		t.Errorf("control: DCID % x at packet number offset %d", h.DCID, h.PNOffset)
	}

	// Every combination of the other six low bits (spin, reserved, key
	// phase, packet number length) with the fixed bit clear and the form
	// bit clear.
	for low := byte(0); low < 0x40; low++ {
		if _, err := parseShortHeader(pkt(low), dcidLen); !errors.Is(err, ErrQUIC) {
			t.Fatalf("first byte %#02x: err = %v, want a refusal wrapping ErrQUIC", low, err)
		}
	}
}
