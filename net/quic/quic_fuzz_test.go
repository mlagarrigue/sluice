package quic

import "testing"

// FuzzParseFrames: a datagram is whatever arrived on a UDP port, so this
// parser sees hostile input by definition and with no handshake in front of
// it. For any bytes at all it must refuse or return frames that point into
// what it was given, and never panic.
func FuzzParseFrames(f *testing.F) {
	f.Add([]byte{framePing})
	f.Add([]byte{FrameStream | streamLEN, 0x00, 0x04, 'a', 'b', 'c', 'd'})
	f.Add([]byte{frameACK, 0x01, 0x00, 0x00, 0x00})
	f.Add([]byte{frameCrypto, 0x00, 0x02, 0xaa, 0xbb})
	f.Add(append([]byte{frameNewConnectionID, 0x01, 0x00, 0x04}, make([]byte, 20)...))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		frames, err := parseFrames(nil, data)
		if err != nil {
			return
		}
		for _, fr := range frames {
			if fr.Type == framePadding {
				t.Error("padding was reported as a frame")
			}
			if len(fr.Data) > len(data) {
				t.Fatalf("a frame carries %d bytes out of a %d-byte payload", len(fr.Data), len(data))
			}
		}
	})
}

// FuzzParseLongHeader: the first thing an endpoint does with a datagram, and
// the one place a length field can be made to point past what arrived.
func FuzzParseLongHeader(f *testing.F) {
	pkt := []byte{0xc0, 0, 0, 0, 1, 4, 'd', 'c', 'i', 'd', 0}
	pkt = AppendVarint(pkt, 0)
	pkt = AppendVarint(pkt, 4)
	f.Add(append(pkt, 1, 2, 3, 4))
	f.Add([]byte{0xc0, 0, 0, 0, 1})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		h, rest, err := parseLongHeader(data)
		if err != nil {
			return
		}
		if len(h.Raw) > len(data) {
			t.Fatalf("the packet is %d bytes out of a %d-byte datagram", len(h.Raw), len(data))
		}
		if len(h.Raw)+len(rest) != len(data) {
			t.Fatalf("%d + %d bytes accounted for out of %d", len(h.Raw), len(rest), len(data))
		}
		if h.PNOffset > len(h.Raw) {
			t.Fatalf("the packet number starts at %d in a %d-byte packet", h.PNOffset, len(h.Raw))
		}
		if len(h.DCID) > maxConnectionIDLen || len(h.SCID) > maxConnectionIDLen {
			t.Fatal("a connection identifier over the maximum was accepted")
		}
	})
}

// FuzzVarint: every length and identifier in QUIC is one of these, so a
// decoder that reads past its buffer here reads past it everywhere.
func FuzzVarint(f *testing.F) {
	f.Add([]byte{0x25})
	f.Add([]byte{0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c})
	f.Add([]byte{0xc0})

	f.Fuzz(func(t *testing.T, data []byte) {
		v, rest, err := Varint(data)
		if err != nil {
			return
		}
		if len(rest) > len(data) {
			t.Fatal("decoding lengthened the buffer")
		}
		if v > MaxVarint {
			t.Fatalf("decoded %d, over the 62-bit maximum", v)
		}
		// Re-encoding must give back the same value, though not necessarily
		// the same bytes: the encoding is not canonical.
		if again, _, err := Varint(AppendVarint(nil, v)); err != nil || again != v {
			t.Fatalf("%d re-encoded to (%d, %v)", v, again, err)
		}
	})
}
