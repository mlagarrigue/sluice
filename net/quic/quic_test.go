package quic

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// RFC 9000 §16's own examples, which pin the four widths and the fact that
// the encoding is not canonical: the same value may be written in any width
// large enough, and a decoder that refused the long forms would refuse legal
// traffic.
func TestVarintRFCExamples(t *testing.T) {
	tests := []struct {
		raw  []byte
		want uint64
	}{
		{[]byte{0x25}, 37},
		{[]byte{0x40, 0x25}, 37}, // the same value, two bytes
		{[]byte{0x7b, 0xbd}, 15293},
		{[]byte{0x9d, 0x7f, 0x3e, 0x7d}, 494878333},
		{[]byte{0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c}, 151288809941952652},
		{[]byte{0x00}, 0},
		{[]byte{0xc0, 0, 0, 0, 0, 0, 0, 0}, 0}, // zero in eight bytes
	}
	for _, tt := range tests {
		got, rest, err := Varint(tt.raw)
		if err != nil {
			t.Errorf("Varint(%x) returned %v", tt.raw, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Varint(%x) = %d, want %d", tt.raw, got, tt.want)
		}
		if len(rest) != 0 {
			t.Errorf("Varint(%x) left %d bytes", tt.raw, len(rest))
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 62, 63, 64, 16382, 16383, 16384, 1<<30 - 2, 1<<30 - 1, 1 << 30, MaxVarint} {
		raw := AppendVarint(nil, v)
		got, rest, err := Varint(raw)
		if err != nil || got != v || len(rest) != 0 {
			t.Errorf("%d round-tripped to (%d, %d bytes left, %v)", v, got, len(rest), err)
		}
		// The shortest form, which is what makes the encoding worth having.
		if len(raw) != varintBytes(v) {
			t.Errorf("%d encoded in %d bytes, want %d", v, len(raw), varintBytes(v))
		}
	}
}

func TestVarintRefusesATruncatedBuffer(t *testing.T) {
	for _, raw := range [][]byte{{}, {0x40}, {0x80, 0x00}, {0xc0, 0, 0, 0}} {
		if _, _, err := Varint(raw); !errors.Is(err, ErrTruncated) {
			t.Errorf("Varint(%x) returned %v, want ErrTruncated", raw, err)
		}
	}
}

// The thesis, at the transport that makes it structural: a datagram carries
// frames for whatever streams the sender had ready, so one read is a batch
// with nothing waited for and nothing unusual asked of the peer.
//
// HTTP/1.1 needed a client willing to pipeline. HTTP/2 needed a protocol
// designed around multiplexing. QUIC needs neither: the datagram *is* the
// batch, and always was.
func TestOneDatagramCarriesSeveralStreams(t *testing.T) {
	var payload []byte
	for _, id := range []uint64{0, 4, 8, 12} {
		payload = append(payload, FrameStream|streamLEN|streamOFF)
		payload = AppendVarint(payload, id)
		payload = AppendVarint(payload, 0) // offset
		body := []byte("stream " + string(rune('0'+id/4)))
		payload = AppendVarint(payload, uint64(len(body)))
		payload = append(payload, body...)
	}
	payload = append(payload, framePing)

	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatalf("ParseFrames returned %v", err)
	}

	streams := map[uint64]string{}
	for _, f := range frames {
		if f.IsStream() {
			streams[f.StreamID] = string(f.Data)
		}
	}
	if len(streams) != 4 {
		t.Fatalf("one datagram yielded %d streams, want 4: %v", len(streams), streams)
	}
	for i, id := range []uint64{0, 4, 8, 12} {
		if want := "stream " + string(rune('0'+i)); streams[id] != want {
			t.Errorf("stream %d carried %q, want %q", id, streams[id], want)
		}
	}
	t.Logf("one datagram: %d frames across %d streams", len(frames), len(streams))
}

func TestParseFramesRefusals(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"an unknown frame type", []byte{0x40}},
		{"a STREAM whose length runs past the packet", []byte{FrameStream | streamLEN, 0x00, 0x7f}},
		{"a CRYPTO with no length", []byte{frameCrypto, 0x00}},
		{"an ACK declaring more ranges than bytes", []byte{frameACK, 0x01, 0x00, 0x3f, 0x00}},
		{"a NEW_CONNECTION_ID retiring past itself", []byte{frameNewConnectionID, 0x01, 0x02, 0x01, 0xaa}},
		{"a connection identifier over the maximum", append([]byte{frameNewConnectionID, 0x01, 0x00, 0xff}, make([]byte, 40)...)},
		{"a PATH_CHALLENGE short of its eight bytes", []byte{framePathChallenge, 0x01, 0x02}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseFrames(nil, tt.payload); !errors.Is(err, ErrQUIC) {
				t.Fatalf("ParseFrames returned %v, want ErrQUIC", err)
			}
		})
	}
}

// A STREAM frame without the LEN bit runs to the end of the packet, which is
// what lets a sender fill a datagram without measuring first.
func TestStreamWithoutLengthRunsToTheEnd(t *testing.T) {
	payload := []byte{FrameStream | streamFIN, 0x04}
	payload = append(payload, "the rest of the packet"...)

	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("%d frames, want 1", len(frames))
	}
	if string(frames[0].Data) != "the rest of the packet" {
		t.Errorf("data = %q", frames[0].Data)
	}
	if !frames[0].Fin {
		t.Error("the FIN bit was lost")
	}
}

// Padding is bytes rather than a frame, and reporting it would make every
// caller filter it out.
func TestPaddingIsNotReported(t *testing.T) {
	payload := append(make([]byte, 32), framePing)
	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Type != framePing {
		t.Errorf("frames = %+v, want one PING", frames)
	}
}

func TestParseLongHeader(t *testing.T) {
	// An Initial packet: type bits 00, version 1, both identifiers, an empty
	// token, then a length covering the packet number and payload.
	pkt := []byte{0xc0, 0, 0, 0, 1, 4, 'd', 'c', 'i', 'd', 2, 's', 'c'}
	pkt = AppendVarint(pkt, 0)  // token length
	pkt = AppendVarint(pkt, 20) // length of packet number + payload
	pkt = append(pkt, make([]byte, 20)...)
	pkt = append(pkt, "the next packet"...) // a coalesced datagram

	h, rest, err := parseLongHeader(pkt)
	if err != nil {
		t.Fatalf("ParseLongHeader returned %v", err)
	}
	if h.Type != packetInitial {
		t.Errorf("type = %d, want Initial", h.Type)
	}
	if string(h.DCID) != "dcid" || string(h.SCID) != "sc" {
		t.Errorf("identifiers = %q %q", h.DCID, h.SCID)
	}
	if h.Length != 20 {
		t.Errorf("length = %d, want 20", h.Length)
	}
	if string(rest) != "the next packet" {
		t.Errorf("the coalesced remainder = %q", rest)
	}
	if h.PNOffset != len(pkt)-len(rest)-20 {
		t.Errorf("PNOffset = %d", h.PNOffset)
	}
}

func TestParseLongHeaderRefusals(t *testing.T) {
	tests := []struct {
		name string
		pkt  []byte
	}{
		{"a short header", []byte{0x40, 0, 0, 0, 1, 0, 0}},
		{"the fixed bit clear", []byte{0x80, 0, 0, 0, 1, 0, 0}},
		{"an unknown version", []byte{0xc0, 0xff, 0, 0, 1, 0, 0}},
		{"truncated before the version", []byte{0xc0, 0, 0}},
		{"a connection identifier over the maximum", append([]byte{0xc0, 0, 0, 0, 1, 0xff}, make([]byte, 40)...)},
		{"a length past the datagram", []byte{0xc0, 0, 0, 0, 1, 0, 0, 0x00, 0x7f}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := parseLongHeader(tt.pkt); !errors.Is(err, ErrQUIC) {
				t.Fatalf("ParseLongHeader returned %v, want ErrQUIC", err)
			}
		})
	}
}

// Packet protection, checked the two ways it can be without a peer: the
// pieces agree with each other, and tampering is detected.
//
// What this does NOT show is interoperability. A wrong label in the key
// schedule would round trip exactly like a right one, and only RFC 9001
// Appendix A's vectors or another implementation can tell the difference.
func TestSealOpenRoundTrip(t *testing.T) {
	secrets, err := initialSecrets([]byte{0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08})
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(secrets.Client)
	if err != nil {
		t.Fatal(err)
	}

	for _, pnLen := range []int{1, 2, 3, 4} {
		for _, pn := range []uint64{0, 1, 42, 1000} {
			header := []byte{0xc0 | byte(pnLen-1), 0, 0, 0, 1, 0, 0}
			header = AppendVarint(header, 0)  // token length
			header = AppendVarint(header, 64) // length, unused by Seal
			pnOffset := len(header)
			for i := pnLen - 1; i >= 0; i-- {
				header = append(header, byte(pn>>(8*uint(i))))
			}
			payload := []byte("a frame or two, and some padding to sample from")

			sealed, err := sealer.Seal(nil, header, payload, pn, pnOffset, pnLen)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if bytes.Contains(sealed, payload) {
				t.Error("the payload is on the wire in the clear")
			}

			// largestPN is what a receiver tracking this space would hold.
			// Passing zero for a packet numbered 1000 asks it to reconstruct
			// 1000 from one byte, which the encoding cannot do and is not
			// meant to: the truncated number only means anything against an
			// expectation close to it.
			var largest uint64
			if pn > 0 {
				largest = pn - 1
			}
			got, gotPN, err := opener.Open(sealed, pnOffset, largest)
			if err != nil {
				t.Fatalf("Open (pnLen %d, pn %d): %v", pnLen, pn, err)
			}
			if gotPN != pn {
				t.Errorf("packet number = %d, want %d", gotPN, pn)
			}
			if string(got) != string(payload) {
				t.Errorf("payload = %q", got)
			}
		}
	}
}

// A packet that was altered must not open, which is the property Initial
// protection actually provides — it authenticates, it does not hide.
func TestOpenRefusesATamperedPacket(t *testing.T) {
	secrets, _ := initialSecrets([]byte("connection"))
	sealer, _ := newPacketSealer(secrets.Server)
	opener, _ := newPacketOpener(secrets.Server)

	header := []byte{0xc0, 0, 0, 0, 1, 0, 0}
	header = AppendVarint(header, 0)
	header = AppendVarint(header, 64)
	pnOffset := len(header)
	header = append(header, 0x00)

	sealed, err := sealer.Seal(nil, header, []byte("a payload long enough to sample from"), 0, pnOffset, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{0, pnOffset, len(sealed) - 1} {
		tampered := append([]byte(nil), sealed...)
		tampered[at] ^= 0x01
		if _, _, err := opener.Open(tampered, pnOffset, 0); err == nil {
			t.Errorf("a packet altered at byte %d opened anyway", at)
		}
	}
}

// The two directions must not share keys, or a peer could seal packets that
// authenticate as the other's.
func TestDirectionsHaveDifferentSecrets(t *testing.T) {
	s, err := initialSecrets([]byte("connection"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(s.Client, s.Server) {
		t.Fatal("both directions derived the same secret")
	}
	sealer, _ := newPacketSealer(s.Client)
	opener, _ := newPacketOpener(s.Server)

	header := []byte{0xc0, 0, 0, 0, 1, 0, 0}
	header = AppendVarint(header, 0)
	header = AppendVarint(header, 64)
	pnOffset := len(header)
	header = append(header, 0x00)
	sealed, err := sealer.Seal(nil, header, []byte("a payload long enough to sample from"), 0, pnOffset, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := opener.Open(sealed, pnOffset, 0); err == nil {
		t.Error("the server's opener read a packet the client sealed")
	}
}

// RFC 9000 §A.3: the wire carries a few low bytes of the number and the
// receiver picks the candidate nearest what it expects. Guessing wrong is not
// a wrong number, it is a wrong nonce and a packet that will not open.
func TestDecodePacketNumber(t *testing.T) {
	tests := []struct {
		largest, truncated uint64
		pnLen              int
		want               uint64
	}{
		{0, 1, 1, 1},
		{0xa82f30ea, 0x9b32, 2, 0xa82f9b32}, // the RFC's own example
		{255, 0, 1, 256},                    // the window rolled over
		{256, 255, 1, 255},                  // and the one just before it
	}
	for _, tt := range tests {
		if got := decodePacketNumber(tt.largest, tt.truncated, tt.pnLen); got != tt.want {
			t.Errorf("decode(largest=%d, truncated=%#x, len=%d) = %d, want %d",
				tt.largest, tt.truncated, tt.pnLen, got, tt.want)
		}
	}
}

// The frames whose whole content is a few variable-length integers must come
// back carrying them.
//
// This is a regression test with a story. The helper that decodes those fields
// took the frame by value and returned it, so every caller got back the copy
// made before the fields were written — a RESET_STREAM arrived naming stream
// zero with error code zero whatever the peer had sent, and so did every
// MAX_DATA, MAX_STREAM_DATA and STOP_SENDING. Nothing in the package read
// those fields, so nothing noticed, until HTTP/3 needed a stream reset and got
// the wrong stream. A parser is only as checked as the questions asked of it.
func TestFramesCarryTheirVarintFields(t *testing.T) {
	reset := AppendVarint(nil, FrameResetStream)
	reset = AppendVarint(reset, 12)    // stream
	reset = AppendVarint(reset, 0x10e) // application error code
	reset = AppendVarint(reset, 4096)  // final size

	stop := AppendVarint(nil, frameStopSending)
	stop = AppendVarint(stop, 8)
	stop = AppendVarint(stop, 0x10c)

	maxStream := AppendVarint(nil, frameMaxStreamData)
	maxStream = AppendVarint(maxStream, 16)
	maxStream = AppendVarint(maxStream, 1<<20)

	maxData := AppendVarint(nil, frameMaxData)
	maxData = AppendVarint(maxData, 1<<22)

	frames, err := parseFrames(nil, concat(reset, stop, maxStream, maxData))
	if err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	if len(frames) != 4 {
		t.Fatalf("%d frames, want 4", len(frames))
	}
	want := []Frame{
		{Type: FrameResetStream, StreamID: 12, value: 0x10e, offset: 4096},
		{Type: frameStopSending, StreamID: 8, value: 0x10c},
		{Type: frameMaxStreamData, StreamID: 16, value: 1 << 20},
		{Type: frameMaxData, value: 1 << 22},
	}
	for i, w := range want {
		got := frames[i]
		if got.Type != w.Type || got.StreamID != w.StreamID || got.value != w.value || got.offset != w.offset {
			t.Errorf("frame %d: type %#x stream %d value %#x offset %d, want type %#x stream %d value %#x offset %d",
				i, got.Type, got.StreamID, got.value, got.offset,
				w.Type, w.StreamID, w.value, w.offset)
		}
	}
}

// MaxIdleTimeoutMS: 0 is RFC 9000's "no timeout" case, and this package's own
// documented meaning for the field (TransportParameters.MaxIdleTimeoutMS). A
// connection built with it must leave idleDeadline zero: readLoop treats a
// zero idleDeadline as "no idle deadline, fall back to a 30s wakeup"
// (receive.go), so a non-zero one that is already in the past makes every
// SetReadDeadline call fail immediately with os.ErrDeadlineExceeded — a hot
// spin that can never receive anything.
func TestNewConnLeavesIdleDeadlineZeroWhenIdleTimeoutIsZero(t *testing.T) {
	pc, peer := dgram.Pair()
	defer func() { _ = pc.Close() }()
	defer func() { _ = peer.Close() }()

	params := DefaultParameters()
	params.MaxIdleTimeout = 0
	c := newConn(pc, peer.LocalAddr(), []byte("dcid"), []byte("scid"), true, params)
	defer c.Close()

	if !c.idleDeadline.IsZero() {
		t.Errorf("idleDeadline = %v, want the zero value — idleTimeout is 0 (no timeout)", c.idleDeadline)
	}
}

// Close must wake the exclusive-socket readLoop rather than leave it parked
// in ReadFrom until the next idle wakeup — up to the whole idle period away,
// the goroutine and its receive buffer lingering the while. The margin is
// wide enough not to be timing-sensitive: the woken loop exits in
// microseconds, the broken one after DefaultParameters' 30-second idle
// deadline, and the test allows five seconds.
//
// A real UDP socket, not the dgram pair: the wake-up rides net.PacketConn's
// contract that SetReadDeadline interrupts a ReadFrom already in progress,
// and the in-memory harness snapshots the deadline on entry instead.
func TestCloseWakesTheOwnedSocketReadLoop(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()

	c := newConn(pc, peer.LocalAddr(), []byte("dcid"), []byte("scid"), true, DefaultParameters())
	done := make(chan struct{})
	go func() {
		c.readLoop()
		close(done)
	}()

	// Give the loop a moment to park in ReadFrom, so Close exercises the
	// wake-up rather than the loop's initial closed check. If the goroutine
	// has not parked yet, the test still passes — it just proves less.
	time.Sleep(10 * time.Millisecond)
	c.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readLoop still running 5s after Close — the read deadline did not wake it")
	}
}

// The ordinary case, for contrast: a non-zero idle timeout does get an
// idleDeadline, so the fix above doesn't silently disable it.
func TestNewConnSetsIdleDeadlineWhenIdleTimeoutIsSet(t *testing.T) {
	pc, peer := dgram.Pair()
	defer func() { _ = pc.Close() }()
	defer func() { _ = peer.Close() }()

	params := DefaultParameters() // MaxIdleTimeoutMS: 30_000
	c := newConn(pc, peer.LocalAddr(), []byte("dcid"), []byte("scid"), true, params)
	defer c.Close()

	if c.idleDeadline.IsZero() {
		t.Error("idleDeadline is zero, want it set from a non-zero idle timeout")
	}
	if until := time.Until(c.idleDeadline); until <= 0 || until > 30*time.Second {
		t.Errorf("idleDeadline is %v from now, want within (0, 30s]", until)
	}
}

// A caller configuring MaxUDPPayloadSize above the read buffer's old fixed
// 2048 bytes was promising the peer a datagram size this endpoint could not
// actually read — net.PacketConn.ReadFrom truncates silently, and the
// truncated bytes fail parsing downstream as a generic malformed packet.
// recvBufSize must grow to honor what was advertised.
func TestRecvBufSizeHonorsMaxUDPPayloadSize(t *testing.T) {
	pc, peer := dgram.Pair()
	defer func() { _ = pc.Close() }()
	defer func() { _ = peer.Close() }()

	params := DefaultParameters()
	params.MaxUDPPayloadSize = 4096
	c := newConn(pc, peer.LocalAddr(), []byte("dcid"), []byte("scid"), true, params)
	defer c.Close()

	if got := c.recvBufSize(); got != 4096 {
		t.Errorf("recvBufSize() = %d, want 4096", got)
	}
}

// Below the floor, recvBufSize stays at the floor rather than shrinking —
// there is no reason to read less than minRecvBuf just because the local
// announcement is modest.
func TestRecvBufSizeStaysAtTheFloorBelowIt(t *testing.T) {
	pc, peer := dgram.Pair()
	defer func() { _ = pc.Close() }()
	defer func() { _ = peer.Close() }()

	params := DefaultParameters() // MaxUDPPayloadSize: 1452, under minRecvBuf
	c := newConn(pc, peer.LocalAddr(), []byte("dcid"), []byte("scid"), true, params)
	defer c.Close()

	if got := c.recvBufSize(); got != minRecvBuf {
		t.Errorf("recvBufSize() = %d, want the %d floor", got, minRecvBuf)
	}
}

// The pre-pad length (header + no padding yet) decides the length field's
// varint width here, and the post-pad length (what the field actually
// states, pad included) can need a wider one — 20 is well under the 64
// threshold, 1200 is well over it. A single-shot calculation using the
// pre-pad width done once comes up short of exactly 1200, as this case's
// fixed numbers show by hand; initialPadding has to converge to the width
// the final total really needs.
func TestInitialPaddingConvergesAcrossAVarintWidthThreshold(t *testing.T) {
	const headerLen, pnLen, payloadLen = 20, 4, 0

	pad := initialPadding(headerLen, pnLen, payloadLen)
	if pad <= 0 {
		t.Fatalf("pad = %d, want positive — this case starts far under 1200", pad)
	}

	finalPayloadLen := payloadLen + pad
	width := varintBytes(uint64(pnLen + finalPayloadLen + 16))
	total := headerLen + width + pnLen + finalPayloadLen + 16

	if total != 1200 {
		t.Errorf("total datagram = %d bytes, want exactly 1200 (pad=%d, width=%d)", total, pad, width)
	}
}

// Comfortably past 1200 before any padding: nothing is added, and the width
// used is simply the payload's own, unaffected by the fixed point.
func TestInitialPaddingIsZeroPastTheFloor(t *testing.T) {
	if pad := initialPadding(20, 4, 2000); pad != 0 {
		t.Errorf("pad = %d, want 0 — the payload alone already clears 1200", pad)
	}
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
