package quic

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The ACK delay counts from the first sight of the largest packet number:
// a duplicate of it arriving later does not restart the clock.
func TestAckDelayCountsFromTheLargestFirstSeen(t *testing.T) {
	var a ackTracker
	t0 := time.Now()
	a.record(5, t0)
	a.record(5, t0.Add(8*time.Millisecond))
	frames, err := parseFrames(nil, a.appendAck(nil, t0.Add(16*time.Millisecond)))
	if err != nil || len(frames) != 1 {
		t.Fatalf("frames = %+v, %v", frames, err)
	}
	if got := frames[0].delay << 3; got != 16000 {
		t.Errorf("ACK delay = %d µs, want 16000 from the first sight of packet 5", got)
	}
}

// ackFrame is an ACK of largest whose first range is first long, followed
// by the given gap and length pairs, all under 64 so one byte each.
func ackFrame(largest, first byte, pairs ...[2]byte) []byte {
	b := []byte{frameACK, largest, 0, byte(len(pairs)), first}
	for _, p := range pairs {
		b = append(b, p[0], p[1])
	}
	return b
}

// A range of an ACK may end exactly at packet zero and not one below,
// whether it is the second range or a later one whose start depends on the
// ranges before it.
func TestAckRangesAtPacketZero(t *testing.T) {
	for _, c := range []struct {
		frame []byte
		ok    bool
	}{
		{ackFrame(10, 5, [2]byte{3, 0}), true},
		{ackFrame(10, 5, [2]byte{4, 0}), false},
		{ackFrame(10, 5, [2]byte{3, 1}), false},
		{ackFrame(10, 0, [2]byte{1, 2}, [2]byte{3, 0}), true},
		{ackFrame(10, 0, [2]byte{1, 2}, [2]byte{3, 1}), false},
	} {
		f, rest, err := parseFrame(c.frame)
		if (err == nil) != c.ok {
			t.Errorf("% x: %v, want ok %v", c.frame, err, c.ok)
			continue
		}
		if !c.ok {
			if !strings.HasSuffix(err.Error(), ": an ACK range runs below packet zero") {
				t.Errorf("% x: %v", c.frame, err)
			}
			continue
		}
		var lows []uint64
		if err := decodeAckRanges(f.largest, f.Data, func(lo, _ uint64) { lows = append(lows, lo) }); err != nil || len(rest) != 0 {
			t.Fatalf("% x: %v, %d bytes left", c.frame, err, len(rest))
		}
		if lows[len(lows)-1] != 0 {
			t.Errorf("% x: lowest range starts at %d, want 0", c.frame, lows[len(lows)-1])
		}
	}
}

// A frame type of 0xff is an unknown type, not one past what a byte holds.
func TestFrameTypeAtTheByteBound(t *testing.T) {
	_, _, err := parseFrame([]byte{0x40, 0xff})
	if want := fmt.Sprintf("%v: unknown frame type 0xff", ErrQUIC); err == nil || err.Error() != want {
		t.Errorf("parseFrame(0xff) = %v, want %q", err, want)
	}
}

// NEW_CONNECTION_ID carries an identifier of one to twenty bytes.
func TestNewConnectionIDAtItsLengthBounds(t *testing.T) {
	for _, n := range []int{1, maxConnectionIDLen} {
		b := append([]byte{frameNewConnectionID, 1, 0, byte(n)}, make([]byte, n+16)...)
		f, rest, err := parseFrame(b)
		if err != nil || len(rest) != 0 || len(f.Data) != n+16 {
			t.Errorf("an identifier of %d bytes: %v, %d bytes of data, %d left", n, err, len(f.Data), len(rest))
		}
	}
}

// Both CONNECTION_CLOSE forms keep their reason and leave the frames after
// them to be read.
func TestConnectionCloseThenAFrame(t *testing.T) {
	for _, b := range [][]byte{
		{frameConnectionClose, 0x0a, 0x08, 3, 'b', 'y', 'e', framePing},
		{frameConnectionClose | 1, 0x05, 3, 'b', 'y', 'e', framePing},
	} {
		frames, err := parseFrames(nil, b)
		if err != nil || len(frames) != 2 || string(frames[0].Data) != "bye" || frames[1].Type != framePing {
			t.Errorf("% x: %+v, %v", b, frames, err)
		}
	}
}

// Seven bytes are a long header's least: what they say is read, not called
// short.
func TestLongHeaderAtItsLeast(t *testing.T) {
	_, _, err := parseLongHeader([]byte{0x80, 0, 0, 0, 1, 0, 0})
	if err == nil || errors.Is(err, ErrTruncated) || !strings.HasSuffix(err.Error(), ": the fixed bit is clear") {
		t.Errorf("seven bytes with the fixed bit clear: %v", err)
	}
}

// A Retry with an empty token is its header and the sixteen-byte tag.
func TestRetryWithAnEmptyToken(t *testing.T) {
	b := append([]byte{0xf0, 0, 0, 0, 1, 0, 0}, make([]byte, 16)...)
	h, _, err := parseLongHeader(b)
	if err != nil || h.Type != packetRetry || len(h.Token) != 0 {
		t.Errorf("a Retry with an empty token: %+v, %v", h, err)
	}
}

// Short headers take identifiers of zero to twenty bytes, and a header
// that ends with its identifier.
func TestShortHeaderAtItsBounds(t *testing.T) {
	for _, n := range []int{0, 8, maxConnectionIDLen} {
		b := append([]byte{0x40}, make([]byte, n)...)
		h, err := parseShortHeader(b, n)
		if err != nil || len(h.DCID) != n || h.PNOffset != 1+n {
			t.Errorf("a %d-byte identifier: %+v, %v", n, h, err)
		}
	}
	if _, _, err := connectionID(append([]byte{maxConnectionIDLen}, make([]byte, maxConnectionIDLen)...)); err != nil {
		t.Errorf("a 20-byte long-header identifier: %v", err)
	}
}

// The Retry tag is computed for an original identifier of twenty bytes,
// and a packet that is its tag alone is checked, not refused unread.
func TestRetryTagAtItsBounds(t *testing.T) {
	odcid := bytes.Repeat([]byte{7}, maxConnectionIDLen)
	if tag := retryTag(odcid, []byte("packet")); tag == [16]byte{} {
		t.Error("no tag for a 20-byte original identifier")
	}
	if tag := retryTag(append(odcid, 7), []byte("packet")); tag != [16]byte{} {
		t.Error("a tag for a 21-byte original identifier")
	}
	tag := retryTag(odcid, nil)
	if !verifyRetryTag(odcid, tag[:]) {
		t.Error("a packet that is its tag alone did not verify")
	}
	if verifyRetryTag(odcid, tag[1:]) {
		t.Error("fifteen bytes verified")
	}
}

// Header protection samples sixteen bytes four past the packet number's
// start: a packet one byte short of that is refused, one that reaches it
// sealed.
func TestSealNeedsItsSample(t *testing.T) {
	s, err := initialSecrets([]byte("sample"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(s.Client)
	if err != nil {
		t.Fatal(err)
	}
	header := []byte{0x40, 0}
	if _, err := sealer.Seal(nil, header, []byte{1, 2}, 0, 1, 1); err == nil || !strings.HasSuffix(err.Error(), ": too short to sample for header protection") {
		t.Errorf("two payload bytes behind a one-byte packet number: %v", err)
	}
	if _, err := sealer.Seal(nil, header, []byte{1, 2, 3}, 0, 1, 1); err != nil {
		t.Errorf("three payload bytes behind a one-byte packet number: %v", err)
	}
}

// Bytes in order at the delivered edge go straight out even when the
// fragment count stands at its bound: they park nothing.
func TestInOrderBytesAtTheFragmentBound(t *testing.T) {
	var r reassembly
	for k := range reassemblyChunkFloor {
		if _, err := r.addThenTake(uint64(10+2*k), []byte{1}, nil); err != nil {
			t.Fatalf("fragment %d: %v", k, err)
		}
	}
	got, err := r.addThenTake(0, []byte{9}, nil)
	if err != nil || !bytes.Equal(got, []byte{9}) {
		t.Errorf("in-order byte at the bound: %v, %v", got, err)
	}
}
