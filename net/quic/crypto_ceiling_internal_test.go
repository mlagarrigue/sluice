package quic

import (
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// cryptoFrame encodes one CRYPTO frame at off carrying data.
func cryptoFrame(off uint64, data string) []byte {
	b := varint.Append(nil, frameCrypto)
	b = varint.Append(b, off)
	b = varint.Append(b, uint64(len(data)))
	return append(b, data...)
}

// RFC 9000 §19.6: a CRYPTO frame whose offset plus length exceeds 2^62-1 is
// a connection error — FRAME_ENCODING_ERROR here, since the frame is
// malformed before any buffer is asked. One byte at the ceiling is over;
// one byte just under it parses.
func TestCryptoFrameReachingPastTheOffsetCeilingIsRefused(t *testing.T) {
	if _, err := parseFrames(nil, cryptoFrame(varint.Max, "x")); err == nil {
		t.Error("a CRYPTO frame at offset 2^62-1 with one byte parsed; §19.6 says refuse it")
	}
	if _, err := parseFrames(nil, cryptoFrame(varint.Max-1, "x")); err != nil {
		t.Errorf("a CRYPTO frame ending exactly at 2^62-1 was refused: %v", err)
	}

	c := newIdleServerConn(t)
	err := c.frames(spaceInitial, cryptoFrame(varint.Max, "x"), time.Now())
	te, ok := errors.AsType[*transportError](err)
	if !ok || te.code != transportFrameEncoding {
		t.Errorf("frames = %v, want FRAME_ENCODING_ERROR (%#x)", err, transportFrameEncoding)
	}
}

// RFC 9000 §19.8: "The largest offset delivered on a stream — the sum of
// the offset and data length — cannot exceed 2^62-1, as it is not possible
// to provide flow control credit for that data. Receipt of a frame that
// exceeds this limit MUST be treated as a connection error of type
// FRAME_ENCODING_ERROR". The parse let such a frame through to the stream,
// which ignored it on a closed stream and answered FINAL_SIZE_ERROR on one
// whose final size was known. The frame is refused at the parse now, in
// every state of the stream.
func TestStreamFrameReachingPastTheOffsetCeilingIsRefused(t *testing.T) {
	noLen := varint.Append(nil, FrameStream|streamOFF)
	noLen = varint.Append(noLen, 0)
	noLen = varint.Append(noLen, varint.Max)
	noLen = append(noLen, 'x')
	for name, frame := range map[string][]byte{
		"with a length":    streamFrame(nil, 0, varint.Max, "x", false),
		"without a length": noLen,
	} {
		if _, err := parseFrames(nil, frame); err == nil {
			t.Errorf("%s: a STREAM frame at offset 2^62-1 with one byte parsed", name)
		}
	}
	if _, err := parseFrames(nil, streamFrame(nil, 0, varint.Max-1, "x", false)); err != nil {
		t.Errorf("a STREAM frame ending exactly at 2^62-1 was refused: %v", err)
	}

	for _, state := range []string{"open", "closed", "final size known"} {
		c := boundsConn(t, false)
		installAppSealer(t, c)
		now := time.Now()
		switch state {
		case "open":
			if err := c.frames(spaceApplication, streamFrame(nil, 0, 0, "ab", false), now); err != nil {
				t.Fatal(err)
			}
		case "final size known":
			if err := c.frames(spaceApplication, streamFrame(nil, 0, 0, "ab", true), now); err != nil {
				t.Fatal(err)
			}
		case "closed":
			if err := c.frames(spaceApplication, streamFrame(nil, 4, 0, "ab", false), now); err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			delete(c.streams, 0) // opened below stream 4, and gone: closed
			c.mu.Unlock()
		}
		err := c.frames(spaceApplication, streamFrame(nil, 0, varint.Max, "x", false), now)
		wantCode(t, err, transportFrameEncoding, "STREAM past 2^62-1 on a stream "+state)
	}
}
