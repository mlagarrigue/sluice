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
