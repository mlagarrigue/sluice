package httpstream

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9114 §7.2.8: the HTTP/2 frame types with no HTTP/3 meaning (PRIORITY,
// PING, WINDOW_UPDATE, CONTINUATION) are reserved, and their receipt is a
// connection error of type H3_FRAME_UNEXPECTED — on a request stream or the
// control stream alike. The parser used to skip them as it skips the GREASE
// types, which the same section wants ignored.
func TestH3HTTP2FrameTypesAreRefused(t *testing.T) {
	head := h3Request("POST", "/t")
	for _, typ := range []uint64{0x02, 0x06, 0x08, 0x09} {
		t.Run(fmt.Sprintf("HTTP/2 frame type %#x on a request stream", typ), func(t *testing.T) {
			a := newH3Assembler(Config{})
			stream := append(append([]byte(nil), head...), appendH3Frame(nil, typ, []byte{0x00, 0x00, 0x00, 0x00})...)
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
			if batch.Len() != 0 || len(failed) != 0 {
				t.Fatalf("served or refused on the stream (batch=%d failed=%v); the connection is at fault", batch.Len(), failed)
			}
			connErr(t, err, h3FrameUnexpected)
		})
		t.Run(fmt.Sprintf("HTTP/2 frame type %#x on the control stream", typ), func(t *testing.T) {
			a := newH3Assembler(Config{})
			open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
			open = append(open, appendH3Frame(nil, typ, []byte{0x00, 0x00, 0x00, 0x00})...)
			_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
			connErr(t, err, h3FrameUnexpected)
		})
	}

	// A GREASE type is still what §7.2.8 says it is: ignored, wherever frames
	// may be sent. The request completes around it.
	t.Run("a GREASE frame between HEADERS and DATA", func(t *testing.T) {
		a := newH3Assembler(Config{})
		stream := append(append([]byte(nil), head...), appendH3Frame(nil, 0x21+0x1f*7, []byte{0xde, 0xad})...)
		stream = appendH3Frame(stream, h3Data, []byte("x"))
		batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
		if err != nil || len(failed) != 0 || batch.Len() != 1 {
			t.Fatalf("a GREASE frame was not ignored: batch=%d failed=%v err=%v", batch.Len(), failed, err)
		}
	})
}
