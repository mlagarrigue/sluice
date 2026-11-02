package httpstream

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9114 §7.2.1, §7.2.2, §7.2.4, §7.2.5: on the client's control stream, a
// DATA, HEADERS or PUSH_PROMISE frame, or a second SETTINGS, is a connection
// error of type H3_FRAME_UNEXPECTED whatever its length. controlFrames used
// to hand the buffer to parseH3Frames with MaxHeaderBytes as the frame
// bound, which refused a known frame declared past it before its type was
// judged, and closed H3_FRAME_ERROR. The type is judged first now, as
// b6d7c62 did on request streams.
func TestH3MisplacedFramesOnTheControlStreamAreJudgedByType(t *testing.T) {
	const bound = 1024
	for _, tt := range []struct {
		name string
		typ  uint64
	}{
		{"DATA", h3Data},
		{"HEADERS", h3Headers},
		{"PUSH_PROMISE", h3PushPromise},
		{"a second SETTINGS", h3Settings},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{MaxHeaderBytes: bound})
			open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 4096)...)
			if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
				t.Fatalf("a conformant control stream was refused: %v", err)
			}
			// Under the bound the type decides: the repository's own check.
			small := appendH3Frame(nil, tt.typ, []byte{0x00})
			b := testAssembler(Config{MaxHeaderBytes: bound})
			if _, _, _, err := b.Frames([]quic.Frame{streamFrame(2, append(append([]byte(nil), open...), small...), false)}); !isConnCode(err, h3FrameUnexpected) {
				t.Fatalf("control check: a %s frame of one byte gave %v, want H3_FRAME_UNEXPECTED", tt.name, err)
			}
			// Past the bound: only the frame's type and declared length are sent.
			head := varint.Append(varint.Append(nil, tt.typ), bound+1)
			_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, head, false)})
			if !isConnCode(err, h3FrameUnexpected) {
				t.Fatalf("a %s frame declaring %d bytes on the control stream gave %v; want a connection error of type H3_FRAME_UNEXPECTED (0x105)",
					tt.name, bound+1, describeConnErr(err))
			}
		})
	}
}

// RFC 9114 §7.2.3 to §7.2.7: a control-stream frame on a request stream, or a
// client's PUSH_PROMISE, is H3_FRAME_UNEXPECTED. Judged only once whole, it
// parked the stream until rawStreamBound while its declared payload
// trickled in; it is refused on its type and length now, at the first
// delivery that carries them.
func TestH3ControlFramesOnARequestStreamAreRefusedOnTheirHeader(t *testing.T) {
	head := h3Request("POST", "/t")
	for _, tt := range []struct {
		name string
		typ  uint64
	}{
		{"SETTINGS", h3Settings},
		{"GOAWAY", h3GoAway},
		{"MAX_PUSH_ID", h3MaxPushID},
		{"CANCEL_PUSH", h3CancelPush},
		{"PUSH_PROMISE", h3PushPromise},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{}
			resolved, err := cfg.resolve()
			if err != nil {
				t.Fatal(err)
			}
			a := testAssembler(cfg)
			declared := varint.Append(varint.Append(nil, tt.typ), uint64(resolved.MaxBodyBytes)) //nolint:gosec // G115: positive config
			stream := append(append([]byte(nil), head...), declared...)
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, false)})
			if batch.Len() != 0 || len(failed) != 0 {
				t.Fatalf("served or refused on the stream (batch=%d failed=%v); the connection is at fault", batch.Len(), failed)
			}
			if !isConnCode(err, h3FrameUnexpected) {
				t.Fatalf("a %s frame declaring %d bytes on a request stream gave %v; want H3_FRAME_UNEXPECTED at its header", tt.name, resolved.MaxBodyBytes, describeConnErr(err))
			}
		})
	}
}

func isConnCode(err error, code uint64) bool {
	ce, ok := errors.AsType[h3ConnectionError](err)
	return ok && ce.Code == code
}

func describeConnErr(err error) string {
	if ce, ok := errors.AsType[h3ConnectionError](err); ok {
		return fmt.Sprintf("a connection error of code %#x (%v)", ce.Code, ce)
	}
	return fmt.Sprint(err)
}
