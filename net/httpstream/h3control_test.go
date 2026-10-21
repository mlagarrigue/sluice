package httpstream

import (
	"bytes"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9114 §10.5 asks an implementation to track the unknown protocol
// elements a peer sends and set limits on their use. This server counts no
// unknown frame, setting or stream, and this test is the measurement that
// position rests on: a control stream fed nothing but GREASE frames — empty
// or a kilobyte each, well under and over what one delivery carries —
// leaves the assembler with no request, no failure, an empty carry buffer
// and no heap allocation per frame. The frames are skipped by counter
// (parseH3Frames, skipAhead), never buffered, so a flood of them is paid in
// the peer's own bandwidth and bounded by the transport's flow control,
// which releases credit only as this side reads. The elements that do cost
// something — field sections, request streams, cancellations — have bounds
// of their own (MaxHeaderBytes, rawStreamBound, countCancel).
func TestH3UnknownControlFramesCostNothingHere(t *testing.T) {
	const grease = 0x21 + 0x1f*7
	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{"empty GREASE frames", nil},
		{"GREASE frames of a kilobyte", make([]byte, 1024)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
			if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
				t.Fatal(err)
			}
			frame := appendH3Frame(nil, grease, tt.payload)
			flood := func(n int) []quic.Frame {
				return []quic.Frame{streamFrame(2, bytes.Repeat(frame, n), false)}
			}
			batch, _, failed, err := a.Frames(flood(400))
			if err != nil {
				t.Fatalf("400 unknown frames failed the connection: %v", err)
			}
			if batch.Len() != 0 || len(failed) != 0 {
				t.Fatalf("batch=%d failed=%v, want nothing out of unknown frames", batch.Len(), failed)
			}
			if kept := len(a.unis[2].buf); kept != 0 {
				t.Errorf("the control stream kept %d bytes of unknown frames, want none", kept)
			}
			// Per frame, nothing: the allocations of a delivery do not grow
			// with the frames in it.
			perCall := func(n int) float64 {
				frames := flood(n)
				return testing.AllocsPerRun(5, func() {
					if _, _, _, err := a.Frames(frames); err != nil {
						t.Fatal(err)
					}
				})
			}
			if ten, thousand := perCall(10), perCall(1000); thousand > ten {
				t.Errorf("%v allocations for 1000 unknown frames against %v for 10: the flood is paid on this side's heap", thousand, ten)
			}
		})
	}
}
