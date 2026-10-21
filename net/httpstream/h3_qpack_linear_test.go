package httpstream

import (
	"bytes"
	"testing"
	"time"
)

// Each QPACK instruction consumed used to move what was left of the buffer
// to its front, so a delivery of n one-byte instructions — Set Dynamic Table
// Capacity 0 on the encoder stream, Stream Cancellation on the decoder
// stream, both legal — cost O(n²): 12.7 ms for 64 KiB. One pass and one
// compaction make it linear; sixteen times the bytes must cost well under
// the 256 times a quadratic walk would.
func TestQPACKInstructionStreamsAreLinear(t *testing.T) {
	a := testAssembler(Config{})
	for _, tc := range []struct {
		name    string
		insn    byte
		consume func(*h3Uni) error
	}{
		{"encoder", 0x20, a.encoderInstructions},
		{"decoder", 0x40, a.decoderInstructions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cost := func(n int) time.Duration {
				best := time.Duration(1<<63 - 1)
				for range 5 {
					u := &h3Uni{buf: bytes.Repeat([]byte{tc.insn}, n)}
					start := time.Now()
					if err := tc.consume(u); err != nil {
						t.Fatal(err)
					}
					best = min(best, time.Since(start))
					if len(u.buf) != 0 {
						t.Fatalf("%d bytes left after %d whole instructions", len(u.buf), n)
					}
				}
				return max(best, time.Microsecond)
			}
			small, large := cost(4<<10), cost(64<<10)
			if ratio := float64(large) / float64(small); ratio > 64 {
				t.Errorf("64 KiB of instructions cost %v, 4 KiB %v: %.0f×, want linear (16×, under 64×)", large, small, ratio)
			}
		})
	}
}
