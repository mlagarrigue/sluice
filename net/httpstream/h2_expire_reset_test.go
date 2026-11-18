package httpstream

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// The writer's expire resets a stalled response with CANCEL; the peer,
// which has not read it yet, then sends a frame that is a stream error on a
// stream the reader still tracks. The stream is closed on this side, and
// RFC 9113 §5.1 allows no frame but PRIORITY on it: no second RST_STREAM.
// The reader's reset sent one, counted against the reset allowance, until
// a handed stream whose answer no longer runs drew nothing (1425415). The
// witness is the malformed DATA alone, which draws its one RST_STREAM.
func TestH2NoSecondResetAfterTheWriterExpiredTheStream(t *testing.T) {
	run := func(t *testing.T, waitExpiry bool) int {
		t.Helper()
		handler := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, b.Len())
			for i := range out {
				out[i] = Response{Status: 200, Body: make([]byte, 1000)}
			}
			return sluice.Batch[Response]{Items: out}
		}
		addr, _ := serveH2(t, Config{WriteTimeout: 200 * time.Millisecond}, handler)
		c := dialH2(t, addr)
		// SETTINGS_INITIAL_WINDOW_SIZE = 0: the response's DATA has no credit.
		c.frame(h2Frame{Type: frameSettings, Payload: []byte{0, 4, 0, 0, 0, 0}})
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
			Payload: headBlock("POST", "/", [2]string{"content-length", "1"}),
		})
		c.flush()
		if waitExpiry {
			c.pump(func() bool { return c.resetCount[1] >= 1 })
			if c.resetCount[1] != 1 || c.resets[1] != errCancel {
				t.Fatalf("precondition: the stalled response was not reset with CANCEL (%d resets, code %d)", c.resetCount[1], c.resets[1])
			}
		}
		// Five bytes against a declared length of one, END_STREAM on them: a
		// stream error.
		c.data(1, []byte("12345"), true)
		c.flush()
		deadline := time.Now().Add(800 * time.Millisecond)
		c.pump(func() bool { return time.Now().After(deadline) })
		return c.resetCount[1]
	}
	if n := run(t, false); n != 1 {
		t.Fatalf("witness: the malformed DATA alone drew %d RST_STREAMs, want 1", n)
	}
	if n := run(t, true); n > 1 {
		t.Fatalf("stream 1 received %d RST_STREAM frames: the expiry's, then the reader's", n)
	}
}
