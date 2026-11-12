package httpstream

import (
	"testing"
)

// A streamed HTTP/2 body holds at most one stream window of the peer's
// bytes, and its ring grew by doubling from 16 KiB until the held bytes fit:
// for a window that is not 16 KiB times a power of two, the ring allocated
// was up to twice the window, where docs/guide/limits.md counts the window.
// The ring stops at the window and its grace; a window of 1 MiB, a power of
// two already, is the witness.
func TestH2StreamedBodyRingStopsAtTheWindow(t *testing.T) {
	for _, window := range []int{1 << 20, 600_000} {
		s := newBenchH2Server(Config{
			BodyDelivery: BodyStreamed, InitialWindowSize: window,
			MaxBodyBytes: 1 << 10, MaxStreamedBodyBytes: 8 << 20,
		})
		s.settingsAcked = true // streams start at the advertised window, no grace
		ready := feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")))
		if len(ready) != 1 {
			t.Fatalf("window %d: %d requests handed over, want 1", window, len(ready))
		}
		var frames []h2Frame
		for sent := 0; sent < window; sent += 16384 {
			frames = append(frames, dataFrame(1, 0, min(16384, window-sent)))
		}
		feed(t, s, frames...)
		q := ready[0].body.q
		q.mu.Lock()
		held, ring := q.n, len(q.buf)
		q.mu.Unlock()
		if held != window {
			t.Fatalf("window %d: %d bytes held, want the whole window", window, held)
		}
		if ring > window+16384 {
			t.Errorf("window %d: the ring holding it is %d bytes, %.2f times the window", window, ring, float64(ring)/float64(window))
		}
	}
}
