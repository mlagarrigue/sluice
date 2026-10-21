package httpstream

import (
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// The connection-wide bound counts what unfinished request streams hold by
// length; what they hold in capacity must stay under it too. A stream that
// had buffered one large DATA frame and moved it into its body used to keep
// the frame's buffer as well — emptied, full size, uncounted — so every such
// stream parked a second copy of its body beyond the bound. The frame scratch
// is the other half: it kept the size of the busiest delivery for the
// connection's life.
func TestH3AssemblerRetainedCapacity(t *testing.T) {
	a := testAssembler(Config{MaxConcurrentStreams: 128})
	maxBody := a.cfg.MaxBodyBytes
	const streams = 100
	// The bound admits every stream's body: the gap is in what is held
	// beside it, not in the bound refusing too early.
	frame := 4*maxBody/streams - 1024
	feed := func(id uint64, data []byte) {
		t.Helper()
		for off := 0; off < len(data); off += 1400 {
			end := min(off+1400, len(data))
			if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, data[off:end], false)}); err != nil || len(failed) > 0 {
				t.Fatalf("stream %d at %d: err %v, failed %v", id, off, err, failed)
			}
		}
	}
	for i := range streams {
		stream := h3Request("POST", "/upload")
		feed(uint64(i)*4, appendH3Frame(stream, h3Data, make([]byte, frame)))
	}
	if len(a.streams) != streams {
		t.Fatalf("%d streams assembling, want %d", len(a.streams), streams)
	}
	held := 0
	for _, st := range a.streams {
		held += cap(st.buf) + cap(st.req.Body)
	}
	if bound := 4*maxBody + 4096*streams; held > bound {
		t.Errorf("unfinished streams hold %d bytes of capacity, over 4×MaxBodyBytes + 4 KiB per stream = %d", held, bound)
	}

	// One delivery of many empty DATA frames — two bytes each, all legal —
	// fills the scratch with one entry per frame; it must not stay that size.
	burst := h3Request("POST", "/upload")
	for range 10000 {
		burst = appendH3Frame(burst, h3Data, nil)
	}
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(streams*4, burst, false)}); err != nil {
		t.Fatal(err)
	}
	if cap(a.frames) > h3FramesKept {
		t.Errorf("the frame scratch keeps %d entries after the burst, over %d", cap(a.frames), h3FramesKept)
	}
}
