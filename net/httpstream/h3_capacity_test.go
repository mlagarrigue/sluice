package httpstream

import (
	"runtime"
	"testing"
	"weak"

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

// The frame scratch keeps no view past its length. Hostile shape: each
// request stream carries HEADERS, k empty DATA frames and one DATA frame
// near MaxBodyBytes, k decreasing from one stream to the next, all in one
// delivery with FIN; each stream's buffer is about MaxBodyBytes. The test
// takes a weak pointer to every payload held by a scratch entry past its
// length, checks they are reachable, then clears those entries alone and
// collects: a buffer that dies then was kept reachable by the scratch and
// nothing else (the request body is a separate copy) — one per entry, up to
// h3FramesKept, before keepFrames cleared what a longer parse left.
func TestH3FrameScratchKeepsNoStreamBufferPastItsLength(t *testing.T) {
	const maxBody = 64 << 10
	a := testAssembler(Config{MaxBodyBytes: maxBody})
	const streams = 20
	big := make([]byte, maxBody-1024)
	for i := range streams {
		for j := range big {
			big[j] = byte(i + 1)
		}
		buf := h3Request("POST", "/upload")
		for range streams - i {
			buf = appendH3Frame(buf, h3Data, nil)
		}
		buf = appendH3Frame(buf, h3Data, big)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(uint64(i)*4, buf, true)}); err != nil {
			t.Fatalf("stream %d: %v", i*4, err)
		}
	}
	full := a.frames[:cap(a.frames)]
	type held struct {
		w    weak.Pointer[byte]
		mark byte
	}
	var ws []held
	seen := map[byte]bool{}
	for _, f := range full[len(a.frames):] {
		if len(f.Payload) == 0 || seen[f.Payload[0]] {
			continue
		}
		seen[f.Payload[0]] = true
		ws = append(ws, held{weak.Make(&f.Payload[0]), f.Payload[0]})
	}
	runtime.GC()
	runtime.GC()
	live := 0
	for _, h := range ws {
		if h.w.Value() != nil {
			live++
		}
	}
	clear(full[len(a.frames):])
	runtime.GC()
	runtime.GC()
	freed := 0
	for _, h := range ws {
		if h.w.Value() == nil {
			freed++
		}
	}
	t.Logf("stale buffers: %d reachable before clearing the stale entries, %d collected after; each about %d bytes; aggregate bound 4*MaxBodyBytes = %d",
		live, freed, maxBody, 4*maxBody)
	if live > 0 && freed > 0 {
		t.Errorf("%d stream buffers of ~%d bytes were kept reachable only by stale frame-scratch entries", freed, maxBody)
	}
}
