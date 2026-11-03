package quic

import (
	"testing"
	"time"
)

// A gap filled releases everything parked behind it into the packet's delta
// buffer at once — up to the connection's window. Kept as it was, the buffer
// stayed at that size for the connection's whole life on the strength of one
// packet, the same rule parseScratch was already held to (maxKeptFrames).
// The frame scratches kept the frames past their length too, with the
// packet buffers their Data pointed into, and settleScratch the streams.
func TestTheReceiveScratchesDoNotKeepOnePacketsSize(t *testing.T) {
	c := newConn(&fuzzSink{addr: "scratch:self"}, fakeFuzzAddr("scratch:peer"),
		[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
	defer c.Close()
	installAppSealer(t, c)
	const parked = 40 << 10
	data := make([]byte, parked)
	ids := []uint64{0, 4, 8, 12}
	var behind, gaps []byte
	for _, id := range ids {
		behind = appendFrame(behind, FrameStream|streamOFF|streamLEN, id, 1, parked)
		behind = append(behind, data...)
		gaps = appendFrame(gaps, FrameStream|streamOFF|streamLEN, id, 0, 1)
		gaps = append(gaps, 0)
	}
	now := time.Now()
	if err := c.frames(spaceApplication, behind, now); err != nil {
		t.Fatal(err)
	}
	if err := c.frames(spaceApplication, gaps, now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	deltaCap := cap(c.deltaScratch)
	c.mu.Unlock()
	if deltaCap > maxKeptDelta {
		t.Errorf("the delta buffer keeps %d bytes after the packet that filled %d gaps of %d, want at most %d",
			deltaCap, len(ids), parked, maxKeptDelta)
	}
	for _, f := range c.batchScratch[:cap(c.batchScratch)] {
		if f.Data != nil {
			t.Fatal("the batch scratch keeps a frame, and the packet buffer it points into, past the packet")
		}
	}
	for _, f := range c.parseScratch[:cap(c.parseScratch)] {
		if f.Data != nil {
			t.Fatal("the parse scratch keeps a frame, and the packet buffer it points into, past the packet")
		}
	}
	for _, s := range c.settleScratch[:cap(c.settleScratch)] {
		if s.s != nil {
			t.Fatal("the settle scratch keeps a stream past the batch")
		}
	}
}
