package quic

import (
	"bytes"
	"runtime"
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

// A peer that sends a dense packet — 1 400 PINGs — then keepDenseFor
// ordinary ones, and again, finds the frame scratch dropped just before
// each dense packet, and had it regrown by doubling every round: 90 bytes
// allocated per byte received, where dense packets in a row allocate
// nothing. A dense packet that comes back lengthens the time the scratch
// is kept, so the round costs no more than a gap one packet shorter, which
// the scratch already survived: at most four bytes allocated per byte
// received (RFC 9000 §21.9), once the peer has kept at it a few rounds.
func TestADensePacketThatComesBackDoesNotRegrowTheScratch(t *testing.T) {
	perRound := func(gap int) (alloc, wire uint64) {
		c, _ := newMigratableServer(t, "dense-again")
		dense := bytes.Repeat([]byte{framePing}, 1400)
		sparse := []byte{framePing}
		now := time.Now()
		round := func() {
			if err := c.frames(spaceApplication, dense, now); err != nil {
				t.Fatal(err)
			}
			for range gap {
				if err := c.frames(spaceApplication, sparse, now); err != nil {
					t.Fatal(err)
				}
			}
		}
		// The steady state of a peer that keeps at it: the rounds before
		// the window has doubled past the gap pay the growth, once each.
		for range 16 { // past maxDenseDoublings + 1
			round()
		}
		const rounds = 20
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range rounds {
			round()
		}
		runtime.ReadMemStats(&after)
		const overhead = 29 // a short header and an AEAD tag, per packet
		return (after.TotalAlloc - before.TotalAlloc) / rounds,
			uint64(len(dense) + overhead + gap*(len(sparse)+overhead))
	}
	for _, gap := range []int{keepDenseFor - 1, keepDenseFor, 4 * keepDenseFor} {
		alloc, wire := perRound(gap)
		if alloc > 4*wire {
			t.Errorf("a dense packet every %d packets: %d bytes allocated per round of %d bytes received, over four per byte", gap+1, alloc, wire)
		}
	}
}
