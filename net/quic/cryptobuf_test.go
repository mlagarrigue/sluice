package quic

import (
	"bytes"
	"math/rand/v2"
	"runtime"
	"testing"
)

// benchCryptoConn is a connection whose Handshake space takes CRYPTO
// frames straight through handleCrypto, the path a peer's frames take.
func benchCryptoConn(b *testing.B) *Conn {
	b.Helper()
	c := assembleConn(&fuzzSink{addr: "crypto:self"}, fakeFuzzAddr("crypto:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
	b.Cleanup(func() { _ = c.Close() })
	return c
}

// benchCryptoFeed resets the Handshake space's CRYPTO buffer and files the
// frames, none of which reaches offset zero: nothing is handed to TLS.
func benchCryptoFeed(b *testing.B, c *Conn, frames []Frame) {
	b.Helper()
	var zero space
	c.spaces[spaceHandshake].crypto = zero.crypto
	for _, f := range frames {
		if err := c.handleCrypto(spaceHandshake, f); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCryptoDescendingFragments is the hostile cut of RFC 9000 §7.5's
// 4096 bytes: one-byte fragments, a one-byte hole between two, sent from
// the highest offset down, so every one lands in front of all the others.
func BenchmarkCryptoDescendingFragments(b *testing.B) {
	const n = 4096
	frames := make([]Frame, n)
	for k := range frames {
		frames[k] = Frame{Type: frameCrypto, offset: uint64(2*(n-k) - 1), Data: []byte{0x5a}}
	}
	c := benchCryptoConn(b)
	b.ReportAllocs()
	for b.Loop() {
		benchCryptoFeed(b, c, frames)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/frame")
}

// BenchmarkCryptoReorderedFlight is the ordinary case: a 4800-byte flight
// in 1200-byte frames whose first is lost, so the other three park behind
// it.
func BenchmarkCryptoReorderedFlight(b *testing.B) {
	frames := []Frame{
		{Type: frameCrypto, offset: 1200, Data: make([]byte, 1200)},
		{Type: frameCrypto, offset: 3600, Data: make([]byte, 1200)},
		{Type: frameCrypto, offset: 2400, Data: make([]byte, 1200)},
	}
	c := benchCryptoConn(b)
	b.ReportAllocs()
	for b.Loop() {
		benchCryptoFeed(b, c, frames)
	}
}

// Overlapping fragments in any order, retransmissions and stale copies
// included, deliver the source exactly once and in order, across ring
// growths and with the delivered prefix wrapping the ring many times over.
// The reach of the bursts widens slowly, so the ring grows after the prefix
// has wrapped it, not only from empty.
func TestCryptoBufferReassemblesAnyCut(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 7)) //nolint:gosec // G404: a seeded cut, reproducible on purpose
	for round := range 40 {
		src := make([]byte, 1+rng.IntN(300_000))
		for i := range src {
			src[i] = byte(rng.Uint32())
		}
		var b reassembly
		var out []byte
		for burst := 0; b.consumed < uint64(len(src)); burst++ {
			// A burst inside the open window, some of it reaching back over
			// delivered bytes, and the front of the gap so the burst ends.
			base := b.consumed
			reach := min(maxCryptoBuffer, uint64(1024)<<(burst/16))
			lo, top := base-min(base, 200), min(uint64(len(src)), base+reach)
			frames := []Frame{{offset: base, Data: src[base:min(top, base+1+rng.Uint64N(3000))]}}
			for range rng.IntN(64) {
				off := lo + rng.Uint64N(top-lo)
				frames = append(frames, Frame{offset: off, Data: src[off:min(top, off+1+rng.Uint64N(1500))]})
			}
			rng.Shuffle(len(frames), func(i, j int) { frames[i], frames[j] = frames[j], frames[i] })
			for _, f := range frames {
				var err error
				if out, err = b.addThenTakeWithin(maxCryptoBuffer, f.offset, f.Data, out); err != nil {
					t.Fatalf("round %d: frame at %d, %d bytes: %v", round, f.offset, len(f.Data), err)
				}
			}
		}
		if !bytes.Equal(out, src) || b.held != 0 {
			t.Fatalf("round %d: delivered %d of %d bytes, equal %v, %d still parked", round, len(out), len(src), bytes.Equal(out, src), b.held)
		}
	}
}

// The first bytes seen for an offset win, as on streams: a retransmission
// that disagrees with what is parked does not overwrite it.
func TestCryptoBufferKeepsTheFirstBytes(t *testing.T) {
	var b reassembly
	out, _ := b.addThenTakeWithin(maxCryptoBuffer, 4, []byte("held"), nil)
	out, _ = b.addThenTakeWithin(maxCryptoBuffer, 0, []byte("0123XXXX89"), out)
	if string(out) != "0123held89" {
		t.Fatalf("delivered %q, want %q", out, "0123held89")
	}
}

// RFC 9000 §7.5's 4096 bytes cut the hostile way: one-byte fragments sent
// from the highest offset down, a hole between two, each landing in front
// of all the others. The reassembly CRYPTO shared with streams shifted its
// table of runs a slot per parked fragment, 4112 allocations and 1.2 MB per
// space; the CRYPTO buffer parks each in place, in the one ring it
// allocates.
func TestDescendingCryptoFragmentsParkInPlace(t *testing.T) {
	const n = 4096
	allocs := testing.AllocsPerRun(10, func() {
		var b reassembly
		for k := range n {
			if _, err := b.addThenTakeWithin(maxCryptoBuffer, uint64(2*(n-k)-1), []byte{0x5a}, nil); err != nil {
				t.Fatal(err)
			}
		}
		if b.held != n {
			t.Fatalf("%d bytes parked, want %d", b.held, n)
		}
	})
	// One ring and its bitmap.
	if allocs > 2 {
		t.Fatalf("%v allocations for %d descending fragments, want at most 2", allocs, n)
	}
}

// A ring grows while the delivered prefix sits mid-word and a byte is
// parked at the window's last offset, whose slot shares a bitmap word with
// the prefix: the two ends of that word go to different words of the new
// ring, and neither leaves a phantom byte behind for a later offset.
func TestCryptoBufferGrowsAroundASharedWord(t *testing.T) {
	src := make([]byte, 3*cryptoRingMin)
	for i := range src {
		src[i] = byte(i*7 + i>>8)
	}
	var b reassembly
	feed := func(out []byte, off, end int) []byte {
		t.Helper()
		out, err := b.addThenTakeWithin(maxCryptoBuffer, uint64(off), src[off:end], out)
		if err != nil {
			t.Fatalf("[%d, %d): %v", off, end, err)
		}
		return out
	}
	out := feed(nil, 0, 100)                                      // the prefix at 100, mid-word
	out = feed(out, 101, 102)                                     // a ring of cryptoRingMin
	out = feed(out, 100+cryptoRingMin-1, 100+cryptoRingMin)       // its last offset
	out = feed(out, 100+cryptoRingMin+500, 100+cryptoRingMin+501) // grows it
	out = feed(out, 100, len(src))                                // grows it again, fills it all
	if !bytes.Equal(out, src) || b.held != 0 {
		t.Fatalf("delivered %d of %d bytes, equal %v, %d bytes parked", len(out), len(src), bytes.Equal(out, src), b.held)
	}
}

// A space whose keys are discarded receives no CRYPTO frame again: what it
// had parked out of order goes with the keys, rather than staying for the
// connection's life.
func TestDiscardedSpaceReleasesItsCryptoBuffer(t *testing.T) {
	c := assembleConn(&fuzzSink{addr: "crypto:self"}, fakeFuzzAddr("crypto:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	for off := uint64(1); off < 2000; off += 2 {
		if err := c.handleCrypto(spaceHandshake, Frame{Type: frameCrypto, offset: off, Data: []byte{0x5a}}); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceHandshake]
	if sp.crypto.held == 0 || sp.crypto.ring == nil {
		t.Fatalf("nothing parked behind the hole (held %d)", sp.crypto.held)
	}
	c.discardSpaceLocked(spaceHandshake)
	if sp.crypto.held != 0 || sp.crypto.ring != nil || sp.crypto.have != nil {
		t.Errorf("after the discard: %d bytes parked, ring %d bytes, bitmap %d words; want none", sp.crypto.held, len(sp.crypto.ring), len(sp.crypto.have))
	}
}

// Pairs of one-byte CRYPTO frames, the second byte sent before the first,
// are what any peer can send from the Initial space on, before its address
// is validated: each pair parks one byte and empties the ring again. Fed as
// handleCrypto feeds them, they allocate no more per wire byte than the
// same frames in order — the bound TestDensePacketsAllocateAtMostFourBytesPerByte
// holds a dense packet to — rather than a whole ring per pair.
func TestReorderedCryptoPairsKeepTheirRing(t *testing.T) {
	const pairs = 2000
	run := func(reordered bool) (alloc, wire uint64) {
		var r reassembly
		one := []byte{0x16}
		var frame []byte
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for i := range uint64(pairs) {
			order := [2]uint64{2 * i, 2*i + 1}
			if reordered {
				order[0], order[1] = order[1], order[0]
			}
			for _, off := range order {
				frame = append(appendFrame(frame[:0], frameCrypto, off, 1), one...)
				wire += uint64(len(frame))
				if _, err := r.addThenTakeWithin(maxCryptoBuffer, off, one, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		runtime.ReadMemStats(&after)
		if r.consumed != 2*pairs || r.held != 0 {
			t.Fatalf("consumed %d, held %d; want %d and 0", r.consumed, r.held, 2*pairs)
		}
		return after.TotalAlloc - before.TotalAlloc, wire
	}
	if alloc, wire := run(false); alloc > 4*wire {
		t.Fatalf("control: in-order frames allocate %d bytes for %d wire bytes", alloc, wire)
	}
	if alloc, wire := run(true); alloc > 4*wire {
		t.Errorf("reordered one-byte pairs allocate %d bytes for %d wire bytes, over 4 per byte", alloc, wire)
	}
}
