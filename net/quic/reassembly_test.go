package quic

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"runtime"
	"testing"
)

// The failure this structure exists to prevent: a retransmission covering
// [5,15) while 10 bytes are already delivered used to be parked forever on
// its unknown offset, wedging the stream with its bytes in hand.
func TestReassemblyMergesOverlapWithDelivered(t *testing.T) {
	var r reassembly
	out, err := r.addThenTake(0, []byte("0123456789"), nil)
	if err != nil || string(out) != "0123456789" {
		t.Fatalf("in-order delivery: %q, %v", out, err)
	}
	// A retransmission that starts before the delivered prefix and extends
	// past it must deliver exactly the new bytes.
	out, err = r.addThenTake(5, []byte("56789abcde"), nil)
	if err != nil || string(out) != "abcde" {
		t.Fatalf("overlapping retransmission delivered %q, %v", out, err)
	}
}

// A duplicate at a known offset must not shrink what is held — the old code
// overwrote the parked chunk with the shorter copy.
func TestReassemblyDuplicateNeverShrinks(t *testing.T) {
	var r reassembly
	if err := r.add(10, []byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	if err := r.add(10, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	out, err := r.addThenTake(0, bytes.Repeat([]byte("x"), 10), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "xxxxxxxxxxabcdefghij" {
		t.Fatalf("delivered %q after a shorter duplicate", out)
	}
}

func TestReassemblyOutOfOrder(t *testing.T) {
	var r reassembly
	if err := r.add(20, []byte("cc")); err != nil {
		t.Fatal(err)
	}
	if err := r.add(10, []byte("bbbbbbbbbb")); err != nil {
		t.Fatal(err)
	}
	out, err := r.addThenTake(0, []byte("aaaaaaaaaa"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "aaaaaaaaaabbbbbbbbbbcc" {
		t.Fatalf("delivered %q", out)
	}
	if r.held != 0 {
		t.Errorf("%d bytes still counted as parked", r.held)
	}
}

// Adjacent runs merge, so the chunk count measures gaps, not arrivals.
func TestReassemblyAdjacentRunsMerge(t *testing.T) {
	var r reassembly
	for i := range 10 {
		if err := r.add(uint64(10+i), []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.chunks) != 1 {
		t.Fatalf("10 adjacent bytes are held as %d chunks", len(r.chunks))
	}
}

// The fragment bound: a peer sending one byte every other offset gets a
// refusal, not this endpoint's memory.
func TestReassemblyBoundsFragmentCount(t *testing.T) {
	var r reassembly
	var lastErr error
	for i := range reassemblyChunkFloor + 2 {
		lastErr = r.add(uint64(2*i+2), []byte{0xaa})
		if lastErr != nil {
			break
		}
	}
	if !errors.Is(lastErr, ErrQUIC) {
		t.Fatalf("unbounded fragmentation was absorbed: %v", lastErr)
	}
}

// Every other packet of a flight lost is a conformant sender's pattern on a
// drop-tail queue that overflows: full packets with holes between them,
// more than the floor allows slivers, are held — up to the ceiling.
func TestReassemblyHoldsEveryOtherPacket(t *testing.T) {
	const packet = 1200
	var r reassembly
	data := make([]byte, packet)
	for i := range reassemblyChunkCeiling + 1 {
		err := r.add(uint64(2*i+1)*packet, data)
		if i < reassemblyChunkCeiling && err != nil {
			t.Fatalf("packet %d of every other one refused: %v", i, err)
		}
		if i == reassemblyChunkCeiling && !errors.Is(err, ErrQUIC) {
			t.Fatalf("%d chunks held past the ceiling: %v", len(r.chunks), err)
		}
	}
}

// Held bytes win over a retransmission that disagrees with itself.
func TestReassemblyFirstBytesWin(t *testing.T) {
	var r reassembly
	if err := r.add(5, []byte("KEEP")); err != nil {
		t.Fatal(err)
	}
	out, err := r.addThenTake(0, []byte("01234junk9"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "01234KEEP9" {
		t.Fatalf("delivered %q; a lying retransmission overwrote held bytes", out)
	}
}

// Stale data — entirely at or below the delivered prefix — is dropped, and
// the fast path stays exact at the boundaries.
func TestReassemblyStaleAndBoundary(t *testing.T) {
	var r reassembly
	if out, err := r.addThenTake(0, []byte("abcde"), nil); err != nil || string(out) != "abcde" {
		t.Fatalf("first delivery: %q, %v", out, err)
	}
	if out, err := r.addThenTake(0, []byte("abcde"), nil); err != nil || len(out) != 0 {
		t.Fatalf("a full duplicate delivered %q, %v", out, err)
	}
	if out, err := r.addThenTake(3, []byte("defgh"), nil); err != nil || string(out) != "fgh" {
		t.Fatalf("a partial duplicate delivered %q, %v", out, err)
	}
}

// reassembleBehindGap feeds total bytes in frag-sized frames with the first
// frame held back until last — every other frame parks behind the hole —
// and returns what came out.
func reassembleBehindGap(tb testing.TB, total, frag int) []byte {
	tb.Helper()
	src := make([]byte, total)
	for i := range src {
		src[i] = byte(i * 7)
	}
	var r reassembly
	var out []byte
	var err error
	for off := frag; off < total; off += frag {
		if out, err = r.addThenTake(uint64(off), src[off:min(off+frag, total)], out); err != nil {
			tb.Fatal(err)
		}
	}
	if len(out) != 0 {
		tb.Fatalf("%d bytes delivered across the hole", len(out))
	}
	if out, err = r.addThenTake(0, src[:frag], out); err != nil {
		tb.Fatal(err)
	}
	if !bytes.Equal(out, src) {
		tb.Fatalf("reassembled %d bytes, not the %d sent", len(out), total)
	}
	if r.held != 0 || len(r.chunks) != 0 {
		tb.Fatalf("held %d bytes in %d chunks after delivery", r.held, len(r.chunks))
	}
	return out
}

// Bytes queued in order behind one hole used to cost a full copy of
// everything parked per frame — quadratic, 1.26 s per MiB. Extending the
// parked run in place makes each frame amortised O(len(frame)).
func TestReassemblyBehindAGapIsLinear(t *testing.T) {
	reassembleBehindGap(t, 1<<20, 1000)

	// Many out-of-order fragments: a permutation across disjoint slots,
	// staying under the fragment bound, then the holes filled.
	var r reassembly
	src := make([]byte, 200*10)
	for i := range src {
		src[i] = byte(i)
	}
	var out []byte
	var err error
	// Odd slots in a scrambled order park as disjoint chunks (100 of
	// them); even slots then fill the holes, each merging two neighbours.
	for k := range 100 {
		slot := 2*((k*37)%100) + 1
		if out, err = r.addThenTake(uint64(slot*10), src[slot*10:slot*10+10], out); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.chunks) != 100 {
		t.Fatalf("%d chunks parked, want 100 disjoint", len(r.chunks))
	}
	for k := range 100 {
		slot := 2 * ((k*53 + 11) % 100)
		if out, err = r.addThenTake(uint64(slot*10), src[slot*10:slot*10+10], out); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(out, src) || r.held != 0 || len(r.chunks) != 0 {
		t.Fatalf("out %d/%d bytes, held %d, chunks %d", len(out), len(src), r.held, len(r.chunks))
	}
}

// BenchmarkReassemblyBehindGap is the quadratic case: 1 MiB in 1000-byte
// frames, all parked behind a missing first frame.
func BenchmarkReassemblyBehindGap(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1 << 20)
	for b.Loop() {
		reassembleBehindGap(b, 1<<20, 1000)
	}
}

// A frame that lands just before a parked run used to rebuild the run around
// itself — a copy of everything parked per frame, so a peer that sent a
// mebibyte and then a thousand one-byte frames walking backwards from it made
// this end copy a gibibyte. The same held for a frame that bridges two runs:
// opening a one-byte hole in front of the big run and filling it cost the
// whole run again, two frames at a time. Both now grow the larger run in
// place, so the work is the size of what arrived.
func TestReassemblyPrependIsLinear(t *testing.T) {
	const parked, frames = 1 << 20, 1000
	src := make([]byte, 2*frames+parked)
	for i := range src {
		src[i] = byte(i * 7)
	}
	base := uint64(2 * frames) // offset of the parked run: a gap before it

	for _, tc := range []struct {
		name string
		feed func(r *reassembly) error
	}{
		{"prepend", func(r *reassembly) error {
			for k := range uint64(frames) {
				off := base - 1 - k
				if err := r.add(off, src[off:off+1]); err != nil {
					return err
				}
			}
			return nil
		}},
		{"open and fill a hole", func(r *reassembly) error {
			for k := range uint64(frames / 2) {
				// A byte one short of the run's start, then the byte between.
				lo := base - 2*k - 2
				if err := r.add(lo, src[lo:lo+1]); err != nil {
					return err
				}
				if err := r.add(lo+1, src[lo+1:lo+2]); err != nil {
					return err
				}
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r reassembly
			if err := r.add(base, src[base:]); err != nil {
				t.Fatal(err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := tc.feed(&r); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4*(parked+frames) {
				t.Errorf("%d bytes allocated for %d one-byte frames in front of %d parked, over 4×(parked+frames)", alloc, frames, parked)
			}
			if len(r.chunks) != 1 {
				t.Fatalf("%d chunks parked, want the one run", len(r.chunks))
			}
			out, err := r.addThenTake(0, src[:base-frames], nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out, src) || r.held != 0 || len(r.chunks) != 0 {
				t.Fatalf("delivered %d/%d bytes (equal %v), held %d, chunks %d", len(out), len(src), bytes.Equal(out, src), r.held, len(r.chunks))
			}
		})
	}
}

// Frames of every shape — overlapping, duplicated, in front of, behind and
// bridging parked runs — in a seeded random order: after each one the chunks
// are sorted, disjoint and non-touching, held counts exactly their bytes,
// and once every byte has arrived the stream is the source.
func TestReassemblyRandomFramesDeliverTheSource(t *testing.T) {
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 0x5eed)) //nolint:gosec // G404: a seeded generator is the point, so a failing seed replays
		src := make([]byte, 64+rng.IntN(4096))
		for i := range src {
			src[i] = byte(rng.Uint32())
		}
		type frame struct{ off, end int }
		var frames []frame
		for off := 0; off < len(src); {
			n := 1 + rng.IntN(300)
			frames = append(frames, frame{off, min(off+n, len(src))})
			off += n
		}
		for range frames {
			a := rng.IntN(len(src))
			frames = append(frames, frame{a, min(a+1+rng.IntN(600), len(src))}) // overlaps and duplicates
		}
		rng.Shuffle(len(frames), func(i, j int) { frames[i], frames[j] = frames[j], frames[i] })

		var r reassembly
		var out []byte
		var err error
		for _, f := range frames {
			if out, err = r.addThenTake(uint64(f.off), src[f.off:f.end], out); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			held := 0
			for k, c := range r.chunks {
				held += len(c.data)
				if k > 0 && r.chunks[k-1].end() >= c.off {
					t.Fatalf("seed %d: chunks %d and %d touch or overlap", seed, k-1, k)
				}
				if c.off < r.consumed {
					t.Fatalf("seed %d: chunk at %d below the delivered %d", seed, c.off, r.consumed)
				}
			}
			if held != r.held {
				t.Fatalf("seed %d: held %d, chunks hold %d", seed, r.held, held)
			}
		}
		if !bytes.Equal(out, src) || r.held != 0 || len(r.chunks) != 0 {
			t.Fatalf("seed %d: delivered %d/%d bytes (equal %v), held %d, chunks %d", seed, len(out), len(src), bytes.Equal(out, src), r.held, len(r.chunks))
		}
	}
}

// Held and delivered bytes win over any later frame that disagrees with
// them, however many runs that frame bridges: frames are cut from two
// sources at random, and every byte delivered is the one the first frame
// to cover it carried. TestReassemblyFirstBytesWin shows it for one held
// run; this is the same rule across merges of several.
func TestReassemblyFirstBytesWinAcrossRuns(t *testing.T) {
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 0x2b175)) //nolint:gosec // G404: a seeded generator is the point, so a failing seed replays
		n := 64 + rng.IntN(2048)
		sources := [2][]byte{make([]byte, n), make([]byte, n)}
		for i := range n {
			sources[0][i], sources[1][i] = byte(rng.Uint32()), byte(rng.Uint32())
		}
		want := make([]byte, n)
		covered := make([]bool, n)
		type frame struct{ off, end, src int }
		var frames []frame
		for off := 0; off < n; {
			k := 1 + rng.IntN(200)
			frames = append(frames, frame{off, min(off+k, n), rng.IntN(2)})
			off += k
		}
		for range frames {
			a := rng.IntN(n)
			frames = append(frames, frame{a, min(a+1+rng.IntN(800), n), rng.IntN(2)})
		}
		rng.Shuffle(len(frames), func(i, j int) { frames[i], frames[j] = frames[j], frames[i] })

		var r reassembly
		var out []byte
		var err error
		for _, f := range frames {
			for i := f.off; i < f.end; i++ {
				if !covered[i] {
					covered[i], want[i] = true, sources[f.src][i]
				}
			}
			if out, err = r.addThenTake(uint64(f.off), sources[f.src][f.off:f.end], out); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		}
		if !bytes.Equal(out, want) {
			i := 0
			for i < min(len(out), n) && out[i] == want[i] {
				i++
			}
			t.Fatalf("seed %d: delivered %d/%d bytes, first difference at %d", seed, len(out), n, i)
		}
	}
}
