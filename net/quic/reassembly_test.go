package quic

import (
	"bytes"
	"errors"
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
	for i := range maxReassemblyChunks + 2 {
		lastErr = r.add(uint64(2*i+2), []byte{0xaa})
		if lastErr != nil {
			break
		}
	}
	if !errors.Is(lastErr, ErrQUIC) {
		t.Fatalf("unbounded fragmentation was absorbed: %v", lastErr)
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
