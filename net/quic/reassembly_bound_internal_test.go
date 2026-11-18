package quic

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// The fragment bound of the reassembly (reassembly.go) closes the
// connection with INTERNAL_ERROR stated as such — a transportError carrying
// the reason — not an error closeCodeFor happens to map there. Below the
// floor of 256 bytes held, the floor of 256 fragments applies: the 257th
// disjoint one-byte run trips it, the 256 before it are kept.
func TestFragmentBoundClosesWithAStatedInternalError(t *testing.T) {
	c, _ := newMigratableServer(t, "frag")
	now := time.Now()
	for i := range 256 {
		if err := c.frames(spaceApplication, streamFrame(nil, 0, uint64(2+2*i), "x", false), now); err != nil {
			t.Fatalf("fragment %d of 256 refused: %v", i+1, err)
		}
	}
	err := c.frames(spaceApplication, streamFrame(nil, 0, 2+2*256, "x", false), now)
	te, ok := errors.AsType[*transportError](err)
	if !ok {
		t.Fatalf("the 257th fragment gave %v (%T), want a *transportError with the code stated", err, err)
	}
	if te.code != transportInternalError {
		t.Errorf("code = %#x, want INTERNAL_ERROR (%#x)", te.code, transportInternalError)
	}
	if !strings.Contains(err.Error(), "fragments") {
		t.Errorf("reason = %q, want it to name the fragment bound", err)
	}
}

// parkAtTheCeiling parks reassemblyChunkCeiling-1 disjoint runs, a hole of
// one byte in front of the first.
func parkAtTheCeiling(tb testing.TB) *reassembly {
	tb.Helper()
	const run = reassemblyChunkBytes + 1 // one chunk per 1024 bytes held keeps the ceiling reachable
	r := &reassembly{}
	off := uint64(2)
	for i := range reassemblyChunkCeiling - 1 {
		if err := r.add(off, make([]byte, run)); err != nil {
			tb.Fatalf("filing run %d: %v", i, err)
		}
		off += run + 1 // a one-byte hole in between: the runs never touch
	}
	return r
}

// At the chunk ceiling, a byte filed in front of every parked run moves all
// of them one slot: the move stays within reassemblyMoveBound, which the
// comment on the ceiling states. A slot grown past 64 bytes doubles it.
func TestAHeadInsertAtTheCeilingMovesWithinItsBound(t *testing.T) {
	r := parkAtTheCeiling(t)
	if got := len(r.chunks); got != reassemblyChunkCeiling-1 {
		t.Fatalf("precondition: %d chunks parked, want %d", got, reassemblyChunkCeiling-1)
	}
	if err := r.add(0, []byte{1}); err != nil {
		t.Fatalf("a byte filed at the head was refused: %v", err)
	}
	if got := len(r.chunks); got != reassemblyChunkCeiling {
		t.Fatalf("%d chunks after the head insert, want %d", got, reassemblyChunkCeiling)
	}
	if moved := (len(r.chunks) - 1) * int(unsafe.Sizeof(rchunk{})); moved > reassemblyMoveBound {
		t.Errorf("one byte filed at the head moved %d bytes of slots (%d a slot), over the %d the ceiling is set for",
			moved, unsafe.Sizeof(rchunk{}), reassemblyMoveBound)
	}
}

// BenchmarkReassemblyHeadInsertAtTheCeiling is the move the chunk ceiling
// bounds: a byte filed in front of every parked run shifts all of them one
// slot, and the byte that fills the hole behind it, merging the two, shifts
// them back. The second shift is done here by hand, the run restored.
func BenchmarkReassemblyHeadInsertAtTheCeiling(b *testing.B) {
	r := parkAtTheCeiling(b)
	for b.Loop() {
		if err := r.add(0, []byte{1}); err != nil {
			b.Fatal(err)
		}
		r.chunks = append(r.chunks[:0], r.chunks[1:]...)
		r.held--
	}
	if len(r.chunks) != reassemblyChunkCeiling-1 {
		b.Fatalf("%d chunks at the end, want %d", len(r.chunks), reassemblyChunkCeiling-1)
	}
}
