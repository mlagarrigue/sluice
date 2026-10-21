//go:build unix

package httpstream

import (
	"runtime/debug"
	"syscall"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// TestH3DrainIncompleteFrameLeavesBufferInPlace: a delivery that completes
// no frame must not touch the stream's buffer — not even to copy it onto
// itself. drain used to end with append(st.buf[:0], rest...) with rest the
// whole of st.buf, which moved every parked byte on each one-byte delivery
// of a trickled DATA frame (RP-20261020-secnet-01).
//
// The buffer's identity — first byte address, length, capacity — is checked,
// but a copy onto itself preserves all three; the witness that tells the two
// apart is a page mapped read-only: any write drain makes into it, even of
// the bytes already there, faults, and SetPanicOnFault turns the fault into
// a panic this test catches. No clock, no threshold. The control stream's
// carry buffer has the same shape and is held to the same rule.
func TestH3DrainIncompleteFrameLeavesBufferInPlace(t *testing.T) {
	cfg := Config{}.mustResolve()
	a := testAssembler(cfg)
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)

	// A DATA frame declared at MaxBodyBytes, 64 KiB of it arrived.
	hdr := quic.AppendVarint(nil, h3Data)
	hdr = quic.AppendVarint(hdr, uint64(cfg.MaxBodyBytes))
	st := &h3Stream{declared: -1, gotHead: true, buf: readOnlyPage(t, hdr, 64<<10)}
	first, length, capacity := &st.buf[0], len(st.buf), cap(st.buf)
	if wrote := faults(func() {
		if err := a.drain(st); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}); wrote {
		t.Fatalf("drain wrote into a buffer it consumed nothing of (%d bytes)", length)
	}
	if &st.buf[0] != first || len(st.buf) != length || cap(st.buf) != capacity {
		t.Fatalf("drain moved the buffer: first byte %p len %d cap %d, want %p %d %d",
			&st.buf[0], len(st.buf), cap(st.buf), first, length, capacity)
	}

	// The control stream: SETTINGS declared longer than what arrived.
	hdr = quic.AppendVarint(nil, h3Settings)
	hdr = quic.AppendVarint(hdr, 64)
	u := &h3Uni{kind: h3StreamControl, buf: readOnlyPage(t, hdr, 32)}
	first, length, capacity = &u.buf[0], len(u.buf), cap(u.buf)
	if wrote := faults(func() {
		if err := a.controlFrames(u); err != nil {
			t.Fatalf("controlFrames: %v", err)
		}
	}); wrote {
		t.Fatalf("controlFrames wrote into a buffer it consumed nothing of (%d bytes)", length)
	}
	if &u.buf[0] != first || len(u.buf) != length || cap(u.buf) != capacity {
		t.Fatalf("controlFrames moved the buffer: first byte %p len %d cap %d, want %p %d %d",
			&u.buf[0], len(u.buf), cap(u.buf), first, length, capacity)
	}
}

// readOnlyPage maps size bytes, writes head at their start and seals the
// mapping read-only: a buffer whose every write faults.
func readOnlyPage(t *testing.T, head []byte, size int) []byte {
	t.Helper()
	mem, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { _ = syscall.Munmap(mem) })
	copy(mem, head)
	if err := syscall.Mprotect(mem, syscall.PROT_READ); err != nil {
		t.Skip(err)
	}
	return mem[:size:size]
}

// faults reports whether fn panicked — under SetPanicOnFault, whether it
// wrote where it may not.
func faults(fn func()) (wrote bool) {
	defer func() {
		if recover() != nil {
			wrote = true
		}
	}()
	fn()
	return false
}
