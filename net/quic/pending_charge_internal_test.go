package quic

import (
	"testing"
	"unsafe"
)

// pendingConn is a server connection with no callback registered and no
// Read taken: every delivered delta is held in c.pending.
func pendingConn() *Conn {
	return assembleConn(&fuzzSink{addr: "pend:self"}, fakeFuzzAddr("pend:peer"),
		[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
}

func bytelessFrames(n int) []Frame {
	batch := make([]Frame, n)
	for i := range batch {
		batch[i] = Frame{Type: FrameResetStream, StreamID: uint64(4 * i)}
	}
	return batch
}

// The held copy keeps a Frame per frame, and holdPending charges
// pendingFrameCost for it: the charge was 64 bytes against an 80-byte Frame
// and a separate allocation of at least 8 bytes per frame with a byte, so
// the bound at twice the connection window let through about 1.3 times what
// it counted. The charge covers the Frame now, and a batch's bytes share one
// allocation.
func TestPendingChargeCoversWhatTheCopyHolds(t *testing.T) {
	if size := unsafe.Sizeof(Frame{}); size > pendingFrameCost {
		t.Fatalf("a Frame is %d bytes, over the %d pendingFrameCost charges", size, pendingFrameCost)
	}

	c := pendingConn()
	const n = 100
	c.deliverMu.Lock()
	c.holdPending(bytelessFrames(n))
	charged, held := c.pendingBytes, len(c.pending)
	c.deliverMu.Unlock()
	if held != 1 || charged != n*pendingFrameCost {
		t.Fatalf("held %d batches charged %d, want 1 batch charged %d", held, charged, n*pendingFrameCost)
	}

	oneByte := make([]Frame, n)
	for i := range oneByte {
		oneByte[i] = Frame{Type: FrameStream, StreamID: uint64(4 * i), Data: []byte{'x'}}
	}
	allocs := testing.AllocsPerRun(10, func() { _ = cloneFrames(oneByte) })
	if allocs > 2 {
		t.Errorf("copying %d one-byte frames made %v allocations, want the Frames and one for their bytes", n, allocs)
	}
	clone := cloneFrames(oneByte)
	for i := range clone {
		if string(clone[i].Data) != "x" || cap(clone[i].Data) != 1 {
			t.Fatalf("frame %d copied as %q, capacity %d", i, clone[i].Data, cap(clone[i].Data))
		}
	}
	if clone := cloneFrames(bytelessFrames(1)); clone[0].Data != nil {
		t.Errorf("a frame without bytes copied with Data %v, want nil", clone[0].Data)
	}
}
