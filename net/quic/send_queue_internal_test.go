package quic

import "testing"

// A batch opens in the free buffer of the least capacity that holds what
// the write expects, or in a new one of that size: neither a buffer grown
// packet by packet nor one of a whole batch's capacity for a one-packet
// write.
func TestBatchBufferFitsTheWrite(t *testing.T) {
	var c Conn
	c.batchHint = 10_000
	if b := c.batchBufLocked(); cap(b) != 10_000 {
		t.Errorf("no free buffer: one of %d bytes, want the hint's 10000", cap(b))
	}
	c.batchHint = 100
	if b := c.batchBufLocked(); cap(b) != minBatchBuf {
		t.Errorf("a hint under a datagram: %d bytes, want %d", cap(b), minBatchBuf)
	}
	c.batchHint = 1 << 20
	if b := c.batchBufLocked(); cap(b) != maxBatchBytes {
		t.Errorf("a hint past a batch: %d bytes, want %d", cap(b), maxBatchBytes)
	}

	c.sendFree = [][]byte{make([]byte, 0, 4096), make([]byte, 0, 60_000), make([]byte, 0, 20_000)}
	c.batchHint = 10_000
	if b := c.batchBufLocked(); cap(b) != 20_000 || len(c.sendFree) != 2 {
		t.Errorf("took a buffer of %d bytes, %d left free; want the 20000 one", cap(b), len(c.sendFree))
	}
}

// The written buffers kept for the next batches are let go of, but the
// largest, once nothing is written or waits to be; while a write is in
// progress they stay.
func TestSendFreeTrimmedWhenIdle(t *testing.T) {
	var c Conn
	full := func() {
		c.sendFree = [][]byte{make([]byte, 0, 4096), make([]byte, 0, 60_000), make([]byte, 0, 20_000)}
	}
	full()
	c.writers = 1
	c.trimSendFreeLocked()
	if len(c.sendFree) != 3 {
		t.Errorf("a write in progress: %d free buffers left, want 3", len(c.sendFree))
	}
	c.writers = 0
	c.sendQ = []queuedWrite{{}}
	c.trimSendFreeLocked()
	if len(c.sendFree) != 3 {
		t.Errorf("a write queued: %d free buffers left, want 3", len(c.sendFree))
	}
	c.sendQ = nil
	c.trimSendFreeLocked()
	if len(c.sendFree) != 1 || cap(c.sendFree[0]) != 60_000 {
		t.Errorf("idle: %d free buffers left, want the 60000-byte one alone", len(c.sendFree))
	}
}
