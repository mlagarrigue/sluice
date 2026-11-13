package httpstream

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// A streamed body's queued job holds about a round of unframed bytes, not
// everything the producer yielded since the job opened. A peer that reads
// each DATA frame and hands its credit back, slower than the producer
// yields, keeps the job queued with bytes left at the end of every round:
// what was framed must be let go while the job stays in the queue.
func TestH2QueuedStreamedJobKeepsNoFramedBytes(t *testing.T) {
	cfg := Config{MaxWriteBufferBytes: 16 << 10, WriteTimeout: 10 * time.Second}.mustResolve()
	srv, cli := net.Pipe()
	defer cli.Close()
	w := newH2Writer(srv, cfg)
	w.open(1)
	go w.run()

	go func() {
		var hdr [9]byte
		buf := make([]byte, 1<<20)
		for {
			if _, err := io.ReadFull(cli, hdr[:]); err != nil {
				return
			}
			n := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
			if _, err := io.ReadFull(cli, buf[:n]); err != nil {
				return
			}
			if hdr[3] == frameData && n > 0 {
				id := binary.BigEndian.Uint32(hdr[5:9]) &^ (1 << 31)
				time.Sleep(200 * time.Microsecond)
				_ = w.credit(id, int32(n)) //nolint:gosec // G115: a frame's length
				_ = w.creditConn(int32(n)) //nolint:gosec // G115: a frame's length
			}
		}
	}()

	var jobs [2]h2Job
	jobs[0].done, jobs[1].done = true, true
	var cur *h2Job
	spare := func() *h2Job {
		if cur == &jobs[0] {
			return &jobs[1]
		}
		return &jobs[0]
	}
	// A batch that does not divide a round, so unframed bytes are left at
	// the end of each one and the job never leaves the queue.
	chunk := make([]byte, 1000)
	most := 0
	for range 512 {
		j, err := w.extend(1, cur, spare(), chunk)
		if err != nil || j.err != nil {
			t.Fatalf("extend: %v / %v", err, j.err)
		}
		cur = j
		w.mu.Lock()
		most = max(most, cap(j.data))
		w.mu.Unlock()
	}
	if err := w.finish(1, cur, spare(), nil, false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	w.stop()
	_ = srv.Close()
	<-w.done
	if bound := 4 * cfg.MaxWriteBufferBytes; most > bound {
		t.Fatalf("a queued streamed job held a buffer of %d bytes, over %d (4 x MaxWriteBufferBytes)", most, bound)
	}
}

// The streamer an idle connection keeps holds buffers no larger than an
// ordinary batch needs, as shed keeps the writer's: a burst of large
// streamed bodies does not stay on an idle connection.
func TestH2KeptStreamerShedsLargeBuffers(t *testing.T) {
	st := &h2Streamer{buf: make([]byte, 1<<20), tail: make([]byte, 1<<20)}
	st.jobs[0].data = make([]byte, 0, 1<<20)
	st.jobs[1].data = make([]byte, 0, 512)
	st.scratch.block = make([]byte, 1<<20)
	st.shed()
	for name, c := range map[string]int{
		"buf": cap(st.buf), "tail": cap(st.tail), "job 0": cap(st.jobs[0].data),
		"scratch": cap(st.scratch.block),
	} {
		if c > h2KeptOut {
			t.Errorf("%s keeps %d bytes, over %d", name, c, h2KeptOut)
		}
	}
	if cap(st.jobs[1].data) != 512 {
		t.Errorf("a small buffer is let go too: job 1 keeps %d bytes", cap(st.jobs[1].data))
	}
}
