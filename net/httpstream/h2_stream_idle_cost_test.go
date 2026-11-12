package httpstream

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// What an idle HTTP/2 connection keeps once it has served a streamed
// response, against one that served a buffered one: the streamer it keeps
// on its free list — its goroutine, parked, and its buffers — and nothing
// more. One streamer and one goroutine per connection is the bound
// trimStreamers holds; the bytes are logged, the figure ADR 0008 states.
func TestH2IdleConnectionKeepsOneStreamer(t *testing.T) {
	if testing.Short() {
		t.Skip("opens 128 connections")
	}
	if raceEnabled || testing.CoverMode() != "" {
		t.Skip("the race detector and the coverage counters move the memory this compares")
	}
	const conns = 64
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			if string(b.Items[i].Target) == "/streamed" {
				out[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					yield(sluice.Batch[[]byte]{Items: [][]byte{make([]byte, 4<<10)}})
				}}
				continue
			}
			out[i] = Response{Status: 200, Body: make([]byte, 4<<10)}
		}
		return sluice.Batch[Response]{Items: out}
	})

	measure := func(path string) (goroutines float64, bytes int64) {
		held := make([]*h2client, 0, conns)
		base := runtime.NumGoroutine()
		before := liveMemory()
		for range conns {
			c := dialH2(t, addr)
			c.request(1, "GET", path)
			c.flush()
			if got := c.bodies(1)[1]; len(got) != 4<<10 {
				t.Fatalf("%s: answered %d bytes, want 4096", path, len(got))
			}
			held = append(held, c)
		}
		// The streamer goes back to its list once its answer is out; give
		// the server's goroutines a moment to park.
		time.Sleep(100 * time.Millisecond)
		goroutines = float64(runtime.NumGoroutine()-base) / conns
		bytes = (int64(liveMemory()) - int64(before)) / conns //nolint:gosec // G115: a few megabytes
		for _, c := range held {
			_ = c.conn.Close()
		}
		deadline := time.Now().Add(10 * time.Second)
		for runtime.NumGoroutine() > base+8 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		return goroutines, bytes
	}

	bg, bb := measure("/buffered")
	sg, sb := measure("/streamed")
	t.Logf("per idle connection: buffered %.2f goroutines and %d bytes, streamed %.2f goroutines and %d bytes (%+d)",
		bg, bb, sg, sb, sb-bb)
	if extra := sg - bg; extra < 0.75 || extra > 1.25 {
		t.Errorf("an idle connection that streamed keeps %.2f goroutines more than one that did not, want one: the parked streamer's", extra)
	}
}

// liveMemory is the live heap and the goroutine stacks in use, after the
// collections that shrink both.
func liveMemory() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc + m.StackInuse
}
