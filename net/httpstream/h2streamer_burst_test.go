package httpstream

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// parkedStreamers counts the streamer goroutines alive in the process.
func parkedStreamers() int {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), ".(*h2Streamer).loop(")
}

// waitStreamers waits, three seconds at most, for the count to fall to
// want, and returns the last count read.
func waitStreamers(want int) int {
	deadline := time.Now().Add(3 * time.Second)
	n := parkedStreamers()
	for n > want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		n = parkedStreamers()
	}
	return n
}

// The free list of streamers was trimmed only once the connection went idle:
// one streamed answer that lasts — server-sent events, a slow download —
// kept it from idling, and the streamers a burst had used stayed parked,
// each with its goroutine and its two jobs' buffers, for as long as that
// answer ran. The list now holds no more than are in use, and one: after a
// burst of 16 while one answer streams, two goroutines are left — the long
// answer's and one free. The witness is the idle connection, which keeps one.
func TestABurstLeavesNoStreamersWhileOneAnswerStreams(t *testing.T) {
	const burst = 16
	release := make(chan struct{})
	var started atomic.Int32
	addr, _ := serveH2(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			if string(b.Items[i].Target) == "/long" {
				out[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("x")}}) {
						return
					}
					<-release
				}}
				continue
			}
			out[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				started.Add(1)
				deadline := time.Now().Add(3 * time.Second)
				for started.Load() < burst && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				yield(sluice.Batch[[]byte]{Items: [][]byte{make([]byte, 1<<10)}})
			}}
		}
		return sluice.Batch[Response]{Items: out}
	})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	base := parkedStreamers()
	c := dialH2(t, addr)
	defer func() { _ = c.conn.Close() }()
	c.request(1, "GET", "/long")
	c.flush()
	c.pump(func() bool { return len(c.got[1]) > 0 })
	if len(c.got[1]) == 0 {
		t.Fatal("precondition lost: the long answer did not start")
	}
	for i := range uint32(burst) {
		c.request(3+2*i, "GET", "/burst")
	}
	c.flush()
	c.pump(func() bool { return len(c.ended) >= burst })
	if got := started.Load(); got != burst {
		t.Fatalf("precondition lost: %d of %d burst answers ran at once", got, burst)
	}
	if len(c.ended) < burst {
		t.Fatalf("precondition lost: %d of %d burst answers ended", len(c.ended), burst)
	}
	if c.ended[1] {
		t.Fatal("precondition lost: the long answer ended")
	}

	held := waitStreamers(base+2) - base
	t.Logf("streamer goroutines with one answer still streaming after a burst of %d: %d", burst, held)

	close(release)
	released = true
	c.pump(func() bool { return c.ended[1] })
	idle := waitStreamers(base+1) - base
	t.Logf("streamer goroutines once idle: %d", idle)
	if idle > 1 {
		t.Fatalf("witness: %d streamer goroutines on an idle connection, want 1", idle)
	}
	if held > 2 {
		t.Errorf("%d streamer goroutines (and their buffers) kept after the burst while one answer streams, want at most 2", held)
	}
}
