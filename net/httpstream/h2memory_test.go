package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// liveHeap is the heap still reachable after a collection. Twice, because the
// first only moves sync.Pool contents to their victim cache.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// A stream outlives its request for the late frames RFC 9113 §5.1 asks this
// end to tolerate or refuse, and every connection that has served
// MaxConcurrentStreams requests holds that many. What those frames need is the
// stream's state; holding the whole assembly state instead made a full ring
// most of an idle connection's heap.
func TestH2ClosedStreamsAreCompact(t *testing.T) {
	const n = 1000
	s := newBenchH2Server(Config{MaxConcurrentStreams: n})
	frames := make([]h2Frame, n)
	for i := range frames {
		raw := h2HeadersFrame(uint32(2*i+1), "/") //nolint:gosec // G115: i < n
		frames[i] = h2Frame{
			Type:     raw[3],
			Flags:    raw[4],
			StreamID: binary.BigEndian.Uint32(raw[5:9]),
			Payload:  raw[9:],
		}
	}

	before := liveHeap()
	// One request at a time, each answered before the next, as a client
	// served in sequence: the writer's own per-stream state, released with
	// the answer, never grows. The requests go nowhere, so what stays
	// reachable is what the server keeps.
	for i := range frames {
		ready, ids, err := s.consume(frames[i : i+1])
		if err != nil || len(ready) != 1 {
			t.Fatalf("stream %d: %d requests, %v", frames[i].StreamID, len(ready), err)
		}
		s.w.forget(ids[0])
		s.inFlight.Add(-1)
	}
	after := liveHeap()
	runtime.KeepAlive(s)
	runtime.KeepAlive(frames)

	per := (int64(after) - int64(before)) / n
	t.Logf("%d bytes held per answered stream", per)
	// An *h2Stream alone is 256 bytes. An identifier and a state are five,
	// which a map slot and a ring entry make about thirty.
	if per > 48 {
		t.Errorf("each closed stream holds %d bytes: more than its identifier and state", per)
	}
}

// The steady state of a connection that has served MaxConcurrentStreams
// requests is the point the server's memory is compared at: HTTP/2 against
// HTTP/1.1, the same requests, the same handler, the same number of
// connections — and clients that keep nothing but their socket, so the heap
// measured is the server's.
func TestH2ConnectionHeapNearH1(t *testing.T) {
	if testing.Short() {
		t.Skip("opens 512 connections")
	}
	if raceEnabled {
		// 1.56× in a burst under -race, where the coverage job measured it,
		// against 1.30× without: the CI job without -race holds the bound.
		t.Skip("the race detector's per-goroutine state is heap the server does not hold")
	}
	if testing.CoverMode() != "" {
		// Under the coverage job's instrumentation the ratio went red twice on
		// 2026-10-22: 1.54× in a burst, and 4.14× one at a time where an
		// HTTP/1.1 pass measured 2981 bytes against its usual 10 KB. The jobs
		// without -cover hold the bound.
		t.Skip("coverage instrumentation moves the live heap this compares")
	}
	const conns, requests = 256, defaultMaxConcurrency
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range out {
			out[i] = Response{Status: 204}
		}
		return sluice.Batch[Response]{Items: out}
	})

	h1 := []byte("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	h2Settings, _ := appendH2Frame(nil, h2Frame{Type: frameSettings})
	h2Ack, _ := appendH2Frame(nil, h2Frame{Type: frameSettings, Flags: flagAck})

	// Each pass waits, once its connections are closed, until the server's
	// goroutines are back to where they were: the server tears a closed
	// connection down on its own goroutines, and a pass that started while
	// the last one's were still alive counted their release against its own
	// connections — on macOS a second HTTP/1.1 pass read 2,180 bytes where
	// the first read 10,881, and the ratio 5.44×. Back to within a handful:
	// the first connection starts one goroutine of the server's that then
	// stays, where a connection's own are one or more per connection.
	measure := func(name string, depth int, open func(c net.Conn, depth int) error) uint64 {
		held := make([]net.Conn, 0, conns)
		base := runtime.NumGoroutine()
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
			deadline := time.Now().Add(10 * time.Second)
			for runtime.NumGoroutine() > base+8 {
				if time.Now().After(deadline) {
					t.Fatalf("%s: %d goroutines 10 s after closing, %d before opening", name, runtime.NumGoroutine(), base)
				}
				time.Sleep(time.Millisecond)
			}
		}()
		before := liveHeap()
		for range conns {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, c)
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			if err := open(c, depth); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		per := (liveHeap() - before) / conns
		// Goroutines alongside the bytes: each one's descriptor is heap,
		// and a CI runner read 6 KB more per HTTP/2 connection than any
		// local machine (three times on 2026-10-21 and 22) — this says
		// whether that is goroutines still running or buffers held.
		t.Logf("%s, %d at a time: %d bytes of live heap and %.1f goroutines per connection",
			name, depth, per, float64(runtime.NumGoroutine()-base)/conns)
		return per
	}

	http1 := func(c net.Conn, depth int) error {
		r := bufio.NewReader(c)
		for sent := 0; sent < requests; sent += depth {
			if _, err := c.Write(bytes.Repeat(h1, depth)); err != nil {
				return err
			}
			for answered := 0; answered < depth; {
				line, err := r.ReadSlice('\n')
				if err != nil {
					return err
				}
				if bytes.HasPrefix(line, []byte("HTTP/1.1 204")) {
					answered++
				}
			}
		}
		return nil
	}
	http2 := func(c net.Conn, depth int) error {
		out := append([]byte(Preface), h2Settings...)
		var head [9]byte
		payload := make([]byte, 1<<14)
		for sent := 0; sent < requests; sent += depth {
			for i := range depth {
				out = append(out, h2HeadersFrame(uint32(2*(sent+i)+1), "/")...) //nolint:gosec // G115: under requests
			}
			if _, err := c.Write(out); err != nil {
				return err
			}
			out = out[:0]
			for answered := 0; answered < depth; {
				if _, err := io.ReadFull(c, head[:]); err != nil {
					return err
				}
				size := int(head[0])<<16 | int(head[1])<<8 | int(head[2])
				if size > len(payload) {
					payload = make([]byte, size)
				}
				if _, err := io.ReadFull(c, payload[:size]); err != nil {
					return err
				}
				switch {
				case head[3] == frameSettings && head[4]&flagAck == 0:
					if _, err := c.Write(h2Ack); err != nil {
						return err
					}
				case head[3] == frameHeaders && head[4]&flagEndStream != 0:
					answered++
				}
			}
		}
		return nil
	}

	// One at a time is a connection under ordinary load: 1.17-1.24× on a
	// quiet machine, 2.02× when every closed stream was kept whole. All at
	// once is a burst: 1.30× since an idle connection lets go of the
	// scratch the burst grew (h2Writer.shed), 1.69-1.74× when it kept it at
	// its high-water mark, 2.93× before that. A CI runner moves a figure by
	// up to 12%, so the bounds sit between the two rather than on the
	// figure: they catch a return of what was removed, not a few hundred
	// bytes.
	for _, tc := range []struct {
		depth int
		ratio float64
	}{{1, 1.6}, {requests, 1.5}} {
		// Twice each, alternating, keeping the smaller: a goroutine's
		// descriptor is heap, and whether one is reused from an earlier
		// test or allocated anew moved a single pass by 1.5 KB a connection.
		one := measure("HTTP/1.1", tc.depth, http1)
		two := measure("HTTP/2", tc.depth, http2)
		one = min(one, measure("HTTP/1.1", tc.depth, http1))
		two = min(two, measure("HTTP/2", tc.depth, http2))
		if r := float64(two) / float64(one); r > tc.ratio {
			t.Errorf("%d at a time: an HTTP/2 connection holds %d bytes of live heap, %.2f× the %d of an HTTP/1.1 one, over %.1f×",
				tc.depth, two, r, one, tc.ratio)
		}
	}
}
