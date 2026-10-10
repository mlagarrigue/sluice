package httpstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// newBenchH2Server builds a server whose reader-side state (streams, HPACK
// decoder) is ready to consume, without the writer goroutine or the socket:
// consume's success path — HEADERS that complete in one frame, no window
// updates — never touches s.w, so a writer that is never run is enough to
// benchmark the frame-to-request path in isolation from the socket.
func newBenchH2Server(cfg H2Config) *h2Server {
	cfg = cfg.withDefaults()
	return &h2Server{
		cfg:      cfg,
		handler:  func(b sluice.Batch[Request]) sluice.Batch[Response] { return sluice.Batch[Response]{} },
		dec:      newHPACKDecoder(4096, cfg.MaxHeaderBlockBytes),
		streams:  make(map[uint32]*h2Stream),
		recvWind: connRecvWindow,
		w:        newH2Writer(&memConn{}, cfg),
	}
}

// resetBenchH2Server returns s to its just-accepted state, so one
// construction serves every benchmark iteration and what the loop allocates
// is consume's own rather than the constructor's. The HPACK decoder needs no
// reset: h2HeadersFrame encodes without incremental indexing, so decoding
// leaves no dynamic-table state behind.
func resetBenchH2Server(s *h2Server) {
	clear(s.streams)
	s.open = 0
	s.inFlight.Store(0)
	s.lastID = 0
	s.closedRing = s.closedRing[:0]
	s.recvWind = connRecvWindow
	s.assembling = 0
	s.resets = 0
	s.served = 0
}

// h2HeadersFrame frames one request's header block as a single HEADERS frame,
// complete and end-of-stream: the common case a browser's GET produces.
func h2HeadersFrame(id uint32, path string) []byte {
	block := appendHPACK(nil, ":method", "GET")
	block = appendHPACK(block, ":path", path)
	block = appendHPACK(block, ":scheme", "https")
	block = appendHPACK(block, ":authority", "example.com")
	block = appendHPACK(block, "accept", "text/html,application/json")
	block = appendHPACK(block, "user-agent", "bench-client/1.0")
	f, err := appendH2Frame(nil, h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: id, Payload: block,
	})
	if err != nil {
		panic(err)
	}
	return f
}

// BenchmarkH2Consume measures s.consume: the frame-to-request path a
// datagram's worth of HEADERS frames goes through, with no socket or writer
// goroutine in the way — the h2 counterpart of BenchmarkReadPipelined.
func BenchmarkH2Consume(b *testing.B) {
	for _, depth := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			frames := make([]h2Frame, 0, depth)
			for i := range depth {
				id := uint32(2*i + 1)
				raw := h2HeadersFrame(id, fmt.Sprintf("/orders/%d", i))
				parsed, _, err := parseFrame(raw, H2Config{}.withDefaults())
				if err != nil {
					b.Fatal(err)
				}
				frames = append(frames, parsed)
			}

			s := newBenchH2Server(H2Config{})
			b.ReportAllocs()
			for b.Loop() {
				resetBenchH2Server(s)
				ready, _, err := s.consume(frames)
				if err != nil {
					b.Fatal(err)
				}
				if len(ready) != depth {
					b.Fatalf("consumed %d requests, want %d", len(ready), depth)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/request")
		})
	}
}

// countingConn wraps memConn to count the write syscalls a benchmark would
// otherwise have to infer from timing alone — the number candidate 3 of the
// performance pass is measured against directly.
type countingConn struct {
	memConn
	writes int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes++
	return c.memConn.Write(p)
}

// chunkStream is a [sluice.Stream] over fixed chunks, replayed once — a
// streamed response's body, the shape streamBody pulls from.
func chunkStream(chunks [][]byte) sluice.Stream[[]byte] {
	return func(yield func(sluice.Batch[[]byte]) bool) {
		if !yield(sluice.Batch[[]byte]{Items: chunks}) {
			return
		}
	}
}

// BenchmarkH2StreamBody measures streamBody's write side: how many syscalls
// (via countingConn) and how much it allocates to frame one streamed
// response's chunks onto the wire. Candidate 3 of the performance pass claims
// one write per DATA frame, and the END_STREAM that rides on the last of them
// rather than on a write of its own; this is the harness those numbers are
// checked against.
func BenchmarkH2StreamBody(b *testing.B) {
	for _, chunksPerBatch := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("chunks=%d", chunksPerBatch), func(b *testing.B) {
			payload := make([]byte, 512)
			chunks := make([][]byte, chunksPerBatch)
			for i := range chunks {
				chunks[i] = payload
			}

			// One connection, writer and server for the whole loop, so the
			// alloc signal is streamBody's own and not the setup's.
			conn := &countingConn{}
			cfg := H2Config{}.withDefaults()
			w := newH2Writer(conn, cfg)
			w.connWind = 1 << 30
			go w.run()
			s := &h2Server{cfg: cfg, w: w}

			spent := int32(chunksPerBatch * len(payload)) //nolint:gosec // G115: a few KB
			b.ReportAllocs()
			for b.Loop() {
				// Each iteration closes stream 1, which drops its window, so
				// it is re-registered; the connection window is topped back
				// up by exactly what the iteration spends.
				w.mu.Lock()
				w.windows[1] = 1 << 30
				w.mu.Unlock()
				s.streamBody(1, chunkStream(chunks))
				if err := w.creditConn(spent); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			w.stop()
			<-w.done
			b.ReportMetric(float64(conn.writes)/float64(b.N), "writes/op")
		})
	}
}

// BenchmarkPark is the A/B behind park's stall timer: what arming the expiry
// costs per wakeup when a fresh AfterFunc is allocated each time, against
// re-arming the one timer the writer keeps. A stalled round wakes once per
// WINDOW_UPDATE the peer trickles in, so this is paid per credit grant.
//
// The callback captures the writer, as park's does, so the per-wakeup
// variant pays for the closure as well as the timer.
func BenchmarkPark(b *testing.B) {
	w := &h2Writer{}
	b.Run("afterfunc", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			t := time.AfterFunc(time.Until(time.Now().Add(10*time.Second)), func() {
				w.mu.Lock()
				w.mu.Unlock() //nolint:gocritic,staticcheck // badLock, SA2001: the bare lock round trip is the cost measured
			})
			t.Stop()
		}
	})
	b.Run("reset", func(b *testing.B) {
		t := time.AfterFunc(time.Hour, func() {
			w.mu.Lock()
			w.mu.Unlock() //nolint:gocritic,staticcheck // badLock, SA2001: the bare lock round trip is the cost measured
		})
		t.Stop()
		b.ReportAllocs()
		for b.Loop() {
			t.Reset(time.Until(time.Now().Add(10 * time.Second)))
			t.Stop()
		}
	})
}
