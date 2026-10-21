package httpstream

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// BenchmarkH3Assemble measures H3Assembler.Frames: the datagram-frames-to-
// requests path, the h3 counterpart of BenchmarkReadPipelined and
// BenchmarkH2Consume. Each request is one client-initiated bidirectional
// stream carrying a single HEADERS frame with no body, which is what drain
// spends its time on (candidate 5 of the performance pass).
func BenchmarkH3Assemble(b *testing.B) {
	for _, depth := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			frames := make([]quic.Frame, depth)
			for i := range depth {
				id := uint64(4 * i) // client-initiated bidirectional
				data := h3Request("GET", fmt.Sprintf("/orders/%d", i))
				frames[i] = streamFrame(id, data, true)
			}

			// One assembler for the whole loop, as one connection has: built
			// per iteration, its constructor's maps were most of what this
			// measured (29 allocs/op against 20 hoisted, at depth 1). A
			// completed stream leaves no state behind, so feeding the same
			// identifiers again assembles exactly as fresh ones would.
			a := testAssembler(Config{})
			b.ReportAllocs()
			for b.Loop() {
				// The writer's retire, in one store: the requests of the
				// previous iteration were answered, as the h2 bench resets
				// its server's count.
				a.inFlight.Store(0)
				batch, _, failed, err := a.Frames(frames)
				if err != nil {
					b.Fatal(err)
				}
				if len(failed) != 0 {
					b.Fatalf("%d streams failed", len(failed))
				}
				if batch.Len() != depth {
					b.Fatalf("assembled %d requests, want %d", batch.Len(), depth)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/request")
		})
	}
}

// BenchmarkH3DrainIncomplete measures what one byte of a trickled DATA frame
// costs once a buffer of the given size is parked ahead of it: the peer
// declared a frame at MaxBodyBytes and delivers its payload one byte at a
// time. The figure must not grow with the parked size — a flat line from
// 64 KiB to 1 MiB — or each byte a peer sends buys a copy of everything it
// sent before (RP-20261020-secnet-01).
func BenchmarkH3DrainIncomplete(b *testing.B) {
	for _, parked := range []int{64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("parked=%dKiB", parked>>10), func(b *testing.B) {
			cfg := Config{MaxBodyBytes: 4 << 20}.mustResolve()
			a := testAssembler(cfg)
			buf := make([]byte, 0, parked+16)
			buf = quic.AppendVarint(buf, h3Data)
			buf = quic.AppendVarint(buf, uint64(cfg.MaxBodyBytes))
			buf = append(buf, make([]byte, parked)...)
			st := &h3Stream{declared: -1, gotHead: true, buf: buf}
			n := len(buf)
			b.ReportAllocs()
			for b.Loop() {
				st.buf = append(st.buf[:n], 0)
				if err := a.drain(st); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/byte")
		})
	}
}
