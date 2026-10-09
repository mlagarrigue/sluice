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
			a := newH3Assembler(Config{})
			b.ReportAllocs()
			for b.Loop() {
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
