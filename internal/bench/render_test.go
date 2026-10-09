package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web/stream"
)

// stream.Render allocates its own headers slice per exchange
// (make([]httpstream.Header, 0, len(e.Headers)+1)), in tension with
// CONTRIBUTING.md's "the batch is the unit of transport, not the element" and
// the ~1.5 ns/element budget. It is likely unavoidable — each response in
// the batch carries its own distinct headers, so there is no shared buffer
// to reuse them from — but that was an assumption, not a measurement. The
// benchmarks below report the figure; TestRenderAllocationCeiling pins it,
// so a future change that makes it worse (or a change that finds a way to
// make it better) shows up as a failure rather than being asserted either
// way from the armchair.
func renderBatch(n int) []stream.Exchange {
	out := make([]stream.Exchange, n)
	for i := range out {
		out[i] = stream.Exchange{
			Headers: []httpstream.Header{
				{Name: []byte("cache-control"), Value: []byte("no-store")},
			},
		}
	}
	return out
}

type renderBody struct {
	ID    int64  `json:"id"`
	Label string `json:"label"`
}

// BenchmarkRenderServedBatch is the ordinary case: a batch of served (not
// refused) exchanges, each answered with a small JSON body and one caller
// header riding alongside Content-Type.
func BenchmarkRenderServedBatch(b *testing.B) {
	const batchSize = 64
	exchanges := renderBatch(batchSize)
	serve := func(i int) (int, any) {
		return 200, renderBody{ID: int64(i), Label: "ok"}
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = stream.Render(exchanges, serve)
	}
}

// BenchmarkRenderOneExchange isolates the per-element cost Render pays: the
// headers slice is allocated once per exchange regardless of batch size, so
// this is what the batch benchmark above is actually paying batchSize times
// over.
func BenchmarkRenderOneExchange(b *testing.B) {
	exchanges := renderBatch(1)
	serve := func(i int) (int, any) {
		return 200, renderBody{ID: int64(i), Label: "ok"}
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = stream.Render(exchanges, serve)
	}
}

// TestRenderAllocationCeiling is the assertion the benchmarks above cannot
// make: a benchmark only reports, and a figure nobody compares regresses in
// silence. Measured: 4 allocations for a lone exchange (the headers slice,
// the JSON body's, the response slice), with or without the race detector;
// 193 for a batch of 64 (~3 per exchange — some of the lone exchange's cost
// amortizes), 240 under the race detector. The batch ceiling of 4 per
// exchange absorbs both, while one added per-exchange allocation lands at
// least batchSize over the measured figure and cannot pass. Allocation
// counts do not depend on machine speed, so unlike the timing assertions in
// this package this needs no -short gate.
func TestRenderAllocationCeiling(t *testing.T) {
	serve := func(i int) (int, any) {
		return 200, renderBody{ID: int64(i), Label: "ok"}
	}

	for _, tc := range []struct {
		name    string
		n       int
		ceiling int64
	}{
		{"one exchange", 1, 4},
		{"a served batch", 64, 64 * 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exchanges := renderBatch(tc.n)
			res := testing.Benchmark(func(b *testing.B) { //nolint:thelper // benchmark body
				for b.Loop() {
					_ = stream.Render(exchanges, serve)
				}
			})
			t.Logf("%d exchanges: %d allocs/op, %d B/op", tc.n, res.AllocsPerOp(), res.AllocedBytesPerOp())
			if res.AllocsPerOp() > tc.ceiling {
				t.Errorf("Render allocated %d times for %d exchanges, over the ceiling of %d: "+
					"a per-exchange allocation was added",
					res.AllocsPerOp(), tc.n, tc.ceiling)
			}
		})
	}
}
