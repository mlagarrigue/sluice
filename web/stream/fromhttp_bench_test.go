package stream_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

type benchOrder struct {
	ID int64 `json:"id"`
}

// benchStages is the unit read written on this package's batch stages.
func benchStages() httpstream.Handler {
	rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{id}"})
	return func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		ex := stream.Exchanges(nil, b)
		rt.Route(ex)
		ids := make([]int64, len(ex))
		for i := range ex {
			ids[i], _ = stream.PathInt64(&ex[i], 0, "id")
		}
		return stream.Render(ex, func(i int) (int, any) { return http.StatusOK, benchOrder{ID: ids[i]} })
	}
}

// benchFromHTTP is the same unit read written on net/http and web, served
// through FromHTTP.
func benchFromHTTP() httpstream.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _, ok := web.PathInt64(r, "id")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, nil)
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, benchOrder{ID: id})
	})
	return stream.FromHTTP(mux)
}

// BenchmarkUnitRead answers a batch of unit reads without a socket, on the
// batch stages and through FromHTTP: what the per-request handler costs
// over the stages, which docs/benchmarks.md reports per request.
func BenchmarkUnitRead(b *testing.B) {
	for _, h := range []struct {
		name   string
		handle func() httpstream.Handler
	}{{"stages", benchStages}, {"fromhttp", benchFromHTTP}} {
		for _, n := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/batch=%d", h.name, n), func(b *testing.B) {
				handle, ctx := h.handle(), context.Background()
				reqs := make([]httpstream.Request, n)
				for i := range reqs {
					reqs[i] = httpstream.Request{
						Method: []byte("GET"),
						Target: []byte("/orders/" + strconv.Itoa(1000+i)),
						Headers: []httpstream.Header{
							{Name: []byte("host"), Value: []byte("example.test")},
							{Name: []byte("accept"), Value: []byte("application/json")},
						},
					}
				}
				batch := sluice.Batch[httpstream.Request]{Items: reqs}
				b.ReportAllocs()
				for b.Loop() {
					if out := handle(ctx, batch); out.Items[0].Status != http.StatusOK {
						b.Fatal(out.Items[0].Status)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/request")
			})
		}
	}
}
