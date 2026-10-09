package benchmarks

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/web"
)

// Latency distribution, and the case where batching has nothing to amortise.
//
// # Why percentiles and not only ns/op
//
// Batching trades latency for throughput by construction: a call waits for
// company before anything happens. An average hides that completely — the mean
// of "most calls fast, some waiting for a batch to fill" looks like a small
// constant. §2 of the brief asks for p50, p99 and p99.9 for exactly this
// reason, and a throughput-only chart would hide the one thing a reader needs
// in order to judge whether the trade suits them.
//
// # These are pipeline tails, not service tails
//
// No socket is involved, so nothing here queues at an accept loop, and queueing
// is what dominates a real service's p99.9. Read every figure below as the tail
// of the handler; the tail of the service needs a load generator over a socket,
// which this file deliberately does not pretend to be.

// percentiles reports the distribution of a handler's latency, in nanoseconds.
type percentiles struct {
	p50, p99, p999 time.Duration
	mean           time.Duration
	n              int
}

func (p percentiles) String() string {
	return fmt.Sprintf("n=%d mean=%v p50=%v p99=%v p99.9=%v", p.n, p.mean, p.p50, p.p99, p.p999)
}

func measure(h http.Handler, iterations int, qty int64) percentiles {
	body := &replayBody{data: []byte(`{"saleQty":` + strconv.FormatInt(qty, 10) + `}`)}
	tmpl := request(qty)
	w := &discardWriter{}

	samples := make([]time.Duration, 0, iterations)
	var total time.Duration

	// A warm-up pass, unrecorded: the first few hundred requests pay for lazily
	// built routing tables and a cold allocator, and reporting those in a tail
	// would say more about start-up than about steady state.
	for range 1000 {
		body.rewind()
		w.reset()
		r := *tmpl
		r.Body = body
		h.ServeHTTP(w, &r)
	}

	for range iterations {
		body.rewind()
		w.reset()
		r := *tmpl
		r.Body = body

		start := time.Now()
		h.ServeHTTP(w, &r)
		d := time.Since(start)

		samples = append(samples, d)
		total += d
	}

	slices.Sort(samples)
	at := func(q float64) time.Duration {
		i := int(q * float64(len(samples)))
		if i >= len(samples) {
			i = len(samples) - 1
		}
		return samples[i]
	}
	return percentiles{
		p50: at(0.50), p99: at(0.99), p999: at(0.999),
		mean: total / time.Duration(len(samples)),
		n:    len(samples),
	}
}

// TestWebLatencyDistribution prints the distribution for every contender.
//
// It is a test rather than a benchmark because `go test -bench` reports one
// number and this reports four. It is skipped unless SLUICE_LATENCY is set, so
// an ordinary `go test` stays fast; the figures are meant to be produced
// deliberately, on a quiet machine, in one session — the same discipline the
// root module's measurement docs impose.
func TestWebLatencyDistribution(t *testing.T) {
	if os.Getenv("SLUICE_LATENCY") == "" {
		t.Skip("set SLUICE_LATENCY=1 to measure the latency distribution")
	}
	const iterations = 200_000

	for _, name := range []string{"NetHTTP", "Sluice", "Gin", "Echo", "Chi"} {
		h := contenders()[name]
		t.Logf("%-8s accepted  %s", name, measure(h, iterations, 9))
	}
	// The rejection path, where this project does more work than the others by
	// answering with the remedy as well as the problem.
	for _, name := range []string{"NetHTTP", "Sluice"} {
		h := contenders()[name]
		t.Logf("%-8s rejected  %s", name, measure(h, iterations, 0))
	}
}

// --- the loss: batching with nothing to amortise -----------------------------

// gatewayHandler serves each request through a batching gateway whose pipeline
// answers immediately.
//
// This is the gateway's bargain with one side removed. Its whole value is that
// sixty-four callers waiting on one database round trip beat sixty-four round
// trips; with no round trip in the picture, what is left is a channel, a
// hand-off to another goroutine, and a batch that usually holds one element.
// The number this produces is the cost of the mechanism, and it is published
// because a comparison that only showed the wins would not be believed.
func gatewayHandler(t testing.TB) http.Handler {
	gw := gateway.New(
		gateway.Config{Size: 64, Within: 200 * time.Microsecond},
		func(calls sluice.Stream[*gateway.Call[amendRequest, amendResult]]) {
			var parts gateway.Partitions[*gateway.Call[amendRequest, amendResult], string]
			calls(func(b sluice.Batch[*gateway.Call[amendRequest, amendResult]]) bool {
				// Partitioned by principal, exactly as §0.3 requires — with no
				// database behind it, so the partitioning cost shows and the
				// round trips it would save do not.
				_ = parts.Each(b, func(c *gateway.Call[amendRequest, amendResult]) string {
					return c.In.tenant
				}, func(_ string, group []*gateway.Call[amendRequest, amendResult]) error {
					for _, c := range group {
						status, payload := answer(c.In.order, c.In.line, c.In.qty)
						c.Reply(amendResult{status: status, payload: payload})
					}
					return nil
				})
				return true
			})
		})
	t.Cleanup(func() { _ = gw.Close() })

	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		order, d, ok := web.PathInt64(r, "order")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		line, d, ok := web.PathInt(r, "line")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		var body amendBody
		if d, ok := web.DecodeJSON(r, bodyLimit, &body); !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		res, err := gw.Do(r.Context(), amendRequest{
			tenant: "acme", order: order, line: line, qty: body.SaleQty,
		})
		if err != nil {
			_ = web.WriteJSON(w, http.StatusServiceUnavailable, web.InternalReport())
			return
		}
		_ = web.WriteJSON(w, res.status, res.payload)
	})
	return mux
}

type amendRequest struct {
	tenant string
	order  int64
	line   int
	qty    int64
}

type amendResult struct {
	status  int
	payload any
}

// The gateway answers the same thing the direct handler does, or the figure
// beside it means nothing.
func TestGatewayHandlerAgreesWithTheDirectOne(t *testing.T) {
	gwH := gatewayHandler(t)
	direct := sluiceHandler()

	for _, qty := range []int64{9, 0} {
		a := runOnce(t, gwH, qty)
		b := runOnce(t, direct, qty)
		if a != b {
			t.Errorf("qty %d: gateway answered %q, direct answered %q", qty, a, b)
		}
	}
}

func runOnce(t *testing.T, h http.Handler, qty int64) string {
	t.Helper()
	body := &replayBody{data: []byte(`{"saleQty":` + strconv.FormatInt(qty, 10) + `}`)}
	w := &discardWriter{}
	r := *request(qty)
	r.Body = body
	h.ServeHTTP(w, &r)
	return fmt.Sprintf("%d/%d", w.status, w.n)
}

// BenchmarkWebSluiceGateway is the loss, measured serially: one caller, so the
// batch never fills and every request pays the hand-off with nothing to share
// it with. This is the worst case and it is the honest one to lead with.
func BenchmarkWebSluiceGateway(b *testing.B) {
	benchHandler(b, gatewayHandler(b), 9)
}

// And the shape the gateway is actually for: many callers at once. Without a
// database it still cannot win — there is no round trip being amortised — but
// it shows the mechanism under the concurrency it was designed for rather than
// under the one that makes it look worst.
func BenchmarkWebSluiceGatewayConcurrent(b *testing.B) {
	h := gatewayHandler(b)
	const callers = 64

	b.ReportAllocs()
	for b.Loop() {
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				body := &replayBody{data: []byte(`{"saleQty":9}`)}
				w := &discardWriter{}
				r := *request(9)
				r.Body = body
				r = *r.WithContext(context.Background())
				h.ServeHTTP(w, &r)
				if w.status != http.StatusOK {
					b.Errorf("status %d, want %d", w.status, http.StatusOK)
				}
			})
		}
		wg.Wait()
	}
}
