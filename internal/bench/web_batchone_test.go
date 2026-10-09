package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// The honesty benchmark for the web vertical: what the all-batch model costs
// when the batch holds one element.
//
// Every figure behind all-batch so far is a throughput figure, measured at
// 1024 elements per batch. The web vertical's currency is per-request latency,
// and a request is one element: assembling a full batch at 10 000 req/s would
// take ~102 ms, which no interactive service accepts. So the web vertical runs
// at batch = 1, where the model's overhead — a slice header and a Batch
// wrapper per element, one uninlinable operator hop per stage — is pure cost
// against a plain function call.
//
// The comparison is against a hand-written chain of the same six
// transformations, and it includes **building the pipeline**, because a web
// handler builds one per request. Leaving construction out would measure a
// pipeline no handler can have.
//
// What the numbers do not include is the network. That is the point of the
// exercise: isolate the library's cost so the reader can compare it to the
// tens of microseconds a socket read costs, and judge whether all-batch is
// affordable for a request rather than being told it is.

type wParams struct {
	id    int64
	qty   int64
	scope int64
}

type wEntity struct {
	id    int64
	qty   int64
	total int64
}

type wResponse struct {
	id    int64
	total int64
}

// The six steps of a §1.1-shaped request, written once so both sides run
// exactly the same work.
func wDecode(v int64) wParams { return wParams{id: v, qty: v % 7, scope: v % 3} }
func wAuthz(p wParams) bool   { return p.scope != 2 }
func wHydrate(p wParams) wEntity {
	return wEntity{id: p.id, qty: p.qty, total: p.qty * 100}
}
func wAmend(e wEntity) wEntity { e.total += e.id % 5; return e }
func wRule(e wEntity) bool     { return e.qty > 0 }
func wProject(e wEntity) wResponse {
	return wResponse{id: e.id, total: e.total}
}

// buildPipeline is what a handler would call per request: six operators over a
// one-element source.
func buildPipeline(src []int64) sluice.Stream[wResponse] {
	s := sluice.Of(src, 1)
	params := sluice.Convert(s, wDecode)
	allowed := sluice.Filter(params, wAuthz)
	entities := sluice.Convert(allowed, wHydrate)
	amended := sluice.Map(entities, wAmend)
	valid := sluice.Filter(amended, wRule)
	return sluice.Convert(valid, wProject)
}

// webInput is the request value, as a variable rather than a literal: the six
// steps are small pure functions the compiler can inline, and a compile-time
// constant fed into an inlined chain can fold to nothing — a near-zero
// denominator that overstates the library-vs-handwritten ratio this file
// exists to report honestly. A package-level variable load cannot fold.
var webInput = int64(1)

// BenchmarkWebHandwritten is the denominator: the same six steps as direct
// calls, with the same early-out semantics the operators have.
func BenchmarkWebHandwritten(b *testing.B) {
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		v := webInput
		p := wDecode(v)
		if !wAuthz(p) {
			continue
		}
		e := wAmend(wHydrate(p))
		if !wRule(e) {
			continue
		}
		acc += wProject(e).total
	}
	sink = acc
}

// BenchmarkWebBatchOne is the honest per-request figure: the pipeline is built
// and run for every request, as a handler would.
func BenchmarkWebBatchOne(b *testing.B) {
	src := []int64{1}
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		buildPipeline(src)(func(bt sluice.Batch[wResponse]) bool {
			for _, r := range bt.Items {
				acc += r.total
			}
			return true
		})
	}
	sink = acc
}

// BenchmarkWebBatchOneReused separates the two terms: the pipeline is built
// once and traversed one element at a time. The gap to BenchmarkWebBatchOne is
// what construction costs; the gap to BenchmarkWebHandwritten is what the
// traversal costs.
func BenchmarkWebBatchOneReused(b *testing.B) {
	src := []int64{1}
	pipeline := buildPipeline(src)
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		pipeline(func(bt sluice.Batch[wResponse]) bool {
			for _, r := range bt.Items {
				acc += r.total
			}
			return true
		})
	}
	sink = acc
}
