package gateway_test

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/parallel"
)

// What the gateway is worth, and — just as important — when it is worth
// nothing.
//
// The library's per-element nanoseconds are the wrong figure for a request
// path: the measurements put the model at 249 ns per request at
// batch = 1 against 1.88 for a hand-written chain. These benchmarks measure
// the figure that is right, which is what happens to throughput when the work
// behind the request is a bounded external resource.
//
// **The bound is the whole point.** A first version of this benchmark let
// every simulated query run concurrently with every other, and reported the
// gateway 65 times slower than one-query-per-request — correctly, because a
// backend that serves a thousand simultaneous queries at no extra cost has
// nothing to gain from being asked fewer, larger questions. No such backend
// exists: a database has a connection pool, a service has a rate limit, a disk
// has a queue depth. Modelling that bound is what makes the comparison mean
// anything, and forgetting it is how a batching layer gets "proven" harmful.

const (
	roundTrip = 100 * time.Microsecond // a modest same-datacenter query
	poolSize  = 16                     // connections the store will serve at once
	clients   = 256                    // concurrent callers
)

var sink int

// pool is the bounded backend: at most poolSize queries run at once, whatever
// the number of callers, and a query costs the same whether it asks for one
// key or sixty-four.
type pool chan struct{}

func newPool() pool {
	p := make(pool, poolSize)
	for range poolSize {
		p <- struct{}{}
	}
	return p
}

func (p pool) query() {
	<-p
	time.Sleep(roundTrip)
	p <- struct{}{}
}

// BenchmarkPerCallRoundTrip: one request, one query — the shape every handler
// has by default. Throughput is capped by the pool: 16 queries in flight,
// 100 µs each.
func BenchmarkPerCallRoundTrip(b *testing.B) {
	p := newPool()
	b.SetParallelism(clients)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			p.query()
			n++
		}
		sink = n
	})
}

// BenchmarkBatchedRoundTrip: the same requests through a gateway, so one query
// answers up to 64 of them. The gateway's own machinery — a rendezvous per
// call, a batch, a reply channel each — is paid on top, and the gain is net of
// it.
//
// Note what this shape does not do: the pipeline is one goroutine, so batches
// are served one after another and the pool sits mostly idle. That ceiling is
// the caller's to lift by composing a fan-out inside the pipeline — which is
// what BenchmarkBatchedParallelRoundTrip measures.
func BenchmarkBatchedRoundTrip(b *testing.B) {
	p := newPool()
	gw := gateway.New(gateway.Config{Size: 64, Within: 500 * time.Microsecond},
		func(calls sluice.Stream[*gateway.Call[int, int]]) {
			calls(func(bt sluice.Batch[*gateway.Call[int, int]]) bool {
				p.query() // one query for the whole batch
				for _, c := range bt.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	ctx := context.Background()
	b.SetParallelism(clients)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			if _, err := gw.Do(ctx, n); err != nil {
				b.Error(err)
				return
			}
			n++
		}
		sink = n
	})
}

// BenchmarkBatchedParallelRoundTrip: the same gateway with the library's own
// fan-out inside the pipeline, so several batches are in flight and the pool
// is actually used. This is the composition the operators exist for —
// ParallelUnordered rather than Parallel, since nothing downstream of a
// request/reply gateway cares what order the batches complete in.
func BenchmarkBatchedParallelRoundTrip(b *testing.B) {
	p := newPool()
	gw := gateway.New(gateway.Config{Size: 64, Within: 500 * time.Microsecond},
		func(calls sluice.Stream[*gateway.Call[int, int]]) {
			served := parallel.Unordered(calls, poolSize,
				func(bt sluice.Batch[*gateway.Call[int, int]]) sluice.Batch[*gateway.Call[int, int]] {
					p.query()
					for _, c := range bt.Items {
						c.Reply(c.In)
					}
					return sluice.Batch[*gateway.Call[int, int]]{}
				})
			served(func(sluice.Batch[*gateway.Call[int, int]]) bool { return true })
		})
	defer func() { _ = gw.Close() }()

	ctx := context.Background()
	b.SetParallelism(clients)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			if _, err := gw.Do(ctx, n); err != nil {
				b.Error(err)
				return
			}
			n++
		}
		sink = n
	})
}
