package parallel

import (
	"sync"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// Unordered applies f to each batch on up to workers goroutines and
// emits results as they complete. Output order is not the input order — the
// name is the contract, and a caller cannot type it without reading it.
//
// It exists for the workload [Ordered] measurably cannot serve: ordered
// emission waits for the oldest batch, so one slow batch idles the other
// workers however ready their results are. When f's cost varies batch to
// batch — mixed record sizes, cache-hit against cache-miss lookups, skewed
// keys — the pool runs at the pace of its stragglers. Dropping the ordering
// constraint lets every worker's result leave as soon as it exists.
//
// Measured on work whose cost varies 8× between batches, four workers
// (BenchmarkParallelSkewed and its pairs): serial 7.67 ms, [Ordered] 7.45 ms
// — ×1.03, the fan-out nearly cancelled by its own ordering — against 2.79 ms
// here, ×2.75. Uniform work gains too, ×1.59 (1.07 ms against 1.70): ordered
// emission turns even scheduling jitter into window stalls, where this
// operator carries twice the in-flight slack and lets nothing wait. [Ordered]
// stays the default all the same: an operator inserted into an existing
// pipeline must not silently change what the pipeline means, and these
// figures are what choosing this one knowingly buys.
//
// Ordering is a sink concern where it is one: re-establish sequence at the
// boundary that needs it — a keyed frontier, a sort — rather than pay for it
// at every transport step that does not.
//
// # Contracts shared with Ordered
//
// The input batch is copied at the hand-off (S14), so composing downstream of
// a buffer-reusing operator is safe. f runs concurrently and must be safe to;
// the batch handed to f is valid for that call only; the batch f returns is
// retained until emitted, so f must not return a buffer it reuses across
// calls. Memory is bounded by O(workers) batches (S11). An early stop or a
// panic anywhere releases every worker before the call returns (S6); a panic
// in f is re-raised on the consuming goroutine after the workers have been
// shut down. A panic is never swallowed: under an early stop the results
// still in flight are discarded, but a panic among them is re-raised all the
// same — exactly as [Ordered] behaves. A panic already unwinding through the
// consumer takes precedence, and of several panics in f the first to arrive
// is the one raised.
//
// # Degenerate arguments
//
// workers of 1 or less is the explicit no-op: f applied inline, in order —
// with one worker there is no reordering to avoid, so the two operators agree.
//
// Unordered panics if f is nil.
func Unordered[A, B any](s sluice.Stream[A], workers int, f func(sluice.Batch[A]) sluice.Batch[B]) sluice.Stream[B] {
	if f == nil {
		panic("sluice: Unordered requires a non-nil f function")
	}
	if workers <= 1 {
		return func(yield func(sluice.Batch[B]) bool) {
			s(func(b sluice.Batch[A]) bool { return yield(f(b)) })
		}
	}
	return func(yield func(sluice.Batch[B]) bool) {
		runParallelUnordered(s, workers, f, yield)
	}
}

// unorderedResult carries one completed batch, or the panic that stopped it.
type unorderedResult[B any] struct {
	out      sluice.Batch[B]
	panicked any
	hadPanic bool
}

// runParallelUnordered is the concurrent path.
//
// Input slots cycle producer → jobs → worker → free list, copied into per S14
// and recycled; results flow workers → the consuming goroutine through a
// channel sized so a worker can never block on it. Emission happens only on
// the consuming goroutine — the iterator contract — which drains ready
// results before feeding each new batch, then drains the tail once the source
// is done.
func runParallelUnordered[A, B any](s sluice.Stream[A], workers int, f func(sluice.Batch[A]) sluice.Batch[B], yield func(sluice.Batch[B]) bool) {
	// At most workers jobs queued and workers in hand: 2×workers inputs
	// outstanding, and 2×workers+1 slots mean the producer always gets one
	// back eventually. results holds one more than that, 2×workers+1: a
	// worker frees its slot before sending its result, so while the
	// consumer is blocked feeding jobs, a job taken from the queue to make
	// room can finish too — the one past 2×workers that would otherwise
	// block a worker.
	slots := make([]sluice.Batch[A], 2*workers+1)
	free := make(chan *sluice.Batch[A], 2*workers+1)
	for i := range slots {
		free <- &slots[i]
	}
	jobs := make(chan *sluice.Batch[A], workers)
	results := make(chan unorderedResult[B], 2*workers+1)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for j := range jobs {
				runUnorderedJob(j, f, free, results)
			}
		})
	}

	var ferried unorderedResult[B]
	stopped := false

	// finish closes jobs exactly once, then drains results until the workers
	// are done — yielding while the consumer still accepts, discarding after —
	// so a worker parked on a full results channel is always released. Every
	// exit runs it: exhaustion, early stop, and the deferred call below that
	// covers a panic in yield. That drain-while-waiting is the shape a bare
	// close-and-wait cannot provide: with the consumer gone, only this loop
	// stands between the workers and a channel nobody empties (S6).
	// finish is re-entrant on purpose: a panic in yield can abort its drain
	// loop mid-way, and the deferred call below then resumes it — the range
	// picks up where the aborted one stopped, now discarding — so the workers
	// are released on that path too.
	finished := false
	finish := func() {
		if !finished {
			finished = true
			close(jobs)
			go func() { wg.Wait(); close(results) }()
		}
		for r := range results {
			switch {
			case r.hadPanic && !ferried.hadPanic:
				// Kept to re-raise after the drain, early stop or not; a
				// second panic in flight is dropped — one arrival, like
				// Ordered.
				ferried, stopped = r, true
			case stopped:
				// Early stop: the tail is discarded, but the draining itself
				// is what lets the workers finish and the wait above return.
			case !yield(r.out):
				stopped = true
			}
		}
	}
	completed := false
	defer func() {
		if !completed {
			// A panic is unwinding through here — in yield, or ferried and
			// re-raised below. The workers still need releasing, and yield
			// must not be called again on the way out.
			stopped = true
			finish()
		}
	}()

	s(func(b sluice.Batch[A]) bool {
		// Emit what is ready before feeding: results leave on the consuming
		// goroutine — the iterator contract — at the upstream's rhythm, with
		// the tail collected by finish.
		for {
			select {
			case r := <-results:
				if r.hadPanic {
					ferried, stopped = r, true
					return false
				}
				if !yield(r.out) {
					stopped = true
					return false
				}
				continue
			default:
			}
			break
		}
		j := <-free // a worker frees one after f; never starves while work flows
		j.Items = batch.Grow(j.Items, len(b.Items))
		copy(j.Items, b.Items)
		jobs <- j
		return true
	})

	finish()
	completed = true
	if ferried.hadPanic {
		panic(ferried.panicked)
	}
}

// runUnorderedJob applies f, frees the input slot, and ships the result — or
// the panic, moved to the goroutine that can re-raise it.
func runUnorderedJob[A, B any](j *sluice.Batch[A], f func(sluice.Batch[A]) sluice.Batch[B], free chan<- *sluice.Batch[A], results chan<- unorderedResult[B]) {
	defer func() {
		if r := recover(); r != nil {
			free <- j
			results <- unorderedResult[B]{panicked: r, hadPanic: true}
		}
	}()
	out := f(*j)
	// Freed after f returns: the batch was valid for that call only, and the
	// producer may refill it the moment it leaves here.
	free <- j
	results <- unorderedResult[B]{out: out}
}
