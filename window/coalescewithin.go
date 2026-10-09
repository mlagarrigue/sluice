package window

import (
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// slot carries one copied batch from the producer goroutine to the consumer.
// Slots travel a fixed loop — free list, producer, data channel, consumer,
// free list — so the steady state allocates nothing.
type slot[T any] struct {
	items []T
}

// CoalesceWithin regroups batches to size elements like [sluice.Coalesce], but never
// holds an element longer than within: a partial batch is emitted when the
// deadline expires, counted from the arrival of its first element.
//
//	requests := CoalesceWithin(incoming, 64, 2*time.Millisecond)
//
// It is the operator for a pipeline that must be efficient under load and
// responsive when idle. [sluice.Coalesce] alone waits for size elements however long
// that takes, which is right for a file and wrong for a socket: at ten
// requests per second a batch of 1024 would be filled a hundred seconds after
// its first element arrived, and that first request would wait all of it. The
// deadline puts a ceiling on that wait, and the ceiling is the caller's, not a
// default — this is Kafka's linger, DataFusion's coalesce-with-timeout, and
// the same bargain both make: latency is bounded, and throughput takes
// whatever batching the arrival rate allows.
//
// # It costs a goroutine, and that is not incidental
//
// A deadline must be able to expire while nothing is arriving — that is the
// case it exists for. In a synchronous pull pipeline the operator would be
// inside the upstream's own loop, waiting for a batch that may never come,
// with no way to also wait on a clock. So the upstream is consumed on its own
// goroutine and handed over through a bounded channel, and the deadline is
// waited on beside that channel. [sluice.Coalesce] costs nothing and cannot do this;
// this operator can and costs a goroutine. Use [sluice.Coalesce] when there is no
// clock to answer to.
//
// The clock is the wall clock — processing time, not the event time of
// [Tumbling] or [IntervalJoin]. The question it answers is "how long has this
// element been waiting in memory", which has nothing to do with when the
// event it describes happened.
//
// # Contracts
//
// Memory is O(size) for the accumulation buffer, claimed on the first element,
// plus two upstream batches in transport. Elements are copied into the buffer as they arrive, so nothing of
// the upstream's is retained (S14) and an upstream that reuses its output
// buffer composes safely.
//
// Output batches reuse the accumulation buffer: valid for the duration of the
// call, retaining requires a copy. Order is preserved. An empty upstream batch
// carries no element and so does not arm the deadline — [sluice.Filter] emitting them
// to hold the cadence cannot make this operator emit an empty batch.
//
// An early stop from the consumer releases the upstream goroutine and unwinds
// the source promptly (S6, S10), discarding what was accumulated — the
// consumer asked to stop, exactly as [sluice.Coalesce] does. The end of the upstream
// flushes the partial batch. A panic in the source is ferried and re-raised on
// the consumer's goroutine after the producer has been shut down. A panic is
// never swallowed: one the source raises after an early stop, or after the
// consumer refused the final partial batch, is re-raised all the same. Only a
// panic already unwinding through the consumer takes precedence.
//
// # Cost, and the shape that makes it expensive
//
// The hand-off is paid **per upstream batch, not per element**, so the
// per-element figure is whatever that division gives: 0.79 ns/element on
// 1024-element upstream batches against [sluice.Coalesce]'s 0.21 — one channel
// round trip and one copy amortized over a thousand elements — but **35
// ns/element on 8-element upstream batches**, the same absolute cost divided
// by a hundred and twenty-eight times fewer elements.
//
// Read that as a rule rather than a table: this operator is cheap when
// batches arrive full and expensive when they arrive fragmented. The two
// cases it is built for are both fine — a saturated pipeline has full batches,
// and an idle one has so few elements that per-element cost is meaningless
// next to the deadline being waited on. The case to avoid is a dense stream
// arriving in tiny batches, which is precisely where [sluice.Coalesce] belongs since
// nothing there is waiting on a clock.
//
// If that case ever has to be served, the known alternative is a mutex-guarded
// accumulator the producer appends into and the deadline steals from — a lock
// per upstream batch instead of a channel round trip, which is roughly an
// order of magnitude cheaper. It is not built, because it puts shared mutable
// state where this design has none, and no measurement has yet asked for it.
//
// CoalesceWithin panics if within is zero or negative: an operator that costs
// a goroutine to watch a clock must not be given a clock it cannot watch. A
// size of zero or less means [sluice.DefaultBatchSize], as in [sluice.Coalesce].
func CoalesceWithin[T any](s sluice.Stream[T], size int, within time.Duration) sluice.Stream[T] {
	if within <= 0 {
		panic("sluice: CoalesceWithin requires a positive within — use Coalesce when there is no deadline")
	}
	if size <= 0 {
		size = sluice.DefaultBatchSize
	}
	return func(yield func(sluice.Batch[T]) bool) {
		runCoalesceWithin(s, size, within, yield)
	}
}

func runCoalesceWithin[T any](s sluice.Stream[T], size int, within time.Duration, yield func(sluice.Batch[T]) bool) {
	// Two slots: one being filled upstream, one in transport. The consumer
	// drains a slot into its own buffer and returns it at once, so the
	// hand-off is short and a deeper queue would only add latency to an
	// operator whose whole purpose is to bound it.
	const depth = 2
	data := make(chan *slot[T], depth)
	free := make(chan *slot[T], depth)
	for range depth {
		free <- &slot[T]{}
	}

	// Closed exactly once, by the consumer's deferred exit below.
	stop := make(chan struct{})

	var panicked any
	var hadPanic bool
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		defer close(data)
		defer func() {
			if r := recover(); r != nil {
				panicked, hadPanic = r, true
			}
		}()
		s(func(b sluice.Batch[T]) bool {
			if len(b.Items) == 0 {
				return true // no element, nothing to hand over or to time
			}
			var sl *slot[T]
			select {
			case sl = <-free:
			case <-stop:
				return false
			}
			// The S14 copy: the slot outlives this call.
			sl.items = batch.Grow(sl.items, len(b.Items))
			copy(sl.items, b.Items)
			select {
			case data <- sl:
				return true
			case <-stop:
				return false
			}
		})
	}()

	// Wakes the producer wherever it is parked and waits for it, on every path
	// out — normal end, early stop, a panic in yield. Then, on a return,
	// re-raises a panic the source died from, early stop or not: the source
	// may panic while it unwinds from the refusal. Unwinding, the consumer's
	// own panic wins. hadPanic is written before prodDone closes.
	returned := false
	defer func() {
		close(stop)
		<-prodDone
		if returned && hadPanic {
			panic(panicked)
		}
	}()

	// Created stopped: the deadline is armed when the first element of a batch
	// arrives and disarmed on every flush. Since Go 1.23 a stopped or reset
	// timer cannot deliver a stale value, so no drain is needed here.
	timer := time.NewTimer(within)
	timer.Stop()
	defer timer.Stop()

	// Claimed on the first element, as in [sluice.Coalesce]: size is the caller's
	// number, and an idle or empty stream must not pay for it up front.
	var buf []T
	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		timer.Stop()
		ok := yield(sluice.Batch[T]{Items: buf})
		buf = buf[:0]
		return ok
	}

	for {
		select {
		case sl, ok := <-data:
			if !ok {
				// The upstream is exhausted: flush what is held; the deferred
				// shutdown then reports a panic it may have died from, whether
				// or not the consumer accepted that final batch.
				_ = flush()
				returned = true
				return
			}
			if buf == nil {
				buf = make([]T, 0, size)
			}
			if len(buf) == 0 {
				// The deadline runs from the first element of a batch, not
				// from the last: what is bounded is how long an element
				// waits, and the oldest one is the one that waits longest.
				timer.Reset(within)
			}
			// The slot is held until it has been fully drained, not returned
			// on receipt: items aliases sl.items, so releasing it early lets
			// the producer refill the very buffer still being copied out —
			// which is the S14 rule read from the receiving end, and what the
			// sluice.Convert-upstream race test catches when it is broken. The
			// producer stalling on the free list while a slow consumer works
			// is back-pressure, and correct.
			items := sl.items
			for len(items) > 0 {
				n := min(size-len(buf), len(items))
				buf = append(buf, items[:n]...)
				items = items[n:]
				if len(buf) == size {
					if !flush() {
						free <- sl
						returned = true
						return
					}
					if len(items) > 0 {
						// What remains starts a new batch, and its deadline
						// starts now.
						timer.Reset(within)
					}
				}
			}
			free <- sl

		case <-timer.C:
			if !flush() {
				returned = true
				return
			}
		}
	}
}
