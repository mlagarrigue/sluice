package parallel

import (
	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// Async decouples the stream from its consumer: s is consumed on its own
// goroutine, up to depth batches ahead of what the consumer has taken.
//
//	rows := Async(readCSV(path), 4)   // reading overlaps what follows
//
// It provides pipeline parallelism where [Ordered] provides data parallelism.
// In a synchronous pull chain source → transform → sink, each batch is read,
// then transformed, then written, strictly in sequence: the source idles while
// the sink works and the sink idles while the source reads. For CPU-shaped
// stages that is fine; for the I/O-shaped stages at both ends of an ETL
// pipeline the waits add end to end. An Async on each side of the expensive
// middle lets the three proceed at once, and the wall clock tends toward the
// slowest stage instead of the sum.
//
// The batch is what makes the operator affordable: the hand-off and the S14
// copy together measured +0.31 ns/element at 1024-element batches — inside
// the per-stage budget, where the same operator element-wise would be two
// orders of magnitude out (the arithmetic that disqualified iter.Pull per
// element at 68.6 ns). On equal producer and consumer work the overlap
// measured ×1.85 against the serial chain, of an ideal ×2
// (BenchmarkAsyncOverlap).
//
// # Bounded, and blocking is the semantics
//
// depth is required, per S1: it is the number of batches the operator may run
// ahead, and its memory bound — depth+2 recycled buffers, two more than the
// slack because one may be in each party's hands. There is deliberately no
// [sluice.Overflow] policy: dropping data in transport is never the right default,
// and blocking is not a failure mode here — when the buffer is full the
// producer waits, which is exactly the back-pressure the model promises. The
// consumer still sets the pace, with depth batches of slack.
//
// A depth of zero or less is the explicit no-op: s is returned as it is, no
// goroutine, no channel, no copy. Async(s, 0) is the way to turn the
// decoupling off, not a slower way to keep it.
//
// # The batches are copied
//
// The producer retains each upstream batch beyond the yield that produced it,
// so it copies Items into a recycled buffer at the hand-off — guarantee S14,
// the same rule and the same memcpy cost as [Ordered]. Downstream, the
// batches Async emits reuse those buffers: valid only for the duration of the
// call, retaining one requires a copy. Order is preserved — single producer,
// FIFO hand-off. sluice.Empty batches cross too, keeping the upstream cadence.
//
// # Stopping and failure
//
// An early stop from the consumer, or a panic in the consumer, releases the
// producer goroutine and unwinds the source promptly before Async returns: no
// goroutine outlives the call (S6), and the source's deferred cleanup runs at
// once (S10). A panic in the source is ferried and re-raised on the consumer's
// goroutine after the producer has been shut down — one arrival, no race, at
// the cost of the native trace, exactly as [Ordered] documents. A panic is
// never swallowed: one the source raises after an early stop, while it
// unwinds, is re-raised all the same. Only a panic already unwinding through
// the consumer takes precedence.
func Async[T any](s sluice.Stream[T], depth int) sluice.Stream[T] {
	if depth <= 0 {
		return s
	}
	return func(yield func(sluice.Batch[T]) bool) {
		runAsync(s, depth, yield)
	}
}

// asyncSlot carries one copied batch from the producer to the consumer. Slots
// travel a fixed loop — free list, producer, data channel, consumer, free
// list — so the steady state allocates nothing.
type asyncSlot[T any] struct {
	items []T
}

func runAsync[T any](s sluice.Stream[T], depth int, yield func(sluice.Batch[T]) bool) {
	// data carries filled slots to the consumer; free returns spent ones. free
	// holds every slot the operator owns (depth in data, one per party), so a
	// send on it can never block — which is what lets the consumer recycle
	// without a select.
	data := make(chan *asyncSlot[T], depth)
	free := make(chan *asyncSlot[T], depth+2)
	for range depth + 2 {
		free <- &asyncSlot[T]{}
	}

	// stop is the wakeup path the producer needs and Ordered does not: the
	// producer can be parked on either channel when the consumer quits, and
	// closing stop is the only way to reach it there. Closed exactly once, by
	// the consumer's deferred exit below.
	stop := make(chan struct{})

	// Written by the producer before prodDone is closed, read by the consumer
	// after it has received from prodDone: the channel close orders the two.
	var panicked any
	var hadPanic bool

	prodDone := make(chan struct{})
	go func() {
		// LIFO: the recover runs first, then data closes — so hadPanic is set
		// before the close that lets the consumer look at it — then prodDone
		// releases the deferred wait below.
		defer close(prodDone)
		defer close(data)
		defer func() {
			if r := recover(); r != nil {
				panicked, hadPanic = r, true
			}
		}()
		s(func(b sluice.Batch[T]) bool {
			var sl *asyncSlot[T]
			select {
			case sl = <-free:
			case <-stop:
				return false
			}
			// The S14 copy, into the slot's recycled buffer.
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

	// Every path out — exhaustion, early stop, panic in yield — goes through
	// this: wake the producer wherever it is parked and wait for it, so no
	// goroutine outlives the call (S6). Then, on a return, re-raise a panic
	// the source died from, here on the consumer's goroutine: after an early
	// stop as much as after exhaustion, since a source can panic while it
	// unwinds from the refusal. Unwinding, the consumer's own panic wins.
	returned := false
	defer func() {
		close(stop)
		<-prodDone
		if returned && hadPanic {
			panic(panicked)
		}
	}()

	for sl := range data {
		if !yield(sluice.Batch[T]{Items: sl.items}) {
			break
		}
		free <- sl // never blocks: free has room for every slot that exists
	}
	returned = true
}
