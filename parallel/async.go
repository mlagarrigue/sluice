package parallel

import (
	"strconv"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/handoff"
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
// The batch is what makes the operator affordable: the hand-off and the copy
// of each batch (a batch is not kept past its call, S14 of the
// architecture's security properties) together measured +0.31 ns/element at 1024-element batches — inside
// the per-stage budget, where the same operator element-wise would be two
// orders of magnitude out (the arithmetic that disqualified iter.Pull per
// element at 53 ns). On equal producer and consumer work the overlap
// measured ×1.85 against the serial chain, of an ideal ×2
// (BenchmarkAsyncOverlap).
//
// # Bounded, and blocking is the semantics
//
// depth is required, since memory is bounded by the caller (S1): it is the
// number of batches the operator may run
// ahead, and its memory bound — depth+2 recycled buffers, two more than the
// slack because one may be in each party's hands. There is deliberately no
// [sluice.Overflow] policy: dropping data in transport is never the right default,
// and blocking is not a failure mode here — when the buffer is full the
// producer waits, which is exactly the back-pressure the model promises. The
// consumer still sets the pace, with depth batches of slack.
//
// A depth of zero is the explicit no-op: s is returned as it is, no
// goroutine, no channel, no copy. Async(s, 0) is the way to turn the
// decoupling off, not a slower way to keep it. A negative depth panics.
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
	if depth < 0 {
		panic("sluice: Async depth " + strconv.Itoa(depth) + " is negative — zero means no decoupling")
	}
	if depth == 0 {
		return s
	}
	return func(yield func(sluice.Batch[T]) bool) {
		runAsync(s, depth, yield)
	}
}

// runAsync is the decoupled path. Slots travel a fixed loop — free list,
// producer, data channel, consumer, free list — so the steady state
// allocates nothing.
func runAsync[T any](s sluice.Stream[T], depth int, yield func(sluice.Batch[T]) bool) {
	// depth slots may wait in the channel, and one more is held by each
	// party — the producer filling, the consumer yielding — so the producer
	// is never short of one while the consumer works.
	p := handoff.Start(s, depth, depth+2, false)

	// Every path out — exhaustion, early stop, panic in yield — goes through
	// Stop: no goroutine outlives the call (S6), and on a return the panic the
	// source died from is raised here.
	returned := false
	defer func() { p.Stop(returned) }()

	for sl := range p.Data {
		if !yield(sluice.Batch[T]{Items: sl.Items}) {
			break
		}
		p.Recycle(sl)
	}
	returned = true
}
