// Package handoff runs a stream's source on a goroutine of its own and hands
// its batches to the consumer through a fixed set of recycled slots: the
// producer that [parallel.Async] and [window.CoalesceWithin] share.
//
// One implementation, because the order of the producer's exits is the subtle
// part and was written out twice: the panic is recorded before the data channel
// closes, so a consumer that sees the close can look at it; the data channel
// closes before done, which releases Stop. Every path out of the consumer goes
// through Stop, so no goroutine outlives the operator (the architecture's S6),
// and a panic the source died from is raised on the consumer's goroutine, with
// the producer's stack ([Panic]).
//
// [parallel.Async]: https://pkg.go.dev/github.com/mlagarrigue/sluice/parallel#Async
// [window.CoalesceWithin]: https://pkg.go.dev/github.com/mlagarrigue/sluice/window#CoalesceWithin
package handoff

import (
	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// Slot is one recycled buffer. Items is valid until the slot is recycled.
type Slot[T any] struct {
	Items []T
}

// Producer is the source running on its goroutine.
type Producer[T any] struct {
	// Data carries filled slots, in the source's order, and is closed when
	// the source returns.
	Data <-chan *Slot[T]

	free chan *Slot[T]
	// stop is the wakeup the producer needs when the consumer quits: it may
	// be parked on either channel, and closing stop is the only way to reach
	// it there. Closed exactly once, by Stop.
	stop chan struct{}
	done chan struct{}
	// Written by the producer before done closes, read after: the close
	// orders the two.
	panicked Panic
}

// Start runs s on a new goroutine. depth is how many filled slots may wait
// for the consumer; slots is how many exist in all, at least depth, so that
// Recycle never blocks. The source's batches are copied into the slots (the
// S14 copy: a slot outlives the source's call); with skipEmpty, a batch of
// no element is not handed over at all.
func Start[T any](s sluice.Stream[T], depth, slots int, skipEmpty bool) *Producer[T] {
	data := make(chan *Slot[T], depth)
	p := &Producer[T]{
		Data: data,
		free: make(chan *Slot[T], slots),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	for range slots {
		p.free <- &Slot[T]{}
	}
	// The channels as locals: the callback below runs once per batch, and
	// read through p there they put BenchmarkAsyncPassThrough 0.8% over the
	// copy each operator had (1 win of 11); as locals, noise (6 of 11).
	free, stop := p.free, p.stop
	go func() {
		// LIFO: the recover runs first, then data closes, then done.
		defer close(p.done)
		defer close(data)
		defer func() { p.panicked = Recovered(recover()) }()
		s(func(b sluice.Batch[T]) bool {
			if skipEmpty && len(b.Items) == 0 {
				return true
			}
			var sl *Slot[T]
			select {
			case sl = <-free:
			case <-stop:
				return false
			}
			sl.Items = batch.Grow(sl.Items, len(b.Items))
			copy(sl.Items, b.Items)
			select {
			case data <- sl:
				return true
			case <-stop:
				return false
			}
		})
	}()
	return p
}

// Recycle hands a drained slot back to the producer. It never blocks: there
// is room for every slot that exists.
func (p *Producer[T]) Recycle(sl *Slot[T]) { p.free <- sl }

// Stop wakes the producer wherever it is parked and waits for it. Then, when
// the consumer returned rather than unwound (returned), it raises the panic
// the source died from, after an early stop as much as after exhaustion: a
// source can panic while it unwinds from the refusal. Unwinding, the
// consumer's own panic wins. Deferred by the consumer, on every path out.
func (p *Producer[T]) Stop(returned bool) {
	close(p.stop)
	<-p.done
	if returned {
		p.panicked.Raise()
	}
}
