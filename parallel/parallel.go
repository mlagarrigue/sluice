package parallel

import (
	"sync"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// Ordered applies f to each batch on up to workers goroutines, preserving
// order.
//
//	decoded := Ordered(rows, 4, func(b sluice.Batch[Row]) sluice.Batch[Entity] { ... })
//
// The batch is what makes this worth doing: coordination is paid once per ~1024
// elements rather than once per element, so f only has to be meaningfully more
// expensive than a channel send — decoding, hashing, compression — for the
// fan-out to pay. On a sluice.Map-sized f it will not.
//
// # Order is preserved
//
// Batches come out in the order they went in. §6.4 allows either that or
// unordered output, and this takes the ordered side: an operator inserted into
// an existing pipeline must not silently change what the pipeline means, and
// nothing about the call site would reveal that it had.
//
// The cost is a reordering buffer of at most workers batches — O(k), which is
// what keeps this in the bounded-state family rather than the unbounded one.
// A caller who genuinely does not need order — and whose work is skewed enough
// that waiting on the oldest batch idles the pool — reaches for
// [Unordered], whose name is its contract; §6.4's worry about two
// operators differing invisibly is answered by making the difference
// impossible to type by accident.
//
// # What the fan-out is worth, measured
//
// Ordering caps the speed-up, and by more than the word "parallel" suggests.
// The emitter waits for the oldest batch, so one slow worker holds up the queue
// however idle the others are: on work heavy enough to be worth distributing,
// four workers measured at 4.10 ms against 5.27 ms serial — a factor of 1.29,
// not 4. Independent workers with no ordering constraint would do better; that
// is the trade §6.4 describes, taken deliberately.
//
// Below that, the fan-out costs. On an f that only passes its batch through,
// four workers measured at 127 microseconds against 37 serial — 3.5 times
// slower. "Add Ordered to speed it up" is a claim to check against these
// numbers, not an assumption: measure the pipeline, and if f is sluice.Map-sized,
// leave it out.
//
// # f runs concurrently
//
// f is called from several goroutines at once, so it must be safe to do so: no
// shared state without synchronisation, no closure over a variable the caller
// also writes. Everything else in this package runs on one goroutine, so this
// is the operator where that assumption stops holding.
//
// The batch handed to f is valid for that call only, exactly as elsewhere. The
// batch f returns is retained until its turn to be emitted, so f must not
// return a buffer it reuses across calls — a reused buffer would be overwritten
// by the next batch while the reordering buffer still holds it. This is the one
// place in the package where returning a fresh slice is required rather than
// merely allowed.
//
// Breaking that rule is a data race — a reused buffer is written by one worker
// while another reads it — which the race detector reports when the tests run
// under -race, and which corrupts values silently in a build without it. The
// contract is checked by this project's suite, not enforced at runtime.
//
// # The input batch is copied
//
// Ordered retains each incoming batch beyond the call that delivered it: a
// worker is still reading it after the upstream has moved on. Since upstream
// operators legitimately reuse their output buffer ([sluice.Convert], [sluice.Filter],
// [sluice.Scan], [sluice.FlatMap], the joins), Ordered copies Items into a buffer owned by
// the job slot at the hand-off (guarantee S14). The copy is what makes
// composing Ordered downstream of any operator safe; measured, it costs ~0.2
// ns/element on a pass-through f and disappears in the noise on the heavy f
// the operator exists for. The slot's buffer is reused, so the steady state
// allocates nothing — recycling the slots removed the ~2.2 allocations per
// batch the previous shape paid for its job, its channel and its queue.
//
// # Stopping and failure
//
// An early stop from the consumer, or a panic anywhere, releases every worker
// before returning: no goroutine outlives the call (guarantee S6). A panic in f
// or in the consumer propagates to the caller on the goroutine that ran
// Ordered, after the workers have been shut down, so it arrives once rather
// than as a race between several. A panic is never swallowed: when the
// consumer stops early while batches are still in flight, those batches are
// discarded but a panic among them is still re-raised — the oldest one, after
// the workers have finished. Only a panic already unwinding — in the consumer,
// in the source, or one re-raised from f — takes precedence over the ones
// behind it.
//
// Because f runs on another goroutine, its stack is not the caller's: a panic
// crossing back loses the native trace §6.4 warns about. Enrich the context in
// f rather than relying on the trace to say where it came from.
//
// # Degenerate arguments
//
// workers of 1 or less is an explicit no-op: f is applied inline on the calling
// goroutine, with no channel, no goroutine and no reordering buffer. §6.4
// requires this to be stated rather than silently concurrent, so that
// Ordered(s, 1, f) is a way to turn the concurrency off and not a slower way
// to keep it.
//
// Ordered panics if f is nil.
func Ordered[A, B any](s sluice.Stream[A], workers int, f func(sluice.Batch[A]) sluice.Batch[B]) sluice.Stream[B] {
	if f == nil {
		panic("sluice: Ordered requires a non-nil f function")
	}
	if workers <= 1 {
		// The explicit no-op §6.4 asks for: same shape, no concurrency.
		return func(yield func(sluice.Batch[B]) bool) {
			s(func(b sluice.Batch[A]) bool { return yield(f(b)) })
		}
	}
	return func(yield func(sluice.Batch[B]) bool) {
		fs := make([]func(sluice.Batch[A]) sluice.Batch[B], workers)
		for i := range fs {
			fs[i] = f
		}
		runParallel(s, fs, yield)
	}
}

// job carries one batch through the workers, keeping its position so the output
// can be put back in order. Jobs are slots, allocated once per Ordered call
// and recycled: in.Items is the slot's owned copy of the input (S14), regrown
// in place, and done is reused rather than reallocated.
type job[A, B any] struct {
	in  sluice.Batch[A]
	out sluice.Batch[B]

	// done receives one token from the worker that finished this job. The
	// emitter waits on it rather than on a shared condition, so waiting for
	// batch n does not wake on the completion of batch n+1. Buffered to 1 so
	// the worker never blocks on it, and signalled by send rather than close
	// so the same channel serves the slot's whole life.
	done chan struct{}

	// panicked holds what f panicked with, if it did, so the emitter can
	// re-panic on the caller's goroutine rather than crashing a worker.
	panicked any
	hadPanic bool
}

// runParallel is the concurrent path, split out so the no-op above stays free
// of it.
//
// The shape is a fixed pool reading one channel, and a queue of in-flight jobs
// the emitter drains in order. The queue is bounded by workers, which is what
// keeps memory O(k) and satisfies S11: the reader cannot run ahead of the
// emitter by more than the pool can hold.
//
// It takes one function per worker rather than one for the pool: [Ordered]
// passes the same f len(fs) times, [WithState] passes one closure per
// worker, each owning that worker's private state. The machinery is identical
// either way, so the ordering, shutdown and panic contracts are written — and
// tested — once.
func runParallel[A, B any](s sluice.Stream[A], fs []func(sluice.Batch[A]) sluice.Batch[B], yield func(sluice.Batch[B]) bool) {
	workers := len(fs)
	// Buffered to workers, which is also the cap on inFlight: the producer
	// below never queues a job without first making room, so this send cannot
	// block for longer than a worker takes to pick the previous one up. There
	// is deliberately no separate cancellation channel — closing jobs in the
	// deferred shutdown is the only signal the workers need, and a second one
	// would suggest a wakeup path that does not exist.
	jobs := make(chan *job[A, B], workers)

	var wg sync.WaitGroup
	for _, f := range fs {
		wg.Go(func() {
			for j := range jobs {
				runJob(j, f)
			}
		})
	}

	// slots is the pool of jobs, allocated once: at most workers batches are in
	// flight, and they complete in the order they were enqueued, so a ring of
	// workers slots is exactly enough. slots[head] is the oldest in flight,
	// count how many are; the slot freed by an emit is the one the next enqueue
	// reuses. Recycling the slot is what removes the per-batch job and channel
	// allocations the previous shape paid.
	slots := make([]job[A, B], workers)
	for i := range slots {
		slots[i].done = make(chan struct{}, 1)
	}
	var head, count int

	// Every path out of here — normal end, early stop, panic in f, panic in the
	// consumer — goes through this. Closing jobs ends the workers' range loops;
	// waiting for them means no worker is still inside f when Ordered returns,
	// which is what S6 asks for and what a bare goroutine count cannot show.
	//
	// On a return — not an unwind — the in-flight jobs the consumer will never
	// see are then checked for a panic: an early stop discards their results,
	// not their failures. wg.Wait orders every worker's writes before the
	// reads. Unwinding, the panic already in progress wins.
	returned := false
	defer func() {
		close(jobs)
		wg.Wait()
		if !returned {
			return
		}
		for i := range count {
			if j := &slots[(head+i)%workers]; j.hadPanic {
				panic(j.panicked)
			}
		}
	}()

	// emit waits for the oldest job and yields it. It reports whether the
	// pipeline should keep going.
	emit := func() bool {
		j := &slots[head]
		head = (head + 1) % workers
		count--
		<-j.done
		if j.hadPanic {
			// Re-panicked on this goroutine, after the deferred shutdown above
			// has released the workers.
			panic(j.panicked)
		}
		return yield(j.out)
	}

	stopped := false
	s(func(b sluice.Batch[A]) bool {
		// The queue is full, so make room before adding: this is the
		// back-pressure. The source cannot outrun the pool.
		if count == workers {
			if !emit() {
				stopped = true
				return false
			}
		}
		// Order comes from the position in the ring, not from a sequence
		// number: the ring is drained oldest-first, so the two would say the
		// same thing and one of them could drift.
		j := &slots[(head+count)%workers]
		// The copy S14 requires: j outlives this call, and b.Items may be a
		// buffer the upstream rewrites for its next batch. The slot's buffer
		// absorbs the copy, so nothing is allocated once it has grown.
		j.in.Items = batch.Grow(j.in.Items, len(b.Items))
		copy(j.in.Items, b.Items)
		count++
		jobs <- j
		return true
	})

	if !stopped {
		// Drain what is still in flight. A refusal here is the consumer
		// stopping on the tail: the rest is discarded like any early stop.
		for count > 0 {
			if !emit() {
				break
			}
		}
	}
	// Reached only by returning, never by unwinding: an emit whose job
	// panicked, or whose yield did, leaves returned false.
	returned = true
}

// runJob applies f and signals completion, turning a panic in f into a value
// the emitter re-panics with.
//
// Recovering here is not swallowing the panic: it moves it to the goroutine
// that can do something with it. Left alone it would take the whole process
// down from a worker stack, with no way for the caller to see it and no chance
// for the other workers to be released first.
func runJob[A, B any](j *job[A, B], f func(sluice.Batch[A]) sluice.Batch[B]) {
	// Declared first so it runs after the recover below. The channel has room
	// for one token and the emitter takes exactly one per job, so this send
	// never blocks — including for jobs still in flight when the consumer
	// stops early, whose tokens the shutdown never collects: it waits on the
	// workers instead.
	defer func() { j.done <- struct{}{} }()
	defer func() {
		if r := recover(); r != nil {
			j.panicked, j.hadPanic = r, true
		}
	}()
	j.hadPanic = false
	j.out = f(j.in)
}
