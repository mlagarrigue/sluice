// Package gateway turns concurrent request/reply calls into batched stream
// processing: many goroutines call, one pipeline serves them a batch at a
// time.
//
// It exists for one reason, and the reason is not CPU. A handler that answers
// one request per database round trip spends its life waiting on the network;
// sixty-four handlers answering the same shape of question could have waited
// once. The gateway is what lets them: calls accumulate for a bounded moment,
// the pipeline sees them as a batch, and one `= ANY($1)` answers all of them.
// Against ~500 µs of round trip, the machinery below is noise — which is the
// whole bargain, and the reason the library's per-element nanoseconds are
// the wrong figure to judge the web vertical by (measured, see docs/benchmarks.md).
//
//	gw := gateway.New(gateway.Config{Size: 64, Within: 2 * time.Millisecond},
//	    func(calls sluice.Stream[*gateway.Call[ID, Order]]) {
//	        calls(func(b sluice.Batch[*gateway.Call[ID, Order]]) bool {
//	            ids := make([]ID, b.Len())
//	            for i, c := range b.Items {
//	                ids[i] = c.In
//	            }
//	            orders, err := db.LoadAll(ids) // one round trip for the batch
//	            for i, c := range b.Items {
//	                if err != nil {
//	                    c.Fail(err)
//	                    continue
//	                }
//	                c.Reply(orders[i])
//	            }
//	            return true
//	        })
//	    })
//	defer gw.Close()
//
//	order, err := gw.Do(ctx, id) // from any goroutine, as many as you like
//
// # The element is the correlation
//
// A batched request/reply gateway has one hard problem: a reply must find its
// way back to the goroutine that asked. Carrying a correlation key alongside
// the data would make every stage responsible for preserving it, and a stage
// that regroups or reorders would silently strand a caller. So the element
// handed to the pipeline is the call itself — [Call] carries the input and
// the private channel its answer goes back through. Nothing to preserve,
// nothing to match up: replying to the element replies to the caller.
//
// The consequence is the rule to design pipelines by: **do not drop a call**.
// A [sluice.Filter] that removes an element removes a caller's only chance of
// an answer, and that caller then waits for its context to expire. This is
// what the library's diagnostics model is for — an element travels with its
// problems attached and keeps moving — so reject by replying with a
// rejection, never by filtering.
//
// The gateway deliberately does not check, batch by batch, that every call
// came back answered. Returning from a yield means the batch was accepted
// downstream, not that it was handled: [parallel.Async] and
// [parallel.Unordered] return as soon as a worker has taken it, with
// the replies still to come. A check there would fail calls the pipeline is
// about to answer, and the real answer arriving afterwards would panic as a
// second one — so composing a fan-out inside the pipeline, which is exactly
// what a batched gateway wants, would be broken by the convenience.
//
// # Failure is not left hanging
//
// What is guaranteed instead is that no caller waits on a pipeline that is
// gone. If the pipeline panics, or returns while calls are outstanding, every
// caller is released at once with that cause — [ErrPipelinePanicked], or
// [ErrNoReply] — and later calls are refused with the same. The cost of
// dropping a call is therefore bounded by the caller's context deadline while
// the pipeline lives, and by nothing at all once it does not.
//
// # A client that pipelines gets the worst of this
//
// The batch is formed from calls that coincide, and over `net/http` two
// requests pipelined on one connection never do: the standard server reads a
// connection's requests strictly in sequence, so the second is not read until
// the first has been answered. Each call therefore reaches Do alone, waits out
// the whole of Within with nobody to join, and pays that latency for a batch
// of one.
//
// Measured (TestTransportBatchingMatrix, with a 500 µs backend round trip and
// Within at 2 ms): 32 requests pipelined on one connection cost **32 round
// trips and 108 ms**, against 2 trips and 27 ms for the same 32 requests on
// separate connections. The gateway is not at fault and neither is net/http —
// they simply cannot see the same thing at the same time. What removes it is
// a transport that hands over what arrived together, which is what
// [httpstream] exists to measure — and [Gateway.DoBatch] is how such a
// transport submits its batch in one motion, joining the same window every
// other caller shares.
//
// So: a gateway suits many callers, not one caller in a hurry. If the clients
// pipeline, the wait is pure cost and Within should be small or the gateway
// left out.
//
// # Fan out inside the pipeline, not around the gateway
//
// One gateway drives one pipeline on one goroutine, so batches are served one
// after another. That is a ceiling, and lifting it is a composition rather
// than a setting: put [parallel.Unordered] inside the pipeline and
// several batches are in flight at once. Unordered rather than ordered,
// because nothing downstream of a request/reply gateway cares which batch
// finishes first — every call carries its own way back.
package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
)

// ErrClosed reports a call submitted to a gateway that is closed or closing.
var ErrClosed = errors.New("gateway: closed")

// ErrNoReply reports a call the pipeline consumed without answering — it was
// filtered out, or the pipeline stopped mid-batch. It is a wiring mistake in
// the pipeline, surfaced rather than left to the caller's deadline when the
// gateway can tell.
var ErrNoReply = errors.New("gateway: the pipeline consumed the call without replying")

// ErrPipelinePanicked reports a pipeline that panicked. Every caller the
// gateway releases gets this same error, and callers are not the same tenant
// — so the panic value, which can quote a fragment of whatever element was
// being processed, is deliberately not in it. A handler that relays
// err.Error() to its client must not be a channel between tenants. The
// pipeline owns its panics: recover where the data is still yours if the
// value matters to a log.
var ErrPipelinePanicked = errors.New("gateway: the pipeline panicked")

// Config states how calls are grouped. Both bounds are the caller's: there is
// no batch size that suits every workload and no latency ceiling that suits
// every client.
type Config struct {
	// Size is the largest number of calls handed to the pipeline at once. It
	// is required and positive: there is no batch size that suits every
	// request path, and the library's default of a thousand is far too large
	// for one.
	Size int

	// Within is how long the first call of a batch may wait for company. It
	// is required and positive: a gateway with no ceiling would hold the only
	// call of a quiet second until a batch it will never fill.
	//
	// It bounds the gathering, not the queueing. The window starts when the
	// run loop takes the first call of a batch, and the loop takes nothing
	// while it is inside the pipeline: with a pipeline that serves a batch
	// before returning, a call arriving during a long batch waits out the
	// rest of that batch first, then Within. So the latency the gateway adds
	// is Within on top of whatever the batch ahead still takes — a fan-out
	// inside the pipeline ([parallel.Unordered]) is what returns the
	// loop to gathering quickly. Pick Within against the round trip it
	// saves: 2 ms of waiting to turn 64 round trips into one is a bargain,
	// 200 ms is not.
	Within time.Duration
}

// Call is one request travelling through the pipeline, and the way back to
// the goroutine waiting for it.
//
// Exactly one of [Call.Reply] or [Call.Fail] answers a call; both are safe to
// use from any goroutine, and answering twice panics, since a second answer
// means the pipeline believes two things about one request.
type Call[In, Out any] struct {
	// In is what the caller submitted.
	In In

	reply    chan result[Out]
	answered sync.Once
}

type result[Out any] struct {
	out Out
	err error
}

// Reply answers the call with a value.
func (c *Call[In, Out]) Reply(out Out) { c.answer(result[Out]{out: out}) }

// Fail answers the call with an error. A nil err is replaced by [ErrNoReply]
// rather than reported as success, since "failed with no reason" is a bug
// worth seeing and a silent success is not.
func (c *Call[In, Out]) Fail(err error) {
	if err == nil {
		err = ErrNoReply
	}
	c.answer(result[Out]{err: err})
}

func (c *Call[In, Out]) answer(r result[Out]) {
	first := false
	c.answered.Do(func() {
		first = true
		c.reply <- r // buffered to one: this never blocks
	})
	if !first {
		panic("gateway: call answered twice")
	}
}

// Gateway batches concurrent calls into one pipeline.
//
// The zero value is not usable; build one with [New] and release it with
// [Gateway.Close].
type Gateway[In, Out any] struct {
	in     chan *Call[In, Out]
	size   int
	within time.Duration

	// closing is closed by Close to stop the run loop. The call channel
	// itself is never closed: Do sends on it from arbitrary goroutines, and a
	// send racing a close would panic in the caller — a library killing its
	// user's goroutine for calling two of its own methods.
	closing   chan struct{}
	closeOnce sync.Once
	drained   chan struct{} // closed when the pipeline goroutine has returned

	// closed flips before closing or drained does, and is what Do consults on
	// its fast path. An atomic rather than a field under mu: the check is
	// fail-fast only — correctness is carried by the drained cases of Do's
	// selects — so it needs no lock, and taking one would put a point every
	// caller passes through in front of the channel send. fatal stays under
	// mu; it is only read once closed is observed.
	closed atomic.Bool

	mu    sync.Mutex
	fatal error // set when the pipeline died; every later call fails with it
}

// New starts a gateway serving calls through pipeline.
//
// pipeline is a terminal consumer: it receives the stream of batched calls and
// answers each one. It runs on its own goroutine, started here and released by
// [Gateway.Close], so a gateway that is never closed leaks that goroutine —
// treat it as the long-lived object it is, one per route or per service, built
// at startup rather than per request.
//
// New panics if pipeline is nil or if Within is not positive.
func New[In, Out any](cfg Config, pipeline func(sluice.Stream[*Call[In, Out]])) *Gateway[In, Out] {
	if pipeline == nil {
		panic("gateway: New requires a non-nil pipeline")
	}
	if cfg.Size <= 0 {
		panic("gateway: Config.Size must be positive — it is the largest batch the pipeline sees")
	}
	if cfg.Within <= 0 {
		panic("gateway: Config.Within must be positive — it is the latency ceiling calls are held under")
	}
	size := cfg.Size

	g := &Gateway[In, Out]{
		// Buffered to one batch: submission is the one place every caller
		// meets, and an unbuffered channel makes each one a rendezvous with
		// the single run loop — a goroutine switch per call, which measured
		// as the gateway's throughput ceiling well before the backend was.
		// The buffer lets callers hand over and leave, and the loop collect
		// in bulk. It does not weaken the latency ceiling: a call waiting in
		// the buffer is a call the loop has not started timing, and the loop
		// drains it on its next turn.
		in:      make(chan *Call[In, Out], size),
		size:    size,
		within:  cfg.Within,
		closing: make(chan struct{}),
		drained: make(chan struct{}),
	}

	go g.run(pipeline)
	return g
}

// run drives the pipeline and guarantees that every call it took in is
// answered, whatever the pipeline does — including panicking.
func (g *Gateway[In, Out]) run(pipeline func(sluice.Stream[*Call[In, Out]])) {
	defer close(g.drained)

	// Three ways this returns, and they are not the same news for a caller:
	// Close asked it to, the pipeline gave up on its own, or the pipeline
	// panicked. Only the first is ordinary.
	asked := false

	defer func() {
		var err error
		switch r := recover(); {
		case r != nil:
			// Not fmt.Errorf("...: %v", r): this error is handed to every
			// waiting and future caller, and the panic value can carry another
			// caller's data. See ErrPipelinePanicked.
			err = ErrPipelinePanicked
		case asked:
			err = ErrClosed
		default:
			err = ErrNoReply
		}
		g.die(err)
		// Nothing is swept here, and nothing needs to be: closing drained —
		// the deferred call above, which runs last — releases every caller
		// still waiting, whether it is queued or already handed to a pipeline
		// that will never answer it. See Do.
	}()

	pipeline(func(yield func(sluice.Batch[*Call[In, Out]]) bool) {
		// The gateway batches from its own channel rather than composing
		// window.CoalesceWithin over a one-call-per-batch source: that
		// operator exists to put a deadline on an arbitrary upstream stream,
		// and doing so costs it a goroutine and a channel hand-off. Here the
		// channel and the goroutine already exist, so the same select does the
		// job with neither.
		buf := make([]*Call[In, Out], 0, g.size)
		// Created stopped, with no drain: since Go 1.23 (and this module
		// requires 1.26) Stop guarantees no stale value is left in C, so the
		// old `if !Stop() { <-C }` idiom has nothing to drain.
		timer := time.NewTimer(g.within)
		timer.Stop()
		defer timer.Stop()

		flush := func() bool {
			if len(buf) == 0 {
				return true
			}
			timer.Stop()
			ok := yield(sluice.Batch[*Call[In, Out]]{Items: buf})
			// No "did they all answer?" sweep here, deliberately. Returning
			// from yield means the batch was accepted downstream, not that it
			// was handled: an asynchronous operator — [parallel.Async],
			// [parallel.Unordered] — returns as soon as a worker has
			// taken it, with the replies still to come. Checking here would
			// fail calls the pipeline is about to answer correctly, and the
			// answer that followed would then panic as a second one. The only
			// safe moment is after the pipeline returns, and by S6 that is
			// after every worker it started has stopped.
			//
			// A fresh slice per batch, because the batch may now outlive this
			// call: an asynchronous operator holds it while the loop moves on,
			// and the batch contract says a retained batch must be copied —
			// here the copy is simply not reusing the buffer.
			buf = make([]*Call[In, Out], 0, g.size)
			return ok
		}

		for {
			select {
			case c := <-g.in:
				if len(buf) == 0 {
					// The ceiling is on how long a call waits, so it runs from
					// the first call of the batch, not the last.
					timer.Reset(g.within)
				}
				buf = append(buf, c)
				if len(buf) == g.size && !flush() {
					return
				}
			case <-timer.C:
				if !flush() {
					return
				}
			case <-g.closing:
				asked = true
				// Serve what is held, then take whatever is already queued
				// behind it rather than making those callers wait for their
				// deadlines to learn the gateway shut.
				if !flush() {
					return
				}
				for {
					select {
					case c := <-g.in:
						buf = append(buf, c)
						if len(buf) == g.size && !flush() {
							return
						}
					default:
						flush()
						return
					}
				}
			}
		}
	})
}

// sweepIn discards calls parked in the buffered channel after the run loop
// has returned, so their payloads are not pinned for as long as the gateway
// value stays reachable. The callers were already released through drained.
//
// It must only run once drained is closed — before that, the run loop is the
// channel's consumer and a sweep would steal live calls. It runs on every
// path that observes drained: Close, and both of Do's drained cases, the
// latter because a Do racing the shutdown can park its call after Close's
// sweep has come and gone.
func (g *Gateway[In, Out]) sweepIn() {
	for {
		select {
		case <-g.in:
		default:
			return
		}
	}
}

// die records the fatal error and stops accepting calls.
func (g *Gateway[In, Out]) die(err error) {
	g.mu.Lock()
	if g.fatal == nil {
		g.fatal = err
	}
	g.mu.Unlock()
	g.closed.Store(true)
}

// Do submits a call and waits for its answer.
//
// It is safe to call from any number of goroutines, which is the point. The
// wait ends when the pipeline answers, when ctx is cancelled or expires, or
// when the gateway is closed — whichever comes first. A ctx with no deadline
// is a caller choosing to wait indefinitely for a pipeline that may never
// answer; give it one. Cancelling ctx ends the wait, not the work: a call
// already submitted is still executed by the pipeline — see [Gateway.DoBatch].
func (g *Gateway[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	var zero Out

	// One atomic load on the fast path. The check is fail-fast only: a Do that
	// races past it is still released by the drained cases of both selects.
	if g.closed.Load() {
		g.mu.Lock()
		err := g.fatal
		g.mu.Unlock()
		if err == nil {
			err = ErrClosed
		}
		return zero, err
	}

	c := &Call[In, Out]{
		In:    in,
		reply: make(chan result[Out], 1),
	}

	// Submission races with Close, which is why the gateway's channel is
	// never closed by Close: the drained signal says the pipeline is gone and
	// the deferred sweep will answer anything already queued.
	select {
	case g.in <- c:
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-g.drained:
		g.sweepIn()
		g.mu.Lock()
		err := g.fatal
		g.mu.Unlock()
		if err == nil {
			err = ErrClosed
		}
		return zero, err
	}

	select {
	case r := <-c.reply:
		return r.out, r.err
	case <-ctx.Done():
		// The pipeline may still answer later; the reply channel is buffered
		// so that answer goes nowhere and blocks nobody.
		return zero, ctx.Err()
	case <-g.drained:
		// The pipeline is gone, so nobody is left to answer this call. It is
		// released now rather than at its deadline — which is what replaces
		// the gateway tracking outstanding calls in a registry, and what makes
		// the "did they all answer?" sweep unnecessary and unsafe.
		//
		// This call may be the one sitting in the buffer — the send above can
		// win its race against drained closing — so the sweep runs here, on
		// the path where the orphan is created. Close's own sweep cannot be
		// complete: it may have already run when this send lands.
		g.sweepIn()
		// The reply is checked first: an answer written just before the
		// pipeline returned is a real answer, and both channels being ready
		// would otherwise make it a coin toss.
		select {
		case r := <-c.reply:
			return r.out, r.err
		default:
		}
		g.mu.Lock()
		err := g.fatal
		g.mu.Unlock()
		if err == nil {
			err = ErrNoReply
		}
		return zero, err
	}
}

// Close stops the gateway and waits for the pipeline to finish.
//
// Calls already accepted are answered — with their result if the pipeline
// gets to them, with [ErrNoReply] if it does not. Calls submitted after Close
// fail with [ErrClosed]. Close is idempotent and safe to call from any
// goroutine.
func (g *Gateway[In, Out]) Close() error {
	g.closeOnce.Do(func() {
		g.closed.Store(true)
		close(g.closing)
	})
	<-g.drained
	g.sweepIn()

	g.mu.Lock()
	defer g.mu.Unlock()
	if errors.Is(g.fatal, ErrClosed) {
		return nil // the ordinary end: the pipeline stopped because it was asked to
	}
	return g.fatal
}
