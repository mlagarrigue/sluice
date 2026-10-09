package gateway

import "context"

// Outcome is one element's answer from [Gateway.DoBatch]: the value the
// pipeline replied, or the error that kept it from answering, at the position
// the input held.
type Outcome[Out any] struct {
	Out Out
	Err error
}

// DoBatch submits a batch of calls in one motion and waits for all of them.
//
// It is [Gateway.Do] for a caller that already holds a batch — a transport
// that read several pipelined or multiplexed requests in one read
// (httpstream), a job runner with a page of work. Submitting them one Do at a
// time would put a goroutine under each; DoBatch costs none: the calls are
// sent down the same channel Do uses, in a loop, and waited for in index
// order — every call is already in flight, so waiting for the first does not
// delay the last.
//
// The batch joins the pipeline as company that has already arrived. Its calls
// share the [Config.Within] window a lone Do would wait out by itself, other
// callers' calls may join the same pipeline batch, and a batch larger than
// [Config.Size] spills into the next pipeline batch rather than being
// refused. What is deliberately not provided is a way to bypass the window: a
// batch that should not wait for company belongs on a pipeline of its own,
// not on a gateway asked to stop gathering.
//
// The result always has len(ins) elements, and outcome i answers ins[i]. That
// is the gateway's own rule — never drop a call — applied on the caller's
// side of the channel: when ctx ends or the gateway closes mid-wait, the
// elements already answered keep their answers and the rest carry the cause.
// As with Do, an answer the pipeline produces after ctx has expired goes into
// the call's buffered channel and blocks nobody.
//
// Expiring ctx abandons the wait, not the work. A call already handed to the
// gateway stays in the pipeline and is still executed — a query run, a write
// applied — with its answer then discarded: the pipeline sees elements, not
// their callers' contexts (ADR 0003). An element whose effect must not
// happen after its caller gave up has to carry its own deadline in In and
// have the pipeline check it.
func (g *Gateway[In, Out]) DoBatch(ctx context.Context, ins []In) []Outcome[Out] {
	out := make([]Outcome[Out], len(ins))
	if len(ins) == 0 {
		return out
	}

	// The same fail-fast check Do makes, once for the batch. A DoBatch racing
	// past it is still released by the drained cases below.
	if g.closed.Load() {
		err := g.fatalErr(ErrClosed)
		for i := range out {
			out[i].Err = err
		}
		return out
	}

	// Each call is its own allocation, as in Do, rather than one backing
	// array: the pipeline retains calls independently, and a shared array
	// would pin the whole batch for as long as any one of them is held.
	calls := make([]*Call[In, Out], 0, len(ins))

	// stop, once set, is why this function no longer blocks: the context
	// ended, or the pipeline is gone. It is the error the unanswered carry.
	var stop error
	for _, in := range ins {
		c := &Call[In, Out]{
			In:    in,
			reply: make(chan result[Out], 1),
		}
		select {
		case g.in <- c:
			calls = append(calls, c)
			continue
		case <-ctx.Done():
			stop = ctx.Err()
		case <-g.drained:
			g.sweepIn()
			stop = g.fatalErr(ErrClosed)
		}
		break
	}
	// The tail that was never submitted fails with the cause; nothing waits
	// on a call the pipeline was never given.
	for i := len(calls); i < len(out); i++ {
		out[i].Err = stop
	}

	for i, c := range calls {
		if stop == nil {
			select {
			case r := <-c.reply:
				out[i] = Outcome[Out]{Out: r.out, Err: r.err}
				continue
			case <-ctx.Done():
				stop = ctx.Err()
			case <-g.drained:
				// This batch's calls may include ones parked in the buffer —
				// the submissions above can win their race against drained
				// closing — so the sweep runs here, on the path where the
				// orphans are created, exactly as Do does.
				g.sweepIn()
				stop = g.fatalErr(ErrNoReply)
			}
		}
		// No longer waiting. An answer that already arrived is a real answer,
		// and a reply channel and a stop signal both being ready must not be
		// a coin toss — the reply is consulted first, as in Do.
		select {
		case r := <-c.reply:
			out[i] = Outcome[Out]{Out: r.out, Err: r.err}
		default:
			out[i].Err = stop
		}
	}
	return out
}

// fatalErr reads the recorded cause of the pipeline's death, falling back
// when the run loop has not recorded one yet.
func (g *Gateway[In, Out]) fatalErr(fallback error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fatal != nil {
		return g.fatal
	}
	return fallback
}
