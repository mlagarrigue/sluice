package gateway

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"testing/synctest"
)

// bareGateway is a Gateway with no run loop: the test plays the pipeline,
// taking calls from in and closing drained, so each of Do's races can be set
// up in the order it names rather than hoped for.
func bareGateway(buffer int) *Gateway[int, string] {
	return &Gateway[int, string]{
		in:      make(chan *Call[int, string], buffer),
		closing: make(chan struct{}),
		drained: make(chan struct{}),
	}
}

// doAsync runs Do on its own goroutine and hands back its outcome.
func doAsync(g *Gateway[int, string]) <-chan result[string] {
	out := make(chan result[string], 1)
	go func() {
		v, err := g.Do(context.Background(), 1)
		out <- result[string]{out: v, err: err}
	}()
	return out
}

// The pipeline is gone before the call could be submitted: nobody takes from
// the channel and drained closes. Do returns the recorded cause, or ErrClosed.
func TestDoSubmissionLosesToDrained(t *testing.T) {
	cause := errors.New("pipeline panicked")
	for _, fatal := range []error{nil, cause} {
		synctest.Test(t, func(t *testing.T) {
			g := bareGateway(0)
			g.fatal = fatal
			got := doAsync(g)
			synctest.Wait() // Do is blocked on the send
			close(g.drained)
			r := <-got
			want := fatal
			if want == nil {
				want = ErrClosed
			}
			if !errors.Is(r.err, want) {
				t.Errorf("fatal %v: Do returned %v, want %v", fatal, r.err, want)
			}
		})
	}
}

// The pipeline answered the call and then returned: when Do looks, both the
// reply and drained are ready. The answer is a real answer and must win every
// time; a plain select would pick between the two at random. One processor,
// so Do does not look before both are ready, and enough rounds that a coin
// toss would lose one.
func TestDoReplyBeforeDrainedIsTheAnswer(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	for range 200 {
		synctest.Test(t, func(t *testing.T) {
			g := bareGateway(0)
			got := doAsync(g)
			synctest.Wait() // Do is blocked on the send
			c := <-g.in
			c.reply <- result[string]{out: "v1"}
			close(g.drained)
			if r := <-got; r.err != nil || r.out != "v1" {
				t.Fatalf("Do returned (%q, %v), want the reply the pipeline wrote", r.out, r.err)
			}
		})
	}
}

// The call was taken and the pipeline returned without answering it: Do is
// released with the recorded cause, or ErrNoReply.
func TestDoUnansweredWhenDrained(t *testing.T) {
	cause := errors.New("pipeline panicked")
	for _, fatal := range []error{nil, cause} {
		synctest.Test(t, func(t *testing.T) {
			g := bareGateway(0)
			g.fatal = fatal
			got := doAsync(g)
			synctest.Wait()
			<-g.in
			synctest.Wait() // Do waits for the reply
			close(g.drained)
			r := <-got
			want := fatal
			if want == nil {
				want = ErrNoReply
			}
			if !errors.Is(r.err, want) {
				t.Errorf("fatal %v: Do returned %v, want %v", fatal, r.err, want)
			}
		})
	}
}

// A gateway already closed fails fast, with the recorded cause or ErrClosed.
func TestDoOnAClosedGateway(t *testing.T) {
	g := bareGateway(0)
	g.closed.Store(true)
	if _, err := g.Do(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Do on a closed gateway returned %v, want ErrClosed", err)
	}
	g.fatal = errors.New("pipeline panicked")
	if _, err := g.Do(context.Background(), 1); !errors.Is(err, g.fatal) {
		t.Errorf("Do on a dead gateway returned %v, want its cause", err)
	}
}

// The caller gives up while the call waits to be submitted: Do returns the
// context's error and leaves nothing in the channel.
func TestDoCancelledBeforeSubmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := bareGateway(0)
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		go func() {
			_, err := g.Do(ctx, 1)
			got <- err
		}()
		synctest.Wait() // Do is blocked on the send
		cancel()
		if err := <-got; !errors.Is(err, context.Canceled) {
			t.Errorf("Do returned %v, want context.Canceled", err)
		}
		if len(g.in) != 0 {
			t.Error("a cancelled call was left in the channel")
		}
	})
}
