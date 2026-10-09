package gateway

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// echo is the trivial pipeline: answer every call with a value derived from
// its input, so a wrong answer is a correlation bug rather than a wrong
// computation.
func echo(calls sluice.Stream[*Call[int, string]]) {
	calls(func(b sluice.Batch[*Call[int, string]]) bool {
		for _, c := range b.Items {
			c.Reply(fmt.Sprintf("v%d", c.In))
		}
		return true
	})
}

// goroutinesSettle waits for every other goroutine in the test's synctest
// bubble to exit or block, then returns the goroutine count: exact, where a
// poll could only hope the scheduler had caught up. It must be called inside
// synctest.Test.
func goroutinesSettle(t *testing.T) int {
	t.Helper()
	synctest.Wait()
	return runtime.NumGoroutine()
}

func TestGatewayDo(t *testing.T) {
	gw := New(Config{Size: 4, Within: 10 * time.Millisecond}, echo)
	defer func() { _ = gw.Close() }()

	got, err := gw.Do(context.Background(), 7)
	if err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if got != "v7" {
		t.Errorf("got %q, want %q", got, "v7")
	}
}

// The point of the gateway: concurrent calls reach the pipeline as one batch.
// With a deadline far away, only the size can trigger the flush, so the batch
// the pipeline sees must be exactly Size — no scheduling luck involved.
func TestGatewayBatchesConcurrentCalls(t *testing.T) {
	const size = 4
	seen := make(chan int, 16)
	gw := New(Config{Size: size, Within: time.Hour},
		func(calls sluice.Stream[*Call[int, string]]) {
			calls(func(b sluice.Batch[*Call[int, string]]) bool {
				seen <- b.Len()
				for _, c := range b.Items {
					c.Reply(fmt.Sprintf("v%d", c.In))
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	var wg sync.WaitGroup
	for i := range size {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := gw.Do(ctx, i); err != nil {
				t.Errorf("Do(%d) returned %v", i, err)
			}
		})
	}
	wg.Wait()

	select {
	case n := <-seen:
		if n != size {
			t.Errorf("the pipeline saw a batch of %d, want %d — calls were not grouped", n, size)
		}
	default:
		t.Fatal("the pipeline saw no batch at all")
	}
}

// Every caller must receive the answer to its own call and no other. This is
// the correlation guarantee, and the reason the element carries its own way
// back.
func TestGatewayCorrelatesUnderConcurrency(t *testing.T) {
	const callers = 200
	gw := New(Config{Size: 16, Within: 2 * time.Millisecond}, echo)
	defer func() { _ = gw.Close() }()

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := range callers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			got, err := gw.Do(ctx, i)
			if err != nil {
				errs <- fmt.Errorf("Do(%d): %w", i, err)
				return
			}
			if want := fmt.Sprintf("v%d", i); got != want {
				errs <- fmt.Errorf("Do(%d) = %q, want %q — a reply went to the wrong caller", i, got, want)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A lone call must not wait for a batch that will never fill: the deadline is
// the ceiling, and it is what makes the gateway usable on a quiet service.
func TestGatewayFlushesOnDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const within = 40 * time.Millisecond
		gw := New(Config{Size: 1000, Within: within}, echo)
		defer func() { _ = gw.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		if _, err := gw.Do(ctx, 1); err != nil {
			t.Fatalf("Do returned %v", err)
		}
		elapsed := time.Since(start)
		if elapsed < within {
			t.Errorf("answered after %v, want at least the %v deadline", elapsed, within)
		}
		if elapsed > 2*time.Second {
			t.Errorf("answered after %v — the deadline did not flush the batch", elapsed)
		}
	})
}

// A pipeline that drops a call leaves its caller with no answer, and while
// the pipeline lives nobody can tell a dropped call from a slow one — so the
// caller's context is what ends the wait. This is the cost the package
// documentation states, pinned here so it stays a stated cost rather than a
// discovered one.
func TestGatewayDroppedCallWaitsForItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gw := New(Config{Size: 1, Within: 10 * time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				// The mistake the documentation warns against: filtering a call
				// out instead of replying with a rejection.
				kept := sluice.Filter(calls, func(c *Call[int, string]) bool { return c.In != 0 })
				kept(func(b sluice.Batch[*Call[int, string]]) bool {
					for _, c := range b.Items {
						c.Reply("kept")
					}
					return true
				})
			})
		defer func() { _ = gw.Close() }()

		short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelShort()
		if _, err := gw.Do(short, 0); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded — a dropped call ends at its deadline", err)
		}

		// A call the pipeline does keep is still answered normally, and the
		// gateway is unharmed by the dropped one.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if got, err := gw.Do(ctx, 5); err != nil || got != "kept" {
			t.Errorf("Do(5) = %q, %v; want \"kept\", nil", got, err)
		}
	})
}

// A call outstanding when the pipeline dies must be released at once, not left
// on its deadline: that release is what replaces the per-batch sweep, and it
// is the guarantee the package makes in its place.
func TestGatewayOutstandingCallReleasedWhenPipelineDies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stuck := make(chan struct{})
		gw := New(Config{Size: 1, Within: 5 * time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				calls(func(b sluice.Batch[*Call[int, string]]) bool {
					<-stuck      // hold the call, answering nothing
					return false // then abandon the pipeline
				})
			})

		// A context far longer than the test: if the caller is released, it is
		// because the gateway released it, not because it timed out.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := gw.Do(ctx, 1)
			done <- err
		}()

		synctest.Wait() // the call has reached the pipeline, which holds it
		close(stuck)    // the pipeline abandons it

		select {
		case err := <-done:
			if !errors.Is(err, ErrNoReply) {
				t.Errorf("got %v, want ErrNoReply", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the caller was left waiting for a pipeline that had returned")
		}
		_ = gw.Close()
	})
}

// The composition the gateway is meant to allow: a fan-out inside the
// pipeline, where yield returns before the replies are sent. A gateway that
// checked for unanswered calls per batch would break exactly this.
func TestGatewayWithAsyncPipeline(t *testing.T) {
	gw := New(Config{Size: 4, Within: 2 * time.Millisecond},
		func(calls sluice.Stream[*Call[int, string]]) {
			served := parallel.Unordered(calls, 4,
				func(b sluice.Batch[*Call[int, string]]) sluice.Batch[*Call[int, string]] {
					for _, c := range b.Items {
						c.Reply(fmt.Sprintf("v%d", c.In))
					}
					return sluice.Batch[*Call[int, string]]{}
				})
			served(func(sluice.Batch[*Call[int, string]]) bool { return true })
		})
	defer func() { _ = gw.Close() }()

	const callers = 64
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := range callers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			got, err := gw.Do(ctx, i)
			if err != nil {
				errs <- fmt.Errorf("Do(%d): %w", i, err)
				return
			}
			if want := fmt.Sprintf("v%d", i); got != want {
				errs <- fmt.Errorf("Do(%d) = %q, want %q", i, got, want)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A panic in the pipeline must fail every call in flight and every later one,
// not leave a room full of goroutines waiting on deadlines.
func TestGatewayPipelinePanicFailsCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		// The panic value quotes what was being processed — here, another
		// caller's data. It must not reach the error every caller shares.
		const secret = "tenant-acme card 4242"
		gw := New(Config{Size: 1, Within: 5 * time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				calls(func(b sluice.Batch[*Call[int, string]]) bool {
					panic("pipeline boom: " + secret)
				})
			})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := gw.Do(ctx, 1)
		if !errors.Is(err, ErrPipelinePanicked) {
			t.Fatalf("err = %v, want ErrPipelinePanicked", err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the error handed to every caller carries the panic value: %v", err)
		}

		// Every later call fails at once with the same cause.
		if _, err := gw.Do(ctx, 2); !errors.Is(err, ErrPipelinePanicked) {
			t.Errorf("a call after the panic got %v, want ErrPipelinePanicked", err)
		}

		_ = gw.Close()
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a pipeline panic", before, got)
		}
	})
}

// Close answers what is queued, refuses what comes after, is idempotent, and
// releases the pipeline goroutine.
func TestGatewayClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		gw := New(Config{Size: 8, Within: 5 * time.Millisecond}, echo)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := gw.Do(ctx, 1); err != nil {
			t.Fatalf("Do before Close returned %v", err)
		}

		if err := gw.Close(); err != nil {
			t.Errorf("Close returned %v, want nil", err)
		}
		if err := gw.Close(); err != nil {
			t.Errorf("second Close returned %v, want nil — Close is idempotent", err)
		}

		if _, err := gw.Do(ctx, 2); !errors.Is(err, ErrClosed) {
			t.Errorf("Do after Close returned %v, want ErrClosed", err)
		}
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after Close", before, got)
		}
	})
}

// A caller whose context expires gets its context's error, and the pipeline
// answering afterwards must block nobody.
func TestGatewayContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		gw := New(Config{Size: 1, Within: time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				calls(func(b sluice.Batch[*Call[int, string]]) bool {
					<-release // hold the batch past the caller's deadline
					for _, c := range b.Items {
						c.Reply("late")
					}
					return true
				})
			})

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := gw.Do(ctx, 1)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want context.DeadlineExceeded", err)
		}

		close(release) // the pipeline answers a caller that is gone
		_ = gw.Close()
	})
}

// Answering twice is a pipeline believing two things about one request, and
// says so rather than letting the second answer vanish.
func TestGatewayDoubleAnswerPanics(t *testing.T) {
	c := &Call[int, string]{In: 1, reply: make(chan result[string], 1)}
	c.Reply("first")

	defer func() {
		if recover() == nil {
			t.Error("answering twice did not panic")
		}
	}()
	c.Reply("second")
}

// Fail with a nil error is a bug in the pipeline, not a success.
func TestGatewayFailNilBecomesNoReply(t *testing.T) {
	c := &Call[int, string]{In: 1, reply: make(chan result[string], 1)}
	c.Fail(nil)
	r := <-c.reply
	if !errors.Is(r.err, ErrNoReply) {
		t.Errorf("Fail(nil) produced %v, want ErrNoReply", r.err)
	}
}

func TestGatewayArgumentValidation(t *testing.T) {
	tests := []struct {
		name string
		f    func()
	}{
		{"nil pipeline", func() { New[int, string](Config{Within: time.Second}, nil) }},
		{"zero Within", func() { New(Config{}, echo) }},
		{"negative Within", func() { New(Config{Within: -time.Second}, echo) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			tt.f()
		})
	}
}

// A call parked in the buffer after the run loop has returned is a payload
// nobody will ever read, held for as long as the gateway value is reachable.
// Its caller was already released through drained, so discarding it is all
// that is left to do.
//
// This exercises the sweep directly. Reaching it through the race it exists
// for — a Do that passes the closed check and lands its send after the loop is
// gone — cannot be forced from a test without a hook inside Do, and a hook
// that only tests can reach is a worse thing to own than an untested branch.
func TestSweepInDiscardsWhatTheRunLoopWillNeverTake(t *testing.T) {
	g := New(Config{Size: 4, Within: time.Millisecond},
		func(calls sluice.Stream[*Call[int, int]]) {
			calls(func(b sluice.Batch[*Call[int, int]]) bool {
				for _, c := range b.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// The state the race produces: the loop is gone, and a call is in the
	// buffer with nobody to take it.
	g.in <- &Call[int, int]{In: 1, reply: make(chan result[int], 1)}
	if len(g.in) != 1 {
		t.Fatalf("the setup did not park a call: %d", len(g.in))
	}

	g.sweepIn()

	if n := len(g.in); n != 0 {
		t.Errorf("%d calls left in the buffer after the sweep", n)
	}
	// It returns on an empty channel rather than waiting for one more.
	g.sweepIn()
}
