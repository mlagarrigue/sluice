package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
)

func TestDoBatchAnswersPositionally(t *testing.T) {
	gw := New(Config{Size: 64, Within: 5 * time.Millisecond}, echo)
	defer func() { _ = gw.Close() }()

	ins := []int{3, 1, 4, 1, 5, 9, 2, 6}
	out := gw.DoBatch(context.Background(), ins)
	if len(out) != len(ins) {
		t.Fatalf("got %d outcomes for %d inputs", len(out), len(ins))
	}
	for i, o := range out {
		if o.Err != nil {
			t.Fatalf("outcome %d: %v", i, o.Err)
		}
		if want := fmt.Sprintf("v%d", ins[i]); o.Out != want {
			t.Errorf("outcome %d = %q, want %q — the answer must hold the input's position", i, o.Out, want)
		}
	}
}

// The reason DoBatch exists: a transport batch reaches the pipeline as one
// batch, not as Size accidental groupings of per-element submissions. With
// Size above the batch and the window wide, only the timer can flush, so
// everything submitted together must land together.
func TestDoBatchReachesThePipelineAsOneBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		seen := make(chan int, 16)
		gw := New(Config{Size: 64, Within: 50 * time.Millisecond},
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

		ins := make([]int, 32)
		for i := range ins {
			ins[i] = i
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i, o := range gw.DoBatch(ctx, ins) {
			if o.Err != nil {
				t.Fatalf("outcome %d: %v", i, o.Err)
			}
		}
		if got := <-seen; got != len(ins) {
			t.Errorf("the pipeline's first batch held %d calls, want %d submitted together", got, len(ins))
		}
	})
}

// A batch past Size is not refused and not truncated: it spills into further
// pipeline batches, and every element is still answered.
func TestDoBatchLargerThanSizeSpills(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const size = 8
		var mu sync.Mutex
		var batches []int
		gw := New(Config{Size: size, Within: 5 * time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				calls(func(b sluice.Batch[*Call[int, string]]) bool {
					mu.Lock()
					batches = append(batches, b.Len())
					mu.Unlock()
					for _, c := range b.Items {
						c.Reply(fmt.Sprintf("v%d", c.In))
					}
					return true
				})
			})
		defer func() { _ = gw.Close() }()

		ins := make([]int, 20)
		for i := range ins {
			ins[i] = i
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out := gw.DoBatch(ctx, ins)
		for i, o := range out {
			if o.Err != nil {
				t.Fatalf("outcome %d: %v", i, o.Err)
			}
			if want := fmt.Sprintf("v%d", i); o.Out != want {
				t.Errorf("outcome %d = %q, want %q", i, o.Out, want)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		total := 0
		for _, n := range batches {
			if n > size {
				t.Errorf("a pipeline batch held %d calls, over the configured %d", n, size)
			}
			total += n
		}
		if total != len(ins) {
			t.Errorf("the pipeline saw %d calls, want %d", total, len(ins))
		}
	})
}

// The batch shares the window with everyone else: single Do callers arriving
// while a DoBatch is parked join the same pipeline batch instead of starting
// their own clock.
//
// No sleep orders the two callers. The window is far longer than any
// scheduling delay and Size is exactly the five calls, so the first pipeline
// batch closes on size the moment the last of them arrives, in whichever
// order they came: a gateway that served the DoBatch on its own would show a
// first batch of 4, and one that made the lone caller wait for a fresh
// window would hang until the bounded context gives up.
func TestDoBatchSharesTheWindowWithLoneCallers(t *testing.T) {
	seen := make(chan int, 16)
	gw := New(Config{Size: 5, Within: time.Minute},
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		for _, o := range gw.DoBatch(ctx, []int{0, 1, 2, 3}) {
			if o.Err != nil {
				t.Errorf("DoBatch: %v", o.Err)
			}
		}
	})
	wg.Go(func() {
		if _, err := gw.Do(ctx, 9); err != nil {
			t.Errorf("Do: %v", err)
		}
	})
	wg.Wait()

	if got := <-seen; got != 5 {
		t.Errorf("the pipeline's first batch held %d calls, want the batch of 4 and the lone caller together", got)
	}
}

func TestDoBatchEmpty(t *testing.T) {
	gw := New(Config{Size: 4, Within: time.Millisecond}, echo)
	defer func() { _ = gw.Close() }()
	if out := gw.DoBatch(context.Background(), nil); len(out) != 0 {
		t.Errorf("got %d outcomes for no inputs", len(out))
	}
}

func TestDoBatchAfterClose(t *testing.T) {
	gw := New(Config{Size: 4, Within: time.Millisecond}, echo)
	_ = gw.Close()

	out := gw.DoBatch(context.Background(), []int{1, 2, 3})
	if len(out) != 3 {
		t.Fatalf("got %d outcomes for 3 inputs", len(out))
	}
	for i, o := range out {
		if !errors.Is(o.Err, ErrClosed) {
			t.Errorf("outcome %d: err = %v, want ErrClosed", i, o.Err)
		}
	}
}

// The context expiring releases the caller with the cause on every element
// that was not answered in time — and an answer that did arrive is kept, not
// discarded for having raced the deadline.
func TestDoBatchContextExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		gw := New(Config{Size: 2, Within: time.Millisecond},
			func(calls sluice.Stream[*Call[int, string]]) {
				calls(func(b sluice.Batch[*Call[int, string]]) bool {
					<-release
					for _, c := range b.Items {
						c.Reply(fmt.Sprintf("v%d", c.In))
					}
					return true
				})
			})
		defer func() {
			close(release)
			_ = gw.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		out := gw.DoBatch(ctx, []int{1, 2, 3})
		if len(out) != 3 {
			t.Fatalf("got %d outcomes for 3 inputs", len(out))
		}
		for i, o := range out {
			if !errors.Is(o.Err, context.DeadlineExceeded) {
				t.Errorf("outcome %d: err = %v, want DeadlineExceeded", i, o.Err)
			}
		}
	})
}

// A pipeline that dies mid-batch leaves nobody hanging: every element comes
// back carrying the pipeline's cause rather than waiting for its deadline.
func TestDoBatchWhenThePipelinePanics(t *testing.T) {
	gw := New(Config{Size: 4, Within: time.Millisecond},
		func(calls sluice.Stream[*Call[int, string]]) {
			calls(func(b sluice.Batch[*Call[int, string]]) bool {
				panic("the pipeline gave up")
			})
		})
	defer func() { _ = gw.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := gw.DoBatch(ctx, []int{1, 2, 3, 4})
	for i, o := range out {
		if o.Err == nil {
			t.Fatalf("outcome %d answered %q after the pipeline panicked", i, o.Out)
		}
	}
}

// DoBatch, Do and Close from many goroutines at once: what this asserts is
// what the race detector sees, plus the invariant that every submission is
// answered one way or the other.
func TestDoBatchConcurrentWithDoAndClose(t *testing.T) {
	gw := New(Config{Size: 8, Within: time.Millisecond}, echo)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 16 {
				out := gw.DoBatch(ctx, []int{1, 2, 3, 4, 5})
				if len(out) != 5 {
					t.Errorf("got %d outcomes for 5 inputs", len(out))
				}
			}
		})
		wg.Go(func() {
			for i := range 16 {
				_, _ = gw.Do(ctx, i)
			}
		})
	}
	time.Sleep(2 * time.Millisecond)
	_ = gw.Close()
	wg.Wait()
}
