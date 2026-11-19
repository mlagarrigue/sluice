package parallel_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

type goexitOp func(s sluice.Stream[int], goexit func(int) bool) sluice.Stream[int]

func goexitPass(b sluice.Batch[int]) sluice.Batch[int] {
	return sluice.Batch[int]{Items: append([]int(nil), b.Items...)}
}

// outcome of draining a pipeline on its own goroutine.
type goexitOutcome struct {
	got      []int
	returned bool // the call came back normally
	ended    bool // the draining goroutine ended (normally or by Goexit)
	panicked bool
}

func goexitDrain(op goexitOp, n int, goexit func(int) bool) goexitOutcome {
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	var out goexitOutcome
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				out.panicked = true
			}
		}()
		op(sluice.Of(items, 1), goexit)(func(b sluice.Batch[int]) bool {
			out.got = append(out.got, b.Items...)
			return true
		})
		out.returned = true
	}()
	select {
	case <-done:
		out.ended = true
	case <-time.After(5 * time.Second):
	}
	return out
}

// runtime.Goexit in f is not a normal return: the call does not come back
// normally with a batch short — the emitter raises it as a panic, as the
// inline path ends the caller's goroutine. The worker's recover saw no
// panic, Ordered emitted an empty batch in place of the lost one and
// Unordered lost the batch, its input slot and a worker with it; repeated,
// the pipeline stopped with no signal. The witness is the same pipeline
// with no Goexit; the control, the inline path.
func TestAGoexitInFDoesNotReturnNormally(t *testing.T) {
	ops := map[string]goexitOp{
		"Ordered": func(s sluice.Stream[int], g func(int) bool) sluice.Stream[int] {
			return parallel.Ordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] {
				if g(b.Items[0]) {
					runtime.Goexit()
				}
				return goexitPass(b)
			})
		},
		"Unordered": func(s sluice.Stream[int], g func(int) bool) sluice.Stream[int] {
			return parallel.Unordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] {
				if g(b.Items[0]) {
					runtime.Goexit()
				}
				return goexitPass(b)
			})
		},
		"OrderedWithState": func(s sluice.Stream[int], g func(int) bool) sluice.Stream[int] {
			return parallel.OrderedWithState(s, 4, func() (int, error) { return 0, nil },
				func(_ int, b sluice.Batch[int]) sluice.Batch[int] {
					if g(b.Items[0]) {
						runtime.Goexit()
					}
					return goexitPass(b)
				})
		},
	}
	const n = 12
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			// Witness: no Goexit.
			w := goexitDrain(op, n, func(int) bool { return false })
			if !w.returned || len(w.got) != n {
				t.Fatalf("witness: returned=%v got=%d", w.returned, len(w.got))
			}
			// One batch exits its worker by Goexit.
			o := goexitDrain(op, n, func(i int) bool { return i == 3 })
			t.Logf("one Goexit: returned=%v panicked=%v ended=%v got=%d of %d", o.returned, o.panicked, o.ended, len(o.got), n)
			if o.returned {
				t.Errorf("call returned normally with %d of %d elements and no panic", len(o.got), n)
			}
			// Every batch exits by Goexit: repeated loss.
			var calls atomic.Int32
			all := goexitDrain(op, n, func(int) bool { calls.Add(1); return true })
			t.Logf("all Goexit: returned=%v ended=%v got=%d calls=%d", all.returned, all.ended, len(all.got), calls.Load())
			if all.returned || !all.ended {
				t.Errorf("all-Goexit: returned=%v ended=%v (a normal return or a hang, neither is a Goexit/panic)", all.returned, all.ended)
			}
		})
	}
	// Control: inline path.
	t.Run("inline control", func(t *testing.T) {
		o := goexitDrain(func(s sluice.Stream[int], g func(int) bool) sluice.Stream[int] {
			return parallel.Ordered(s, 1, func(b sluice.Batch[int]) sluice.Batch[int] {
				if g(b.Items[0]) {
					runtime.Goexit()
				}
				return goexitPass(b)
			})
		}, n, func(i int) bool { return i == 3 })
		t.Logf("inline: returned=%v ended=%v got=%d", o.returned, o.ended, len(o.got))
		if o.returned {
			t.Errorf("control: inline Goexit returned normally")
		}
	})
}

// Every worker leaves by runtime.Goexit just as the job queue is full: the
// producer, about to hand over one more batch, has no worker left to take
// it, and only the panics the workers sent can end that send. It waited on
// the queue alone and hung (CI, Go 1.26, 2026-11-19). The gate opens as the
// source yields the batch that finds the queue full, so the workers exit
// while the producer stands at the hand-off.
func TestUnorderedEndsWhenEveryWorkerLeavesByGoexitAtAFullQueue(t *testing.T) {
	const workers = 4
	gate := make(chan struct{})
	src := func(yield func(sluice.Batch[int]) bool) {
		for i := range 4 * workers {
			if i == 2*workers { // workers in hand and a full queue: this one waits
				close(gate)
			}
			if !yield(sluice.Batch[int]{Items: []int{i}}) {
				return
			}
		}
	}
	f := func(sluice.Batch[int]) sluice.Batch[int] {
		<-gate
		runtime.Goexit()
		return sluice.Batch[int]{}
	}
	done := make(chan bool, 1)
	go func() {
		panicked := false
		defer func() { done <- panicked }()
		defer func() { panicked = recover() != nil }()
		parallel.Unordered(src, workers, f)(func(sluice.Batch[int]) bool { return true })
	}()
	select {
	case panicked := <-done:
		if !panicked {
			t.Error("the call returned normally after every worker left by Goexit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the call hung with every worker gone")
	}
}
