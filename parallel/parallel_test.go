package parallel

import (
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
)

// goroutinesSettle waits for every other goroutine in the test's synctest
// bubble to exit or block, then returns the goroutine count. Workers exit
// asynchronously after wg.Wait returns to their caller; synctest.Wait is what
// makes the count exact rather than a poll that hopes the scheduler has
// caught up. It must be called inside synctest.Test.
func goroutinesSettle(t *testing.T) int {
	t.Helper()
	synctest.Wait()
	return runtime.NumGoroutine()
}

func TestParallel(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		src     []int
		size    int
		want    []int
	}{
		{"four workers", 4, []int{1, 2, 3, 4, 5, 6, 7, 8}, 2, []int{10, 20, 30, 40, 50, 60, 70, 80}},
		{"more workers than batches", 8, []int{1, 2}, 1, []int{10, 20}},
		{"one batch", 4, []int{1, 2, 3}, 8, []int{10, 20, 30}},
		{"empty stream", 4, nil, 2, nil},
		{"workers = 1 is the no-op path", 1, []int{1, 2, 3}, 2, []int{10, 20, 30}},
		{"workers = 0 is the no-op path", 0, []int{1, 2, 3}, 2, []int{10, 20, 30}},
		{"negative workers is the no-op path", -5, []int{1, 2, 3}, 2, []int{10, 20, 30}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sluice.Collect(Ordered(sluice.Of(tc.src, tc.size), tc.workers, func(b sluice.Batch[int]) sluice.Batch[int] {
				out := make([]int, len(b.Items))
				for i, v := range b.Items {
					out[i] = v * 10
				}
				return sluice.Batch[int]{Items: out}
			}))
			if !slices.Equal(got, tc.want) {
				t.Errorf("Ordered = %v, want %v", got, tc.want)
			}
		})
	}
}

// Order is the property that lets Ordered be dropped into an existing
// pipeline. Workers finish out of order by construction here — the first batch
// is the slowest — so a implementation that emitted on completion would fail.
func TestParallelPreservesOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const batches = 16

		src := make([]int, batches)
		for i := range src {
			src[i] = i
		}

		got := sluice.Collect(Ordered(sluice.Of(src, 1), 8, func(b sluice.Batch[int]) sluice.Batch[int] {
			// Earlier batches take longer, so completion order is the reverse of
			// arrival order.
			time.Sleep(time.Duration(batches-b.Items[0]) * time.Millisecond)
			return sluice.Batch[int]{Items: []int{b.Items[0]}}
		}))

		if !slices.Equal(got, src) {
			t.Errorf("Ordered = %v, want %v: output must follow input order, not completion order", got, src)
		}
	})
}

// f must actually run on several goroutines, or the operator is an expensive
// no-op. This observes concurrency rather than asserting a speed-up, which
// would be flaky on a loaded machine.
func TestParallelActuallyRunsConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 4

		var live, peak atomic.Int64
		src := make([]int, workers*4)

		sluice.Collect(Ordered(sluice.Of(src, 1), workers, func(b sluice.Batch[int]) sluice.Batch[int] {
			n := live.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			live.Add(-1)
			return b
		}))

		if peak.Load() < 2 {
			t.Errorf("peak concurrency was %d: f never ran on more than one goroutine", peak.Load())
		}
		if peak.Load() > workers {
			t.Errorf("peak concurrency was %d, above the %d workers asked for", peak.Load(), workers)
		}
	})
}

// S6 — no worker is still running when Ordered returns, on every exit path.
//
// Counting goroutines after the fact is not enough: closing the job channel
// ends the workers on its own, so they exit shortly afterwards whether or not
// Ordered waited for them, and a count taken a moment later cannot tell the
// two apart. What matters is that they are gone at the instant Ordered
// returns — otherwise a worker is still touching f's arguments while the caller
// believes the stage is finished.
//
// This counts workers directly, on each exit path.
func TestParallelWaitsForWorkers(t *testing.T) {
	run := func(t *testing.T, consume func(sluice.Stream[int])) {
		t.Helper()
		synctest.Test(t, func(t *testing.T) {
			var live atomic.Int64

			s := Ordered(sluice.Of(make([]int, 64), 4), 8, func(b sluice.Batch[int]) sluice.Batch[int] {
				live.Add(1)
				defer live.Add(-1)
				time.Sleep(time.Millisecond)
				return b
			})

			func() {
				defer func() { _ = recover() }()
				consume(s)
			}()

			// Checked immediately, with no settling: a worker still inside f here
			// means Ordered returned while its stage was still running.
			if n := live.Load(); n != 0 {
				t.Errorf("%d workers still inside f when Ordered returned", n)
			}
		})
	}

	t.Run("normal completion", func(t *testing.T) {
		run(t, func(s sluice.Stream[int]) { sluice.Collect(s) })
	})
	t.Run("early stop", func(t *testing.T) {
		run(t, func(s sluice.Stream[int]) {
			n := 0
			s(func(sluice.Batch[int]) bool { n++; return n < 2 })
		})
	})
	t.Run("panic in the consumer", func(t *testing.T) {
		run(t, func(s sluice.Stream[int]) {
			s(func(sluice.Batch[int]) bool { panic("consumer boom") })
		})
	})
}

// S6 — no goroutine outlives the call, on every exit path.
func TestParallelLeavesNoGoroutines(t *testing.T) {
	run := func(t *testing.T, call func()) {
		t.Helper()
		synctest.Test(t, func(t *testing.T) {
			before := runtime.NumGoroutine()
			call()
			if after := goroutinesSettle(t); after > before {
				t.Errorf("goroutines: %d before, %d after — workers outlived the call", before, after)
			}
		})
	}

	t.Run("normal completion", func(t *testing.T) {
		run(t, func() {
			sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 8, func(b sluice.Batch[int]) sluice.Batch[int] { return b }))
		})
	})

	t.Run("early stop", func(t *testing.T) {
		run(t, func() {
			n := 0
			Ordered(sluice.Of(make([]int, 64), 4), 8, func(b sluice.Batch[int]) sluice.Batch[int] { return b })(
				func(sluice.Batch[int]) bool {
					n++
					return n < 2
				})
		})
	})

	t.Run("panic in f", func(t *testing.T) {
		run(t, func() {
			defer func() { _ = recover() }()
			sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 8, func(sluice.Batch[int]) sluice.Batch[int] {
				panic("boom")
			}))
		})
	})

	t.Run("panic in the consumer", func(t *testing.T) {
		run(t, func() {
			defer func() { _ = recover() }()
			Ordered(sluice.Of(make([]int, 64), 4), 8, func(b sluice.Batch[int]) sluice.Batch[int] { return b })(
				func(sluice.Batch[int]) bool {
					panic("consumer boom")
				})
		})
	})
}

// A panic in f must reach the caller rather than take the process down from a
// worker stack, and it must arrive once even when several workers panic.
func TestParallelPropagatesPanicFromF(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 8, func(sluice.Batch[int]) sluice.Batch[int] {
			panic("from f")
		}))
	}()

	if recovered != "from f" {
		t.Errorf("recovered %v, want %q: the panic must cross back to the caller", recovered, "from f")
	}
}

// The consumer's refusal must reach the source, as everywhere else (S10).
func TestParallelPropagatesEarlyStop(t *testing.T) {
	released := false
	produced := 0
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		defer func() { released = true }()
		for i := range 1000 {
			produced++
			if !yield(sluice.Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	n := 0
	Ordered(src, 4, func(b sluice.Batch[int]) sluice.Batch[int] { return b })(func(sluice.Batch[int]) bool {
		n++
		return false
	})

	if n != 1 {
		t.Errorf("consumer saw %d batches after refusing the first, want 1", n)
	}
	// The pool reads ahead by at most workers batches: that is the bounded
	// buffer, and it must stay bounded.
	if produced > 8 {
		t.Errorf("source produced %d batches, want at most 8: the reader ran away", produced)
	}
	if !released {
		t.Error("the source never unwound after an early stop")
	}
}

// S11 — the in-flight queue is bounded by workers, so the source cannot outrun
// the pool however slow the consumer is.
func TestParallelBoundsReadAhead(t *testing.T) {
	const workers = 4

	var produced atomic.Int64
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		for i := range 200 {
			produced.Add(1)
			if !yield(sluice.Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	consumed := 0
	Ordered(src, workers, func(b sluice.Batch[int]) sluice.Batch[int] { return b })(func(sluice.Batch[int]) bool {
		consumed++
		// Checked mid-flight rather than at the end, and against workers
		// exactly rather than a tolerance. The bound is the queue length, which
		// is a fixed number and not an approximation: allowing workers+1 lets
		// through precisely the off-by-one that would relax the bound, which is
		// the regression this test exists to catch.
		if gap := int(produced.Load()) - consumed; gap > workers {
			t.Errorf("source ran %d batches ahead of the consumer, want at most %d", gap, workers)
			return false
		}
		return consumed < 100
	})
}

func TestParallelNilFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic at construction rather than a later nil dereference")
		}
	}()
	Ordered[int, int](sluice.Empty[int](), 4, nil)
}

// Ordered(1) must be the inline path: no goroutine at all, not a pool of one.
func TestParallelOneIsInline(t *testing.T) {
	before := runtime.NumGoroutine()

	var ran bool
	sluice.Collect(Ordered(sluice.Of([]int{1, 2, 3}, 1), 1, func(b sluice.Batch[int]) sluice.Batch[int] {
		ran = true
		if got := runtime.NumGoroutine(); got > before {
			t.Errorf("Ordered(1) started %d goroutines, want none", got-before)
		}
		return b
	}))
	if !ran {
		t.Error("f never ran")
	}
}

// The consumer may stop while the tail is being drained — after the source has
// run out, with jobs still in flight. That path emits from the queue rather
// than from the source loop, so it is reached only by a consumer that accepts
// everything the source produced and then refuses.
func TestParallelStopDuringTailDrain(t *testing.T) {
	const (
		workers = 4
		batches = 6
	)

	src := make([]int, batches)
	for i := range src {
		src[i] = i
	}

	var live atomic.Int64
	var seen []int

	// Accept every batch the source produced, then refuse: by then the source
	// is exhausted and what remains is the in-flight queue.
	Ordered(sluice.Of(src, 1), workers, func(b sluice.Batch[int]) sluice.Batch[int] {
		live.Add(1)
		defer live.Add(-1)
		return b
	})(func(b sluice.Batch[int]) bool {
		seen = append(seen, b.Items[0])
		return len(seen) < batches-1
	})

	if len(seen) != batches-1 {
		t.Errorf("consumer saw %d batches, want %d", len(seen), batches-1)
	}
	if !slices.IsSorted(seen) {
		t.Errorf("batches arrived as %v, want them in order", seen)
	}
	if n := live.Load(); n != 0 {
		t.Errorf("%d workers still inside f when Ordered returned", n)
	}
}

// Several workers panicking at once must still produce exactly one panic at the
// caller, with nothing left running. A single-panic test cannot show this: the
// interesting case is the race between workers to report, and it only appears
// under repetition.
func TestParallelConcurrentPanics(t *testing.T) {
	const iterations = 100

	for range iterations {
		var live atomic.Int64
		var recovered any

		func() {
			defer func() { recovered = recover() }()
			sluice.Collect(Ordered(sluice.Of(make([]int, 256), 4), 8, func(sluice.Batch[int]) sluice.Batch[int] {
				live.Add(1)
				defer live.Add(-1)
				panic("boom")
			}))
		}()

		if recovered == nil {
			t.Fatal("no panic reached the caller")
		}
		if n := live.Load(); n != 0 {
			t.Fatalf("%d workers still inside f after a panic", n)
		}
	}
}

// A panic in f while the consumer is also panicking: whichever wins, no worker
// may be left running. This is the exit path with the most ways to go wrong,
// since both the deferred shutdown and the re-panic are unwinding at once.
func TestParallelPanicInBothFAndConsumer(t *testing.T) {
	const iterations = 100

	for range iterations {
		var live atomic.Int64

		func() {
			defer func() { _ = recover() }()
			Ordered(sluice.Of(make([]int, 256), 4), 8, func(b sluice.Batch[int]) sluice.Batch[int] {
				live.Add(1)
				defer live.Add(-1)
				if b.Items[0] == 0 {
					panic("f boom")
				}
				return b
			})(func(sluice.Batch[int]) bool { panic("consumer boom") })
		}()

		if n := live.Load(); n != 0 {
			t.Fatalf("%d workers still inside f after a double panic", n)
		}
	}
}

// The S14 composition test: Ordered retains each incoming batch while a
// worker reads it, so it must copy at the hand-off. sluice.Convert is the upstream
// that proves it — its output buffer is rewritten on every batch, which is
// legitimate under the batch-ownership rule. Before the copy existed, this
// exact composition raced under -race and summed to a silently wrong total
// (docs/FIELD-REPORT-cartobuilder.md §10).
func TestParallelCopiesInputBatch(t *testing.T) {
	const n = 1 << 16
	src := make([]int64, n)
	var want int64
	for i := range src {
		src[i] = int64(i)
		want += int64(i)
	}

	conv := sluice.Convert(sluice.Of(src, 1024), func(v int64) int64 { return v })
	sums := Ordered(conv, 4, func(b sluice.Batch[int64]) sluice.Batch[int64] {
		var s int64
		for _, v := range b.Items {
			s += v
		}
		return sluice.Batch[int64]{Items: []int64{s}}
	})

	var got int64
	sums(func(b sluice.Batch[int64]) bool {
		for _, v := range b.Items {
			got += v
		}
		return true
	})

	if got != want {
		t.Fatalf("totals differ: got %d want %d — an input batch was rewritten while a worker read it", got, want)
	}
}

// A panic is never swallowed: when the consumer stops early with batches still
// in flight, their results are discarded but a panic among them still reaches
// the caller. Both exits are covered — the stop while the source is feeding,
// and the stop during the tail drain after it ran out.
func TestParallelEarlyStopStillRaisesInFlightPanic(t *testing.T) {
	tests := []struct {
		name    string
		batches int
	}{
		// Two workers, five batches: batch 0 is emitted to make room for
		// batch 2, with batch 1 in flight, so the refusal happens inside the
		// source loop.
		{"while feeding", 5},
		// Two batches: the source runs out with both in flight, and batch 0
		// is refused in the tail drain with batch 1 still behind it.
		{"during the tail drain", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := make([]int, tt.batches)
			for i := range src {
				src[i] = i
			}
			var live atomic.Int64
			var recovered any
			seen := 0
			func() {
				defer func() { recovered = recover() }()
				Ordered(sluice.Of(src, 1), 2, func(b sluice.Batch[int]) sluice.Batch[int] {
					live.Add(1)
					defer live.Add(-1)
					if b.Items[0] == 1 {
						panic("in flight")
					}
					return b
				})(func(sluice.Batch[int]) bool {
					seen++
					return false
				})
			}()

			if recovered != "in flight" {
				t.Errorf("recovered %v, want %q: a panic in flight at an early stop was swallowed",
					recovered, "in flight")
			}
			if seen != 1 {
				t.Errorf("consumer saw %d batches, want 1", seen)
			}
			if n := live.Load(); n != 0 {
				t.Errorf("%d workers still inside f when Ordered returned", n)
			}
		})
	}
}
