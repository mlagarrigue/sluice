package parallel

import (
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

func TestAsync(t *testing.T) {
	src := make([]int, 1000)
	for i := range src {
		src[i] = i
	}
	for _, depth := range []int{1, 2, 8} {
		got := sluice.Collect(Async(sluice.Of(src, 32), depth))
		if !slices.Equal(got, src) {
			t.Errorf("depth %d: data altered or reordered", depth)
		}
	}
}

// A depth of zero or less is the documented no-op: the very stream, no wrapper.
func TestAsyncZeroDepthIsIdentity(t *testing.T) {
	before := runtime.NumGoroutine()
	want := []int{1, 2, 3}
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		// Probed from inside the source, while it is being driven: under a
		// real wrapper this closure runs on a spawned producer while the
		// consumer's goroutine waits on the hand-off, so the count rises
		// here deterministically. Sampled after the stream returns — as
		// this test once did — the producer is already joined and the
		// assertion can never fail.
		if now := runtime.NumGoroutine(); now > before {
			t.Errorf("the no-op spawned a goroutine: %d -> %d", before, now)
		}
		for _, v := range want {
			if !yield(sluice.Batch[int]{Items: []int{v}}) {
				return
			}
		}
	})
	got := sluice.Collect(Async(src, 0))
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// sluice.Empty batches must cross the boundary: downstream keeps the upstream
// cadence, exactly as with sluice.Filter.
func TestAsyncKeepsEmptyBatchCadence(t *testing.T) {
	filtered := sluice.Filter(sluice.Of([]int{1, 2, 3, 4}, 2), func(int) bool { return false })
	n := 0
	Async(filtered, 2)(func(b sluice.Batch[int]) bool {
		if b.Len() != 0 {
			t.Errorf("unexpected non-empty batch %v", b.Items)
		}
		n++
		return true
	})
	if n != 2 {
		t.Errorf("saw %d batches, want 2 empty ones", n)
	}
}

// The point of the operator: the producer runs ahead of a slow consumer, up
// to its depth of slack. The consumer takes one batch and then holds; the
// producer must keep going without it.
func TestAsyncProducerRunsAhead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var produced atomic.Int64
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			for i := range 100 {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
				produced.Add(1)
			}
		})

		const depth = 4
		taken := 0
		Async(src, depth)(func(sluice.Batch[int]) bool {
			taken++
			if taken > 1 {
				return false
			}
			// Hold the first batch until the producer has gone as far as it
			// can: synctest.Wait returns once it is blocked on the full buffer.
			synctest.Wait()
			if produced.Load() < depth {
				t.Errorf("producer stalled at %d batches while the consumer held; want >= %d", produced.Load(), depth)
			}
			return true
		})
	})
}

// The S14 composition: Async downstream of a buffer-reusing operator must be
// race-free and correct — the producer copies at the hand-off.
func TestAsyncCopiesInputBatch(t *testing.T) {
	const n = 1 << 14
	src := make([]int64, n)
	var want int64
	for i := range src {
		src[i] = int64(i)
		want += int64(i)
	}
	conv := sluice.Convert(sluice.Of(src, 512), func(v int64) int64 { return v })

	var got int64
	Async(conv, 4)(func(b sluice.Batch[int64]) bool {
		for _, v := range b.Items {
			got += v
		}
		return true
	})
	if got != want {
		t.Fatalf("totals differ: got %d want %d — a batch was rewritten while retained", got, want)
	}
}

// An early stop releases the producer wherever it is parked — on a full data
// channel or on the free list — and the source's deferred cleanup runs before
// Async returns (S6, S10).
func TestAsyncEarlyStopReleasesProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		finalized := false
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			defer func() { finalized = true }()
			for i := range 1000 {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
			}
		})

		n := 0
		Async(src, 2)(func(sluice.Batch[int]) bool {
			n++
			return n < 3
		})

		if !finalized {
			t.Error("the source's deferred cleanup did not run before Async returned")
		}
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after an early stop", before, got)
		}
	})
}

// A panic in the source arrives once, on the consumer's goroutine, after the
// producer has been shut down.
func TestAsyncPropagatesSourcePanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			yield(sluice.Batch[int]{Items: []int{1}})
			panic("source boom")
		})

		func() {
			defer func() {
				if r := recover(); r != "source boom" {
					t.Errorf("recovered %v, want the source's panic", r)
				}
			}()
			sluice.Collect(Async(src, 2))
			t.Error("the source's panic never reached the consumer")
		}()

		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a source panic", before, got)
		}
	})
}

// A panic in the consumer must not strand the producer: the deferred shutdown
// runs during the unwind.
func TestAsyncConsumerPanicReleasesProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		finalized := false
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			defer func() { finalized = true }()
			for i := range 1000 {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
			}
		})

		func() {
			defer func() { _ = recover() }()
			Async(src, 2)(func(sluice.Batch[int]) bool { panic("consumer boom") })
		}()

		if !finalized {
			t.Error("the source was left running after a consumer panic")
		}
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a consumer panic", before, got)
		}
	})
}

// A panic is never swallowed: a source that panics while it unwinds from the
// consumer's refusal still reaches the caller, after the producer has been
// shut down.
func TestAsyncEarlyStopStillRaisesSourcePanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			for i := 0; ; i++ {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					panic("source boom")
				}
			}
		})

		seen := 0
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			Async(src, 2)(func(sluice.Batch[int]) bool {
				seen++
				return false
			})
		}()

		if recovered != "source boom" {
			t.Errorf("recovered %v, want the source's panic: it was swallowed by the early stop", recovered)
		}
		if seen != 1 {
			t.Errorf("consumer saw %d batches after refusing the first, want 1", seen)
		}
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after", before, got)
		}
	})
}
