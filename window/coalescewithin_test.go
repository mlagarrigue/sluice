package window

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// Full batches are emitted on size, exactly like sluice.Coalesce, whatever the
// deadline: the deadline is a ceiling on waiting, not a pace.
func TestCoalesceWithinFillsToSize(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6, 7}
	var sizes []int
	var got []int
	CoalesceWithin(sluice.Of(src, 1), 3, time.Hour)(func(b sluice.Batch[int]) bool {
		sizes = append(sizes, b.Len())
		got = append(got, b.Items...)
		return true
	})
	if want := []int{3, 3, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
	if !slices.Equal(got, src) {
		t.Errorf("data altered: %v, want %v", got, src)
	}
}

// The case the operator exists for: a source that stops producing before the
// batch is full. The partial batch must come out on the deadline rather than
// wait for elements that are not coming.
func TestCoalesceWithinFlushesOnDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Two elements, then a long silence: with a size of 1000 and no deadline
		// this would hang until the source ends.
		release := make(chan struct{})
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			if !yield(sluice.Batch[int]{Items: []int{1, 2}}) {
				return
			}
			<-release // the source goes quiet
			yield(sluice.Batch[int]{Items: []int{3}})
		})

		var first []int
		start := time.Now()
		n := 0
		CoalesceWithin(src, 1000, 20*time.Millisecond)(func(b sluice.Batch[int]) bool {
			n++
			if n == 1 {
				first = slices.Clone(b.Items)
				close(release) // let the source finish once we have the timed batch
			}
			return true
		})

		if want := []int{1, 2}; !slices.Equal(first, want) {
			t.Errorf("first batch = %v, want %v — the deadline did not flush it", first, want)
		}
		if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
			t.Errorf("flushed after %v, want at least the 20ms deadline", elapsed)
		}
	})
}

// The deadline runs from the first element of a batch, not from the last:
// what is bounded is how long an element waits, and the oldest waits longest.
// A trickle of elements must not push the flush back indefinitely.
func TestCoalesceWithinDeadlineRunsFromFirstElement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const deadline = 100 * time.Millisecond
		const total = 6
		release := make(chan struct{})
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			// One element every 50ms: six of them span the 100ms deadline
			// several times over, so a deadline reset per arrival would not
			// fire until after the last one.
			for i := range total {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Hold the stream open until the first flush has been judged: with
			// the source still running, the end-of-stream flush cannot stand in
			// for the timed one.
			<-release
		})

		flushed := make(chan int, total)
		go func() {
			CoalesceWithin(src, 1000, deadline)(func(b sluice.Batch[int]) bool {
				flushed <- b.Len()
				return true
			})
			close(flushed)
		}()

		select {
		case n := <-flushed:
			if n == 0 {
				t.Error("flushed an empty batch")
			}
			if n >= total {
				t.Errorf("the first flush carried all %d elements: the deadline is being reset by each arrival", n)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no batch was flushed while the source was still trickling")
		}
		close(release)
		for range flushed {
		}
	})
}

// sluice.Empty batches hold the upstream cadence and carry no element, so they must
// neither arm the deadline nor produce an empty output batch.
func TestCoalesceWithinIgnoresEmptyBatches(t *testing.T) {
	src := sluice.Filter(sluice.Of([]int{1, 2, 3, 4}, 1), func(int) bool { return false })
	n := 0
	CoalesceWithin(src, 10, 20*time.Millisecond)(func(sluice.Batch[int]) bool {
		n++
		return true
	})
	if n != 0 {
		t.Errorf("emitted %d batches, want none — empty batches carry no element", n)
	}
}

// An input batch larger than size is cut into several, and the remainder
// starts a new deadline rather than inheriting the previous one.
func TestCoalesceWithinSplitsLargeBatches(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var sizes []int
	var got []int
	CoalesceWithin(sluice.Of(src, 10), 3, time.Hour)(func(b sluice.Batch[int]) bool {
		sizes = append(sizes, b.Len())
		got = append(got, b.Items...)
		return true
	})
	if want := []int{3, 3, 3, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
	if !slices.Equal(got, src) {
		t.Errorf("data altered: %v, want %v", got, src)
	}
}

// S14: composed downstream of a buffer-reusing operator, every element must
// arrive intact — the producer copies at the hand-off.
func TestCoalesceWithinCopiesInputBatch(t *testing.T) {
	const n = 1 << 13
	src := make([]int64, n)
	var want int64
	for i := range src {
		src[i] = int64(i)
		want += int64(i)
	}
	conv := sluice.Convert(sluice.Of(src, 256), func(v int64) int64 { return v })

	var got int64
	count := 0
	CoalesceWithin(conv, 512, time.Hour)(func(b sluice.Batch[int64]) bool {
		for _, v := range b.Items {
			got += v
		}
		count += b.Len()
		return true
	})
	if got != want || count != n {
		t.Fatalf("got sum %d over %d elements, want %d over %d", got, count, want, n)
	}
}

// An early stop releases the producer goroutine and unwinds the source
// promptly, discarding what was accumulated (S6, S10).
func TestCoalesceWithinEarlyStopReleasesProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		finalized := false
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			defer func() { finalized = true }()
			for i := range 10_000 {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
			}
		})

		n := 0
		CoalesceWithin(src, 4, time.Hour)(func(sluice.Batch[int]) bool {
			n++
			return n < 3
		})

		if !finalized {
			t.Error("the source's deferred cleanup did not run")
		}
		if got := streamtest.GoroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after an early stop", before, got)
		}
	})
}

// A panic in the source arrives once, on the consumer's goroutine, with the
// producer already shut down.
func TestCoalesceWithinPropagatesSourcePanic(t *testing.T) {
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
			sluice.Collect(CoalesceWithin(src, 100, time.Hour))
			t.Error("the source's panic never reached the consumer")
		}()

		if got := streamtest.GoroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a source panic", before, got)
		}
	})
}

// A panic in the consumer must not strand the producer.
func TestCoalesceWithinConsumerPanicReleasesProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		finalized := false
		src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
			defer func() { finalized = true }()
			for i := range 10_000 {
				if !yield(sluice.Batch[int]{Items: []int{i}}) {
					return
				}
			}
		})

		func() {
			defer func() { _ = recover() }()
			CoalesceWithin(src, 2, time.Hour)(func(sluice.Batch[int]) bool { panic("consumer boom") })
		}()

		if !finalized {
			t.Error("the source was left running after a consumer panic")
		}
		if got := streamtest.GoroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a consumer panic", before, got)
		}
	})
}

func TestCoalesceWithinArgumentValidation(t *testing.T) {
	tests := []struct {
		name   string
		within time.Duration
	}{
		{"zero deadline", 0},
		{"negative deadline", -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			CoalesceWithin(sluice.Empty[int](), 10, tt.within)
		})
	}
}

// A size of zero or less means sluice.DefaultBatchSize, like sluice.Coalesce.
func TestCoalesceWithinDefaultSize(t *testing.T) {
	src := make([]int, sluice.DefaultBatchSize+1)
	var sizes []int
	CoalesceWithin(sluice.Of(src, 1), 0, time.Hour)(func(b sluice.Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{sluice.DefaultBatchSize, 1}; !slices.Equal(sizes, want) {
		t.Errorf("size<=0 must mean DefaultBatchSize: got %v, want %v", sizes, want)
	}
}

// A panic is never swallowed: the source panicking after the consumer stopped
// still reaches the caller, whichever flush the consumer refused — a full
// batch, the deadline's partial one, or the final one at the end of the
// upstream. The deadline case is also the only test of a refusal on that path.
func TestCoalesceWithinEarlyStopStillRaisesSourcePanic(t *testing.T) {
	tests := []struct {
		name   string
		size   int
		within time.Duration
		// src yields its batches, waits for the refusal when asked to, and
		// then panics or not.
		items     []int
		awaitStop bool
		want      []int
	}{
		// Two elements fill a batch of 2: refused on size.
		{"on size", 2, time.Hour, []int{1, 2}, false, []int{1, 2}},
		// One element under a short deadline, the source holding back until
		// the consumer refused it: refused on the deadline.
		{"on deadline", 100, time.Millisecond, []int{1}, true, []int{1}},
		// One element and the end of the upstream: refused on the final flush.
		{"on the final flush", 100, time.Hour, []int{1}, false, []int{1}},
	}
	for _, tt := range tests {
		for _, panics := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/panic %v", tt.name, panics), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					before := runtime.NumGoroutine()
					refused := make(chan struct{})
					src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
						yield(sluice.Batch[int]{Items: tt.items})
						if tt.awaitStop {
							<-refused
						}
						if panics {
							panic("source boom")
						}
					})

					var seen [][]int
					var recovered any
					func() {
						defer func() { recovered = recover() }()
						CoalesceWithin(src, tt.size, tt.within)(func(b sluice.Batch[int]) bool {
							seen = append(seen, slices.Clone(b.Items))
							close(refused)
							return false
						})
					}()

					if len(seen) != 1 || !slices.Equal(seen[0], tt.want) {
						t.Errorf("consumer saw %v, want exactly [%v]", seen, tt.want)
					}
					want := any(nil)
					if panics {
						want = "source boom"
					}
					if recovered != want {
						t.Errorf("recovered %v, want %v", recovered, want)
					}
					if got := streamtest.GoroutinesSettle(t); got > before {
						t.Errorf("%d goroutines before, %d after", before, got)
					}
				})
			})
		}
	}
}

// The accumulation buffer is claimed on the first element, as in sluice.Coalesce: a
// large configured size must cost nothing on a stream that yields nothing.
// Measured in bytes rather than allocations, since the goroutine and channels
// are paid either way and the buffer is the one allocation that scales.
func TestCoalesceWithinEmptyStreamClaimsNoBuffer(t *testing.T) {
	const size = 1 << 20 // 8 MiB if claimed eagerly for int64
	run := func() {
		CoalesceWithin(sluice.Empty[int64](), size, time.Hour)(func(sluice.Batch[int64]) bool {
			t.Fatal("an empty stream yielded a batch")
			return false
		})
	}
	run() // warm up whatever the runtime claims once
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got >= size {
		t.Errorf("CoalesceWithin on an empty stream allocated %d bytes, want under %d", got, size)
	}
}

func BenchmarkCoalesceWithinEmptyStream(b *testing.B) {
	const size = 1 << 20
	b.ReportAllocs()
	for b.Loop() {
		CoalesceWithin(sluice.Empty[int64](), size, time.Hour)(func(sluice.Batch[int64]) bool {
			return true
		})
	}
}

// The first garbage collection of the process starts the runtime's mark
// worker goroutines, which runtime.NumGoroutine counts. Running one up
// front keeps the goroutine baselines in this package from blaming a leak
// on the runtime when that first cycle happens to land inside a test.
func init() { runtime.GC() }
