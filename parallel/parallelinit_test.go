package parallel

import (
	"errors"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

// closableState is a worker state that counts its Close calls, so the tests
// can assert teardown runs exactly once per state on every exit path.
type closableState struct {
	id      int
	closed  *atomic.Int64
	scratch []int // non-thread-safe on purpose: isolation is the contract
}

func (c *closableState) Close() error {
	c.closed.Add(1)
	return nil
}

func TestParallelInit(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6, 7, 8}
	var made, closed atomic.Int64

	mk := func() (*closableState, error) {
		return &closableState{id: int(made.Add(1)), closed: &closed}, nil
	}
	double := func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
		// The per-worker scratch is reused across this worker's batches with
		// no synchronization: -race proves the isolation claim.
		w.scratch = append(w.scratch[:0], b.Items...)
		out := make([]int, len(w.scratch))
		for i, v := range w.scratch {
			out[i] = v * 2
		}
		return sluice.Batch[int]{Items: out}
	}

	got := sluice.Collect(WithState(sluice.Of(src, 2), 4, mk, double))
	want := []int{2, 4, 6, 8, 10, 12, 14, 16}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if made.Load() != 4 {
		t.Errorf("mkState ran %d times, want 4 — once per worker", made.Load())
	}
	if closed.Load() != made.Load() {
		t.Errorf("%d states made but %d closed", made.Load(), closed.Load())
	}
}

// WithState(s, 1, mk, f) and Ordered(s, 1, wrap(f)) must agree: the inline
// no-op is the same path with a state threaded through.
func TestParallelInitOneMatchesParallel(t *testing.T) {
	src := []int{3, 1, 4, 1, 5}
	var closed atomic.Int64
	mk := func() (*closableState, error) {
		return &closableState{closed: &closed}, nil
	}
	f := func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
		out := make([]int, len(b.Items))
		for i, v := range b.Items {
			out[i] = v + 10
		}
		return sluice.Batch[int]{Items: out}
	}

	init := sluice.Collect(WithState(sluice.Of(src, 2), 1, mk, f))
	plain := sluice.Collect(Ordered(sluice.Of(src, 2), 1, func(b sluice.Batch[int]) sluice.Batch[int] {
		return f(&closableState{closed: &closed}, b)
	}))
	if !slices.Equal(init, plain) {
		t.Errorf("inline WithState %v differs from Ordered %v", init, plain)
	}
	if closed.Load() == 0 {
		t.Error("the inline path never closed its state")
	}
}

// A failing factory must panic with an error wrapping ErrState and the
// cause, close the states already created, and leak nothing — the failure
// happens before any worker goroutine exists.
func TestParallelInitFactoryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		causeErr := errors.New("no such file")
		var made, closed atomic.Int64

		mk := func() (*closableState, error) {
			if made.Load() == 2 {
				return nil, causeErr
			}
			return &closableState{id: int(made.Add(1)), closed: &closed}, nil
		}

		func() {
			defer func() {
				r := recover()
				err, ok := r.(error)
				if !ok {
					t.Fatalf("panicked with %v, want an error", r)
				}
				if !errors.Is(err, ErrState) {
					t.Errorf("errors.Is(err, ErrState) is false for %v", err)
				}
				if !errors.Is(err, causeErr) {
					t.Errorf("the cause is not reachable through errors.Is: %v", err)
				}
			}()
			WithState(sluice.Of([]int{1, 2, 3}, 1), 4, mk, func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
				return b
			})(func(sluice.Batch[int]) bool { return true })
		}()

		if closed.Load() != 2 {
			t.Errorf("%d states closed, want the 2 created before the failure", closed.Load())
		}
		if got := goroutinesSettle(t); got > before {
			t.Errorf("%d goroutines before, %d after a factory failure", before, got)
		}
	})
}

// The inline path — one worker — fails the same way: the factory's error
// panics wrapped in ErrState, before the source is read or f is called.
func TestParallelInitFactoryFailureInline(t *testing.T) {
	causeErr := errors.New("no such file")
	read, called := false, false
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		read = true
		yield(sluice.Batch[int]{Items: []int{1}})
	})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		WithState(src, 1,
			func() (*closableState, error) { return nil, causeErr },
			func(*closableState, sluice.Batch[int]) sluice.Batch[int] {
				called = true
				return sluice.Batch[int]{}
			})(func(sluice.Batch[int]) bool { return true })
	}()

	err, ok := recovered.(error)
	if !ok {
		t.Fatalf("panicked with %v, want an error", recovered)
	}
	if !errors.Is(err, ErrState) || !errors.Is(err, causeErr) {
		t.Errorf("panic %v does not wrap both ErrState and the cause", err)
	}
	if read || called {
		t.Errorf("source read %v, f called %v: neither may run without a state", read, called)
	}
}

// Every exit path must close every state exactly once, after the workers have
// stopped: exhaustion is covered above, this table covers early stop, a panic
// in f, and a panic in the consumer.
func TestParallelInitClosesStatesOnEveryPath(t *testing.T) {
	tests := []struct {
		name string
		run  func(s sluice.Stream[int], mk func() (*closableState, error))
	}{
		{"early stop", func(s sluice.Stream[int], mk func() (*closableState, error)) {
			n := 0
			WithState(s, 4, mk, func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
				return b
			})(func(sluice.Batch[int]) bool {
				n++
				return n < 2
			})
		}},
		{"panic in f", func(s sluice.Stream[int], mk func() (*closableState, error)) {
			defer func() { _ = recover() }()
			WithState(s, 4, mk, func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
				panic("f boom")
			})(func(sluice.Batch[int]) bool { return true })
		}},
		{"panic in consumer", func(s sluice.Stream[int], mk func() (*closableState, error)) {
			defer func() { _ = recover() }()
			WithState(s, 4, mk, func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] {
				return b
			})(func(sluice.Batch[int]) bool { panic("consumer boom") })
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				before := runtime.NumGoroutine()
				var made, closed atomic.Int64
				mk := func() (*closableState, error) {
					return &closableState{id: int(made.Add(1)), closed: &closed}, nil
				}
				tt.run(sluice.Of(make([]int, 256), 4), mk)
				if made.Load() == 0 {
					t.Fatal("no state was ever created")
				}
				if closed.Load() != made.Load() {
					t.Errorf("%d states made, %d closed", made.Load(), closed.Load())
				}
				if got := goroutinesSettle(t); got > before {
					t.Errorf("%d goroutines before, %d after", before, got)
				}
			})
		})
	}
}

// The S10 contract: an early stop reaches the source promptly and its deferred
// cleanup runs before WithState returns.
func TestParallelInitPropagatesEarlyStop(t *testing.T) {
	finalized := false
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		defer func() { finalized = true }()
		for i := range 100 {
			if !yield(sluice.Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})
	var closed atomic.Int64
	mk := func() (*closableState, error) { return &closableState{closed: &closed}, nil }

	n := 0
	WithState(src, 4, mk, func(w *closableState, b sluice.Batch[int]) sluice.Batch[int] { return b })(
		func(sluice.Batch[int]) bool {
			n++
			return n < 3
		})

	if !finalized {
		t.Error("the source's deferred cleanup did not run")
	}
}

// The S14 composition: WithState downstream of a buffer-reusing upstream
// must be race-free and sum correctly — the input copy is inherited from the
// shared machinery, and this pins it for the second entry point.
func TestParallelInitCopiesInputBatch(t *testing.T) {
	const n = 1 << 14
	src := make([]int64, n)
	var want int64
	for i := range src {
		src[i] = int64(i)
		want += int64(i)
	}

	conv := sluice.Convert(sluice.Of(src, 512), func(v int64) int64 { return v })
	mk := func() (*[]int64, error) { s := make([]int64, 0, 512); return &s, nil }
	sums := WithState(conv, 4, mk, func(w *[]int64, b sluice.Batch[int64]) sluice.Batch[int64] {
		// The worker's scratch absorbs the batch — reused, unsynchronized —
		// then a fresh single-element result goes out, per the output rule.
		*w = append((*w)[:0], b.Items...)
		var s int64
		for _, v := range *w {
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
		t.Fatalf("totals differ: got %d want %d", got, want)
	}
}

func TestParallelInitNilArguments(t *testing.T) {
	assertPanics := func(name string, f func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", name)
			}
		}()
		f()
	}
	assertPanics("nil mkState", func() {
		WithState[int, int, int](sluice.Empty[int](), 2, nil, func(int, sluice.Batch[int]) sluice.Batch[int] { return sluice.Batch[int]{} })
	})
	assertPanics("nil f", func() {
		WithState[int, int, int](sluice.Empty[int](), 2, func() (int, error) { return 0, nil }, nil)
	})
}
