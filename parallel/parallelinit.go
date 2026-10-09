package parallel

import (
	"fmt"
	"io"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/batch"
)

// ErrState reports that a [WithState] state factory failed.
//
// It is panicked rather than returned, for the reason [sluice.ErrOverflow] is: an
// iter.Seq has nowhere to put an error, and a state factory that fails is a
// pipeline that cannot run at all — not a datum that is missing. The panic
// value wraps both this sentinel and the factory's error, so a recover
// boundary can test errors.Is against either.
var ErrState = batch.NewSentinel("sluice: WithState state factory failed")

// WithState is [Ordered] with a private state per worker, built once by
// mkState before that worker processes anything.
//
//	enriched := WithState(rows, 4,
//	    func() (*Reader, error) { return OpenReader(path) },
//	    func(r *Reader, b sluice.Batch[Row]) sluice.Batch[Entity] { ... })
//
// It exists for the state that Ordered's contract shuts out: f runs
// concurrently, so a decoder's scratch table, a compressor's dictionary or a
// reader's cache had exactly three options — allocate per batch, serialize
// behind a mutex, or stay out of the fan-out. Per-worker state is the fourth:
// the state is never shared, so it needs no synchronization and may be freely
// reused across the batches its worker processes. f itself must still be safe
// to run concurrently against *other* state — it closes over nothing shared —
// but everything reachable from its W parameter is that worker's alone.
//
// Ordering, shutdown, panic propagation and the S14 input copy are exactly
// [Ordered]'s: same machinery, same guarantees, same measured costs.
//
// What the private state is worth, measured: an f needing a batch-sized
// workspace ran at 532 µs and 150 allocs/op under [Ordered] — the workspace
// allocated on every batch — against 360 µs and 98 under WithState with the
// workspace owned by the worker: −32% time and −45% memory for the same work
// (BenchmarkParallelFreshScratch / BenchmarkParallelInitReusedScratch).
//
// # States are created up front, on the consuming goroutine
//
// All workers' states are built by mkState — sequentially, on the goroutine
// that consumes the stream — when consumption starts, not on the worker
// goroutines. That trade is deliberate: a factory that fails can then panic
// before a single goroutine or resource exists, instead of ferrying a partial
// failure out of a half-started pool. The cost is that mkState runs serially,
// and that a state requiring goroutine affinity — thread-local storage under
// cgo, an OS-thread-locked handle — cannot be built here. Both are rare; the
// failure path being trivially correct is permanent.
//
// A failing mkState panics with an error wrapping [ErrState] and the
// cause. States already created by then are closed before the panic leaves.
//
// # Teardown: a state that implements io.Closer is closed
//
// If W implements [io.Closer], each worker's state is closed exactly once when
// the operator shuts down — after every worker has stopped, on every path out:
// exhaustion, early stop, a panic in f or in the consumer. A state that owns
// nothing needing release implements nothing and pays nothing.
//
// Close's error is discarded. Teardown failures belong to [sluice.Source] at the
// pipeline's edges; mid-pipeline there is no element left to attach one to. A
// state whose Close must be error-checked should be managed by the caller
// around the pipeline instead.
//
// # The output buffer rule is inherited, not lifted
//
// Per-worker state does not make returning a reused output buffer safe: a
// worker can start its next job while its previous result is still waiting in
// the reordering buffer, and would overwrite it. f must return a fresh slice,
// exactly as under [Ordered]. What the private state does make safe is
// everything f reads and scratches *before* producing that output.
//
// # Degenerate arguments
//
// workers of 1 or less is the explicit no-op, like [Ordered]: mkState is
// called exactly once, f is applied inline on the calling goroutine, and the
// state is closed when the stream ends. WithState(s, 1, mk, f) is the way
// to turn the concurrency off, not a slower way to keep it.
//
// WithState panics if mkState or f is nil.
func WithState[A, B, W any](
	s sluice.Stream[A],
	workers int,
	mkState func() (W, error),
	f func(W, sluice.Batch[A]) sluice.Batch[B],
) sluice.Stream[B] {
	if mkState == nil {
		panic("sluice: WithState requires a non-nil mkState function")
	}
	if f == nil {
		panic("sluice: WithState requires a non-nil f function")
	}
	if workers <= 1 {
		return func(yield func(sluice.Batch[B]) bool) {
			w, err := mkState()
			if err != nil {
				panic(fmt.Errorf("%w: %w", ErrState, err))
			}
			defer closeState(w)
			s(func(b sluice.Batch[A]) bool { return yield(f(w, b)) })
		}
	}
	return func(yield func(sluice.Batch[B]) bool) {
		states := make([]W, 0, workers)
		// Deferred before the states exist, so a factory that fails after
		// creating some still releases them, and so does every path out of
		// runParallel below — which has waited for its workers by the time
		// this runs, so no state is still in use when it is closed.
		defer func() {
			for _, w := range states {
				closeState(w)
			}
		}()
		for range workers {
			w, err := mkState()
			if err != nil {
				panic(fmt.Errorf("%w: %w", ErrState, err))
			}
			states = append(states, w)
		}

		fs := make([]func(sluice.Batch[A]) sluice.Batch[B], workers)
		for i := range fs {
			w := states[i]
			fs[i] = func(b sluice.Batch[A]) sluice.Batch[B] { return f(w, b) }
		}
		runParallel(s, fs, yield)
	}
}

// closeState closes a worker state that implements io.Closer, and does nothing
// for one that does not. The type assertion runs once per state per pipeline —
// teardown, not the hot path.
func closeState[W any](w W) {
	if c, ok := any(w).(io.Closer); ok {
		_ = c.Close()
	}
}
