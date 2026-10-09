// Package streamtest is test support shared by the core package's tests and
// the operator packages' tests: a source whose single-pass contract is
// checked, a panic matcher, and a deterministic random source for property
// tests.
//
// It is internal so that it never reaches a user's binary. The core package's
// own tests keep private copies of the same helpers, because an internal test
// of the core cannot import a package that imports the core.
package streamtest

import (
	"errors"
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

// SinglePass returns a source over values in batches of batchSize that fails
// the test if traversed twice, and a function reporting how many values it
// has delivered so far.
func SinglePass(t *testing.T, values []int, batchSize int) (s sluice.Stream[int], consumed func() int) {
	t.Helper()
	var (
		pos       int
		traversed bool
	)
	s = func(yield func(sluice.Batch[int]) bool) {
		if traversed {
			t.Error("the source was traversed twice: it is single-pass")
			return
		}
		traversed = true
		for pos < len(values) {
			end := min(pos+batchSize, len(values))
			b := sluice.Batch[int]{Items: values[pos:end]}
			pos = end
			if !yield(b) {
				return
			}
		}
	}
	return s, func() int { return pos }
}

// IsErr reports whether a recovered panic value is an error matching want.
func IsErr(recovered any, want error) bool {
	err, ok := recovered.(error)
	return ok && errors.Is(err, want)
}

// Rand is a small deterministic generator (a 64-bit LCG with a mixing
// finaliser): the same seed gives the same sequence on every platform, so a
// failing property case is replayable from the seed alone.
type Rand struct{ s uint64 }

// NewRand returns a generator for seed.
func NewRand(seed uint64) *Rand { return &Rand{s: seed*2862933555777941757 + 3037000493} }

// Next returns the next value.
func (r *Rand) Next() uint64 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	x := r.s
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

// Intn returns a value in [0, n); it panics on a non-positive n.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("streamtest: Intn needs a positive bound")
	}
	return int(r.Next() % uint64(n)) //nolint:gosec // G115: n is positive
}

// Chunks returns src as a stream cut at random points, with batches of zero to
// maxBatch elements — empty batches included, since operators must tolerate
// them. The cuts are chosen up front so that the stream is replayable.
func Chunks[T any](src []T, maxBatch int, rng *Rand) sluice.Stream[T] {
	maxBatch = max(maxBatch, 1)
	var cuts []int
	for i := 0; i < len(src); {
		n := rng.Intn(maxBatch + 1)
		i = min(i+n, len(src))
		cuts = append(cuts, i)
	}
	return func(yield func(sluice.Batch[T]) bool) {
		prev := 0
		for _, c := range cuts {
			if !yield(sluice.Batch[T]{Items: src[prev:c]}) {
				return
			}
			prev = c
		}
	}
}

// GoroutinesSettle waits for every goroutine in the current synctest bubble
// to block, then reports how many goroutines exist: the count a test compares
// before and after an operator ran, to prove none outlived it.
func GoroutinesSettle(t *testing.T) int {
	t.Helper()
	synctest.Wait()
	return runtime.NumGoroutine()
}
