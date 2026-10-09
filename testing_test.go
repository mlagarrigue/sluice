package sluice

import (
	"errors"
	"testing"
)

// singlePass returns a Stream that can be traversed exactly once, and a
// function reporting how many elements it produced.
//
// Most sources the library targets — a database cursor, a network read, an
// io.Reader — cannot be replayed. Tests built on [Of] silently hide any
// operator that consumes its source more than once, because replaying a slice
// yields the same values again. This helper turns that silent data loss into a
// loud failure: a second traversal fails the test instead of returning nothing.
// It fails with t.Error, not t.Fatal: the stream may run inside a pulled
// coroutine, and t.Fatal is only legal on the test goroutine.
func singlePass(t *testing.T, values []int, batchSize int) (s Stream[int], consumed func() int) {
	t.Helper()

	var (
		pos       int
		traversed bool
	)
	s = func(yield func(Batch[int]) bool) {
		if traversed {
			t.Error("the source was traversed twice: it is single-pass")
			return
		}
		traversed = true
		for pos < len(values) {
			end := min(pos+batchSize, len(values))
			b := Batch[int]{Items: values[pos:end]}
			pos = end
			if !yield(b) {
				return
			}
		}
	}
	return s, func() int { return pos }
}

// isErr reports whether a recovered value is the given sentinel, unwrapping
// through errors.Is rather than comparing directly: a sentinel panicked today
// may be panicked wrapped tomorrow, and a test that compares with == would
// pass while the boundary it guards stopped working.
func isErr(recovered any, want error) bool {
	err, ok := recovered.(error)
	return ok && errors.Is(err, want)
}
