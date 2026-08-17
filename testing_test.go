package sluice

import "testing"

// singlePass returns a Stream that can be traversed exactly once, and a
// function reporting how many elements it produced.
//
// Most sources the framework targets — a database cursor, a network read, an
// io.Reader — cannot be replayed. Tests built on [Of] silently hide any
// operator that consumes its source more than once, because replaying a slice
// yields the same values again. This helper turns that silent data loss into a
// loud failure: a second traversal calls t.Fatal instead of returning nothing.
func singlePass(t *testing.T, values []int, batchSize int) (s Stream[int], consumed func() int) {
	t.Helper()

	var (
		pos       int
		traversed bool
	)
	s = func(yield func(Batch[int]) bool) {
		if traversed {
			t.Fatal("the source was traversed twice: it is single-pass")
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
