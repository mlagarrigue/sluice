// Package batch holds what the core and its operator packages share and no
// caller should see: buffer reuse on the hot path, and the convention by which
// an operator raises a named error from inside an iterator.
//
// It is internal so that sharing it never widens the public API. The core
// package imports it, the operator packages (join, window, parallel, distinct)
// import it, and nothing outside the module can.
package batch

import "slices"

// Grow returns a slice of exactly n elements backed by buf's array, reusing it
// when it is large enough.
//
// It is for operators that write by index and so need the length up front,
// where append's growth does not apply. Reusing the buffer across batches makes
// a source whose batches get bigger cost log(n) reallocations rather than one
// per new high-water mark.
//
// The contents are not zeroed: every returned element is expected to be
// written.
//
// Nor is the tail cleared when a batch shrinks, and that is deliberate. A
// reused buffer whose length drops leaves the previous batch's values live
// beyond the new length — for a T holding pointers, that is retention. It does
// not leak here, because a batch is documented as valid only for the duration
// of the call that receives it: nothing downstream holds the buffer, so the
// whole array is released when the operator is. Clearing per batch would cost a
// pass over the buffer on the hot path to protect against a caller who is
// already outside the contract.
func Grow[T any](buf []T, n int) []T {
	return slices.Grow(buf[:0], n)[:n]
}

// Reserve returns an empty slice backed by buf's array with room for at least
// n elements, ready to be filled with append.
//
// It is [Grow]'s counterpart for operators whose output length is not known up
// front: reserving the input's length keeps append from regrowing the buffer
// from empty on every batch, while leaving it free to grow further when the
// output turns out to be longer.
func Reserve[T any](buf []T, n int) []T {
	return slices.Grow(buf[:0], n)
}
