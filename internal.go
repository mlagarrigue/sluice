package sluice

import "iter"

// pull turns a Stream into a pull-based reader. It exists to name the
// conversion once: Stream is a defined type over iter.Seq, so every call site
// would otherwise repeat iter.Pull(iter.Seq[Batch[T]](s)).
//
// The caller must call stop on every path, including panics — an unexhausted
// sequence whose stop is never called leaks its coroutine for the life of the
// process (guarantee S9).
func pull[T any](s Stream[T]) (next func() (Batch[T], bool), stop func()) {
	return iter.Pull(iter.Seq[Batch[T]](s))
}
