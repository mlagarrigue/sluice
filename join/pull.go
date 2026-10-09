package join

import (
	"iter"

	"github.com/mlagarrigue/sluice"
)

// pull turns a sluice.Stream into a pull-based reader. The caller must call stop on
// every path, including panics: an unexhausted sequence whose stop is never
// called leaks its coroutine for the life of the process.
func pull[T any](s sluice.Stream[T]) (next func() (sluice.Batch[T], bool), stop func()) {
	return iter.Pull(iter.Seq[sluice.Batch[T]](s))
}
