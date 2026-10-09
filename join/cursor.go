package join

import (
	"github.com/mlagarrigue/sluice"
)

// cursor reads one input of a merge join element by element, checking as it
// goes that the keys never go backwards.
//
// The current element and its key are cached: the merge loop peeks the same
// element several times before consuming it — once to compare, once more to
// gather a run — and extracting the key each time was measured at 44% of the
// operator's runtime. The cache is invalidated by [cursor.next], the only thing
// that moves the cursor.
type cursor[T, K any] struct {
	pull  func() (sluice.Batch[T], bool)
	stop  func()
	batch []T
	pos   int
	done  bool
	key   func(T) K
	cmp   func(K, K) int

	cur     T
	curKey  K
	cached  bool
	lastKey K
	hasLast bool
	run     []T // scratch for joinEqual's many-to-many run, reused between calls
}

func newCursor[T any, K any](s sluice.Stream[T], key func(T) K, cmp func(K, K) int) *cursor[T, K] {
	n, stop := pull(s)
	return &cursor[T, K]{pull: n, stop: stop, key: key, cmp: cmp}
}

// peek returns the current element and its key without consuming it. Repeated
// calls return the cached value rather than re-extracting the key.
//
// Upstream operators may emit empty batches — [sluice.Filter] does, to keep the
// cadence — so the advance loops rather than testing once.
func (c *cursor[T, K]) peek() (elem T, key K, ok bool) {
	if c.cached {
		return c.cur, c.curKey, true
	}
	for c.pos >= len(c.batch) {
		if c.done {
			var (
				zeroT T
				zeroK K
			)
			return zeroT, zeroK, false
		}
		b, ok := c.pull()
		if !ok {
			c.done = true
			continue
		}
		c.batch, c.pos = b.Items, 0
	}

	v := c.batch[c.pos]
	k := c.key(v)

	// Check monotonicity as each element is first seen, so every element is
	// checked exactly once whichever path goes on to consume it.
	if c.hasLast && c.cmp(c.lastKey, k) > 0 {
		panic(sluice.ErrUnsorted)
	}
	c.lastKey, c.hasLast = k, true

	c.cur, c.curKey, c.cached = v, k, true
	return v, k, true
}

// next consumes the current element, invalidating the cache.
func (c *cursor[T, K]) next() {
	c.pos++
	c.cached = false
}

// leftInBatch reports how many elements remain in the current batch after the
// cached one, without touching the source.
func (c *cursor[T, K]) leftInBatch() int { return len(c.batch) - c.pos }

// takeChecked consumes and returns the next element of the current batch,
// running the same monotonicity check as [cursor.peek] but skipping its cache:
// the caller is draining, so nothing will peek the element again. The caller
// guarantees availability via [cursor.leftInBatch]; taking past the batch
// panics on the slice index.
func (c *cursor[T, K]) takeChecked() T {
	v := c.batch[c.pos]
	k := c.key(v)
	if c.hasLast && c.cmp(c.lastKey, k) > 0 {
		panic(sluice.ErrUnsorted)
	}
	c.lastKey, c.hasLast = k, true
	c.pos++
	c.cached = false
	return v
}

// aloneInBatch reports whether the current element is the last one under key k
// that this batch can settle — that is, whether the next element is in the same
// batch and carries a different key.
//
// It answers without moving the cursor and without crossing a batch boundary,
// which is what makes it safe: a lookahead that refills from the source would
// replace the current batch and leave nothing to come back to. When the current
// element is the last of its batch it returns false, and the caller falls back
// to the general path — a correct answer, just not the cheap one, on one
// element in [sluice.DefaultBatchSize].
//
// It must be called with an element available, which the equal-keys branch has
// just established by peeking.
func (c *cursor[T, K]) aloneInBatch(k K) bool {
	next := c.pos + 1
	if next >= len(c.batch) {
		return false // the answer is in the next batch; let joinEqual fetch it
	}
	return c.cmp(c.key(c.batch[next]), k) != 0
}

// peekKey returns the current element if one is available and it carries key
// k, without consuming it.
func (c *cursor[T, K]) peekKey(k K) (T, bool) {
	v, vk, ok := c.peek()
	if !ok || c.cmp(vk, k) != 0 {
		var zero T
		return zero, false
	}
	return v, true
}

// emitter batches rows on their way out, so a merge join produces full batches
// rather than one-element ones.
type emitter[T any] struct {
	yield func(sluice.Batch[T]) bool
	buf   []T
}

func newEmitter[T any](yield func(sluice.Batch[T]) bool) *emitter[T] {
	return &emitter[T]{yield: yield}
}

// push adds a row, flushing when the buffer fills. It reports false once the
// consumer has stopped.
func (e *emitter[T]) push(v T) bool {
	if e.buf == nil {
		// Claimed on the first row rather than in the constructor — the
		// package's convention (see sluice.Coalesce): a join that emits nothing
		// allocates nothing.
		e.buf = make([]T, 0, sluice.DefaultBatchSize)
	}
	e.buf = append(e.buf, v)
	if len(e.buf) < sluice.DefaultBatchSize {
		return true
	}
	ok := e.yield(sluice.Batch[T]{Items: e.buf})
	e.buf = e.buf[:0]
	return ok
}

// flush emits what is left, if anything, and reports whether the consumer took
// it. Today's callers return immediately either way, but emitter is the shared
// way for an N-to-1 operator to batch its output: the next one to have work
// after the flush would silently lose the refusal if the result were dropped
// here. Returning it costs nothing and keeps that from being a new bug.
func (e *emitter[T]) flush() bool {
	if len(e.buf) == 0 {
		return true
	}
	ok := e.yield(sluice.Batch[T]{Items: e.buf})
	e.buf = e.buf[:0]
	return ok
}
