package distinct

import "github.com/mlagarrigue/sluice"

// By drops elements whose key was among the last n distinct keys seen.
//
//	unique := By(orders, 1000, func(o Order) string { return o.Ref }, sluice.DropOldest)
//
// # This is a window, not a global deduplication
//
// It remembers n keys. An element whose key left that window passes again, so
// the output is deduplicated locally and not globally. That is the whole point:
// global deduplication over a stream needs a set that grows with the number of
// distinct keys, which is unbounded by construction and is why §7.1 rules out
// an unsorted Union, Intersect or Except. A bound that the caller states is
// what makes the operator expressible at all.
//
// Two consequences worth stating plainly, because assuming the other behaviour
// is the trap this doc exists to close:
//
//   - duplicates further apart than n distinct keys are not caught;
//   - the output is exact when the stream holds at most n distinct keys, which
//     is the case most callers actually have.
//
// If exact global deduplication is required and the key space is genuinely
// unbounded, sort the stream and use [MergeJoinBy] — the sort-merge strategy —
// or accept O(distinct) memory outside this package. Do not reach for a larger
// n and hope.
//
// # sluice.Overflow
//
// The policy applies when a new key arrives and the window is full:
//
//   - [sluice.DropOldest] evicts the oldest-inserted key — insertion order, not LRU:
//     a duplicate hit does not refresh its key's place in the window. The
//     window follows the stream, which is what a deduplication window
//     normally means.
//   - [sluice.DropNewest] keeps the window as it is and lets the new key through
//     untracked — it passes, and so will its next occurrence.
//   - [sluice.Fail] panics with [sluice.ErrOverflow]. For a stream whose key count was
//     supposed to be bounded, where exceeding it is a fact worth learning
//     rather than absorbing.
//
// # Cost
//
// O(n) memory for the window, one map lookup per element. The map is claimed on
// first use, so a large n costs nothing on a stream that yields nothing.
//
// Measured on int64 keys with a window of 1024: ~57 ns/element when every key
// is distinct — every element inserts and evicts — and ~4.3 ns/element when
// they all repeat, where the lookup hits and nothing changes. Both are far
// above the ~1.5 ns per-stage budget, and that budget does not apply here: it
// bounds stateless operators, where the only cost is the indirect call. What
// this costs is a hash map, which is what deduplication is. Place it where
// duplicates are actually expected rather than defensively.
//
// The saturated figure is three map operations per element — a lookup that
// misses, a delete to evict, an insert — and a profile puts 90% of the runtime
// in the map. Hoisting the policy switch out of the element loop was tried and
// reverted: it measured 13% *slower* on the all-duplicates case, because the
// branch it removes is one the predictor already had right and the three
// unrolled loops cost more instruction cache than they save.
//
// By panics if key is nil or if policy is not a declared [sluice.Overflow].
func By[T any, K comparable](s sluice.Stream[T], n int, key func(T) K, policy sluice.Overflow) sluice.Stream[T] {
	if key == nil {
		panic("sluice: By requires a non-nil key function")
	}
	if !policy.Valid() {
		panic("sluice: By requires a declared Overflow policy")
	}
	return func(yield func(sluice.Batch[T]) bool) {
		if n <= 0 {
			// A window of zero remembers nothing, so nothing is ever a
			// duplicate. Passing everything through is the honest reading, and
			// it keeps By(s, 0, ...) from silently becoming a filter that
			// drops the whole stream.
			s(yield)
			return
		}

		// seen is a set: membership is the whole question, and the eviction
		// order lives in the ring beside it. An earlier version stored a
		// position here and never read it, paying a word per entry for a field
		// nothing consulted.
		var seen map[K]struct{}
		order := newRing[K](n)

		var out []T
		s(func(b sluice.Batch[T]) bool {
			out = out[:0]
			for _, v := range b.Items {
				k := key(v)
				if seen == nil {
					seen = make(map[K]struct{}, min(n, initialDistinct))
				}
				if _, dup := seen[k]; dup {
					continue
				}
				if len(seen) == n {
					switch policy {
					case sluice.DropOldest:
						delete(seen, order.pop())
					case sluice.DropNewest:
						// The key is not tracked, so the element passes and its
						// next occurrence will too. Documented, not silent.
						out = append(out, v)
						continue
					case sluice.Fail:
						panic(sluice.ErrOverflow)
					}
				}
				seen[k] = struct{}{}
				order.push(k)
				out = append(out, v)
			}
			return yield(sluice.Batch[T]{Items: out})
		})
	}
}

// initialDistinct caps the map claimed on the first element, for the reason
// every bound in this library is claimed lazily: n is often a configuration
// value, and a large one must not become a large allocation the moment one
// element arrives.
const initialDistinct = 64

// ring is a fixed-capacity queue of keys in arrival order, used to find the
// least recently added one without scanning.
//
// It exists because eviction needs insertion order and a Go map has none. A
// slice with an advancing head would grow without bound on a long stream; this
// wraps in place, so memory stays at the capacity the caller asked for.
// The backing array is claimed on the first push and grown to capacity rather
// than allocated up front, for the same reason: a window sized from
// configuration must not allocate for a stream that yields nothing.
// Once it reaches capacity it wraps in place and never grows again.
type ring[K comparable] struct {
	items    []K
	capacity int
	head     int
	size     int
}

func newRing[K comparable](capacity int) *ring[K] {
	return &ring[K]{capacity: capacity}
}

// push adds a key. The caller guarantees room by popping first; pushing into a
// full ring overwrites the oldest entry, which would desynchronise it from the
// map, so it is the caller's job not to.
func (r *ring[K]) push(k K) {
	if r.size == len(r.items) && len(r.items) < r.capacity {
		// Not yet at capacity: grow rather than wrap. Doubling from a small
		// start costs log(capacity) copies over the life of the stream, against
		// one large allocation on the first element.
		grown := max(2*len(r.items), min(r.capacity, initialDistinct))
		next := make([]K, min(grown, r.capacity))
		for i := range r.size {
			next[i] = r.items[(r.head+i)%len(r.items)]
		}
		r.items, r.head = next, 0
	}
	r.items[(r.head+r.size)%len(r.items)] = k
	r.size++
}

// pop removes and returns the oldest key.
func (r *ring[K]) pop() K {
	k := r.items[r.head]
	var zero K
	// Cleared rather than left behind: for a string or pointer key, an evicted
	// entry would otherwise stay live in the array for as long as the stream
	// runs, which is retention nobody asked for.
	r.items[r.head] = zero
	r.head = (r.head + 1) % len(r.items)
	r.size--
	return k
}
