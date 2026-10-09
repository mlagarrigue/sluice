package sluice

// Helpers shared by the property tests (*_prop_test.go). Names carry a prop
// prefix so they cannot collide with the per-operator helpers.

// propRand is a small deterministic generator (PCG-style LCG step, xorshift
// output). The property tests derive every choice from a fuzzed seed through
// it, so a failing input reproduces exactly, and it needs no import.
type propRand struct{ s uint64 }

func newPropRand(seed uint64) *propRand { return &propRand{s: seed*2862933555777941757 + 3037000493} }

func (r *propRand) next() uint64 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	x := r.s
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

// intn returns a value in [0, n); n must be positive.
func (r *propRand) intn(n int) int { return int(r.next() % uint64(n)) }

// propChunks cuts src into batches of random sizes in [0, maxBatch], empty
// ones included: upstream operators such as Filter emit empty batches, and a
// property that only holds for full ones is not a property of the operator.
func propChunks[T any](src []T, maxBatch int, rng *propRand) Stream[T] {
	maxBatch = max(maxBatch, 1)
	var cuts []int
	for i := 0; i < len(src); {
		n := rng.intn(maxBatch + 1)
		i = min(i+n, len(src))
		cuts = append(cuts, i)
	}
	return func(yield func(Batch[T]) bool) {
		prev := 0
		for _, c := range cuts {
			if !yield(Batch[T]{Items: src[prev:c]}) {
				return
			}
			prev = c
		}
	}
}
