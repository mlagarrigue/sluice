package sluice

import (
	"slices"
	"testing"
)

// FuzzMergeConservesSources checks Merge over n sources of random lengths and
// irregular batches, empty sources and empty batches included.
//
// WhenAll: the output is the multiset union of the sources, and each source's
// elements keep their relative order. WhenAny: each source contributes a
// prefix of itself, in order, and at least one source — the one whose
// exhaustion ended the merge — contributes all of itself.
func FuzzMergeConservesSources(f *testing.F) {
	f.Add(uint64(1), uint8(3), uint8(40), uint8(5), false)
	f.Add(uint64(2), uint8(3), uint8(40), uint8(5), true)
	f.Add(uint64(3), uint8(1), uint8(10), uint8(1), false)
	f.Add(uint64(4), uint8(8), uint8(100), uint8(7), true)
	f.Add(uint64(5), uint8(5), uint8(0), uint8(3), true) // every source empty

	f.Fuzz(func(t *testing.T, seed uint64, nsrc, maxLen, maxBatch uint8, whenAny bool) {
		rng := newPropRand(seed)
		n := int(nsrc%8) + 1
		mb := int(maxBatch%8) + 1

		// Element value encodes (source, index), so the output can be split
		// back into per-source sequences.
		const stride = 1 << 16
		srcs := make([][]int, n)
		streams := make([]Stream[int], n)
		for i := range srcs {
			l := rng.intn(int(maxLen) + 1)
			for j := range l {
				srcs[i] = append(srcs[i], i*stride+j)
			}
			streams[i] = propChunks(srcs[i], mb, rng)
		}

		done := WhenAll
		if whenAny {
			done = WhenAny
		}
		got := Collect(Merge(done, streams...))

		per := make([][]int, n)
		for _, v := range got {
			s := v / stride
			if s < 0 || s >= n {
				t.Fatalf("element %d belongs to no source", v)
			}
			per[s] = append(per[s], v)
		}
		complete := false
		for i := range srcs {
			switch {
			case done == WhenAll && !slices.Equal(per[i], srcs[i]):
				t.Fatalf("WhenAll: source %d delivered %v, want %v", i, per[i], srcs[i])
			case done == WhenAny && !slices.Equal(per[i], srcs[i][:len(per[i])]):
				t.Fatalf("WhenAny: source %d delivered %v, not a prefix of %v", i, per[i], srcs[i])
			}
			if len(per[i]) == len(srcs[i]) {
				complete = true
			}
		}
		if !complete {
			t.Fatalf("WhenAny: no source delivered in full, yet the merge ended")
		}
	})
}
