package sluice

import (
	"iter"
	"slices"
	"testing"
)

// FuzzSplitRoutesEachElementOnce checks a random partition over k branches:
// every element reaches exactly the branch its batch was routed to, once, and
// each branch sees its elements in source order.
//
// Branches are consumed in alternation from one goroutine, in a random order
// that never stalls: the driver knows the route sequence in advance, so it
// only lets a branch drive the source when every batch that would be placed on
// the way lands in an empty slot. That exercises deposits into branches not
// yet started, branches driving the source for others, and the end of the
// source reached from any branch.
func FuzzSplitRoutesEachElementOnce(f *testing.F) {
	f.Add(uint64(1), uint8(3), uint8(50), uint8(4))
	f.Add(uint64(2), uint8(5), uint8(200), uint8(1))
	f.Add(uint64(3), uint8(8), uint8(0), uint8(3))
	f.Add(uint64(4), uint8(4), uint8(17), uint8(8))

	f.Fuzz(func(t *testing.T, seed uint64, kk, nn, maxBatch uint8) {
		rng := newPropRand(seed)
		k := int(kk%7) + 3 // k > 2: the two-branch case is covered by hand
		n := int(nn)
		size := int(maxBatch%8) + 1

		src := make([]int, n)
		for i := range src {
			src[i] = i
		}
		nb := (n + size - 1) / size

		// The route of the j-th batch is fixed up front so the driver can plan.
		dest := make([]int, nb)
		for j := range dest {
			dest[j] = rng.intn(k)
		}
		routed := 0
		branches := Split(Of(src, size), k, func(b Batch[int]) []int {
			j := b.Items[0] / size
			if j != routed {
				t.Fatalf("route called for batch %d, want %d: route runs once per batch, in order", j, routed)
			}
			routed++
			return []int{dest[j]}
		})

		next := make([]func() (Batch[int], bool), k)
		stop := make([]func(), k)
		for i := range branches {
			next[i], stop[i] = iter.Pull(iter.Seq[Batch[int]](branches[i]))
			defer stop[i]() //nolint:gocritic // deferInLoop: every pull stops when the property returns, as intended
		}

		got := make([][]int, k)
		pending := make([]bool, k)
		ended := make([]bool, k)
		pos := 0 // index of the next batch the source will produce

		// safe reports whether branch i may drive the source without stalling:
		// every batch placed before i is served lands in an empty slot, and no
		// two of them share a slot.
		safe := func(i int) (served int, ok bool) {
			seen := slices.Clone(pending)
			for j := pos; j < nb; j++ {
				d := dest[j]
				if d == i {
					return j, true
				}
				if seen[d] {
					return 0, false
				}
				seen[d] = true
			}
			return nb, true // drains the source
		}

		for live := k; live > 0; {
			i := rng.intn(k)
			if ended[i] {
				continue
			}
			if !pending[i] {
				// i drives the source: the batches before its own are placed
				// in their slots, and its own comes straight through.
				j, ok := safe(i)
				if !ok {
					continue
				}
				for ; pos < j; pos++ {
					pending[dest[pos]] = true
				}
				if pos < nb {
					pos++
				}
			}

			b, ok := next[i]()
			if !ok {
				if pos < nb || pending[i] {
					t.Fatalf("branch %d ended early: batch %d of %d still due", i, pos, nb)
				}
				ended[i] = true
				live--
				continue
			}
			pending[i] = false
			got[i] = append(got[i], b.Items...)
		}

		var all []int
		for i := range got {
			for _, v := range got[i] {
				if d := dest[v/size]; d != i {
					t.Fatalf("element %d reached branch %d, routed to %d", v, i, d)
				}
			}
			if !slices.IsSorted(got[i]) {
				t.Fatalf("branch %d out of source order: %v", i, got[i])
			}
			all = append(all, got[i]...)
		}
		slices.Sort(all)
		if !slices.Equal(all, src) {
			t.Fatalf("branches together hold %v, want each of %v exactly once", all, src)
		}
	})
}
