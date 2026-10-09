package sluice_test

import (
	"fmt"
	"iter"

	"github.com/mlagarrigue/sluice"
)

// Split with a routing function that always returns every index broadcasts:
// each branch sees every batch, from a single traversal of the source.
//
// Branches read at the same time must be advanced in alternation — each holds
// one batch at a time — so pull from them rather than ranging over one to
// exhaustion.
func ExampleSplit_broadcast() {
	src := sluice.Of([]int{1, 2, 3}, 1)
	branches := sluice.Split(src, 2, func(sluice.Batch[int]) []int {
		return []int{0, 1}
	})

	next0, stop0 := iter.Pull(iter.Seq[sluice.Batch[int]](branches[0]))
	defer stop0()
	next1, stop1 := iter.Pull(iter.Seq[sluice.Batch[int]](branches[1]))
	defer stop1()

	for {
		b0, ok0 := next0()
		b1, ok1 := next1()
		if !ok0 && !ok1 {
			break
		}
		fmt.Println(b0.Items, b1.Items)
	}
	// Output:
	// [1] [1]
	// [2] [2]
	// [3] [3]
}

// Returning a single index partitions: each batch goes to exactly one branch.
func ExampleSplit_partition() {
	src := sluice.Of([]int{1, 2, 3, 4}, 1)
	branches := sluice.Split(src, 2, func(b sluice.Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	for b := range branches[0] {
		fmt.Println(b.Items)
	}
	// Output:
	// [1]
	// [3]
}

// Split traverses the source once, so it works on a stream that cannot be
// replayed — a cursor, a network read, anything consumed as it is produced.
func ExampleSplit_singlePass() {
	// A source that yields each value once and cannot be rewound.
	remaining := []int{1, 2, 3, 4}
	src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		for len(remaining) > 0 {
			b := sluice.Batch[int]{Items: remaining[:1]}
			remaining = remaining[1:]
			if !yield(b) {
				return
			}
		}
	})

	// Route odd values to branch 0, even ones to branch 1.
	branches := sluice.Split(src, 2, func(b sluice.Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	nextOdd, stopOdd := iter.Pull(iter.Seq[sluice.Batch[int]](branches[0]))
	defer stopOdd()
	nextEven, stopEven := iter.Pull(iter.Seq[sluice.Batch[int]](branches[1]))
	defer stopEven()

	for {
		odd, okOdd := nextOdd()
		if okOdd {
			fmt.Println("odd:", odd.Items)
		}
		even, okEven := nextEven()
		if okEven {
			fmt.Println("even:", even.Items)
		}
		if !okOdd && !okEven {
			break
		}
	}
	// Output:
	// odd: [1]
	// even: [2]
	// odd: [3]
	// even: [4]
}
