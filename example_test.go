package sluice_test

import (
	"fmt"
	"iter"

	"github.com/mlagarrigue/sluice"
)

func ExampleOf() {
	s := sluice.Of([]int{1, 2, 3, 4, 5}, 2)

	for b := range s {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

func ExampleMap() {
	s := sluice.Of([]int{1, 2, 3}, sluice.DefaultBatchSize)
	s = sluice.Map(s, func(v int) int { return v * 10 })

	for b := range s {
		fmt.Println(b.Items)
	}
	// Output:
	// [10 20 30]
}

func ExampleFilter() {
	s := sluice.Of([]int{1, 2, 3, 4, 5, 6}, 3)
	s = sluice.Filter(s, func(v int) bool { return v%2 == 0 })

	// Filter keeps batch boundaries, so output batches may be smaller than
	// input ones. Use Coalesce to recompact them.
	for b := range s {
		fmt.Println(b.Items)
	}
	// Output:
	// [2]
	// [4 6]
}

func ExampleCoalesce() {
	// A fragmented source: one element per batch.
	s := sluice.Of([]int{1, 2, 3, 4, 5}, 1)
	s = sluice.Coalesce(s, 2)

	for b := range s {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

func ExampleConvert() {
	s := sluice.Of([]int{1, 2, 3}, sluice.DefaultBatchSize)
	names := sluice.Convert(s, func(v int) string {
		return fmt.Sprintf("item-%d", v)
	})

	for b := range names {
		fmt.Println(b.Items)
	}
	// Output:
	// [item-1 item-2 item-3]
}

func ExampleConcat() {
	s := sluice.Concat(
		sluice.Of([]int{1, 2}, 2),
		sluice.Of([]int{3, 4}, 2),
	)

	for b := range s {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2]
	// [3 4]
}

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

// Merge interleaves streams a batch at a time. WhenAll drains every source.
func ExampleMerge() {
	a := sluice.Of([]int{1, 2, 3}, 1)
	b := sluice.Of([]int{10, 20}, 1)

	for batch := range sluice.Merge(sluice.WhenAll, a, b) {
		fmt.Println(batch.Items)
	}
	// Output:
	// [1]
	// [10]
	// [2]
	// [20]
	// [3]
}

// WhenAny ends the merged stream as soon as one source runs out — useful when
// the sources are meant to advance together and a short one signals the end.
func ExampleMerge_whenAny() {
	a := sluice.Of([]int{1, 2, 3}, 1)
	b := sluice.Of([]int{10}, 1)

	for batch := range sluice.Merge(sluice.WhenAny, a, b) {
		fmt.Println(batch.Items)
	}
	// Output:
	// [1]
	// [10]
	// [2]
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

// A pipeline chains operators; nothing runs until the stream is consumed.
func Example_pipeline() {
	type order struct {
		id    int
		total int
	}

	orders := []order{
		{id: 1, total: 100},
		{id: 2, total: 0},
		{id: 3, total: 250},
		{id: 4, total: 0},
		{id: 5, total: 75},
	}

	s := sluice.Of(orders, sluice.DefaultBatchSize)
	s = sluice.Filter(s, func(o order) bool { return o.total > 0 })
	s = sluice.Map(s, func(o order) order {
		o.total = o.total * 110 / 100 // apply a 10% surcharge
		return o
	})

	for b := range s {
		for _, o := range b.Items {
			fmt.Printf("order %d: %d\n", o.id, o.total)
		}
	}
	// Output:
	// order 1: 110
	// order 3: 275
	// order 5: 82
}
