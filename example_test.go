package sluice_test

import (
	"fmt"

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
// each branch sees every batch.
func ExampleSplit_broadcast() {
	src := sluice.Of([]int{1, 2, 3}, 3)
	branches := sluice.Split(src, 2, func(sluice.Batch[int]) []int {
		return []int{0, 1}
	})

	for b := range branches[0] {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2 3]
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
