package sluice_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
)

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
