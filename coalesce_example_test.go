package sluice_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
)

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
