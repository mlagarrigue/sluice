package distinct_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/distinct"
)

func ExampleBy() {
	orders := sluice.Of([]string{"A", "B", "A", "C", "B"}, 2)

	// A window of 10 keys: more than this stream holds, so the deduplication
	// is exact here. It is a window and not a global set — a key that falls out
	// of it passes again — because a set that grows with the key count is
	// unbounded by construction.
	unique := distinct.By(orders, 10, func(s string) string { return s }, sluice.DropOldest)

	fmt.Println(sluice.Collect(unique))
	// Output:
	// [A B C]
}

func ExampleBy_window() {
	// A window of 2. A and B fill it, C evicts A, so the second A is no longer
	// remembered and passes.
	s := sluice.Of([]string{"A", "B", "C", "A"}, 4)
	unique := distinct.By(s, 2, func(v string) string { return v }, sluice.DropOldest)

	fmt.Println(sluice.Collect(unique))
	// Output:
	// [A B C A]
}
