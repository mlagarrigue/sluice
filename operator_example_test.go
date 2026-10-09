package sluice_test

import (
	"fmt"
	"strings"

	"github.com/mlagarrigue/sluice"
)

func ExampleFlatMap() {
	s := sluice.Of([]string{"a b", "c", ""}, 3)

	// Returning an empty slice drops the element. The whole input batch
	// expands into one output batch rather than several small ones.
	words := sluice.FlatMap(s, strings.Fields)

	fmt.Println(sluice.Collect(words))
	// Output:
	// [a b c]
}

func ExamplePeek() {
	s := sluice.Of([]int{1, 2, 3}, 3)

	seen := 0
	s = sluice.Peek(s, func(int) { seen++ })

	// Peek observes without changing the stream: it is for effects, not
	// transformations.
	fmt.Println(sluice.Collect(s), seen)
	// Output:
	// [1 2 3] 3
}

func ExampleScan() {
	s := sluice.Of([]int{1, 2, 3, 4}, 2)

	// Where Reduce returns only the final value, Scan emits every intermediate
	// one — here a running total, carried across batch boundaries.
	running := sluice.Scan(s, 0, func(acc, v int) int { return acc + v })

	fmt.Println(sluice.Collect(running))
	// Output:
	// [1 3 6 10]
}

func ExampleTake() {
	s := sluice.Of([]int{1, 2, 3, 4, 5, 6}, 4)

	// Take counts elements, not batches: it cuts inside the first batch rather
	// than yielding all four of its elements. It stops the source, which makes
	// it the bound to put in front of Collect on an unbounded stream.
	fmt.Println(sluice.Collect(sluice.Take(s, 3)))
	// Output:
	// [1 2 3]
}

func ExampleDrop() {
	s := sluice.Of([]int{1, 2, 3, 4, 5, 6}, 4)

	// Drop also counts elements, so it can start mid-batch.
	fmt.Println(sluice.Collect(sluice.Drop(s, 3)))
	// Output:
	// [4 5 6]
}

func ExampleTakeWhile() {
	s := sluice.Of([]int{1, 2, 3, 1}, 4)

	// The stream stops at the first element that fails the test — the trailing
	// 1 is never reached, which is what separates this from Filter.
	fmt.Println(sluice.Collect(sluice.TakeWhile(s, func(v int) bool { return v < 3 })))
	// Output:
	// [1 2]
}

func ExampleDropWhile() {
	s := sluice.Of([]int{1, 2, 3, 1}, 4)

	// Once the predicate has failed once it is never called again, so the
	// trailing 1 passes through where Filter would have dropped it.
	fmt.Println(sluice.Collect(sluice.DropWhile(s, func(v int) bool { return v < 3 })))
	// Output:
	// [3 1]
}

func ExampleMerge_strict() {
	odd := sluice.Of([]int{1, 3, 5}, 1)
	even := sluice.Of([]int{2, 4, 6}, 1)

	// Merge with WhenAll rotates strictly between sources, a batch at a time. Merge
	// makes no such promise; here the order is deterministic.
	fmt.Println(sluice.Collect(sluice.Merge(sluice.WhenAll, odd, even)))
	// Output:
	// [1 2 3 4 5 6]
}

func ExampleMerge_uneven() {
	short := sluice.Of([]int{1}, 1)
	long := sluice.Of([]int{2, 3, 4}, 1)

	// A source that runs out drops out of the rotation; the others carry on to
	// the end.
	fmt.Println(sluice.Collect(sluice.Merge(sluice.WhenAll, short, long)))
	// Output:
	// [1 2 3 4]
}
