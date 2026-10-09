package sluice_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
)

func ExampleCollect() {
	s := sluice.Of([]int{1, 2, 3, 4, 5}, 2)

	// Collect copies, so the result outlives the batches that carried it.
	fmt.Println(sluice.Collect(s))
	// Output:
	// [1 2 3 4 5]
}

func ExampleCount() {
	s := sluice.Filter(sluice.Of([]int{1, 2, 3, 4, 5, 6}, 2), func(v int) bool {
		return v%2 == 0
	})

	fmt.Println(sluice.Count(s))
	// Output:
	// 3
}

func ExampleReduce() {
	s := sluice.Of([]int{1, 2, 3, 4}, 2)

	// The accumulator type is free: it need not match the element type.
	sum := sluice.Reduce(s, 0, func(acc, v int) int { return acc + v })
	fmt.Println(sum)
	// Output:
	// 10
}

func ExampleForEach() {
	s := sluice.Of([]string{"alpha", "beta", "gamma"}, 2)

	// Returning false stops the stream: the source unwinds and its deferred
	// calls run before ForEach returns.
	sluice.ForEach(s, func(v string) bool {
		fmt.Println(v)
		return v != "beta"
	})
	// Output:
	// alpha
	// beta
}

func ExampleFirst() {
	s := sluice.Of([]int{7, 8, 9}, 2)

	// First stops the stream as soon as it has its element, so it terminates
	// on an infinite source.
	v, ok := sluice.First(s)
	fmt.Println(v, ok)

	_, ok = sluice.First(sluice.Empty[int]())
	fmt.Println(ok)
	// Output:
	// 7 true
	// false
}
