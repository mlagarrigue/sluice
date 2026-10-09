package probe_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/probe"
)

// A probe on each side of a stage says how many elements went in and did not
// come out. In a synchronous pull pipeline that is the stage's selectivity —
// here, the rows the filter dropped — not a measure of how slow it is: every
// stage runs at the same pace, and time is a profiler's question.
func ExampleProbe() {
	var in, out probe.Counter

	src := probe.Probe(sluice.Of([]int{1, 2, 3, 4, 5, 6, 7, 8}, 2), &in)
	kept := sluice.Filter(src, func(v int) bool { return v%2 == 0 })
	result := probe.Probe(kept, &out)

	fmt.Println(sluice.Collect(result))
	fmt.Println("in: ", in.Elems(), "elements in", in.Batches(), "batches")
	fmt.Println("out:", out.Elems(), "elements in", out.Batches(), "batches")

	// The lag is the subtraction, and in a real pipeline it can be read from
	// another goroutine while this one runs. On one goroutine it is volume:
	// what the filter consumed without emitting.
	fmt.Println("dropped by the filter:", in.Elems()-out.Elems())
	// Output:
	// [2 4 6 8]
	// in:  8 elements in 4 batches
	// out: 4 elements in 4 batches
	// dropped by the filter: 4
}

// A refused batch is not counted, which is what makes the lag arithmetic exact:
// the counter means "accepted downstream", not "offered downstream".
func ExampleCounter() {
	var c probe.Counter

	seen := 0
	probe.Probe(sluice.Of([]int{1, 2, 3, 4, 5, 6}, 2), &c)(func(sluice.Batch[int]) bool {
		seen++
		return seen < 2 // take one batch, refuse the second
	})

	fmt.Println("accepted:", c.Batches(), "batches,", c.Elems(), "elements")
	// Output: accepted: 1 batches, 2 elements
}
