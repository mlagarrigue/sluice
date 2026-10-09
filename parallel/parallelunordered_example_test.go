package parallel_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// Unordered emits batches as they finish rather than in input order,
// which is what makes it worth having when f's cost varies: the ordered form
// waits for the oldest batch and idles the workers that are ready.
func ExampleUnordered() {
	src := sluice.Of([]int{1, 2, 3, 4, 5, 6, 7, 8}, 2)

	doubled := parallel.Unordered(src, 4, func(b sluice.Batch[int]) sluice.Batch[int] {
		out := make([]int, len(b.Items))
		for i, v := range b.Items {
			out[i] = v * 2
		}
		return sluice.Batch[int]{Items: out}
	})

	// The batches arrive in whatever order they completed, so anything printed
	// per batch would be a different answer each run. A total does not care.
	total := 0
	for _, v := range sluice.Collect(doubled) {
		total += v
	}
	fmt.Println("total:", total)
	// Output: total: 72
}
