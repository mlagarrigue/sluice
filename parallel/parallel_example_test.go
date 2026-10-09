package parallel_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

func ExampleOrdered() {
	s := sluice.Of([]int{1, 2, 3, 4, 5, 6}, 2)

	// f runs on up to four goroutines, one batch at a time, and the output
	// keeps the input order — so this can be dropped into a pipeline without
	// changing what the pipeline means.
	doubled := parallel.Ordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] {
		out := make([]int, len(b.Items))
		for i, v := range b.Items {
			out[i] = v * 2
		}
		return sluice.Batch[int]{Items: out}
	})

	fmt.Println(sluice.Collect(doubled))
	// Output:
	// [2 4 6 8 10 12]
}
