package parallel_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// WithState gives each worker a private state, built once, for the decoder
// table or scratch buffer that Ordered's contract shuts out — f runs
// concurrently, so anything it shares would need a mutex on the hot path.
func ExampleWithState() {
	src := sluice.Of([]int{1, 2, 3, 4, 5, 6, 7, 8}, 2)

	// Each worker owns its own scratch slice and reuses it across every batch
	// it handles, which no shared state could do without synchronising.
	sums := parallel.WithState(src, 4,
		func() (*[]int, error) { s := make([]int, 0, 8); return &s, nil },
		func(scratch *[]int, b sluice.Batch[int]) sluice.Batch[int] {
			*scratch = append((*scratch)[:0], b.Items...)
			total := 0
			for _, v := range *scratch {
				total += v
			}
			// The output must be a fresh slice: the batch is retained until
			// its turn to be emitted, so a reused one would be overwritten.
			return sluice.Batch[int]{Items: []int{total}}
		})

	fmt.Println(sluice.Collect(sums))
	// Output: [3 7 11 15]
}
