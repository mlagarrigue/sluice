package window_test

import (
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/window"
)

// CoalesceWithin regroups batches like sluice.Coalesce, but never holds an element
// longer than its deadline — which is what makes it usable on a socket, where
// waiting for a full batch can mean waiting for the next request.
func ExampleCoalesceWithin() {
	fragmented := sluice.Of([]int{1, 2, 3, 4, 5, 6, 7}, 1)

	// A deadline far away, so this example is decided by the size alone and
	// prints the same thing every run. On a live stream the deadline is what
	// releases a partial batch.
	for b := range window.CoalesceWithin(fragmented, 3, time.Hour) {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2 3]
	// [4 5 6]
	// [7]
}
