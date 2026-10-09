package parallel_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// Async lets a stage run ahead of its consumer, so reading and processing
// overlap instead of taking turns. Order is preserved, so composing it changes
// when work happens and not what comes out.
func ExampleAsync() {
	// A source whose work is I/O-shaped: the point of decoupling it is that
	// the consumer can be busy while the next batch is being fetched.
	rows := sluice.Of([]int{1, 2, 3, 4, 5, 6}, 2)

	for b := range parallel.Async(rows, 4) {
		fmt.Println(b.Items)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5 6]
}
