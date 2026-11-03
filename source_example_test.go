package sluice_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
)

// A Source is a stream and the error to check once it is drained: what a
// connector returns, and what any stream and its error function become.
func ExampleNewSource() {
	var failed error // set by the producer if it stops early
	src := sluice.NewSource(sluice.Of([]int{1, 2, 3}, 2), func() error { return failed })

	n := 0
	for b := range src.Stream() {
		n += b.Len()
	}
	fmt.Println(n, src.Err())
	// Output: 3 <nil>
}
