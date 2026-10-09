package sluice_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/distinct"
)

// Try converts this package's sentinel panics into errors and re-raises
// everything else. It is the boundary a long-running server needs: a
// data-dependent panic must not take down every in-flight request with it.
func ExampleTry() {
	// Distinct under Fail panics when its window is full, which is the policy
	// asking to be told rather than to drop silently.
	err := sluice.Try(func() {
		sluice.Collect(distinct.By(
			sluice.Of([]int{1, 2, 3}, 1), 1,
			func(v int) int { return v }, sluice.Fail))
	})
	fmt.Println("converted:", err)

	// A panic that is not a sentinel is a bug, and crosses Try untouched.
	fmt.Println("clean run:", sluice.Try(func() {}))
	// Output:
	// converted: sluice: bounded operator reached its limit under the Fail policy
	// clean run: <nil>
}
