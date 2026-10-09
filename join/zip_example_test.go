package join_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
)

// ZipLongest pairs two streams positionally and carries on to the longer one,
// so nothing is silently dropped when the lengths differ.
func ExampleZipLongest() {
	ids := sluice.Of([]int{1, 2, 3}, 2)
	names := sluice.Of([]string{"ana", "bo"}, 2)

	for b := range join.ZipLongest(ids, names) {
		for _, row := range b.Items {
			if row.Both() {
				fmt.Printf("%d: %s\n", row.Left, row.Right)
			} else {
				fmt.Printf("%d: (no name)\n", row.Left)
			}
		}
	}
	// Output:
	// 1: ana
	// 2: bo
	// 3: (no name)
}

// Zip — stopping at the shorter stream — is a filter rather than an operator.
// Asking for it explicitly is what keeps a length mismatch from passing
// unnoticed.
func ExampleZipLongest_zip() {
	ids := sluice.Of([]int{1, 2, 3}, 2)
	names := sluice.Of([]string{"ana", "bo"}, 2)

	pairs := sluice.Filter(
		join.ZipLongest(ids, names),
		join.EitherOrBoth[int, string].Both,
	)

	for b := range pairs {
		for _, row := range b.Items {
			fmt.Printf("%d: %s\n", row.Left, row.Right)
		}
	}
	// Output:
	// 1: ana
	// 2: bo
}
