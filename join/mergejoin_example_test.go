package join_test

import (
	"cmp"
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
)

// Merge walks two sorted streams once and reports what it found for each
// key. Every join semantics is a filter over that output — here, an inner join
// keeps the rows matched on both sides.
func ExampleMerge() {
	type order struct {
		customerID int
		total      int
	}
	type customer struct {
		id   int
		name string
	}

	// Both sides sorted by the join key.
	orders := sluice.Of([]order{{1, 100}, {2, 250}, {4, 75}}, 2)
	customers := sluice.Of([]customer{{1, "ana"}, {3, "bo"}, {4, "cy"}}, 2)

	joined := join.Merge(orders, customers,
		func(o order) int { return o.customerID },
		func(c customer) int { return c.id },
		cmp.Compare[int],
	)

	for b := range joined {
		for _, row := range b.Items {
			switch {
			case row.Both():
				fmt.Printf("%s spent %d\n", row.Right.name, row.Left.total)
			case row.HasLeft:
				fmt.Printf("order from unknown customer %d\n", row.Left.customerID)
			default:
				fmt.Printf("%s has no orders\n", row.Right.name)
			}
		}
	}
	// Output:
	// ana spent 100
	// order from unknown customer 2
	// bo has no orders
	// cy spent 75
}
