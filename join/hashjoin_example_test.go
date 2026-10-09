package join_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/join"
)

type customer struct {
	ID   int
	Name string
}

type order struct {
	ID         int
	CustomerID int
}

func ExampleStreamTable() {
	orders := sluice.Of([]order{{10, 1}, {11, 2}, {12, 99}}, 2)
	customers := sluice.Of([]customer{{1, "ana"}, {2, "bo"}}, 2)

	// The build side is materialised and must be finite; the probe side is
	// streamed and may not be. The limit is required, not optional: a join that
	// materialises one side is the operator most likely to exhaust memory.
	enriched := join.StreamTable(orders, customers,
		func(o order) int { return o.CustomerID },
		func(c customer) int { return c.ID },
		func(o order, c customer) string { return fmt.Sprintf("%s ordered %d", c.Name, o.ID) },
		join.BuildLimit{MaxEntries: 10_000, OnOverflow: sluice.Fail})

	// Order 12 names a customer that does not exist: an inner join drops it.
	for _, line := range sluice.Collect(enriched) {
		fmt.Println(line)
	}
	// Output:
	// ana ordered 10
	// bo ordered 11
}

func ExampleStreamTable_overflow() {
	orders := sluice.Of([]order{{10, 1}, {11, 2}}, 2)
	customers := sluice.Of([]customer{{1, "ana"}, {2, "bo"}}, 1)

	// Under sluice.DropNewest the join runs against a truncated table, so the result
	// is missing rows rather than wrong. The diagnostic is what tells the two
	// apart — without it, a short result looks like a legitimate one.
	diags := diagnostics.NewCollector(10)
	enriched := join.StreamTable(orders, customers,
		func(o order) int { return o.CustomerID },
		func(c customer) int { return c.ID },
		func(o order, c customer) string { return c.Name },
		join.BuildLimit{MaxEntries: 1, OnOverflow: sluice.DropNewest, Report: func(limit, held int) {
			diags.Add(diagnostics.NewDiagnostic(diagnostics.Critical, "Sluice.Join.BuildTableOverflow", diagnostics.Path{}).
				WithMessage("BuildTableOverflow", map[string]any{"limit": limit, "held": held}))
		}})

	fmt.Println(sluice.Collect(enriched))
	for _, d := range diags.All() {
		fmt.Println(d)
	}
	// Output:
	// [ana]
	// Critical Sluice.Join.BuildTableOverflow
}
