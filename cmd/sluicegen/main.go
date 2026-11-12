// Command sluicegen generates hydration code: the loop that turns a batch of
// decoded PostgreSQL rows into a slice of a Go struct.
//
// It exists because reflection costs on every field of every row, even
// written carefully: the gap is measured by BenchmarkHydrateReflect against
// BenchmarkHydrateRowMajor in database/postgres/hydrate_test.go, and the figure lives
// in docs/design/architecture.md, "Hydration". Generated code is how a caller gets the
// hand-written figure without writing it per entity and keeping it in step
// with the schema.
//
// The same measurement decides what to generate. Columnar decoding — one pass
// per column, then assemble — measures *slower* than the row-major loop, by
// 11% at a thousand rows and 43% at one. So this generator emits row-major
// code.
//
//	//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type Order
//
// The flags:
//
//	-type  the struct type to generate a hydrator for (required)
//	-in    the source file holding the type (default: $GOFILE, which go:generate sets)
//	-out   the file to write (default: <type>_hydrate.go, lowercased)
//
// Two properties of the generated code are worth more than its speed.
//
// **Columns bind by name, never by position.** A SELECT whose columns are
// reordered would otherwise write each value into a different field of the
// same type — total into id, both bigint — with no error anywhere. The
// generated binder resolves names against the RowDescription the server sent
// and fails on a column it cannot find.
//
// **Types are checked against the schema at bind time.** A column altered from
// int4 to bigint in a migration is caught when the result opens, not by
// decoding eight bytes as four somewhere in the middle of a batch.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mlagarrigue/sluice/cmd/sluicegen/internal/generator"
)

func main() {
	typeName := flag.String("type", "", "name of the struct type to generate a hydrator for")
	input := flag.String("in", "", "source file holding the type (default: $GOFILE, set by go:generate)")
	output := flag.String("out", "", "file to write (default: <type>_hydrate.go, lowercased)")
	flag.Parse()

	if err := generator.Run(*typeName, *input, *output); err != nil {
		fmt.Fprintln(os.Stderr, "sluicegen:", err)
		os.Exit(1)
	}
}
