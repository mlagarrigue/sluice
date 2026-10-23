package pgtype_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// numeric is kept exactly, because a column is declared numeric precisely when
// a float would lose what matters. Every lossy reading has to be named.
func ExampleNumeric() {
	// A value with more significant digits than a float64 can hold.
	n, err := pgtype.ParseNumeric("12345678901234567890.12345678901234567890")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Through the wire and back, unchanged.
	back, err := pgtype.DecodeNumeric(pgtype.AppendNumeric(nil, n))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("exact: ", back.String())
	fmt.Println("as a float64, which loses what the column was chosen to keep:")
	fmt.Printf("        %.6f...\n", back.Float64())

	// The display scale is data, not decoration: numeric(12,2) holding ten is
	// "10.00", and that is what an invoice prints.
	scaled, _ := pgtype.ParseNumeric("10.00")
	fmt.Println("scale: ", scaled.String())
	// Output:
	// exact:  12345678901234567890.12345678901234567890
	// as a float64, which loses what the column was chosen to keep:
	//         12345678901234567168.000000...
	// scale:  10.00
}
