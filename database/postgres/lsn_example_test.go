package postgres_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// A position in the write-ahead log, as pg_replication_slots shows it, read
// into an LSN to start a replication from, and written back the same way.
func ExampleParseLSN() {
	lsn, err := postgres.ParseLSN("16/B374D848")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lsn, lsn > 0)
	// Output: 16/B374D848 true
}
