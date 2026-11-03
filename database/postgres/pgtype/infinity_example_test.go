package pgtype_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// PostgreSQL's 'infinity' has no time.Time of its own: it decodes to
// InfinityTime, which encodes back to it. Compare it with Equal.
func ExampleInfinityTime() {
	back, err := pgtype.DecodeTimestampTZ(pgtype.AppendTimestampTZ(nil, pgtype.InfinityTime()))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(back.Equal(pgtype.InfinityTime()), back.Equal(pgtype.NegInfinityTime()))
	// Output: true false
}
