package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice/gateway"
)

// A gateway that fetches by key: the batch's keys in one call, each caller
// answered with its own value or the error for a key not found.
func ExampleLookup() {
	errNotFound := errors.New("no such order")
	stock := map[int64]string{7: "seven", 8: "eight"} // the database, here
	gw := gateway.New(gateway.Config{Size: 64, Within: 2 * time.Millisecond},
		gateway.Lookup(errNotFound, func(ids []int64, found map[int64]string) error {
			for _, id := range ids { // one `= ANY($1)` for the batch, in a program
				if v, ok := stock[id]; ok {
					found[id] = v
				}
			}
			return nil
		}))
	defer gw.Close()

	for _, id := range []int64{7, 9} {
		v, err := gw.Do(context.Background(), id)
		fmt.Println(v, err)
	}
	// Output:
	// seven <nil>
	//  no such order
}
