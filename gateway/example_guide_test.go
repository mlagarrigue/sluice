package gateway_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
)

// New starts the pipeline once, over the stream of every call made through
// the gateway: here batches of up to 8 calls, or what has arrived within
// 5 ms, each answered in its own Reply.
func ExampleNew() {
	gw := gateway.New(gateway.Config{Size: 8, Within: 5 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[string, string]]) {
			calls(func(b sluice.Batch[*gateway.Call[string, string]]) bool {
				for _, c := range b.Items {
					c.Reply(strings.ToUpper(c.In))
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	out, err := gw.Do(context.Background(), "hello")
	fmt.Println(out, err)
	// Output: HELLO <nil>
}
