package stream_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web/stream"
)

// The middle of a handler, one batch at a time: route every request, read
// the wildcard of those that matched, answer them. A request the table does
// not know is refused by the router and answered with its problem; the
// business stage never sees it.
func Example() {
	rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})

	batch := sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/orders/7")},
		{Method: []byte("GET"), Target: []byte("/invoices/7")},
		{Method: []byte("GET"), Target: []byte("/orders/seven")},
	}}

	ex := stream.Exchanges(nil, batch) // one envelope per request
	rt.Route(ex)                       // which route for each, or a 404
	orders := make([]int64, len(ex))
	for i := range ex {
		if ex[i].Refused() {
			continue
		}
		order, ok := stream.PathInt64(&ex[i], 0, "order") // refuses a non-integer with 400
		if ok {
			orders[i] = order
		}
	}
	out := stream.Render(ex, func(i int) (int, any) {
		return 200, map[string]int64{"order": orders[i]}
	})

	for i, r := range out.Items {
		if r.Status == 200 {
			fmt.Printf("%s %d %s", batch.Items[i].Target, r.Status, r.Body) // the JSON body ends its line
		} else {
			fmt.Println(string(batch.Items[i].Target), r.Status)
		}
	}
	// Output:
	// /orders/7 200 {"order":7}
	// /invoices/7 404
	// /orders/seven 400
}
