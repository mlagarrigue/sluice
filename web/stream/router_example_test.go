package stream_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web/stream"
)

// A routing table: a literal row above the wildcard row that would otherwise
// swallow it. Each request learns the index of the row it matched; one no
// row matches is refused, here a method the table does not list.
func ExampleNewRouter() {
	rt := stream.NewRouter(
		stream.Route{Method: "GET", Pattern: "/orders/new"},
		stream.Route{Method: "GET", Pattern: "/orders/{order}"},
	)
	ex := stream.Exchanges(nil, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/orders/new")},
		{Method: []byte("GET"), Target: []byte("/orders/7")},
		{Method: []byte("DELETE"), Target: []byte("/orders/7")},
	}})
	rt.Route(ex)
	for i := range ex {
		if ex[i].Refused() {
			fmt.Println(string(ex[i].Request.Target), "refused", ex[i].Status)
			continue
		}
		fmt.Println(string(ex[i].Request.Target), "row", ex[i].RouteIndex)
	}
	// Output:
	// /orders/new row 0
	// /orders/7 row 1
	// /orders/7 refused 405
}
