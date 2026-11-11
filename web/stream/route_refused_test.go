package stream

import (
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
)

// A stage steps over an exchange an earlier stage refused (Exchange.Status:
// "the first refusal sets it and later stages step over the element"), and
// DecodeJSON does, and so does PathInt64 now: an exchange the router refused
// has no Params, and the helper panicked on it as on a pattern with no
// wildcard. The guide's own handler (docs/guide/httpstream.md, "web/stream")
// calls it on every exchange of the batch, with no Refused() in front: any
// target the table did not know panicked the handler, and the transport
// answered the whole batch 500.
func TestPathInt64StepsOverARefusedExchange(t *testing.T) {
	rt := NewRouter(Route{Method: "GET", Pattern: "/orders/{order}"})

	// Control: the route this handler was written for is served.
	ok := []Exchange{{Request: httpstream.Request{Method: []byte("GET"), Target: []byte("/orders/12")}, RouteIndex: RouteNone}}
	rt.Route(ok)
	if v, parsed := PathInt64(&ok[0], 0, "order"); !parsed || v != 12 {
		t.Fatalf("control: PathInt64 on a matched route = %d, %v", v, parsed)
	}

	for _, tc := range []struct {
		name, method, target string
		status               int
	}{
		{"no route", "GET", "/nope", 404},
		{"another method", "POST", "/orders/12", 405},
		{"a method nobody declares", "BREW", "/orders/12", 501},
		{"OPTIONS answered by the router", "OPTIONS", "/orders/12", 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := []Exchange{{Request: httpstream.Request{Method: []byte(tc.method), Target: []byte(tc.target)}, RouteIndex: RouteNone}}
			rt.Route(ex)
			if ex[0].Status != tc.status {
				t.Fatalf("setup: the router answered %d, want %d", ex[0].Status, tc.status)
			}
			// Control: the other per-element helper steps over it.
			var dst struct{}
			if DecodeJSON(&ex[0], 1024, &dst) {
				t.Fatal("control: DecodeJSON served an exchange the router refused")
			}
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PathInt64 panicked on an exchange the router had answered %d: %v", tc.status, r)
				}
			}()
			if _, parsed := PathInt64(&ex[0], 0, "order"); parsed {
				t.Error("PathInt64 read a wildcard off a refused exchange")
			}
			if ex[0].Status != tc.status || len(ex[0].Diagnostics) > 1 {
				t.Errorf("the refusal changed: status %d, %d diagnostics", ex[0].Status, len(ex[0].Diagnostics))
			}
		})
	}
}
