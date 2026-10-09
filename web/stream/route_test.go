package stream

import (
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
)

func exchangeFor(method, target string) Exchange {
	return Exchange{
		Request: httpstream.Request{Method: []byte(method), Target: []byte(target)},
		Route:   RouteNone,
	}
}

func table(t *testing.T) *Router {
	t.Helper()
	rt, err := NewRouter(
		Route{Method: "PATCH", Pattern: "/orders/{order}/lines/{line}"},
		Route{Method: "GET", Pattern: "/orders/{order}"},
		Route{Method: "GET", Pattern: "/health"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestRouteMatches(t *testing.T) {
	rt := table(t)
	tests := []struct {
		name, method, target string
		route                int
		params               []string
	}{
		{"wildcards", "PATCH", "/orders/12/lines/3", 0, []string{"12", "3"}},
		{"one wildcard", "GET", "/orders/12", 1, []string{"12"}},
		{"exact", "GET", "/health", 2, nil},
		{"the query is not matched on", "GET", "/health?verbose=1", 2, nil},
		{"a wildcard value stays raw", "GET", "/orders/a%2Fb", 1, []string{"a%2Fb"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ex := []Exchange{exchangeFor(tc.method, tc.target)}
			rt.Route(ex)
			e := &ex[0]
			if e.Refused() {
				t.Fatalf("refused with %d: %v", e.Status, e.Diagnostics)
			}
			if e.Route != tc.route {
				t.Errorf("route = %d, want %d", e.Route, tc.route)
			}
			if len(e.Params) != len(tc.params) {
				t.Fatalf("%d params, want %d", len(e.Params), len(tc.params))
			}
			for i, want := range tc.params {
				if string(e.Params[i]) != want {
					t.Errorf("param %d = %q, want %q", i, e.Params[i], want)
				}
			}
		})
	}
}

func TestRouteRefusals(t *testing.T) {
	rt := table(t)
	tests := []struct {
		name, method, target string
		status               int
		code                 string
	}{
		{"no such path", "GET", "/nothing", 404, CodeNoRoute},
		{"PATCH on a path that exists under GET", "PATCH", "/orders/12", 405, CodeMethodNotAllowed},
		{"the path goes on past the pattern", "GET", "/health/x", 404, CodeNoRoute},
		{"a wildcard does not match an empty segment", "GET", "/orders/", 404, CodeNoRoute},
		{"a percent-encoded literal does not match", "GET", "/%68ealth", 404, CodeNoRoute},
		{"wrong method on an existing path", "DELETE", "/orders/12", 405, CodeMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ex := []Exchange{exchangeFor(tc.method, tc.target)}
			rt.Route(ex)
			e := &ex[0]
			if e.Status != tc.status {
				t.Fatalf("status = %d, want %d (diagnostics %v)", e.Status, tc.status, e.Diagnostics)
			}
			if len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != tc.code {
				t.Errorf("diagnostics = %v, want one %s", e.Diagnostics, tc.code)
			}
		})
	}
}

// RFC 9110 §10.2.1: a 405 must say what would have been allowed.
func TestRouteMethodNotAllowedCarriesAllow(t *testing.T) {
	rt := table(t)
	ex := []Exchange{exchangeFor("DELETE", "/orders/12")}
	rt.Route(ex)
	e := &ex[0]
	if e.Status != 405 {
		t.Fatalf("status = %d, want 405", e.Status)
	}
	var allow string
	for _, h := range e.Headers {
		if string(h.Name) == "allow" {
			allow = string(h.Value)
		}
	}
	if allow != "GET, HEAD" {
		t.Errorf("allow = %q, want GET, HEAD", allow)
	}
}

// RFC 9110 §9.1: a server that supports GET on a resource MUST support HEAD
// on it. Without its own HEAD route, a HEAD request takes the GET route.
func TestRouteHeadFollowsGet(t *testing.T) {
	rt := table(t)
	ex := []Exchange{exchangeFor("HEAD", "/orders/12"), exchangeFor("HEAD", "/orders/12/lines/3")}
	rt.Route(ex)
	if e := &ex[0]; e.Refused() || e.Route != 1 || len(e.Params) != 1 || string(e.Params[0]) != "12" {
		t.Errorf("HEAD on a GET route: status %d, route %d, params %q; want route 1 with param 12", e.Status, e.Route, e.Params)
	}
	// HEAD stands in for GET only: a PATCH-only path still answers 405.
	if e := &ex[1]; e.Status != 405 {
		t.Errorf("HEAD on a PATCH-only route: status %d, want 405", e.Status)
	}
}

// Routed to GET, rendered as GET, and the wire carries the head alone.
func TestRouteHeadSendsNoBody(t *testing.T) {
	rt := table(t)
	ex := []Exchange{exchangeFor("HEAD", "/health")}
	rt.Route(ex)
	out := Render(ex, func(int) (int, any) { return 200, map[string]any{"ok": true} })
	wire, err := httpstream.WriteBatch(nil, []httpstream.Request{ex[0].Request}, out.Items)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(wire), "HTTP/1.1 200 ") {
		t.Fatalf("response = %q, want a 200", wire)
	}
	if strings.Contains(string(wire), `"ok"`) {
		t.Errorf("a HEAD response carried the body: %q", wire)
	}
}

// An explicit HEAD route is preferred over the GET it would otherwise borrow,
// wherever it sits in the table.
func TestRouteHeadPrefersItsOwnRoute(t *testing.T) {
	rt, err := NewRouter(
		Route{Method: "GET", Pattern: "/a"},
		Route{Method: "HEAD", Pattern: "/a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ex := []Exchange{exchangeFor("HEAD", "/a")}
	rt.Route(ex)
	if ex[0].Route != 1 {
		t.Errorf("route = %d, want the HEAD route 1", ex[0].Route)
	}
}

// Two rows that overlap without either covering the other — both match
// /a/b/c, each matches paths the other does not — are both reachable, and the
// overlap goes to the first. A row covered whole is refused by NewRouter.
func TestRouteFirstMatchWins(t *testing.T) {
	rt, err := NewRouter(
		Route{Method: "GET", Pattern: "/a/{x}/c"},
		Route{Method: "GET", Pattern: "/a/b/{y}"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ex := []Exchange{exchangeFor("GET", "/a/b/c"), exchangeFor("GET", "/a/b/d")}
	rt.Route(ex)
	if ex[0].Route != 0 {
		t.Errorf("route = %d; the table is ordered and the first match wins", ex[0].Route)
	}
	if ex[1].Route != 1 {
		t.Errorf("route = %d; the second row is reachable where the first does not match", ex[1].Route)
	}
}

func TestRouteSkipsARefusedExchange(t *testing.T) {
	rt := table(t)
	ex := []Exchange{exchangeFor("GET", "/health")}
	ex[0].Status = 401
	rt.Route(ex)
	if ex[0].Route != RouteNone {
		t.Errorf("a refused exchange was routed to %d", ex[0].Route)
	}
}

func TestNewRouterRefusesWhatItCannotRead(t *testing.T) {
	bad := []Route{
		{Method: "GET", Pattern: "orders"},
		{Method: "GET", Pattern: "/orders/{}"},
		{Method: "GET", Pattern: "/orders/{a}/x/{a}"},
		{Method: "GET", Pattern: "/or{d}ers"},
		{Method: "", Pattern: "/orders"},
	}
	for _, r := range bad {
		if _, err := NewRouter(r); err == nil {
			t.Errorf("NewRouter accepted %+v", r)
		}
	}
	if _, err := NewRouter(); err == nil {
		t.Error("NewRouter accepted an empty table")
	}
}

// First match wins, so a row an earlier row of the same method covers is dead
// code that routes nothing — refused at compilation rather than discovered
// when /orders/new reaches the wildcard's handler.
func TestNewRouterRefusesAnUnreachableRoute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		routes []Route
	}{
		{"literal under wildcard", []Route{{"GET", "/orders/{id}"}, {"GET", "/orders/new"}}},
		{"wildcard under wildcard", []Route{{"GET", "/orders/{a}/lines/{b}"}, {"GET", "/orders/{x}/lines/{y}"}}},
		{"duplicate", []Route{{"GET", "/health"}, {"POST", "/health"}, {"GET", "/health"}}},
		{"partly literal", []Route{{"PATCH", "/{t}/{id}"}, {"PATCH", "/orders/7"}}},
	} {
		if _, err := NewRouter(tc.routes...); err == nil || !strings.Contains(err.Error(), "unreachable") {
			t.Errorf("%s: err = %v, want the route refused as unreachable", tc.name, err)
		}
	}

	for _, tc := range []struct {
		name   string
		routes []Route
	}{
		{"literal first", []Route{{"GET", "/orders/new"}, {"GET", "/orders/{id}"}}},
		{"other method", []Route{{"GET", "/orders/{id}"}, {"DELETE", "/orders/new"}}},
		{"other depth", []Route{{"GET", "/orders/{id}"}, {"GET", "/orders/{id}/lines"}}},
		{"literal does not cover wildcard", []Route{{"GET", "/orders/new"}, {"GET", "/orders/{id}"}}},
		// A wildcard never matches an empty segment, so /a/ stays reachable.
		{"empty segment", []Route{{"GET", "/a/{x}"}, {"GET", "/a/"}}},
	} {
		if _, err := NewRouter(tc.routes...); err != nil {
			t.Errorf("%s: a reachable table was refused: %v", tc.name, err)
		}
	}
}

func TestParamInt64(t *testing.T) {
	rt := table(t)

	t.Run("parses", func(t *testing.T) {
		ex := []Exchange{exchangeFor("PATCH", "/orders/12/lines/3")}
		rt.Route(ex)
		order, ok := PathInt64(&ex[0], 0, "order")
		if !ok || order != 12 {
			t.Fatalf("order = %d, %v", order, ok)
		}
		line, ok := PathInt(&ex[0], 1, "line")
		if !ok || line != 3 {
			t.Fatalf("line = %d, %v", line, ok)
		}
	})

	t.Run("refuses and names the parameter", func(t *testing.T) {
		ex := []Exchange{exchangeFor("PATCH", "/orders/twelve/lines/3")}
		rt.Route(ex)
		if _, ok := PathInt64(&ex[0], 0, "order"); ok {
			t.Fatal("PathInt64 parsed \"twelve\"")
		}
		e := &ex[0]
		if e.Status != 400 {
			t.Errorf("status = %d, want 400", e.Status)
		}
		if len(e.Diagnostics) != 1 || !strings.Contains(e.Diagnostics[0].Path.String(), "order") {
			t.Errorf("the diagnostic does not name the parameter: %v", e.Diagnostics)
		}
	})

	t.Run("an index the route has no wildcard for panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("no panic for a parameter index outside the pattern")
			}
		}()
		ex := []Exchange{exchangeFor("GET", "/health")}
		rt.Route(ex)
		PathInt64(&ex[0], 0, "nope")
	})
}
