package stream

import (
	"bytes"
	"slices"
	"testing"
)

// FuzzRoute matches arbitrary targets against a fixed table. The target is
// the client's, so routing must never panic, and a match must be exact:
// every wildcard value one non-empty segment, and the pattern rebuilt from
// those values the very path that was matched.
func FuzzRoute(f *testing.F) {
	for _, s := range []string{
		"/orders/12/lines/3", "/orders/12", "/health", "/health?x=1", "/orders/a%2Fb",
		"/", "", "?", "//", "/orders//lines/3", "/orders/12/", "/orders/12/lines/3/", "health",
		"/orders/12/lines/", "/health/x", "/a/b",
	} {
		f.Add("GET", s)
		f.Add("PATCH", s)
		f.Add("HEAD", s)
		f.Add("OPTIONS", s)
		f.Add("BREW", s)
	}
	rt := NewRouter(
		Route{Method: "PATCH", Pattern: "/orders/{order}/lines/{line}"},
		Route{Method: "GET", Pattern: "/orders/{order}"},
		Route{Method: "GET", Pattern: "/health"},
		Route{Method: "GET", Pattern: "/"},
		Route{Method: "DELETE", Pattern: "/a/{x}"},
	)
	f.Fuzz(func(t *testing.T, method, target string) {
		ex := []Exchange{exchangeFor(method, target)}
		rt.Route(ex)
		e := &ex[0]
		if e.Refused() {
			// 501 exactly for a method neither the table nor the standard
			// names, 204 only for an OPTIONS the router answers itself,
			// 404 or 405 for the rest.
			known := slices.Contains(rt.methods, method) || slices.Contains(standardMethods, method)
			switch {
			case !known:
				if e.Status != 501 {
					t.Fatalf("unknown method %q on %q answered %d, want 501", method, target, e.Status)
				}
			case e.Status == 204:
				if method != "OPTIONS" {
					t.Fatalf("%s %q answered 204", method, target)
				}
			case e.Status != 404 && e.Status != 405:
				t.Fatalf("refused %s %q with %d", method, target, e.Status)
			}
			return
		}
		path := []byte(target)
		if q := bytes.IndexByte(path, '?'); q >= 0 {
			path = path[:q]
		}
		c := rt.routes[e.Route]
		if c.method != method && (method != "HEAD" || c.method != "GET") {
			t.Fatalf("%s %q routed to a %s route", method, target, c.method)
		}
		if len(e.Params) != c.params {
			t.Fatalf("%q: %d params for a route with %d wildcards", target, len(e.Params), c.params)
		}
		rebuilt := []byte{}
		p := 0
		for _, seg := range c.segments {
			rebuilt = append(rebuilt, '/')
			if seg.param {
				v := e.Params[p]
				p++
				if len(v) == 0 || bytes.IndexByte(v, '/') >= 0 {
					t.Fatalf("%q: wildcard value %q is not one non-empty segment", target, v)
				}
				rebuilt = append(rebuilt, v...)
				continue
			}
			rebuilt = append(rebuilt, seg.literal...)
		}
		if !bytes.Equal(rebuilt, path) {
			t.Fatalf("%q matched route %d, which rebuilds as %q", target, e.Route, rebuilt)
		}
	})
}
