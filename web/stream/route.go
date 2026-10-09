package stream

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// Route is one row of a routing table: a method and a pattern of literal
// segments and single-segment wildcards.
//
//	{Method: "PATCH", Pattern: "/orders/{order}/lines/{line}"}
//
// A wildcard matches exactly one non-empty segment; there is no multi-segment
// or suffix wildcard, deliberately — the ambiguity they introduce is the cost
// [net/http.ServeMux]'s precedence rules exist to arbitrate, and a table
// small enough to read does not need an arbiter.
type Route struct {
	Method  string
	Pattern string
}

// Router matches requests against a table of routes. It is data compiled
// once, not a registry: the table is given whole to [NewRouter], first match
// wins in table order, and nothing registers into it afterwards.
type Router struct {
	routes []compiledRoute
}

type compiledRoute struct {
	method   string
	pattern  string // as written, for the refusal that names it
	segments []routeSegment
	params   int
}

type routeSegment struct {
	literal string
	param   bool
}

// NewRouter compiles a table. It refuses a pattern it cannot read completely
// — a brace inside a literal, an empty wildcard name, a name used twice —
// because a route that half-parses routes traffic somewhere the table does
// not say. For the same reason it refuses a row no request can reach, because
// an earlier row of the same method matches everything it would: a literal
// route belongs above the wildcard route that would otherwise swallow it.
func NewRouter(routes ...Route) (*Router, error) {
	if len(routes) == 0 {
		return nil, errors.New("stream: a router needs at least one route")
	}
	rt := &Router{routes: make([]compiledRoute, 0, len(routes))}
	for _, r := range routes {
		if r.Method == "" || strings.ContainsAny(r.Method, " \t") {
			return nil, fmt.Errorf("stream: %q is not a method", r.Method)
		}
		if !strings.HasPrefix(r.Pattern, "/") {
			return nil, fmt.Errorf("stream: pattern %q does not begin with /", r.Pattern)
		}
		c := compiledRoute{method: r.Method, pattern: r.Pattern}
		seen := map[string]bool{}
		for seg := range strings.SplitSeq(r.Pattern[1:], "/") {
			if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") && len(seg) > 2 {
				name := seg[1 : len(seg)-1]
				if strings.ContainsAny(name, "{}") {
					return nil, fmt.Errorf("stream: pattern %q holds a malformed wildcard", r.Pattern)
				}
				if seen[name] {
					return nil, fmt.Errorf("stream: pattern %q uses wildcard %q twice", r.Pattern, name)
				}
				seen[name] = true
				c.segments = append(c.segments, routeSegment{param: true})
				c.params++
				continue
			}
			if strings.ContainsAny(seg, "{}") {
				return nil, fmt.Errorf("stream: pattern %q holds a brace outside a wildcard", r.Pattern)
			}
			c.segments = append(c.segments, routeSegment{literal: seg})
		}
		// First match wins, so a row an earlier row of the same method
		// already covers can never be reached: `/orders/{id}` above
		// `/orders/new` sends every /orders/new to the wildcard's handler,
		// and nothing at runtime would say so. Caught here, where the table
		// is read whole.
		for j, earlier := range rt.routes {
			if earlier.covers(c) {
				return nil, fmt.Errorf("stream: route %s %s is unreachable: route %d (%s %s) matches every path it does, and the first match wins — list the more specific route first",
					r.Method, r.Pattern, j, earlier.method, earlier.pattern)
			}
		}
		rt.routes = append(rt.routes, c)
	}
	return rt, nil
}

// covers reports whether c matches every request later matches: same
// method, same depth, and at each segment either a wildcard here or the
// same literal. A wildcard does not cover an empty literal segment, since a
// wildcard never matches an empty segment.
func (c compiledRoute) covers(later compiledRoute) bool {
	if c.method != later.method || len(c.segments) != len(later.segments) {
		return false
	}
	for i, seg := range c.segments {
		ls := later.segments[i]
		switch {
		case seg.param && (ls.param || ls.literal != ""):
		case !seg.param && !ls.param && seg.literal == ls.literal:
		default:
			return false
		}
	}
	return true
}

// Route resolves every open exchange against the table, filling
// [Exchange.Route] and [Exchange.Params] or refusing: 404 when no pattern
// matches the target's path, 405 — with the Allow field RFC 9110 §10.2.1
// requires — when patterns match under other methods.
//
// A HEAD request matches a HEAD route when the table has one, and otherwise
// the first GET route for its path, as RFC 9110 §9.1 requires; the handler
// sees the method as HEAD and answers as for GET, and the transport drops
// the body. Every Allow listing GET lists HEAD with it.
//
// Matching is on the raw bytes of the target, before the query, and nothing
// is percent-decoded. That is a stated rule, not a shortcut: decoding before
// routing is how two hops route one request differently — %2F is the classic
// — so a literal segment matches byte for byte, and a wildcard's value is
// handed over exactly as the client sent it. A client that percent-encodes a
// literal path is answered 404; a wildcard value that needs decoding is the
// caller's to decode, knowing what the segment means.
func (rt *Router) Route(exchanges []Exchange) {
	for i := range exchanges {
		e := &exchanges[i]
		if e.Refused() {
			continue
		}
		path := e.Request.Target
		if q := bytes.IndexByte(path, '?'); q >= 0 {
			path = path[:q]
		}

		var allowed []string
		matched := false
		head := string(e.Request.Method) == "HEAD"
		// A HEAD request with no HEAD route of its own is served by the
		// first GET route that matches: RFC 9110 §9.1 makes HEAD support
		// mandatory wherever GET is supported, and §9.3.2 defines HEAD as
		// GET without the content. The transport sends no body for HEAD.
		getRoute, getParams := -1, [][]byte(nil)
		for r := range rt.routes {
			params, ok := rt.routes[r].match(path)
			if !ok {
				continue
			}
			if string(e.Request.Method) == rt.routes[r].method {
				e.Route, e.Params = r, params
				matched = true
				break
			}
			if head && getRoute < 0 && rt.routes[r].method == "GET" {
				getRoute, getParams = r, params
			}
			allowed = appendMethod(allowed, rt.routes[r].method)
			if rt.routes[r].method == "GET" {
				allowed = appendMethod(allowed, "HEAD")
			}
		}
		if !matched && getRoute >= 0 {
			e.Route, e.Params = getRoute, getParams
			matched = true
		}
		switch {
		case matched:
		case allowed != nil:
			allow := strings.Join(allowed, ", ")
			e.Headers = append(e.Headers, httpstream.Header{Name: allowFieldName, Value: []byte(allow)})
			e.Refuse(405, diagnostics.NewDiagnostic(diagnostics.Error, CodeMethodNotAllowed, diagnostics.Path{}).
				WithMessage("MethodNotAllowed", map[string]any{
					"method": string(e.Request.Method), "allow": allow,
				}).
				WithOrigin(diagnostics.OriginProtocol))
		default:
			e.Refuse(404, diagnostics.NewDiagnostic(diagnostics.Error, CodeNoRoute, diagnostics.Path{}).
				WithMessage("NoRoute", map[string]any{"target": string(path)}).
				WithOrigin(diagnostics.OriginProtocol))
		}
	}
}

// allowFieldName is shared: the name is a constant, the value is built per
// refusal.
var allowFieldName = []byte("allow")

func appendMethod(allowed []string, method string) []string {
	if slices.Contains(allowed, method) {
		return allowed
	}
	return append(allowed, method)
}

// match walks the path against the pattern, segment by segment, collecting
// wildcard values. It allocates only when the route has wildcards and the
// path matches.
func (c compiledRoute) match(path []byte) ([][]byte, bool) {
	if len(path) == 0 || path[0] != '/' {
		return nil, false
	}
	var params [][]byte
	rest := path[1:]
	for i, seg := range c.segments {
		last := i == len(c.segments)-1
		var part []byte
		// IndexByte rather than bytes.Cut: measured 24% faster on match,
		// which runs per route per request.
		if j := bytes.IndexByte(rest, '/'); j >= 0 {
			if last {
				return nil, false // the path goes on past the pattern
			}
			part, rest = rest[:j], rest[j+1:]
		} else {
			if !last {
				return nil, false // the pattern goes on past the path
			}
			part = rest
		}
		if seg.param {
			if len(part) == 0 {
				return nil, false // a wildcard names a segment, and an absent segment is not one
			}
			if params == nil {
				params = make([][]byte, 0, c.params)
			}
			params = append(params, part)
			continue
		}
		if !bytes.Equal(part, []byte(seg.literal)) {
			return nil, false
		}
	}
	return params, true
}

// PathInt64 reads wildcard i as a base-10 integer, in the order the
// pattern's wildcards appear. name is what the refusal calls it, so a client
// is told which parameter was wrong rather than that something was.
//
// An index the matched route has no wildcard for is the handler and the
// table disagreeing about the same pattern — a programming error, so it
// panics rather than inventing a client-facing refusal for it.
func PathInt64(e *Exchange, i int, name string) (int64, bool) {
	if i < 0 || i >= len(e.Params) {
		panic(fmt.Sprintf("stream: param %d of a route that matched %d wildcards", i, len(e.Params)))
	}
	v, err := strconv.ParseInt(string(e.Params[i]), 10, 64)
	if err != nil {
		refuseInteger(e, i, name)
		return 0, false
	}
	return v, true
}

// refuseInteger refuses e for wildcard i not reading as an integer.
func refuseInteger(e *Exchange, i int, name string) {
	e.Refuse(400, diagnostics.NewDiagnostic(diagnostics.Error, web.CodeInvalidParam, diagnostics.Path{}.Field(name)).
		WithMessage("ParameterInvalid", map[string]any{
			"name": name, "got": string(e.Params[i]), "want": "integer",
		}).
		WithOrigin(diagnostics.OriginProtocol))
}

// PathInt is [PathInt64] for an int, refusing a value the platform's int
// cannot hold.
func PathInt(e *Exchange, i int, name string) (int, bool) {
	v, ok := PathInt64(e, i, name)
	if !ok {
		return 0, false
	}
	// Dead on 64-bit platforms, where int is int64; it is the 32-bit guard.
	if v < math.MinInt || v > math.MaxInt {
		refuseInteger(e, i, name)
		return 0, false
	}
	return int(v), true
}
