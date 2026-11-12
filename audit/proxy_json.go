package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Caddy and Envoy, read in JSON: Caddy's native configuration (what caddy
// adapt makes of a Caddyfile) and Envoy's bootstrap, which Envoy takes in
// JSON as it takes it in YAML. Reading YAML would need a dependency, which
// this module does not take; converting is the caller's one command.
//
// The questions are the nginx and HAProxy ones, and some have no answer
// to give: Caddy flushes at once a response whose length is unknown — a
// stream sent in chunks — and sets X-Forwarded-For by default; Envoy
// streams without buffering. What each can get wrong is below.

const (
	caddyRP    = "https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#the-http-transport"
	caddyTLS   = "https://caddyserver.com/docs/json/apps/http/servers/tls_connection_policies/protocol_min/"
	envoyRoute = "https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto#envoy-v3-api-field-config-route-v3-routeaction-timeout"
	envoyHTTP  = "https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/protocol.proto#envoy-v3-api-field-config-core-v3-httpprotocoloptions-max-requests-per-connection"
	envoyTLS   = "https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/transport_sockets/tls/v3/common.proto#envoy-v3-api-msg-extensions-transport-sockets-tls-v3-tlsparameters"
	envoyXFF   = "https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_conn_man/headers#x-forwarded-for"
)

// parseProxyJSON tells Caddy's configuration (apps.http) from Envoy's
// (static_resources).
func parseProxyJSON(text string) (kind string, doc map[string]any, err error) {
	if err := json.Unmarshal([]byte(text), &doc); err != nil {
		return "", nil, fmt.Errorf("not JSON: %w", err)
	}
	switch {
	case jget(doc, "apps", "http") != nil:
		return "caddy", doc, nil
	case jget(doc, "static_resources") != nil:
		return "envoy", doc, nil
	}
	return "", nil, errors.New("JSON that is neither Caddy's (apps.http) nor Envoy's (static_resources)")
}

// jget follows keys through nested objects, nil where one is missing.
func jget(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

// jwalk calls fn on every object under v, v included, depth first.
func jwalk(v any, fn func(map[string]any)) {
	switch x := v.(type) {
	case map[string]any:
		fn(x)
		for _, k := range slices.Sorted(maps.Keys(x)) {
			jwalk(x[k], fn)
		}
	case []any:
		for _, e := range x {
			jwalk(e, fn)
		}
	}
}

// jnum reads a number Envoy may write bare or wrapped ({"value": n}).
func jnum(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case map[string]any:
		return jnum(x["value"])
	}
	return 0, false
}

func jname(m map[string]any) string {
	for _, k := range []string{"name", "cluster", "dial", "stat_prefix"} {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return "?"
}

// jsonKeepaliveGaps lists what closes the connections to the application:
// Caddy's keep_alive disabled, an Envoy cluster that closes a connection
// after a few requests.
func (p *Proxy) jsonKeepaliveGaps() []string {
	var bad []string
	switch p.Kind {
	case "caddy":
		jwalk(p.JSON, func(m map[string]any) {
			if m["handler"] != "reverse_proxy" {
				return
			}
			if jget(m, "transport", "keep_alive", "enabled") == false {
				var dials []string
				if ups, ok := m["upstreams"].([]any); ok {
					for _, u := range ups {
						if d, ok := jget(u, "dial").(string); ok {
							dials = append(dials, d)
						}
					}
				}
				bad = append(bad, "reverse_proxy to "+strings.Join(dials, ", ")+": transport.keep_alive.enabled false")
			}
		})
	case "envoy":
		clusters, _ := jget(p.JSON, "static_resources", "clusters").([]any)
		for _, c := range clusters {
			cm, _ := c.(map[string]any)
			for _, at := range [][]string{
				{"max_requests_per_connection"},
				{"common_http_protocol_options", "max_requests_per_connection"},
				{"typed_extension_protocol_options", "envoy.extensions.upstreams.http.v3.HttpProtocolOptions", "common_http_protocol_options", "max_requests_per_connection"},
			} {
				if n, ok := jnum(jget(cm, at...)); ok && n < fewRequestsPerConnection {
					bad = append(bad, fmt.Sprintf("cluster %s: %s %.0f", jname(cm), strings.Join(at, "."), n))
				}
			}
		}
	}
	return bad
}

// jsonCheck is the rule named for a Caddy or an Envoy configuration.
func (p *Proxy) jsonCheck(rule string) []Finding {
	switch rule {
	case "upstream-keepalive":
		bad := p.jsonKeepaliveGaps()
		if len(bad) == 0 {
			return nil
		}
		f := Finding{
			Current:  "connections to the application closed after a few requests in " + p.Path + ": " + strings.Join(bad, "; "),
			Proposed: "Caddy: keep_alive left enabled (its default, 2 minutes idle); Envoy: max_requests_per_connection unset",
			Why: "each request then opens a TCP connection to the application, and a TLS session when " +
				"the hop is encrypted: a round trip or two before the first byte, and under load " +
				"ephemeral ports run out on the proxy's side",
			Doc: caddyRP,
		}
		if p.Kind == "envoy" {
			f.Doc = envoyHTTP
		}
		return []Finding{f}
	case "timeouts":
		if p.Kind != "envoy" {
			return nil
		}
		var routes []string
		jwalk(p.JSON, func(m map[string]any) {
			r, ok := m["route"].(map[string]any)
			if !ok || r["cluster"] == nil && r["weighted_clusters"] == nil {
				return
			}
			if _, set := r["timeout"]; !set {
				routes = append(routes, fmt.Sprintf("%v", jget(m, "match")))
			}
		})
		if len(routes) == 0 {
			return nil
		}
		return []Finding{{
			Current:  fmt.Sprintf("%d route(s) without timeout in %s, so 15 s, Envoy's default: %s", len(routes), p.Path, strings.Join(routes, ", ")),
			Proposed: "timeout: 0s on the routes whose responses stream, with idle_timeout bounding a stalled stream",
			Why: "the route's timeout runs from the end of the request until the response is complete: " +
				"a stream that lasts longer than 15 s is cut in the middle, and the client receives " +
				"a truncated body",
			Doc: envoyRoute,
		}}
	case "tls-protocols":
		var bad []string
		jwalk(p.JSON, func(m map[string]any) {
			if v, ok := m["protocol_min"].(string); ok && (v == "tls1.0" || v == "tls1.1") {
				bad = append(bad, "protocol_min "+v)
			}
			if v, ok := m["tls_minimum_protocol_version"].(string); ok && (v == "TLSv1_0" || v == "TLSv1_1") {
				bad = append(bad, "tls_minimum_protocol_version "+v)
			}
		})
		if len(bad) == 0 {
			return nil
		}
		f := Finding{
			Current:  "TLS versions older than 1.2 accepted in " + p.Path + ": " + strings.Join(bad, "; "),
			Proposed: "Caddy: protocol_min tls1.2 (its default); Envoy: tls_minimum_protocol_version TLSv1_2 (its default for servers)",
			Why: "TLS 1.0 and 1.1 are deprecated by RFC 8996: their handshakes rest on SHA-1 and MD5, and " +
				"a client offered them can be steered down to them",
			Doc: caddyTLS,
		}
		if p.Kind == "envoy" {
			f.Doc = envoyTLS
		}
		return []Finding{f}
	case "forwarded-for":
		if p.Kind != "envoy" {
			return nil // Caddy sets X-Forwarded-For by default
		}
		var bad []string
		jwalk(p.JSON, func(m map[string]any) {
			t, _ := m["@type"].(string)
			if !strings.HasSuffix(t, "HttpConnectionManager") {
				return
			}
			hops, _ := jnum(m["xff_num_trusted_hops"])
			if m["use_remote_address"] != true && hops == 0 {
				bad = append(bad, "connection manager "+jname(m))
			}
		})
		if len(bad) == 0 {
			return nil
		}
		return []Finding{{
			Current:  "X-Forwarded-For not set by " + strings.Join(bad, ", ") + " in " + p.Path + ": use_remote_address is false and no hop is trusted",
			Proposed: "use_remote_address: true on the Envoy that faces the clients",
			Why: "Envoy then neither appends the client's address nor takes it as the downstream address: " +
				"the application sees the proxy as every client, and its logs and limits apply to one address",
			Doc: envoyXFF,
		}}
	}
	return nil // buffering: neither buffers a stream
}
