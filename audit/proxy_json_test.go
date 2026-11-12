package audit

import (
	"slices"
	"strings"
	"testing"
)

// Caddy's JSON as caddy adapt writes it, and Envoy's bootstrap in JSON:
// each on the defaults that matter, then set for a streamed application.

const caddyDefaults = `{"apps": {"http": {"servers": {"srv0": {
  "listen": [":443"],
  "routes": [{"handle": [{"handler": "subroute", "routes": [{"handle": [{
    "handler": "reverse_proxy",
    "upstreams": [{"dial": "127.0.0.1:8080"}],
    "transport": {"protocol": "http", "keep_alive": {"enabled": false}}
  }]}]}]}],
  "tls_connection_policies": [{"protocol_min": "tls1.0"}]
}}}}}`

const caddyTuned = `{"apps": {"http": {"servers": {"srv0": {
  "listen": [":443"],
  "routes": [{"handle": [{"handler": "reverse_proxy", "upstreams": [{"dial": "127.0.0.1:8080"}]}]}]
}}}}}`

const envoyDefaults = `{"static_resources": {
  "listeners": [{"name": "web", "filter_chains": [{
    "filters": [{"name": "envoy.filters.network.http_connection_manager", "typed_config": {
      "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
      "stat_prefix": "ingress",
      "route_config": {"virtual_hosts": [{"name": "app", "domains": ["*"],
        "routes": [{"match": {"prefix": "/"}, "route": {"cluster": "app"}}]}]}
    }}],
    "transport_socket": {"name": "envoy.transport_sockets.tls", "typed_config": {
      "common_tls_context": {"tls_params": {"tls_minimum_protocol_version": "TLSv1_0"}}}}
  }]}],
  "clusters": [{"name": "app", "max_requests_per_connection": 1}]
}}`

func envoyTuned() string {
	s := strings.Replace(envoyDefaults, `"route": {"cluster": "app"}`, `"route": {"cluster": "app", "timeout": "0s", "idle_timeout": "30s"}`, 1)
	s = strings.Replace(s, `"stat_prefix": "ingress",`, `"stat_prefix": "ingress", "use_remote_address": true,`, 1)
	s = strings.Replace(s, `"TLSv1_0"`, `"TLSv1_2"`, 1)
	return strings.Replace(s, `, "max_requests_per_connection": 1`, ``, 1)
}

func TestCaddyAndEnvoyAreReadInJSON(t *testing.T) {
	sel := Selection{Components: {"proxy"}, CVEs: {"no"}}
	for name, c := range map[string]struct {
		config string
		want   []string
	}{
		"caddy": {caddyDefaults, []string{"proxy/upstream-keepalive", "proxy/tls-protocols"}},
		"envoy": {envoyDefaults, []string{"proxy/upstream-keepalive", "proxy/timeouts", "proxy/tls-protocols", "proxy/forwarded-for"}},
	} {
		got := proxyFindings(t, c.config, "", sel)
		if names := ruleNames(got); !slices.Equal(names, c.want) {
			t.Errorf("%s: %v, want %v", name, names, c.want)
		}
		for i := range got {
			if !strings.Contains(got[i].Doc, name) {
				t.Errorf("%s: %s links %s, not %s's documentation", name, got[i].Rule, got[i].Doc, name)
			}
		}
	}
	for name, config := range map[string]string{"caddy": caddyTuned, "envoy": envoyTuned()} {
		if got := proxyFindings(t, config, "", sel); len(got) != 0 {
			t.Errorf("%s set for a stream: %+v", name, got)
		}
	}
}

func TestEnvoyFindingsNameWhatTheyRead(t *testing.T) {
	byRule := map[string]string{}
	for _, f := range proxyFindings(t, envoyDefaults, "", Selection{Components: {"proxy"}, CVEs: {"no"}}) {
		byRule[f.Rule] = f.Current
	}
	for rule, want := range map[string]string{
		"proxy/upstream-keepalive": "cluster app: max_requests_per_connection 1",
		"proxy/timeouts":           "15 s",
		"proxy/tls-protocols":      "TLSv1_0",
		"proxy/forwarded-for":      "connection manager ingress",
	} {
		if !strings.Contains(byRule[rule], want) {
			t.Errorf("%s said %q, want %q", rule, byRule[rule], want)
		}
	}
}

func TestJSONThatIsNeitherIsRefused(t *testing.T) {
	if _, err := Open(Spec{ProxyConfig: seedFile(t, "x.json", `{"listen": ":80"}`)}); err == nil {
		t.Error("an unknown JSON was read as a proxy")
	}
}
