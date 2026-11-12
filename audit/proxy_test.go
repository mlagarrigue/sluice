package audit

import (
	"slices"
	"strings"
	"testing"
)

// Configurations as an operator saves them: one left on the defaults
// that matter, one set for a streamed, batched application.

const nginxDefaults = `user nginx;
http {
    server {
        listen 443 ssl;
        ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
        location / {
            proxy_pass http://127.0.0.1:8080;   # straight to the app
        }
        location /videos/ { mp4; }
    }
}
`

const nginxTuned = `http {
    upstream app {
        server 127.0.0.1:8080;
        keepalive 32;
    }
    ssl_protocols TLSv1.2 TLSv1.3;
    server {
        listen 443 ssl;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        location / {
            proxy_pass http://app;
            proxy_buffering off;
        }
    }
}
`

const haproxyDefaults = `global
    log stdout format raw local0

defaults
    mode http

frontend web
    bind :443 ssl crt /etc/ssl/site.pem ssl-min-ver TLSv1.0
    default_backend app

backend app
    option http-server-close
    server a 127.0.0.1:8080
`

const haproxyTuned = `defaults
    mode http
    timeout connect 5s
    timeout client 30s
    timeout server 300s
    option forwardfor

frontend web
    bind :443 ssl crt /etc/ssl/site.pem ssl-min-ver TLSv1.2
    default_backend app

backend app
    server a 127.0.0.1:8080
`

func proxyFindings(t *testing.T, config, version string, sel Selection) []Finding {
	t.Helper()
	target, err := Open(Spec{ProxyConfig: seedFile(t, "proxy.conf", config), ProxyVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(sel, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Current == "" || f.Proposed == "" || f.Why == "" || f.Doc == "" || f.Goal == "" {
			t.Errorf("a finding of %s is missing one of its five fields: %+v", f.Rule, f)
		}
	}
	return findings
}

func ruleNames(fs []Finding) []string {
	var out []string
	for _, f := range fs {
		out = append(out, f.Rule)
	}
	return out
}

func TestAProxyOnItsDefaultsIsReported(t *testing.T) {
	sel := Selection{Components: {"proxy"}, CVEs: {"no"}}
	for name, c := range map[string]struct {
		config string
		want   []string
	}{
		"nginx":   {nginxDefaults, []string{"proxy/upstream-keepalive", "proxy/buffering", "proxy/tls-protocols", "proxy/forwarded-for"}},
		"haproxy": {haproxyDefaults, []string{"proxy/upstream-keepalive", "proxy/timeouts", "proxy/tls-protocols", "proxy/forwarded-for"}},
	} {
		got := ruleNames(proxyFindings(t, c.config, "", sel))
		for _, w := range c.want {
			if !slices.Contains(got, w) {
				t.Errorf("%s: %s stayed silent: %v", name, w, got)
			}
		}
		if len(got) != len(c.want) {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
}

func TestATunedProxyIsSilent(t *testing.T) {
	for name, config := range map[string]string{"nginx": nginxTuned, "haproxy": haproxyTuned} {
		if got := proxyFindings(t, config, "nginx version: nginx/1.26.2", Selection{Components: {"proxy"}}); len(got) != 0 {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

// TestTheFindingsNameTheLines: what the operator has to change is where
// the report says, the timeouts of HAProxy each named with their section.
func TestTheFindingsNameTheLines(t *testing.T) {
	byRule := map[string]string{}
	for _, f := range proxyFindings(t, nginxDefaults, "", Selection{Components: {"proxy"}, CVEs: {"no"}}) {
		byRule[f.Rule] = f.Current
	}
	if !strings.Contains(byRule["proxy/upstream-keepalive"], "line 7: proxy_pass to 127.0.0.1:8080") {
		t.Errorf("nginx keepalive: %q", byRule["proxy/upstream-keepalive"])
	}
	if !strings.Contains(byRule["proxy/tls-protocols"], "line 5") {
		t.Errorf("nginx TLS: %q", byRule["proxy/tls-protocols"])
	}
	for _, f := range proxyFindings(t, haproxyDefaults, "", Selection{Components: {"proxy"}, CVEs: {"no"}}) {
		byRule[f.Rule] = f.Current
	}
	for _, w := range []string{"timeout client in frontend web (line 7)", "timeout connect in backend app (line 11)", "timeout server in backend app"} {
		if !strings.Contains(byRule["proxy/timeouts"], w) {
			t.Errorf("HAProxy timeouts %q does not name %q", byRule["proxy/timeouts"], w)
		}
	}
}

// TestAnUpstreamWithoutHTTP11IsNotKeptAlive: keepalive in the upstream
// block is not enough, nginx speaks HTTP/1.0 to it unless told otherwise.
func TestAnUpstreamWithoutHTTP11IsNotKeptAlive(t *testing.T) {
	config := strings.Replace(nginxTuned, "proxy_http_version 1.1;", "", 1)
	got := proxyFindings(t, config, "", Selection{Components: {"proxy"}, Goals: {"performance"}})
	if len(got) != 1 || !strings.Contains(got[0].Current, "HTTP/1.0") {
		t.Errorf("%+v", got)
	}
}

func TestCVE20247347IsJudgedOnTheReleaseAndTheDirective(t *testing.T) {
	sel := Selection{Components: {"proxy"}, CVEs: {"yes"}, Goals: {"security"}}
	withMP4 := nginxTuned + "\n# videos\nhttp { server { location /v/ { mp4; } } }\n"
	for _, c := range []struct {
		name, config, version, verdict string
	}{
		{"1.26.1 with mp4", withMP4, "nginx version: nginx/1.26.1", VerdictApplies},
		{"1.25.5 with mp4", withMP4, "nginx/1.25.5", VerdictApplies},
		{"1.26.1 without mp4", nginxTuned, "nginx/1.26.1", VerdictAbsent},
		{"no release, mp4", withMP4, "", VerdictUndetermined},
		{"no release, no mp4", nginxTuned, "", ""},
		{"1.26.2, fixed", withMP4, "nginx/1.26.2", ""},
		{"1.27.1, fixed", withMP4, "nginx/1.27.1", ""},
		{"1.28.0, later", withMP4, "nginx/1.28.0", ""},
		{"1.4.7, older than the flaw", withMP4, "nginx/1.4.7", ""},
	} {
		var got []Finding
		for _, f := range proxyFindings(t, c.config, c.version, sel) {
			if f.Rule == "proxy/cve" {
				got = append(got, f)
			}
		}
		switch {
		case c.verdict == "" && len(got) != 0:
			t.Errorf("%s: %+v", c.name, got)
		case c.verdict == "":
		case len(got) != 1 || got[0].Verdict != c.verdict || !strings.Contains(got[0].Current, "CVE-2024-7347"):
			t.Errorf("%s: %+v, want %q", c.name, got, c.verdict)
		}
	}
}

func TestAConfigurationMustBeOneOfTheTwo(t *testing.T) {
	for name, body := range map[string]string{
		"neither":     "just some text\n",
		"unbalanced":  "http { server { listen 80; }\n",
		"stray brace": "http { listen 80; } }\n",
		"open quote":  "add_header X \"never closed;\n",
	} {
		if _, err := Open(Spec{ProxyConfig: seedFile(t, "p.conf", body)}); err == nil {
			t.Errorf("%s: read as a proxy configuration", name)
		}
	}
	if v := proxyVersion("HAProxy version 2.8.5-1 2023/12/09 - https://haproxy.org/"); v != "2.8.5" {
		t.Errorf("HAProxy's version line: %q", v)
	}
}
