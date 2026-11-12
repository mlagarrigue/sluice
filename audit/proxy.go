package audit

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// The reverse proxy in front of the application, read from its saved
// configuration: nginx.conf or haproxy.cfg, told apart by what they hold.
// What the audit asks of it is what a streamed, batched response needs on
// its way out — a connection to the application kept between requests,
// the response passed on as it comes — and what any proxy owes the
// application behind it: bounded waits, a modern TLS, the client's address.

// Proxy is one proxy's configuration as read.
type Proxy struct {
	Path    string
	Kind    string // "nginx" or "haproxy"
	Version string // "1.26.1", empty when none was given
	// Nginx is the configuration's tree, its root the main context.
	Nginx *ngxBlock
	// HAProxy is the configuration's sections in file order.
	HAProxy []haSection
}

// ngxBlock is one nginx directive: its name, its arguments, the line it
// starts on, and the directives of its block when it has one.
type ngxBlock struct {
	Name   string
	Args   []string
	Line   int
	Parent *ngxBlock
	Kids   []*ngxBlock
}

// haSection is one HAProxy section: global, defaults, frontend, backend or
// listen, its name, and its lines split into words.
type haSection struct {
	Kind  string
	Name  string
	Line  int
	Lines [][]string
}

func parseProxy(text, version string) (*Proxy, error) {
	p := &Proxy{Version: proxyVersion(version)}
	switch {
	case haproxyText(text):
		p.Kind, p.HAProxy = "haproxy", parseHAProxy(text)
	case strings.Contains(text, "{") && strings.Contains(text, ";"):
		root, err := parseNginx(text)
		if err != nil {
			return nil, err
		}
		p.Kind, p.Nginx = "nginx", root
	default:
		return nil, errors.New("neither an nginx nor an HAProxy configuration")
	}
	return p, nil
}

// proxyVersion reads "nginx version: nginx/1.26.1" or "HAProxy version
// 2.8.5-1 2023/12/09": the first word that starts with a digit, after a
// slash when there is one.
func proxyVersion(s string) string {
	for f := range strings.FieldsSeq(s) {
		if _, after, ok := strings.Cut(f, "/"); ok {
			f = after
		}
		if f != "" && f[0] >= '0' && f[0] <= '9' {
			v, _, _ := strings.Cut(f, "-")
			return v
		}
	}
	return ""
}

func haproxyText(text string) bool {
	for line := range strings.SplitSeq(text, "\n") {
		if f := strings.Fields(line); len(f) > 0 && line[0] != ' ' && line[0] != '\t' {
			switch f[0] {
			case "global", "defaults", "frontend", "backend", "listen":
				return true
			}
		}
	}
	return false
}

// parseNginx reads nginx's syntax: directives ended by ';', blocks in
// braces, '#' comments, quoted arguments.
func parseNginx(text string) (*ngxBlock, error) {
	root := &ngxBlock{Name: "main"}
	cur := root
	var words []string
	line, start := 1, 0
	var word strings.Builder
	inWord := false
	flush := func() {
		if inWord {
			if len(words) == 0 {
				start = line
			}
			words = append(words, word.String())
			word.Reset()
			inWord = false
		}
	}
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '\n':
			flush()
			line++
		case c == '#' && !inWord:
			for i < len(text) && text[i] != '\n' {
				i++
			}
			i--
		case c == '"' || c == '\'':
			j := strings.IndexByte(text[i+1:], c)
			if j < 0 {
				return nil, fmt.Errorf("line %d: a quote never closed", line)
			}
			word.WriteString(text[i+1 : i+1+j])
			line += strings.Count(text[i+1:i+1+j], "\n")
			inWord = true
			i += j + 1
		case c == ' ' || c == '\t' || c == '\r':
			flush()
		case c == ';' || c == '{':
			flush()
			if len(words) == 0 {
				return nil, fmt.Errorf("line %d: %q with no directive", line, c)
			}
			b := &ngxBlock{Name: words[0], Args: words[1:], Line: start, Parent: cur}
			cur.Kids = append(cur.Kids, b)
			if c == '{' {
				cur = b
			}
			words = nil
		case c == '}':
			flush()
			if cur.Parent == nil || len(words) > 0 {
				return nil, fmt.Errorf("line %d: an unbalanced '}'", line)
			}
			cur = cur.Parent
		default:
			word.WriteByte(c)
			inWord = true
		}
	}
	if cur != root {
		return nil, fmt.Errorf("the block %q of line %d is never closed", cur.Name, cur.Line)
	}
	return root, nil
}

// all returns every directive named name in the tree under b, in order.
func (b *ngxBlock) all(name string) []*ngxBlock {
	var out []*ngxBlock
	for _, k := range b.Kids {
		if k.Name == name {
			out = append(out, k)
		}
		out = append(out, k.all(name)...)
	}
	return out
}

// own returns the directives named name directly in b.
func (b *ngxBlock) own(name string) []*ngxBlock {
	var out []*ngxBlock
	for _, k := range b.Kids {
		if k.Name == name {
			out = append(out, k)
		}
	}
	return out
}

// inherited returns the directives named name in effect in b: its own, or
// the nearest enclosing block's — nginx's rule for every proxy_ and ssl_
// directive, proxy_set_header included, whose list a level replaces whole.
func (b *ngxBlock) inherited(name string) []*ngxBlock {
	for c := b; c != nil; c = c.Parent {
		if d := c.own(name); len(d) > 0 {
			return d
		}
	}
	return nil
}

// header returns the value proxy_set_header gives a header in b, and
// whether it is set.
func (b *ngxBlock) header(name string) (string, bool) {
	for _, d := range b.inherited("proxy_set_header") {
		if len(d.Args) >= 2 && strings.EqualFold(d.Args[0], name) {
			return d.Args[1], true
		}
	}
	return "", false
}

func parseHAProxy(text string) []haSection {
	var out []haSection
	for n, line := range strings.Split(text, "\n") {
		if c := strings.IndexByte(line, '#'); c >= 0 {
			line = line[:c]
		}
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "global", "defaults", "frontend", "backend", "listen":
			if line[0] != ' ' && line[0] != '\t' {
				s := haSection{Kind: f[0], Line: n + 1}
				if len(f) > 1 {
					s.Name = f[1]
				}
				out = append(out, s)
				continue
			}
		}
		if len(out) > 0 {
			out[len(out)-1].Lines = append(out[len(out)-1].Lines, f)
		}
	}
	return out
}

// has says whether a section, or the defaults section before it, holds a
// line starting with words. A "no option" line in the section itself
// cancels the defaults'.
func (p *Proxy) has(i int, words ...string) bool {
	match := func(s haSection) (found, cancelled bool) {
		for _, l := range s.Lines {
			if len(l) > len(words) && l[0] == "no" && slices.Equal(l[1:1+len(words)], words) {
				cancelled = true
			}
			if len(l) >= len(words) && slices.Equal(l[:len(words)], words) {
				found = true
			}
		}
		return found, cancelled
	}
	found, cancelled := match(p.HAProxy[i])
	if found || cancelled {
		return found
	}
	for j := i - 1; j >= 0; j-- {
		if p.HAProxy[j].Kind == "defaults" {
			f, _ := match(p.HAProxy[j])
			return f
		}
	}
	return false
}

func (s haSection) label() string { return strings.TrimSpace(s.Kind + " " + s.Name) }

// httpMode says whether a section proxies HTTP: mode http in it or in the
// defaults before it.
func (p *Proxy) httpMode(i int) bool { return p.has(i, "mode", "http") }

const (
	haproxyDoc = "https://docs.haproxy.org/3.2/configuration.html"
	nginxProxy = "https://nginx.org/en/docs/http/ngx_http_proxy_module.html"
)

// init registers the proxy rules, in this order: the connections to the
// application kept alive, the response passed on as it comes, the waits
// bounded, then what the proxy owes the application and its clients — a
// modern TLS and the client's address. Each is one call, read on its own.
func init() {
	register := func(name, goal, doc string, check func(*Proxy) []Finding) {
		Register(Rule{
			Name:  "proxy/" + name,
			Needs: Selection{Components: {"proxy"}, Goals: {goal}, Sources: {"config"}},
			Doc:   doc,
			Check: func(t *Target, _ Answers) []Finding {
				if t.Proxy == nil {
					return nil
				}
				return check(t.Proxy)
			},
		})
	}

	register("upstream-keepalive", "performance",
		"https://nginx.org/en/docs/http/ngx_http_upstream_module.html#keepalive",
		func(p *Proxy) []Finding {
			bad := p.keepaliveGaps()
			if len(bad) == 0 {
				return nil
			}
			f := Finding{
				Current: "connections to the application closed after each request in " + p.Path + ": " + strings.Join(bad, "; "),
				Proposed: "nginx: keepalive in the upstream block, proxy_http_version 1.1 and proxy_set_header Connection \"\"; " +
					"HAProxy: neither httpclose nor http-server-close, http-reuse safe (its default)",
				Why: "each request then opens a TCP connection to the application, and a TLS session when " +
					"the hop is encrypted: a round trip or two before the first byte, a socket in TIME_WAIT " +
					"after the last, and under load ephemeral ports run out on the proxy's side",
			}
			if p.Kind == "haproxy" {
				f.Doc = haproxyDoc + "#4-http-reuse"
			}
			return []Finding{f}
		})

	register("buffering", "performance", nginxProxy+"#proxy_buffering",
		func(p *Proxy) []Finding {
			if p.Kind != "nginx" {
				return nil
			}
			var lines []string
			for _, l := range p.Nginx.all("proxy_pass") {
				d := l.Parent.inherited("proxy_buffering")
				if len(d) == 0 || (len(d[0].Args) > 0 && d[0].Args[0] == "on") {
					lines = append(lines, strconv.Itoa(l.Line))
				}
			}
			if len(lines) == 0 {
				return nil
			}
			return []Finding{{
				Current:  "proxy_buffering on (nginx's default) for the proxy_pass of line " + strings.Join(lines, ", ") + " in " + p.Path,
				Proposed: "proxy_buffering off where the application streams its responses, or the X-Accel-Buffering: no header on those responses",
				Why: "nginx then holds the response until its buffers fill or the response ends: a stream " +
					"sent batch by batch reaches the client in lumps, its first byte as late as the " +
					"buffers are large, and the memory a stream was meant to spare is spent in the proxy",
			}}
		})

	register("timeouts", "reliability", haproxyDoc+"#4-timeout%20client",
		func(p *Proxy) []Finding {
			var missing []string
			for i, s := range p.HAProxy {
				var want []string
				switch s.Kind {
				case "frontend":
					want = []string{"client"}
				case "backend":
					want = []string{"connect", "server"}
				case "listen":
					want = []string{"client", "connect", "server"}
				}
				for _, w := range want {
					if !p.has(i, "timeout", w) {
						missing = append(missing, fmt.Sprintf("timeout %s in %s (line %d)", w, s.label(), s.Line))
					}
				}
			}
			if len(missing) == 0 {
				return nil
			}
			return []Finding{{
				Current:  "no " + strings.Join(missing, ", ") + ", nor in a defaults section, in " + p.Path,
				Proposed: "timeout connect, client and server in the defaults section, the server's longer than the longest pause of a streamed response",
				Why: "an absent timeout is an infinite one: a client that stops reading, or an application " +
					"that stops answering, holds a connection and its buffers for ever, and the proxy " +
					"reaches maxconn with idle connections — HAProxy warns about it at start",
			}}
		})

	register("tls-protocols", "security", "https://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_protocols",
		func(p *Proxy) []Finding {
			old := func(v string) bool {
				v = strings.ToLower(v)
				return v == "sslv3" || v == "tlsv1" || v == "tlsv1.0" || v == "tlsv1.1"
			}
			var bad []string
			if p.Kind == "nginx" {
				for _, d := range p.Nginx.all("ssl_protocols") {
					if slices.ContainsFunc(d.Args, old) {
						bad = append(bad, fmt.Sprintf("line %d: ssl_protocols %s", d.Line, strings.Join(d.Args, " ")))
					}
				}
			}
			for _, s := range p.HAProxy {
				for _, l := range s.Lines {
					// on a bind line, or in ssl-default-bind-options
					if k := slices.Index(l, "ssl-min-ver"); k >= 0 && k+1 < len(l) && old(l[k+1]) {
						bad = append(bad, fmt.Sprintf("%s: %s", s.label(), strings.Join(l, " ")))
					}
				}
			}
			if len(bad) == 0 {
				return nil
			}
			f := Finding{
				Current:  "TLS versions older than 1.2 accepted in " + p.Path + ": " + strings.Join(bad, "; "),
				Proposed: "TLSv1.2 TLSv1.3 (nginx's default since 1.23.4); HAProxy ssl-min-ver TLSv1.2",
				Why: "TLS 1.0 and 1.1 are deprecated by RFC 8996: their handshakes rest on SHA-1 and MD5, and " +
					"a client offered them can be steered down to them; no current browser needs them",
			}
			if p.Kind == "haproxy" {
				f.Doc = haproxyDoc + "#5.1-ssl-min-ver"
			}
			return []Finding{f}
		})

	register("forwarded-for", "reliability", nginxProxy+"#proxy_set_header",
		func(p *Proxy) []Finding {
			var bad []string
			if p.Kind == "nginx" {
				for _, l := range p.Nginx.all("proxy_pass") {
					if _, ok := l.Parent.header("X-Forwarded-For"); !ok {
						bad = append(bad, fmt.Sprintf("the proxy_pass of line %d", l.Line))
					}
				}
			}
			for i, s := range p.HAProxy {
				if (s.Kind == "backend" || s.Kind == "listen") && p.httpMode(i) && !p.has(i, "option", "forwardfor") {
					bad = append(bad, fmt.Sprintf("%s (line %d)", s.label(), s.Line))
				}
			}
			if len(bad) == 0 {
				return nil
			}
			f := Finding{
				Current:  "X-Forwarded-For not set for " + strings.Join(bad, ", ") + " in " + p.Path,
				Proposed: "nginx: proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for; HAProxy: option forwardfor",
				Why: "the application then sees the proxy as every client: its logs, its rate limits and " +
					"any decision by address apply to the proxy's address, one for all",
			}
			if p.Kind == "haproxy" {
				f.Doc = haproxyDoc + "#4-option%20forwardfor"
			}
			return []Finding{f}
		})
}

// nginxKeepalive lists what keeps the connections to the application from
// being reused: an upstream block without keepalive, a proxy_pass to an
// address no upstream block names, HTTP/1.0 towards it (nginx's default),
// or a Connection header left at "close".
func nginxKeepalive(p *Proxy) []string {
	upstreams := map[string]*ngxBlock{}
	for _, u := range p.Nginx.all("upstream") {
		if len(u.Args) > 0 {
			upstreams[u.Args[0]] = u
		}
	}
	var bad []string
	for _, l := range p.Nginx.all("proxy_pass") {
		if len(l.Args) == 0 {
			continue
		}
		host := l.Args[0]
		if _, after, ok := strings.Cut(host, "://"); ok {
			host = after
		}
		host, _, _ = strings.Cut(host, "/")
		u, named := upstreams[host]
		switch {
		case !named:
			bad = append(bad, fmt.Sprintf("line %d: proxy_pass to %s, which no upstream block with keepalive names", l.Line, host))
			continue
		case len(u.own("keepalive")) == 0:
			bad = append(bad, fmt.Sprintf("line %d: upstream %s without keepalive", u.Line, host))
			continue
		}
		v := l.Parent.inherited("proxy_http_version")
		if len(v) == 0 || len(v[0].Args) == 0 || v[0].Args[0] != "1.1" {
			bad = append(bad, fmt.Sprintf("line %d: proxy_pass over HTTP/1.0 (proxy_http_version not 1.1)", l.Line))
		}
		if c, ok := l.Parent.header("Connection"); !ok || c != "" {
			bad = append(bad, fmt.Sprintf("line %d: proxy_pass with Connection: close (proxy_set_header Connection \"\" absent)", l.Line))
		}
	}
	return bad
}

// keepaliveGaps lists what closes the proxy's connections to the
// application after each request, empty when nothing does.
func (p *Proxy) keepaliveGaps() []string {
	var bad []string
	if p.Kind == "nginx" {
		bad = nginxKeepalive(p)
	}
	for i, s := range p.HAProxy {
		if s.Kind == "frontend" || s.Kind == "global" {
			continue
		}
		for _, opt := range []string{"httpclose", "http-server-close"} {
			if p.has(i, "option", opt) {
				bad = append(bad, fmt.Sprintf("%s (line %d): option %s", s.label(), s.Line, opt))
			}
		}
		if p.has(i, "http-reuse", "never") {
			bad = append(bad, fmt.Sprintf("%s (line %d): http-reuse never", s.label(), s.Line))
		}
	}
	return bad
}
