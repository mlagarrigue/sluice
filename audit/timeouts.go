package audit

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// How long a contender's HTTP server waits for a request: the next one on
// a connection left idle (Node's keepAliveTimeout, uvicorn's
// --timeout-keep-alive, Tomcat's keep-alive-timeout or the
// connection-timeout it follows, Kestrel's KeepAliveTimeout), and the
// request itself as it arrives (Node's headersTimeout and requestTimeout,
// Tomcat's connection-timeout, Kestrel's RequestHeadersTimeout).
//
// The idle timeout has a symptom the study's generator counts: a budget of
// connections too large for the rate leaves each idle for conns / rate
// seconds between two requests, and past the timeout the server ends every
// one of them — closed then counts nearly every request, which
// app/requests-per-connection would read as a limit of one request per
// connection. The read timeouts have none under that load: its requests
// arrive whole, in microseconds, and never meet a timeout of seconds. What
// they guard against is a client that sends slowly or not at all, so the
// rule on them is the one setting that removes the guard.

// idleSetting is how long a server keeps a connection idle, and the
// setting that raises it.
type idleSetting struct {
	server  string
	setting string // with its value, and where it comes from when read
	d       time.Duration
	fix     string
	match   func(t *Target) bool // the target shows this server; nil when nothing read can say
}

// idleDefaults are the idle timeouts the audit knows, each read in the
// server's own documentation, for a load whose server was not read. Go's
// net/http has none unless IdleTimeout or ReadTimeout is set.
var idleDefaults = []idleSetting{
	{server: "Node's http server", setting: "server.keepAliveTimeout 5s", d: nodeKeepAliveDefault, fix: "server.keepAliveTimeout"},
	{server: "uvicorn", setting: "--timeout-keep-alive 5", d: uvicornKeepAliveDflt, fix: "--timeout-keep-alive"},
	{server: "Apache httpd", setting: "KeepAliveTimeout 5", d: 5 * time.Second, fix: "KeepAliveTimeout"},
	{
		server: "Tomcat (Spring Boot's embedded server)", setting: "server.tomcat.connection-timeout 60s, which keep-alive-timeout follows",
		d: tomcatTimeoutDefault, fix: "server.tomcat.keep-alive-timeout",
	},
	{
		server: "nginx", setting: "keepalive_timeout 75s", d: 75 * time.Second, fix: "keepalive_timeout",
		match: func(t *Target) bool { return t.Proxy != nil && t.Proxy.Kind == "nginx" },
	},
	{
		server: "Sluice.go's httpstream", setting: "IdleTimeout 2m0s (the package's default)", d: 2 * time.Minute, fix: "IdleTimeout",
		match: func(t *Target) bool { return t.App != nil && t.App.httpstream() },
	},
	{server: "Kestrel", setting: "Kestrel:Limits:KeepAliveTimeout 2m10s", d: kestrelKeepAliveDflt, fix: "Kestrel:Limits:KeepAliveTimeout"},
}

// idleSettings are the idle timeouts of the stacks read, with a value and
// a limit: a timeout set to an expression, or to none, explains nothing.
func (st *Stack) idleSettings() []idleSetting {
	var out []idleSetting
	add := func(server, path string, o timeout, fix string) {
		if o.known && o.d > 0 {
			out = append(out, idleSetting{server: server, setting: o.String() + ", " + o.where(path), d: o.d, fix: fix})
		}
	}
	if n := st.Node; n != nil {
		add("Node's http server", n.Path, n.keepAlive, "server.keepAliveTimeout")
	}
	if u := st.Uvicorn; u != nil {
		add("uvicorn", u.Path, u.idleTimeout(), "--timeout-keep-alive")
	}
	if s := st.Spring; s != nil {
		add("Tomcat (Spring Boot's embedded server)", s.Path, s.idleTimeout(), "server.tomcat.keep-alive-timeout")
	}
	if k := st.Kestrel; k != nil {
		add("Kestrel", k.Path, k.keepAlive, "Kestrel:Limits:KeepAliveTimeout")
	}
	return out
}

// idleClosures is the idle gap of the load's connections, and the idle
// timeouts it passes when they explain the closures: most requests found
// their connection ended (each of the budget's carries one request every
// conns / rate seconds, the workers taking the jobs in turn), and the gap
// is past the timeout. The stack read says which timeout; without one,
// every default the gap passes is named, the proxy or the server read
// choosing when one of them fits.
func idleClosures(t *Target) (time.Duration, []idleSetting) {
	l := t.Load
	if l == nil || l.Rate <= 0 || l.Conns <= 0 {
		return 0, nil
	}
	kept := l.Sent - l.Fresh()
	if kept <= 0 || l.Closed*2 < kept {
		return 0, nil
	}
	gap := time.Duration(int64(l.Conns) * int64(time.Second) / int64(l.Rate))
	cands := stackOf(t).idleSettings()
	if len(cands) == 0 {
		cands = idleDefaults
	}
	var under, shown []idleSetting
	for _, c := range cands {
		if c.d <= gap {
			under = append(under, c)
			if c.match != nil && c.match(t) {
				shown = append(shown, c)
			}
		}
	}
	if len(shown) > 0 {
		under = shown
	}
	return gap, under
}

// readGuards are the stack's timeouts on a request still arriving, or on
// the wait for the next one, set to no limit.
func (st *Stack) readGuards() []string {
	var off []string
	none := func(o timeout, path string) {
		off = append(off, fmt.Sprintf("%s, in %s", o, path))
	}
	if n := st.Node; n != nil {
		for _, o := range []timeout{n.headers, n.request, n.keepAlive} {
			if o.known && o.d == 0 {
				off = append(off, fmt.Sprintf("%s 0 (no limit), in %s", o.setting, n.Path))
			}
		}
		if !n.headers.set && n.requestOption && n.request.known && n.request.d == 0 {
			off = append(off, fmt.Sprintf("server.headersTimeout left out with requestTimeout 0 given to createServer, "+
				"whose default is the minimum of the two: 0 (no limit), in %s", n.Path))
		}
	}
	if s := st.Spring; s != nil {
		conn := s.tomcatTimeout("server.tomcat.connection-timeout", timeout{})
		if conn.known && conn.d < 0 {
			none(conn, s.Path)
		}
		if keep := s.tomcatTimeout("server.tomcat.keep-alive-timeout", timeout{}); keep.known && keep.d < 0 {
			none(keep, s.Path)
		}
	}
	if k := st.Kestrel; k != nil {
		for _, o := range []timeout{k.headers, k.keepAlive} {
			if o.known && o.d < 0 {
				none(o, k.Path)
			}
		}
	}
	return off
}

func init() {
	Register(Rule{
		Name:  "app/idle-timeout",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://www.rfc-editor.org/rfc/rfc9112#section-9.5",
		Check: func(t *Target, _ Answers) []Finding {
			gap, under := idleClosures(t)
			if len(under) == 0 {
				return nil
			}
			l := t.Load
			var names, fixes []string
			least := under[0].d
			for _, s := range under {
				names = append(names, s.server+" ("+s.setting+")")
				if !slices.Contains(fixes, s.fix) {
					fixes = append(fixes, s.fix)
				}
				least = min(least, s.d)
			}
			above := gap.Truncate(time.Second) + time.Second
			return []Finding{{
				Current: fmt.Sprintf("the server ended %d of the connections kept for %d requests (%s): each of the %d "+
					"connections waits %s between two requests at %d a second, past the idle timeout of %s",
					l.Closed, l.Sent-l.Fresh(), l.Path, l.Conns, gap, l.Rate, strings.Join(names, ", or ")),
				Proposed: fmt.Sprintf("%s above %s; or a budget below %d connections at %d requests a second",
					strings.Join(fixes, " or "), above, int64(least)*int64(l.Rate)/int64(time.Second), l.Rate),
				Why: "the server closes a connection left idle past the timeout: the next request on it opens " +
					"another, a TCP handshake and a TLS one before it is sent, or fails with a reset when the close " +
					"crosses it. Every request then pays it, and the latencies published are the handshakes'; " +
					"the closures are no limit of requests per connection",
			}}
		},
	})

	Register(Rule{
		Name:  "app/read-timeouts",
		Needs: Selection{Components: {"app"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   "https://nodejs.org/api/http.html#serverheaderstimeout",
		Check: func(t *Target, _ Answers) []Finding {
			off := stackOf(t).readGuards()
			if len(off) == 0 {
				return nil
			}
			return []Finding{{
				Current: strings.Join(off, "; "),
				Proposed: "each left at its default (Node: headersTimeout 60 s, requestTimeout 300 s, keepAliveTimeout 5 s; " +
					"Tomcat: connection-timeout 60 s; Kestrel: RequestHeadersTimeout 30 s, KeepAliveTimeout 130 s), " +
					"or set to a number of seconds",
				Why: "a client that sends its request a byte at a time, or opens a connection and sends nothing, " +
					"holds the connection for as long as it likes: a few hundred such clients hold every connection " +
					"the server accepts, and the others wait or are refused. A load of whole requests never shows " +
					"it; the guard is the timeout, and nothing stands in for it unless a proxy in front buffers the request",
			}}
		},
	})
}
