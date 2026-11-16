package audit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

// What the study's load generator saw of a stage (comparison/loadgen,
// report.go): the requests it sent, the connections it kept for them —
// the budget — and how many of those the server ended. A closure is the
// server's decision: the generator closes none of the budget's before the
// end, and the fresh connections a stage opens on purpose (§3.6,
// new_conns_pct) are its own and count in neither.

// LoadLine is the generator's own line for a stage.
type LoadLine struct {
	Path        string
	Sent        int // requests sent, the fresh connections' included
	Rate        int // requests a second the stage was set to send
	Conns       int // the budget: connections kept between requests
	NewConnsPct int // the share of requests sent on a fresh connection of their own
	Closed      int // connections of the budget the server ended
}

// Fresh is the requests sent on a fresh connection, opened for them alone.
func (l *LoadLine) Fresh() int { return l.Sent * l.NewConnsPct / 100 }

// openLoadLine reads a stage's line: the generator's stage.jsonl, or the
// chain's merged line, which carries the same fields. A file of several
// lines (a plateau's windows, then its own) gives the run's line, the last
// one without a window index.
func openLoadLine(path string) (*LoadLine, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
	if err != nil {
		return nil, err
	}
	var found *LoadLine
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(nil, 1<<22)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var v struct {
			Window   *int `json:"window"`
			Sent     *int `json:"sent"`
			Rate     int  `json:"rate"`
			Conns    *int `json:"conns"`
			NewConns int  `json:"new_conns_pct"`
			Closed   *int `json:"closed"`
		}
		if err := json.Unmarshal(line, &v); err != nil {
			return nil, fmt.Errorf("%s: not the load generator's JSON lines: %w", path, err)
		}
		if v.Window != nil {
			continue
		}
		if v.Sent == nil || v.Conns == nil || v.Closed == nil {
			return nil, fmt.Errorf("%s: a line without sent, conns and closed: want the load generator's stage line", path)
		}
		found = &LoadLine{Path: path, Sent: *v.Sent, Rate: v.Rate, Conns: *v.Conns, NewConnsPct: v.NewConns, Closed: *v.Closed}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if found == nil {
		return nil, fmt.Errorf("%s: no stage line: want the load generator's stage.jsonl", path)
	}
	return found, nil
}

// perConnSetting is a server that closes a connection after a number of
// requests by default, and the setting that raises it.
type perConnSetting struct {
	n       int
	server  string
	setting string
	fix     string
	match   func(t *Target) bool // the target shows this server; nil when nothing read can say
}

// perConnDefaults are the defaults the audit knows, each read in the
// server's own documentation. Go's net/http, Node's http (maxRequestsPerSocket
// 0), Kestrel, uvicorn, hyper and HAProxy set no such limit.
var perConnDefaults = []perConnSetting{
	{
		100, "Tomcat (Spring Boot's embedded server)", "server.tomcat.max-keep-alive-requests 100",
		"server.tomcat.max-keep-alive-requests=-1 (no limit) or 10000 in application.properties", nil,
	},
	{100, "Apache httpd", "MaxKeepAliveRequests 100", "MaxKeepAliveRequests 0 (no limit) or 10000", nil},
	{
		100, "nginx before 1.19.10", "keepalive_requests 100", "keepalive_requests 10000, or nginx 1.19.10 or later",
		func(t *Target) bool { return t.Proxy != nil && t.Proxy.Kind == "nginx" },
	},
	{
		1000, "nginx 1.19.10 and later", "keepalive_requests 1000", "keepalive_requests 10000",
		func(t *Target) bool { return t.Proxy != nil && t.Proxy.Kind == "nginx" },
	},
	{
		1024, "Sluice.go's httpstream", "MaxRequestsPerConn 1024 (the package's default)",
		"-max-requests-per-conn above 1024; the study's axis D measures it",
		func(t *Target) bool { return t.App != nil && t.App.httpstream() },
	},
}

// rareClosures is the bound past which a server's closures stop being
// seen by the percentiles a comparison publishes: up to the p99.9, one
// request in a thousand; a closure every 1,250 requests still puts a
// handshake on eight of every ten requests of that tail.
const rareClosures = 1250

func init() {
	Register(Rule{
		Name:  "app/requests-per-connection",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://tomcat.apache.org/tomcat-10.1-doc/config/http.html",
		Check: func(t *Target, _ Answers) []Finding {
			l := t.Load
			if l == nil || l.Closed <= 0 {
				return nil
			}
			if _, idle := idleClosures(t); len(idle) > 0 {
				return nil // app/idle-timeout: the closures are an idle timeout's
			}
			kept := l.Sent - l.Fresh()
			// Each connection of the budget carries kept / conns requests and
			// is closed after every n of them: closed is between
			// kept / n − conns and kept / n.
			hi := kept / l.Closed
			lo := (kept + l.Closed + l.Conns - 1) / (l.Closed + l.Conns)
			if hi > rareClosures {
				return nil
			}
			var known, shown []perConnSetting
			for _, s := range perConnDefaults {
				if lo <= s.n && s.n <= hi {
					known = append(known, s)
					if s.match != nil && s.match(t) {
						shown = append(shown, s)
					}
				}
			}
			if len(shown) > 0 {
				known = shown
			}
			current := fmt.Sprintf("the server ended %d of the connections kept for %d requests (%s): "+
				"it closes each after %d to %d requests", l.Closed, kept, l.Path, lo, hi)
			proposed := "the server's limit of requests per connection raised or removed"
			if len(known) > 0 {
				var names, fixes []string
				for _, s := range known {
					names = append(names, s.server+" ("+s.setting+")")
					if !slices.Contains(fixes, s.fix) {
						fixes = append(fixes, s.fix)
					}
				}
				current += ", the default of " + strings.Join(names, ", or ")
				proposed = strings.Join(fixes, "; or ")
			} else {
				current += ", a limit that is no default the audit knows"
			}
			return []Finding{{
				Current:  current,
				Proposed: proposed,
				Why: fmt.Sprintf("one request in %d is sent on a connection opened again, after a TCP handshake "+
					"and a TLS one: a round trip or two that the published percentiles up to the p99.9 "+
					"see, and that a server without the limit does not pay", hi),
			}}
		},
	})
}
