package audit

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func openStackOf(t *testing.T, name, body string) *Stack {
	t.Helper()
	st, err := openStack(seedFile(t, name, body))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// TestStackTimeoutsAreRead: each stack's idle and read timeouts are read
// from the file that sets them, in the unit and the forms that stack
// takes, and their defaults stand when the file leaves them out.
func TestStackTimeoutsAreRead(t *testing.T) {
	check := func(name string, got timeout, d time.Duration, set, known bool) {
		t.Helper()
		if got.d != d || got.set != set || got.known != known {
			t.Errorf("%s: %+v, want %v set=%v known=%v", name, got, d, set, known)
		}
	}

	n := openStackOf(t, "server.js", "const server = http.createServer({ requestTimeout: 120_000 }, app);\n"+
		"server.keepAliveTimeout = 65 * 1000;\nserver.headersTimeout = cfg.headers;\nif (server.headersTimeout == 0) {}\n").Node
	check("node keepAliveTimeout", n.keepAlive, 65*time.Second, true, true)
	check("node headersTimeout", n.headers, 0, true, false)
	check("node requestTimeout", n.request, 120*time.Second, true, true)
	if !n.requestOption {
		t.Error("requestTimeout given to createServer, not seen as an option")
	}
	n = openStackOf(t, "server.js", "http.createServer(app).listen(8443);\n").Node
	check("node keepAliveTimeout default", n.keepAlive, 5*time.Second, false, true)
	check("node headersTimeout default", n.headers, 60*time.Second, false, true)
	check("node requestTimeout default", n.request, 300*time.Second, false, true)

	for props, want := range map[string]time.Duration{
		"server.port=8443\n":                                                          60 * time.Second,
		"server.tomcat.connection-timeout=20s\n":                                      20 * time.Second,
		"server.tomcat.connection-timeout=5000\n":                                     5 * time.Second,
		"SERVER_TOMCAT_CONNECTIONTIMEOUT=PT1M30S\n":                                   90 * time.Second,
		"server.tomcat.connection-timeout=20s\nserver.tomcat.keep-alive-timeout=1d\n": 24 * time.Hour,
		"server.tomcat.keep-alive-timeout=-1\n":                                       -time.Millisecond,
	} {
		s := openStackOf(t, "application.properties", props).Spring
		if got := s.idleTimeout(); got.d != want || !got.known {
			t.Errorf("%q: Tomcat idle %+v, want %v", props, got, want)
		}
	}

	k := openStackOf(t, "appsettings.json", `{"kestrel": {"Limits": {"KeepAliveTimeout": "00:00:10", `+
		`"requestheaderstimeout": "1.00:00:30.5"}}}`).Kestrel
	check("kestrel KeepAliveTimeout", k.keepAlive, 10*time.Second, true, true)
	check("kestrel RequestHeadersTimeout", k.headers, 24*time.Hour+30*time.Second+500*time.Millisecond, true, true)
	k = openStackOf(t, "appsettings.json", `{"Kestrel": {"Limits": {"RequestHeadersTimeout": "-00:00:00.0010000", `+
		`"KeepAliveTimeout": "soon"}}}`).Kestrel
	check("kestrel infinite", k.headers, -time.Millisecond, true, true)
	check("kestrel unreadable", k.keepAlive, 0, true, false)
	// The study's own appsettings.json: logging only, Kestrel at its defaults.
	st := openStackOf(t, "appsettings.json", `{"Logging": {"LogLevel": {"Default": "Information"}}, "AllowedHosts": "*"}`)
	check("kestrel KeepAliveTimeout default", st.Kestrel.keepAlive, 130*time.Second, false, true)
	check("kestrel RequestHeadersTimeout default", st.Kestrel.headers, 30*time.Second, false, true)
	if st.Npgsql != nil {
		t.Errorf("no connection string, read as %+v", st.Npgsql)
	}

	u := openStackOf(t, "start.txt", "uvicorn main:app --timeout-keep-alive 75\n").Uvicorn
	check("uvicorn", u.idleTimeout(), 75*time.Second, true, true)
}

// idleLine is a stage of the given rate whose budget the server ended
// closed times.
func idleLine(sent, rate, conns, closed int) string {
	return fmt.Sprintf(`{"scenario":"unit","rate":%d,"conns":%d,"new_conns_pct":0,"sent":%d,"ok":%d,"closed":%d,"reconnects":%d}`+"\n",
		rate, conns, sent, sent, closed, closed)
}

// TestIdleClosuresArePutOnTheTimeout: a budget that leaves each
// connection idle for 8 s, at 250 requests a second over 2 000
// connections, and a server that ended nearly every one: the closures are
// put on the idle timeout the stack read keeps — Node's 5 s default — and
// not on a limit of requests per connection; a stack whose timeout is
// past the gap, or closures too few to be every request's, say nothing of
// it.
func TestIdleClosuresArePutOnTheTimeout(t *testing.T) {
	load := seedFile(t, "stage.jsonl", idleLine(100000, 250, 2000, 99000))
	node := seedFile(t, "server.js", "http.createServer(app);\n")
	got := stackFindings(t, Spec{LoadLine: load, StackConfig: node})
	idle := got["app/idle-timeout"]
	if len(idle) != 1 || !mentions(idle, "waits 8s between two requests") ||
		!mentions(idle, "Node's http server (server.keepAliveTimeout 5s, the default, "+node+" leaves it out)") ||
		!strings.Contains(idle[0].Proposed, "server.keepAliveTimeout above 9s; or a budget below 1250 connections") {
		t.Errorf("Node's default: %+v", idle)
	}
	if len(got["app/requests-per-connection"]) != 0 {
		t.Errorf("the closures put on a request limit as well: %+v", got["app/requests-per-connection"])
	}

	// Nothing read of the server: every default under 8 s is named.
	got = stackFindings(t, Spec{LoadLine: load})
	if idle := got["app/idle-timeout"]; len(idle) != 1 || !mentions(idle, "uvicorn") || !mentions(idle, "Apache httpd") ||
		mentions(idle, "Tomcat") || mentions(idle, "Kestrel") {
		t.Errorf("no stack read: %+v", idle)
	}

	// Tomcat's connection-timeout set to 5 s, which keep-alive-timeout follows.
	spring := seedFile(t, "application.properties", "server.tomcat.connection-timeout=5s\nspring.threads.virtual.enabled=true\n")
	got = stackFindings(t, Spec{LoadLine: load, StackConfig: spring})
	if idle := got["app/idle-timeout"]; len(idle) != 1 || !mentions(idle, "server.tomcat.connection-timeout 5s, in "+spring) {
		t.Errorf("Tomcat's connection-timeout: %+v", idle)
	}

	for name, spec := range map[string]Spec{
		"Node's timeout past the gap": {LoadLine: load, StackConfig: seedFile(t, "server.js", "server.keepAliveTimeout = 10000;\n")},
		"Kestrel's 130 s":             {LoadLine: load, StackConfig: seedFile(t, "appsettings.json", `{"AllowedHosts": "*"}`)},
		"closures too few":            {LoadLine: seedFile(t, "stage.jsonl", idleLine(100000, 250, 2000, 1000)), StackConfig: node},
		"no rate on the line":         {LoadLine: seedFile(t, "stage.jsonl", idleLine(100000, 0, 2000, 99000))},
	} {
		if got := stackFindings(t, spec)["app/idle-timeout"]; len(got) != 0 {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

// TestReadTimeoutsTurnedOff: a timeout on a request still arriving, or on
// the wait for the next one, set to no limit is found in each stack's own
// form; the defaults are guards, and say nothing.
func TestReadTimeoutsTurnedOff(t *testing.T) {
	node := seedFile(t, "server.js", "const server = http.createServer({ requestTimeout: 0 }, app);\n")
	spring := seedFile(t, "application.properties", "server.tomcat.connection-timeout=-1\n")
	dotnet := seedFile(t, "appsettings.json", `{"Kestrel": {"Limits": {"RequestHeadersTimeout": "-00:00:00.0010000"}}}`)
	got := stackFindings(t, Spec{StackConfig: strings.Join([]string{node, spring, dotnet}, ",")})["app/read-timeouts"]
	for _, want := range []string{
		"server.requestTimeout 0 (no limit), in " + node,
		"server.headersTimeout left out with requestTimeout 0 given to createServer",
		"server.tomcat.connection-timeout with no limit, in " + spring,
		"Kestrel:Limits:RequestHeadersTimeout with no limit, in " + dotnet,
	} {
		if !mentions(got, want) {
			t.Errorf("no %q in %+v", want, got)
		}
	}

	node = seedFile(t, "server.js", "const server = http.createServer(app);\nserver.requestTimeout = 0;\nserver.headersTimeout = 30000;\n")
	got = stackFindings(t, Spec{StackConfig: node})["app/read-timeouts"]
	if len(got) != 1 || !mentions(got, "server.requestTimeout 0") || mentions(got, "headersTimeout") {
		t.Errorf("headersTimeout set, requestTimeout off after createServer: %+v", got)
	}

	spring = seedFile(t, "application.properties", "server.port=8443\n")
	dotnet = seedFile(t, "appsettings.json", `{"AllowedHosts": "*"}`)
	node = seedFile(t, "server.js", "http.createServer(app);\n")
	if got := stackFindings(t, Spec{StackConfig: strings.Join([]string{node, spring, dotnet}, ",")})["app/read-timeouts"]; len(got) != 0 {
		t.Errorf("every default: %+v", got)
	}
}
