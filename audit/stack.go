package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The settings a contender's stack starts with, read from the file that
// holds them, each kind recognised by its content: a Spring Boot
// application.properties (or the SPRING_ and SERVER_ variables of its
// environment, the same keys relaxed), the command line that starts
// uvicorn, a Node server's source, an ASP.NET Core appsettings.json with
// its Kestrel limits and the Npgsql connection string in it. A setting
// left out is the stack's default, which the rules below name with the
// place it is documented.

// Stack is the settings files read, by kind.
type Stack struct {
	Spring  *springConfig
	Uvicorn *uvicornConfig
	Node    *nodeConfig
	Kestrel *kestrelConfig
	Npgsql  *npgsqlConfig
}

// timeout is a delay a stack's server keeps: how long it waits for a
// request to arrive, or for the next one on an idle connection.
type timeout struct {
	setting string        // as the file names it
	d       time.Duration // negative: no limit
	set     bool          // the file sets it; false is the stack's default
	known   bool          // d was read; false for an expression the audit does not evaluate
}

// where says where a timeout's value comes from.
func (o timeout) where(path string) string {
	if o.set {
		return "in " + path
	}
	return "the default, " + path + " leaves it out"
}

// String is the timeout as a person reads it.
func (o timeout) String() string {
	switch {
	case !o.known:
		return o.setting + " set to an expression"
	case o.d < 0:
		return o.setting + " with no limit"
	}
	return o.setting + " " + o.d.String()
}

func defaultTimeout(setting string, d time.Duration) timeout {
	return timeout{setting: setting, d: d, known: true}
}

type springConfig struct {
	Path  string
	props map[string]string // canonical key → value
}

// springKey is Spring Boot's relaxed binding reduced to one form: case,
// dots, dashes and underscores dropped, so that server.tomcat.threads.max
// and SERVER_TOMCAT_THREADS_MAX are one key.
func springKey(k string) string {
	return strings.NewReplacer(".", "", "-", "", "_", "").Replace(strings.ToLower(strings.TrimSpace(k)))
}

func (s *springConfig) get(key string) (string, bool) {
	v, ok := s.props[springKey(key)]
	return v, ok
}

type uvicornConfig struct {
	Path  string
	flags map[string]string // --name → value, "" for a switch
}

type nodeConfig struct {
	Path      string
	keepAlive timeout // server.keepAliveTimeout, 5 s
	headers   timeout // server.headersTimeout, 60 s
	request   timeout // server.requestTimeout, 300 s
	// requestOption: requestTimeout given to createServer, where it also
	// sets the default of headersTimeout, "the minimum between
	// server.requestTimeout or 60000".
	requestOption bool
	poolMax       bool // a pg Pool given its max
	pool          bool // a pg Pool built at all
}

// kestrelConfig is the Kestrel:Limits section of an appsettings.json,
// which ASP.NET Core binds onto KestrelServerOptions.
type kestrelConfig struct {
	Path      string
	keepAlive timeout // Limits.KeepAliveTimeout, 130 s
	headers   timeout // Limits.RequestHeadersTimeout, 30 s
}

type npgsqlConfig struct {
	Path    string
	maxPool string // Maximum Pool Size, empty when left out
}

// The defaults, each from the server's own documentation: Node's
// http.Server, Tomcat's HTTP connector (connectionTimeout, which
// keepAliveTimeout follows when it is not set), Kestrel's
// KestrelServerLimits, uvicorn's settings.
const (
	nodeKeepAliveDefault  = 5 * time.Second
	nodeHeadersDefault    = 60 * time.Second
	nodeRequestDefault    = 300 * time.Second
	tomcatTimeoutDefault  = 60 * time.Second
	kestrelKeepAliveDflt  = 130 * time.Second
	kestrelHeadersDefault = 30 * time.Second
	uvicornKeepAliveDflt  = 5 * time.Second
)

// openStack reads every file named, comma-separated.
func openStack(paths string) (*Stack, error) {
	st := &Stack{}
	for _, path := range strings.Split(paths, ",") {
		path = strings.TrimSpace(path)
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		text := string(b)
		switch ext := strings.ToLower(filepath.Ext(path)); {
		case ext == ".js" || ext == ".mjs" || ext == ".cjs" || ext == ".ts":
			st.Node = parseNode(path, text)
		case ext == ".json":
			if st.Kestrel, st.Npgsql, err = parseAppSettings(path, b); err != nil {
				return nil, err
			}
		case uvicornLine.MatchString(text):
			st.Uvicorn = parseUvicorn(path, text)
		case springLine.MatchString(text):
			st.Spring = parseSpring(path, text)
		default:
			return nil, fmt.Errorf("%s: not a stack's settings this audit reads: want application.properties, "+
				"the command line that starts uvicorn, a Node server's source or an appsettings.json", path)
		}
	}
	return st, nil
}

var (
	uvicornLine = regexp.MustCompile(`(?m)^\s*(\S*/)?(uvicorn|python3?\s+-m\s+uvicorn)\s`)
	springLine  = regexp.MustCompile(`(?mi)^\s*(spring|server)[._]`)
)

func parseSpring(path, text string) *springConfig {
	s := &springConfig{Path: path, props: map[string]string{}}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			k, v, ok = strings.Cut(line, ":")
		}
		if ok {
			s.props[springKey(k)] = strings.TrimSpace(v)
		}
	}
	return s
}

// idleTimeout is --timeout-keep-alive, in seconds.
func (u *uvicornConfig) idleTimeout() timeout {
	v, set := u.flags["--timeout-keep-alive"]
	if !set {
		return defaultTimeout("--timeout-keep-alive", uvicornKeepAliveDflt)
	}
	f, err := strconv.ParseFloat(v, 64)
	return timeout{setting: "--timeout-keep-alive", d: time.Duration(f * float64(time.Second)), set: true, known: err == nil}
}

func parseUvicorn(path, text string) *uvicornConfig {
	u := &uvicornConfig{Path: path, flags: map[string]string{}}
	f := strings.Fields(text[uvicornLine.FindStringIndex(text)[0]:])
	for i := 0; i < len(f); i++ {
		if !strings.HasPrefix(f[i], "--") {
			continue
		}
		name, value, ok := strings.Cut(f[i], "=")
		if !ok && i+1 < len(f) && !strings.HasPrefix(f[i+1], "-") {
			value = f[i+1]
			i++
		}
		u.flags[name] = value
	}
	return u
}

var (
	nodePool    = regexp.MustCompile(`new\s+(pg\.)?Pool\s*\(([^)]*)\)`)
	nodePoolMax = regexp.MustCompile(`\bmax\s*:`)
)

// nodeTimeout reads a timeout of Node's http.Server, assigned to the
// server (server.name = …) or given to createServer (name: …); the last
// in the file wins, as it would when the code runs. The value is in
// milliseconds: a literal, 75_000, or a product of literals, 75 * 1000.
func nodeTimeout(text, name string, def time.Duration) (o timeout, option bool) {
	o = defaultTimeout("server."+name, def)
	re := regexp.MustCompile(`(\.` + name + `\s*=|\b` + name + `\s*:)\s*([^=,;)}\n][^,;)}\n]*)`)
	ms := re.FindAllStringSubmatch(text, -1)
	if len(ms) == 0 {
		return o, false
	}
	m := ms[len(ms)-1]
	o.d, o.set, o.known = 0, true, false
	product := int64(1)
	for _, f := range strings.Split(strings.ReplaceAll(m[2], "_", ""), "*") {
		n, err := strconv.ParseInt(strings.TrimSpace(f), 10, 64)
		if err != nil {
			return o, strings.HasSuffix(m[1], ":")
		}
		product *= n
	}
	o.d, o.known = time.Duration(product)*time.Millisecond, true
	return o, strings.HasSuffix(m[1], ":")
}

func parseNode(path, text string) *nodeConfig {
	n := &nodeConfig{Path: path}
	n.keepAlive, _ = nodeTimeout(text, "keepAliveTimeout", nodeKeepAliveDefault)
	n.headers, _ = nodeTimeout(text, "headersTimeout", nodeHeadersDefault)
	n.request, n.requestOption = nodeTimeout(text, "requestTimeout", nodeRequestDefault)
	for _, m := range nodePool.FindAllStringSubmatch(text, -1) {
		n.pool = true
		if nodePoolMax.MatchString(m[2]) {
			n.poolMax = true
		}
	}
	return n
}

// springDuration reads a Spring Boot Duration: a number of milliseconds
// (the unit of the server.tomcat timeouts), a number with its unit (20s,
// 500ms, 1d) or ISO-8601 (PT20S). -1 is no limit.
func springDuration(v string) (time.Duration, bool) {
	v = strings.ToLower(strings.TrimSpace(v))
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Duration(n) * time.Millisecond, true
	}
	if iso, ok := strings.CutPrefix(strings.TrimPrefix(v, "-"), "pt"); ok {
		d, err := time.ParseDuration(iso)
		if strings.HasPrefix(v, "-") {
			d = -d
		}
		return d, err == nil
	}
	if days, ok := strings.CutSuffix(v, "d"); ok {
		n, err := strconv.ParseInt(days, 10, 64)
		return time.Duration(n) * 24 * time.Hour, err == nil
	}
	d, err := time.ParseDuration(v)
	return d, err == nil
}

// tomcatTimeout is a server.tomcat timeout of a Spring Boot application,
// its default when the file leaves it out.
func (s *springConfig) tomcatTimeout(key string, def timeout) timeout {
	v, set := s.get(key)
	if !set {
		return def
	}
	d, ok := springDuration(v)
	return timeout{setting: key, d: d, set: true, known: ok}
}

// idleTimeout is how long Tomcat keeps a connection idle between two
// requests: keep-alive-timeout, or connection-timeout when it is left out
// (the connector's keepAliveTimeout "is to use the value that has been
// set for the connectionTimeout attribute").
func (s *springConfig) idleTimeout() timeout {
	conn := s.tomcatTimeout("server.tomcat.connection-timeout",
		defaultTimeout("server.tomcat.connection-timeout", tomcatTimeoutDefault))
	return s.tomcatTimeout("server.tomcat.keep-alive-timeout", conn)
}

// timeSpan reads a .NET TimeSpan as configuration binds it:
// [-][d.]hh:mm:ss[.fffffff], or a whole number of days.
func timeSpan(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	neg := strings.HasPrefix(v, "-")
	v = strings.TrimPrefix(v, "-")
	parts := strings.Split(v, ":")
	var days int64
	if len(parts) == 1 {
		n, err := strconv.ParseInt(v, 10, 64)
		return time.Duration(n) * 24 * time.Hour, err == nil
	}
	if len(parts) > 3 {
		return 0, false
	}
	if d, h, ok := strings.Cut(parts[0], "."); ok {
		n, err := strconv.ParseInt(d, 10, 64)
		if err != nil {
			return 0, false
		}
		days, parts[0] = n, h
	}
	units := []time.Duration{time.Hour, time.Minute, time.Second}
	d := time.Duration(days) * 24 * time.Hour
	for i, p := range parts {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil || f < 0 {
			return 0, false
		}
		d += time.Duration(f * float64(units[i]))
	}
	if neg {
		d = -d
	}
	return d, true
}

// lookup is a configuration key read as .NET reads it, case ignored.
func lookup(m map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}

// parseAppSettings reads an ASP.NET Core appsettings.json: the
// Kestrel:Limits section, and the Npgsql connection string under
// ConnectionStrings, either left out. A JSON file is taken for one by its
// name (appsettings*.json) or by one of the two sections.
func parseAppSettings(path string, b []byte) (*kestrelConfig, *npgsqlConfig, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, nil, fmt.Errorf("%s: not appsettings.json: %w", path, err)
	}
	var limits, strs map[string]json.RawMessage
	if raw, ok := lookup(doc, "Kestrel"); ok {
		var kestrel map[string]json.RawMessage
		if err := json.Unmarshal(raw, &kestrel); err != nil {
			return nil, nil, fmt.Errorf("%s: Kestrel: %w", path, err)
		}
		if raw, ok := lookup(kestrel, "Limits"); ok {
			if err := json.Unmarshal(raw, &limits); err != nil {
				return nil, nil, fmt.Errorf("%s: Kestrel:Limits: %w", path, err)
			}
		}
	}
	_, hasKestrel := lookup(doc, "Kestrel")
	raw, hasStrings := lookup(doc, "ConnectionStrings")
	if hasStrings {
		if err := json.Unmarshal(raw, &strs); err != nil {
			return nil, nil, fmt.Errorf("%s: ConnectionStrings: %w", path, err)
		}
	}
	if !hasKestrel && !hasStrings && !strings.HasPrefix(strings.ToLower(filepath.Base(path)), "appsettings") {
		return nil, nil, errors.New(path + ": neither Kestrel nor ConnectionStrings: not an appsettings.json")
	}
	k := &kestrelConfig{
		Path:      path,
		keepAlive: limitOf(limits, "KeepAliveTimeout", kestrelKeepAliveDflt),
		headers:   limitOf(limits, "RequestHeadersTimeout", kestrelHeadersDefault),
	}
	for _, raw := range strs {
		var cs string
		if json.Unmarshal(raw, &cs) != nil {
			continue
		}
		n := &npgsqlConfig{Path: path}
		npgsql := false
		for _, kv := range strings.Split(cs, ";") {
			k, v, _ := strings.Cut(kv, "=")
			switch strings.ToLower(strings.Join(strings.Fields(k), " ")) {
			case "host", "server":
				npgsql = true
			case "maximum pool size", "max pool size", "maxpoolsize":
				n.maxPool = strings.TrimSpace(v)
			}
		}
		if npgsql {
			return k, n, nil
		}
	}
	return k, nil, nil
}

// limitOf is one of Kestrel's limits, its default when left out.
func limitOf(limits map[string]json.RawMessage, name string, def time.Duration) timeout {
	setting := "Kestrel:Limits:" + name
	raw, ok := lookup(limits, name)
	if !ok {
		return defaultTimeout(setting, def)
	}
	var v string
	if json.Unmarshal(raw, &v) != nil {
		return timeout{setting: setting, set: true}
	}
	d, known := timeSpan(v)
	return timeout{setting: setting, d: d, set: true, known: known}
}

func init() {
	Register(Rule{
		Name:  "app/tomcat-threads",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}},
		Doc:   "https://docs.spring.io/spring-boot/appendix/application-properties/index.html#appendix.application-properties.server",
		Check: func(t *Target, _ Answers) []Finding {
			s := stackOf(t).Spring
			if s == nil {
				return nil
			}
			var out []Finding
			if v, _ := s.get("spring.threads.virtual.enabled"); v != "true" {
				if _, set := s.get("server.tomcat.threads.max"); !set {
					out = append(out, Finding{
						Current: "server.tomcat.threads.max left at 200 and virtual threads off, in " + s.Path,
						Proposed: "spring.threads.virtual.enabled=true (Spring Boot 3.2 and later, Java 21), " +
							"or server.tomcat.threads.max set from the requests in flight the load holds",
						Why: "Spring MVC holds a worker thread for the whole of a request, its wait on the " +
							"database included: past 200 requests in flight the others queue in Tomcat " +
							"before any code runs, a wait the server's own timings never show",
					})
				}
			}
			if _, set := s.get("server.tomcat.max-connections"); !set && t.Load != nil && t.Load.Conns > 8192 {
				out = append(out, Finding{
					Current: fmt.Sprintf("server.tomcat.max-connections left at 8192 in %s, under a load of %d connections (%s)",
						s.Path, t.Load.Conns, t.Load.Path),
					Proposed: "server.tomcat.max-connections above the connections the load keeps open",
					Why: "past it Tomcat accepts no connection more: the next ones wait in the kernel's " +
						"accept queue (server.tomcat.accept-count, 100) and are refused once it is full",
				})
			}
			if _, set := s.get("spring.datasource.hikari.maximum-pool-size"); !set {
				out = append(out, Finding{
					Current:  "spring.datasource.hikari.maximum-pool-size left at HikariCP's 10, in " + s.Path,
					Proposed: "spring.datasource.hikari.maximum-pool-size set from the database's cores and max_connections",
					Why: "ten connections serve the requests in flight ten at a time: the others wait " +
						"for one (connectionTimeout, 30 s) while the database has cores to spare",
				})
			}
			return out
		},
	})

	Register(Rule{
		Name:  "app/uvicorn-defaults",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}},
		Doc:   "https://uvicorn.dev/settings/",
		Check: func(t *Target, _ Answers) []Finding {
			u := stackOf(t).Uvicorn
			if u == nil {
				return nil
			}
			var out []Finding
			if _, set := u.flags["--timeout-keep-alive"]; !set {
				out = append(out, Finding{
					Current:  "--timeout-keep-alive left at 5 seconds, in " + u.Path,
					Proposed: "--timeout-keep-alive above the longest pause of the clients, and above a proxy's own keep-alive towards it",
					Why: "uvicorn closes a connection idle for 5 s: a client that pauses longer, or a " +
						"proxy that keeps its connections for 60 s, sends its next request on a closed " +
						"one and pays a new handshake, or an error when the close crosses it",
				})
			}
			_, off := u.flags["--no-access-log"]
			if _, logging := u.flags["--access-log"]; logging || !off {
				out = append(out, Finding{
					Current:  "the access log on (no --no-access-log), in " + u.Path,
					Proposed: "--no-access-log, and the access log taken at a proxy if it is needed",
					Why: "uvicorn writes a line for every request from the event loop that serves them: " +
						"the formatting and the write are taken from the requests, see app/access-log",
				})
			}
			if _, set := u.flags["--workers"]; !set {
				out = append(out, Finding{
					Current:  "--workers left at 1 (or $WEB_CONCURRENCY), in " + u.Path,
					Proposed: "--workers set to the cores the server is given",
					Why: "one uvicorn process runs its event loop on one core: the others the server " +
						"is given stay idle, whatever the load",
				})
			}
			return out
		},
	})

	Register(Rule{
		Name:  "app/node-defaults",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}},
		Doc:   "https://nodejs.org/api/http.html#serverkeepalivetimeout",
		Check: func(t *Target, _ Answers) []Finding {
			n := stackOf(t).Node
			if n == nil {
				return nil
			}
			var out []Finding
			if !n.keepAlive.set {
				out = append(out, Finding{
					Current:  "server.keepAliveTimeout left at 5 seconds, in " + n.Path,
					Proposed: "server.keepAliveTimeout above the longest pause of the clients, and above a proxy's own keep-alive towards it",
					Why: "Node closes a connection idle for 5 s: a client or a proxy that keeps it longer " +
						"sends its next request on a closed one and pays a new handshake, or a reset",
				})
			}
			if n.pool && !n.poolMax {
				out = append(out, Finding{
					Current:  "a pg Pool without max, node-postgres's 10, in " + n.Path,
					Proposed: "max set from the database's cores and max_connections",
					Why: "ten connections serve the queries ten at a time: the others wait in the pool " +
						"while the database has cores to spare (https://node-postgres.com/apis/pool)",
				})
			}
			return out
		},
	})

	Register(Rule{
		Name:  "app/npgsql-pool",
		Needs: Selection{Components: {"app"}, Goals: {"reliability"}, Sources: {"config"}},
		Doc:   "https://www.npgsql.org/doc/connection-string-parameters.html#pooling",
		Check: func(t *Target, _ Answers) []Finding {
			n := stackOf(t).Npgsql
			if n == nil || n.maxPool != "" {
				return nil
			}
			current := "Maximum Pool Size left at Npgsql's 100, in " + n.Path
			if t.Postgres != nil {
				if limit, ok := t.Postgres.integer("max_connections"); ok {
					current += ", against max_connections " + strconv.FormatInt(limit, 10)
					if limit > 100 {
						return nil
					}
				}
			}
			return []Finding{{
				Current:  current,
				Proposed: "Maximum Pool Size in the connection string, below max_connections divided by the server's processes",
				Why: "the pool grows to 100 connections under load: one process can then take every slot " +
					"of a server at 100, and the next connection is refused (\"too many clients\")",
			}}
		},
	})
}

// stackOf is the target's stack settings, empty when none was read.
func stackOf(t *Target) *Stack {
	if t.Stack == nil {
		return &Stack{}
	}
	return t.Stack
}
