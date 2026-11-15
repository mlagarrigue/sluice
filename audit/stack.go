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
)

// The settings a contender's stack starts with, read from the file that
// holds them, each kind recognised by its content: a Spring Boot
// application.properties (or the SPRING_ and SERVER_ variables of its
// environment, the same keys relaxed), the command line that starts
// uvicorn, a Node server's source, an ASP.NET Core appsettings.json and
// the Npgsql connection string in it. A setting left out is the stack's
// default, which the rules below name with the place it is documented.

// Stack is the settings files read, by kind.
type Stack struct {
	Spring  *springConfig
	Uvicorn *uvicornConfig
	Node    *nodeConfig
	Npgsql  *npgsqlConfig
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
	keepAlive bool // server.keepAliveTimeout assigned
	poolMax   bool // a pg Pool given its max
	pool      bool // a pg Pool built at all
}

type npgsqlConfig struct {
	Path    string
	maxPool string // Maximum Pool Size, empty when left out
}

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
			if st.Npgsql, err = parseAppSettings(path, b); err != nil {
				return nil, err
			}
		case uvicornLine.MatchString(text):
			st.Uvicorn = parseUvicorn(path, text)
		case springLine.MatchString(text):
			st.Spring = parseSpring(path, text)
		default:
			return nil, fmt.Errorf("%s: not a stack's settings this audit reads: want application.properties, "+
				"the command line that starts uvicorn, a Node server's source or appsettings.json", path)
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
	nodeKeepAlive = regexp.MustCompile(`\.keepAliveTimeout\s*=|keepAliveTimeout\s*:`)
	nodePool      = regexp.MustCompile(`new\s+(pg\.)?Pool\s*\(([^)]*)\)`)
	nodePoolMax   = regexp.MustCompile(`\bmax\s*:`)
)

func parseNode(path, text string) *nodeConfig {
	n := &nodeConfig{Path: path, keepAlive: nodeKeepAlive.MatchString(text)}
	for _, m := range nodePool.FindAllStringSubmatch(text, -1) {
		n.pool = true
		if nodePoolMax.MatchString(m[2]) {
			n.poolMax = true
		}
	}
	return n
}

func parseAppSettings(path string, b []byte) (*npgsqlConfig, error) {
	var doc struct {
		ConnectionStrings map[string]string `json:"ConnectionStrings"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: not appsettings.json: %w", path, err)
	}
	for _, cs := range doc.ConnectionStrings {
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
			return n, nil
		}
	}
	return nil, errors.New(path + ": no Npgsql connection string under ConnectionStrings")
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
		Doc:   "https://www.uvicorn.org/settings/",
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
			if !n.keepAlive {
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
