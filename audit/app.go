package audit

import (
	"cmp"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The application layer: the comparison study's Sluice.go server
// (comparison/sluice), read from the command line and the environment it
// was started with, saved to a file. The audit does not import the server
// — a module of its own, with the dependencies the study measures — so the
// flags, their defaults (the study's reference configuration, R) and the
// variables of the study's contract are written here again, and a flag this
// table does not know is refused rather than skipped: a report on a
// configuration half read would vouch for the half it did not read.

// App is the study server's configuration as started: every flag, the
// defaults filled in.
type App struct {
	Path  string
	Flags map[string]string
}

// appFlags are the server's flags and their defaults, R's, in the order
// its options.go declares them; bool flags take no value.
var appFlags = []struct {
	name, def string
	bool      bool
}{
	{"listen", "127.0.0.1:8443", false},
	{"tls", "true", true},
	{"cert", "", false},
	{"key", "", false},
	{"tls-resume", "false", true},
	{"cpuprofile", "", false},
	{"transport", "nethttp-h1", false},
	{"driver", "sluice", false},
	{"gateway-size", "64", false},
	{"gateway-within", "2ms", false},
	{"gateway-eager", "false", true},
	{"max-requests-per-batch", "0", false},
	{"max-requests-per-conn", "0", false},
	{"response", "buffered", false},
	{"compression", "none", false},
	{"json", "std", false},
	{"db", "", false},
	{"db-user", "", false},
	{"db-name", "", false},
	{"db-password", "", false},
	{"db-conns", "8", false},
	{"list-max", "100000", false},
}

// appEnv is the study's contract (comparison/README.md, "The contract"),
// each variable with the flag it sets and, for some, the translation of
// its value; the repository's SLUICE_PG* are the defaults below it.
var appEnv = []struct {
	env, flag string
	values    map[string]string
}{
	{"SLUICE_PG", "db", nil},
	{"SLUICE_PG_USER", "db-user", nil},
	{"SLUICE_PG_DB", "db-name", nil},
	{"SLUICE_PG_PASSWORD", "db-password", nil},
	{"STUDY_LISTEN", "listen", nil},
	{"STUDY_TLS", "tls", map[string]string{"on": "true", "off": "false"}},
	{"STUDY_TLS_CERT", "cert", nil},
	{"STUDY_TLS_KEY", "key", nil},
	{"STUDY_TLS_RESUME", "tls-resume", map[string]string{"on": "true", "off": "false"}},
	{"STUDY_PROTOCOL", "transport", map[string]string{"h1": "nethttp-h1", "h2": "nethttp-h2", "h3": "httpstream-h3"}},
	{"STUDY_COMPRESSION", "compression", map[string]string{"off": "none", "gzip": "gzip1", "deflate": "deflate6"}},
	{"STUDY_DB_POOL", "db-conns", nil},
	{"PGUSER", "db-user", nil},
	{"PGDATABASE", "db-name", nil},
	{"PGPASSWORD", "db-password", nil},
}

// parseApp reads a saved start of the server: NAME=value lines for the
// environment, as env prints it, and the command line, the program's name
// first or not, split on white space. The environment is applied first,
// then the flags, as the server does.
func parseApp(text string) (*App, error) {
	a := &App{Flags: map[string]string{}}
	bools := map[string]bool{}
	for _, f := range appFlags {
		a.Flags[f.name] = f.def
		bools[f.name] = f.bool
	}
	env := map[string]string{}
	var args []string
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok && k != "" && !strings.HasPrefix(k, "-") && !strings.ContainsAny(k, " \t") {
			env[k] = v
			continue
		}
		args = append(args, strings.Fields(line)...)
	}
	for _, e := range appEnv {
		v := env[e.env]
		if v == "" {
			continue
		}
		if e.values != nil {
			mapped, ok := e.values[v]
			if !ok {
				return nil, fmt.Errorf("%s=%q is not a value of the contract", e.env, v)
			}
			v = mapped
		}
		a.Flags[e.flag] = v
	}
	if host := env["PGHOST"]; host != "" {
		a.Flags["db"] = net.JoinHostPort(host, cmp.Or(env["PGPORT"], "5432"))
	}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = args[1:] // the program
	}
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(strings.TrimLeft(args[i], "-"), "=")
		if !strings.HasPrefix(args[i], "-") {
			return nil, fmt.Errorf("%q is not a flag", args[i])
		}
		isBool, known := bools[name]
		switch {
		case !known:
			return nil, fmt.Errorf("-%s is not a flag of the study's server this audit knows", name)
		case hasValue:
		case isBool:
			value = "true"
		case i+1 < len(args):
			i++
			value = args[i]
		default:
			return nil, fmt.Errorf("-%s has no value", name)
		}
		a.Flags[name] = value
	}
	return a, nil
}

// httpstream says whether the transport is one of the httpstream ones,
// where the connection budget applies.
func (a *App) httpstream() bool { return strings.HasPrefix(a.Flags["transport"], "httpstream-") }

const (
	repoDoc       = "https://github.com/mlagarrigue/sluice/blob/main/docs/"
	connBudgetDoc = repoDoc + "benchmarks.md#the-connection-budget-and-the-batch-bound-2026-10-14"
)

// init registers the application rules, in this order: the connection
// budget, the database link (in clear, unauthenticated, too many
// connections for the server), the clients' TLS, then the comparison's own
// rule on session resumption. Each is one call, read on its own.
func init() {
	register := func(name, goal, doc string, needs Selection, check func(*App, *Target, Answers) []Finding) {
		needs[Components] = []string{"app"}
		needs[Goals] = []string{goal}
		if needs[Sources] == nil {
			needs[Sources] = []string{"config"}
		}
		Register(Rule{
			Name: "app/" + name, Needs: needs, Doc: doc,
			Check: func(t *Target, ans Answers) []Finding {
				if t.App == nil {
					return nil
				}
				return check(t.App, t, ans)
			},
		})
	}

	register("max-requests-per-conn", "performance", connBudgetDoc, Selection{},
		func(a *App, _ *Target, _ Answers) []Finding {
			n, err := strconv.Atoi(a.Flags["max-requests-per-conn"])
			if err != nil || !a.httpstream() || n <= 0 || n >= 1024 {
				return nil // 0 is the package's 1,024, -1 unbounded
			}
			return []Finding{{
				Current:  fmt.Sprintf("-max-requests-per-conn %d on %s, in %s", n, a.Flags["transport"], a.Path),
				Proposed: "0, the package's 1,024, or -1 where every client is trusted",
				Why: fmt.Sprintf("the server closes each connection after %d requests: one request in %d "+
					"then pays a new connection, a TCP and a TLS handshake, two round trips more, and "+
					"under a steady load that request is the 99.9th percentile", n, n),
			}}
		})

	register("db-in-clear", "security", "https://pkg.go.dev/github.com/mlagarrigue/sluice/database/postgres#Config", Selection{},
		func(a *App, _ *Target, _ Answers) []Finding {
			host, _, err := net.SplitHostPort(a.Flags["db"])
			if err != nil || host == "localhost" || loopbackAddress(host) {
				return nil
			}
			current := "-db " + a.Flags["db"] + ": the server dials PostgreSQL in clear (postgres.DialTCP) " +
				"and does not set Config.RequireAuth"
			if a.Flags["db-password"] == "" {
				current += ", and no password is given (-db-password, PGPASSWORD, SLUICE_PG_PASSWORD): the role is let in by trust"
			}
			return []Finding{{
				Current:  current,
				Proposed: "a private network between the two machines, or TLS to the database (postgres.DialTLS) with RequireAuth and a SCRAM password",
				Why: "across machines, the queries, the rows and the authentication cross the network in " +
					"clear, and a server that answers AuthenticationOk without asking for anything is " +
					"accepted: whatever stands at that address is handed the application's queries",
			}}
		})

	register("db-conns", "reliability", "https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS", Selection{},
		func(a *App, t *Target, _ Answers) []Finding {
			conns, err := strconv.Atoi(a.Flags["db-conns"])
			if err != nil || t.Postgres == nil {
				return nil
			}
			limit, ok := t.Postgres.integer("max_connections")
			if !ok {
				return nil
			}
			reserved, _ := t.Postgres.integer("superuser_reserved_connections")
			more, _ := t.Postgres.integer("reserved_connections")
			open, want := limit-reserved-more, int64(conns)+1 // the gateway has a connection of its own
			if want <= open {
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("-db-conns %d and the gateway's own connection, %d in all, against %d max_connections less %d reserved",
					conns, want, limit, reserved+more),
				Proposed: fmt.Sprintf("-db-conns %d at most, or max_connections raised", open-1),
				Why: "the server opens every connection at start: past the slots, the last ones are refused " +
					"and the server does not start — or, with another client on the same database, starts " +
					"and leaves that client without a connection",
			}}
		})

	register("tls-off", "security", repoDoc+"guide/deployments.md#tls-in-front", Selection{},
		func(a *App, _ *Target, ans Answers) []Finding {
			if a.Flags["tls"] != "false" || ans.Get("exposure") != "internet" || a.Flags["transport"] == "httpstream-h3" {
				return nil
			}
			return []Finding{{
				Current:  "-tls=false on a machine reachable from the internet, in " + a.Path,
				Proposed: "-tls (R), or a TLS proxy in front on the same machine",
				Why: "the responses, the requests and whatever credentials they carry cross the internet in " +
					"clear, and any hop on the path may read or change them",
			}}
		})

	register("tls-resume", "performance", connBudgetDoc, Selection{Profile: {"comparative"}},
		func(a *App, _ *Target, _ Answers) []Finding {
			if a.Flags["tls-resume"] != "true" {
				return nil
			}
			return []Finding{{
				Current:  "-tls-resume in " + a.Path + ": returning clients resume their TLS session",
				Proposed: "-tls-resume=false, R's: the study's new connection is a new visitor",
				Why: "a resumed handshake skips the certificate and the key exchange: a run with it measures " +
					"a cheaper new connection than R's, and a figure published beside R's compares two " +
					"workloads, not two servers",
			}}
		})
}

// appSettings are the flags a comparison publishes for the server, in the
// order the study's axes name them; the password is never one of them.
var appSettings = []string{
	"transport", "tls", "tls-resume", "driver", "gateway-size", "gateway-within", "gateway-eager",
	"max-requests-per-batch", "max-requests-per-conn", "response", "compression", "json", "db-conns", "list-max",
}

// appSystem returns the server's line of the system table.
func appSystem(a *App) []Setting {
	out := make([]Setting, 0, len(appSettings))
	for _, name := range appSettings {
		v := a.Flags[name]
		if name == "gateway-within" {
			if d, err := time.ParseDuration(v); err == nil {
				v = d.String()
			}
		}
		out = append(out, Setting{"app " + name, cmp.Or(v, "unset")})
	}
	return slices.Clip(out)
}
