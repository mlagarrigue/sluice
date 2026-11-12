package audit

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

// What a PostgreSQL server was set to, read from files the caller saved:
// pg_settings as psql prints it in CSV, the text of pg_hba.conf, and the
// server's log. The audit does not connect: it is a module of its own on
// the standard library, so it cannot borrow the connector it would be
// auditing, and a saved file is what a test can seed and a reader can
// check against the report.

// Postgres is what was read of one server. A part not given is nil or
// empty, and the rules that read it stay silent.
type Postgres struct {
	Settings  map[string]PGSetting
	HBA       []HBALine
	HBAPath   string
	Log       string
	LogPath   string
	Roles     []PGRole
	RolesPath string
	// Activity is pg_stat_activity read during a load, one entry per
	// reading, each holding the sessions of that moment.
	Activity     [][]PGSession
	ActivityPath string
}

// A PGSetting is one row of pg_settings: the value, in the unit the
// server counts it in ("8kB" for shared_buffers, "s" for a timeout).
type PGSetting struct {
	Value string
	Unit  string
}

// An HBALine is one rule of pg_hba.conf: who may connect, from where, and
// how they prove who they are.
type HBALine struct {
	Line    int
	Type    string // local, host, hostssl, hostnossl, hostgssenc, hostnogssenc
	Address string // empty for local
	Method  string
	Options []string // what follows the method: clientcert=verify-full, map=…
}

func openPostgres(settings, hba, log, roles, activity string) (*Postgres, error) {
	pg := &Postgres{}
	if settings != "" {
		b, err := os.ReadFile(settings) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if pg.Settings, err = parsePGSettings(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", settings, err)
		}
	}
	if hba != "" {
		b, err := os.ReadFile(hba) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		pg.HBA, pg.HBAPath = parseHBA(string(b)), hba
	}
	if log != "" {
		b, err := os.ReadFile(log) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		pg.Log, pg.LogPath = string(b), log
	}
	if roles != "" {
		b, err := os.ReadFile(roles) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if pg.Roles, err = parsePGRoles(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", roles, err)
		}
		pg.RolesPath = roles
	}
	if activity != "" {
		b, err := os.ReadFile(activity) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if pg.Activity, err = parsePGActivity(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", activity, err)
		}
		pg.ActivityPath = activity
	}
	return pg, nil
}

// parsePGSettings reads psql's CSV: a header naming at least name and
// setting, unit optional. A file without them is refused rather than read
// as a server with no settings.
func parsePGSettings(text string) (map[string]PGSetting, error) {
	rows, err := csv.NewReader(strings.NewReader(text)).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("not psql's CSV output: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("empty: want psql --csv output of pg_settings")
	}
	name, setting, unit := slices.Index(rows[0], "name"), slices.Index(rows[0], "setting"), slices.Index(rows[0], "unit")
	if name < 0 || setting < 0 {
		return nil, fmt.Errorf("the header %q names no name and setting columns", rows[0])
	}
	out := make(map[string]PGSetting, len(rows)-1)
	for _, r := range rows[1:] {
		s := PGSetting{Value: r[setting]}
		if unit >= 0 {
			s.Unit = r[unit]
		}
		out[r[name]] = s
	}
	return out, nil
}

// parseHBA reads the rules of pg_hba.conf, comments and include directives
// left out. A rule's method is the field after its address, and an
// address written as an IP and a separate mask takes two fields.
func parseHBA(text string) []HBALine {
	var out []HBALine
	lines := strings.Split(text, "\n")
	for i := 0; i < len(lines); i++ {
		n, line := i+1, lines[i]
		for strings.HasSuffix(line, `\`) && i+1 < len(lines) { // continued (PostgreSQL 16)
			i++
			line = strings.TrimSuffix(line, `\`) + " " + lines[i]
		}
		if c := strings.IndexByte(line, '#'); c >= 0 {
			line = line[:c]
		}
		f := strings.Fields(line)
		if len(f) < 4 || strings.HasPrefix(f[0], "include") {
			continue
		}
		l := HBALine{Line: n, Type: f[0]}
		method := 3
		switch {
		case f[0] == "local":
		case len(f) >= 5:
			l.Address, method = f[3], 4
			if len(f) >= 6 && !strings.Contains(f[3], "/") && looksLikeMask(f[4]) {
				method = 5
			}
		default:
			continue
		}
		l.Method, l.Options = f[method], f[method+1:]
		out = append(out, l)
	}
	return out
}

func looksLikeMask(s string) bool { return strings.Count(s, ".") == 3 || strings.Contains(s, ":") }

// loopback reports whether a rule's address only admits the machine itself.
func loopback(address string) bool {
	switch address {
	case "127.0.0.1/32", "::1/128", "127.0.0.1", "::1", "localhost", "samehost":
		return true
	}
	return false
}

// setting returns a server setting and whether pg_settings held it.
func (pg *Postgres) setting(name string) (PGSetting, bool) {
	if pg == nil || pg.Settings == nil {
		return PGSetting{}, false
	}
	s, ok := pg.Settings[name]
	return s, ok
}

// integer returns a setting as a number.
func (pg *Postgres) integer(name string) (int64, bool) {
	s, ok := pg.setting(name)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(s.Value, 10, 64)
	return n, err == nil
}

// bytes returns a memory setting in bytes, from its value and the unit
// pg_settings counts it in.
func (pg *Postgres) bytes(name string) (int64, bool) {
	n, ok := pg.integer(name)
	if !ok {
		return 0, false
	}
	s, _ := pg.setting(name)
	switch s.Unit {
	case "B", "":
		return n, true
	case "kB":
		return n << 10, true
	case "8kB":
		return n * 8 << 10, true
	case "MB":
		return n << 20, true
	case "GB":
		return n << 30, true
	}
	return 0, false
}

// logLines returns the lines of the server's log holding any of the
// markers.
func (pg *Postgres) logLines(markers ...string) []string {
	if pg == nil {
		return nil
	}
	var out []string
	for line := range strings.SplitSeq(pg.Log, "\n") {
		if slices.ContainsFunc(markers, func(m string) bool { return strings.Contains(line, m) }) {
			out = append(out, line)
		}
	}
	return out
}

// init registers the PostgreSQL rules, in this order: the connections the
// server accepts, its memory, autovacuum, then authentication
// (password_encryption, ssl, the trust and md5 rules of pg_hba.conf), and
// last the two log lines whose cause is a setting above. Each is one call,
// read on its own.
func init() {
	register := func(name, goal, source, doc string, check func(*Postgres, Answers) []Finding) {
		Register(Rule{
			Name:  "postgres/" + name,
			Needs: Selection{Components: {"postgres"}, Goals: {goal}, Sources: {source}},
			Doc:   doc,
			Check: func(t *Target, a Answers) []Finding {
				if t.Postgres == nil {
					return nil
				}
				return check(t.Postgres, a)
			},
		})
	}

	register("max-connections", "reliability", "config",
		"https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS",
		func(pg *Postgres, a Answers) []Finding {
			pools, err := strconv.ParseInt(a.Get("pools"), 10, 64)
			if err != nil {
				return nil // unknown: what the clients open is read nowhere
			}
			limit, ok := pg.integer("max_connections")
			if !ok {
				return nil
			}
			reserved, _ := pg.integer("superuser_reserved_connections")
			more, _ := pg.integer("reserved_connections") // PostgreSQL 16
			const margin = 5                              // a psql, a monitor, a migration
			open := limit - reserved - more
			if open >= pools+margin {
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("max_connections = %d, of which %d reserved: %d for clients whose pools open %d",
					limit, reserved+more, open, pools),
				Proposed: fmt.Sprintf("max_connections = %d at least (the pools, the reserved slots, and %d for the connections a person opens)",
					pools+reserved+more+margin, margin),
				Why: "a pool that opens one connection too many is refused with \"sorry, too many " +
					"clients already\", and the client reads it as its own error under load — a " +
					"comparison would charge the database's limit to the framework. Default pools " +
					"differ widely (Npgsql 100 per connection string, SQLAlchemy 15 per process), " +
					"so the sum is counted, not guessed",
			}}
		})

	register("shared-buffers", "performance", "config",
		"https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-SHARED-BUFFERS",
		func(pg *Postgres, _ Answers) []Finding {
			b, ok := pg.bytes("shared_buffers")
			if !ok || b > 128<<20 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("shared_buffers = %d MB, the default", b>>20),
				Proposed: "about a quarter of the machine's memory, as the documentation starts from",
				Why: "128 MB is a default chosen to start on any machine, not to serve: a working " +
					"set larger than it is read through the operating system's cache, a copy per " +
					"read, and a benchmark on it measures the default rather than the server",
			}}
		})

	register("autovacuum", "reliability", "config",
		"https://www.postgresql.org/docs/current/routine-vacuuming.html#AUTOVACUUM",
		func(pg *Postgres, _ Answers) []Finding {
			if s, ok := pg.setting("autovacuum"); !ok || s.Value != "off" {
				return nil
			}
			return []Finding{{
				Current:  "autovacuum = off for the whole server",
				Proposed: "autovacuum = on; switched off per table (autovacuum_enabled = off) where a measurement needs it",
				Why: "dead rows then accumulate in every table that is written, the tables grow " +
					"and slow down, and past two billion transactions the server stops accepting " +
					"writes to protect itself from wraparound. A table frozen for a benchmark is " +
					"the exception the per-table setting exists for",
			}}
		})

	register("password-encryption", "security", "config",
		"https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-PASSWORD-ENCRYPTION",
		func(pg *Postgres, _ Answers) []Finding {
			s, ok := pg.setting("password_encryption")
			if !ok || s.Value == "scram-sha-256" {
				return nil
			}
			return []Finding{{
				Current:  "password_encryption = " + s.Value,
				Proposed: "password_encryption = scram-sha-256, then each password set again",
				Why: "an MD5 hash stored by the server is enough to log in as the role, without the " +
					"password: whoever reads it — a backup, a replica — has the account. SCRAM " +
					"stores what cannot be replayed, and is the default since PostgreSQL 14",
			}}
		})

	register("ssl", "security", "config",
		"https://www.postgresql.org/docs/current/ssl-tcp.html",
		func(pg *Postgres, a Answers) []Finding {
			if s, ok := pg.setting("ssl"); !ok || s.Value != "off" {
				return nil
			}
			var remote []string
			for _, l := range pg.HBA {
				if l.Type != "local" && l.Type != "hostssl" && !loopback(l.Address) {
					remote = append(remote, strconv.Itoa(l.Line))
				}
			}
			if len(remote) == 0 && a.Get("exposure") != "internet" {
				return nil // reached from the machine alone, or nothing says otherwise
			}
			current := "ssl = off"
			if len(remote) > 0 {
				current += ", and pg_hba.conf admits other machines (line " + strings.Join(remote, ", ") + ")"
			}
			return []Finding{{
				Current:  current,
				Proposed: "ssl = on with a certificate, and hostssl in place of host for the remote rules",
				Why: "without TLS the queries, the rows and the authentication exchange cross the " +
					"network in clear: anyone on the path reads the data, and with md5 or password " +
					"authentication, enough to log in",
			}}
		})

	register("hba-trust", "security", "config",
		"https://www.postgresql.org/docs/current/auth-trust.html",
		func(pg *Postgres, _ Answers) []Finding {
			var local, remote []string
			for _, l := range pg.HBA {
				if l.Method != "trust" {
					continue
				}
				if l.Type == "local" {
					local = append(local, strconv.Itoa(l.Line))
				} else {
					remote = append(remote, strconv.Itoa(l.Line))
				}
			}
			var out []Finding
			if len(remote) > 0 {
				out = append(out, Finding{
					Current:  "trust over the network in " + pg.HBAPath + ", line " + strings.Join(remote, ", "),
					Proposed: "scram-sha-256 on every host rule",
					Why: "trust lets whoever reaches the port connect as any role it names, the " +
						"superuser included, with no password: on loopback that is every process " +
						"and every container sharing the network namespace; elsewhere, the network",
				})
			}
			if len(local) > 0 {
				out = append(out, Finding{
					Current:  "trust on the Unix socket in " + pg.HBAPath + ", line " + strings.Join(local, ", "),
					Proposed: "peer, so that a system user connects as the role of the same name only",
					Why: "any user of the machine may connect as any role through the socket; the " +
						"official image sets it so that its init scripts run, and a server kept on it " +
						"has no boundary between its system users and its superuser",
				})
			}
			return out
		})

	register("hba-md5", "security", "config",
		"https://www.postgresql.org/docs/current/auth-password.html",
		func(pg *Postgres, _ Answers) []Finding {
			var weak []string
			for _, l := range pg.HBA {
				if l.Method == "md5" || l.Method == "password" {
					weak = append(weak, fmt.Sprintf("%d (%s)", l.Line, l.Method))
				}
			}
			if len(weak) == 0 {
				return nil
			}
			return []Finding{{
				Current:  "password methods weaker than SCRAM in " + pg.HBAPath + ", line " + strings.Join(weak, ", "),
				Proposed: "scram-sha-256",
				Why: "password sends the password in clear; md5 sends a hash that a captured " +
					"exchange or a stolen hash replays. Both are deprecated, and a client that " +
					"requires SCRAM refuses them",
			}}
		})

	register("checkpoints", "performance", "logs",
		"https://www.postgresql.org/docs/current/wal-configuration.html",
		func(pg *Postgres, _ Answers) []Finding {
			lines := pg.logLines("checkpoints are occurring too frequently")
			if len(lines) == 0 {
				return nil
			}
			current := fmt.Sprintf("%d line(s) \"checkpoints are occurring too frequently\" in %s", len(lines), pg.LogPath)
			if s, ok := pg.setting("max_wal_size"); ok {
				current += ", with max_wal_size = " + s.Value + s.Unit
			}
			return []Finding{{
				Current:  current,
				Proposed: "max_wal_size raised until the line stops, so that checkpoint_timeout starts the checkpoints",
				Why: "a checkpoint writes every dirty page; forced by the WAL filling up, they come " +
					"close together under a write load, each one followed by full page writes " +
					"that double the WAL — the throughput drops in waves that follow the " +
					"checkpoints, not the load",
			}}
		})

	register("too-many-clients", "reliability", "logs",
		"https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS",
		func(pg *Postgres, _ Answers) []Finding {
			lines := pg.logLines("sorry, too many clients already", "remaining connection slots are reserved")
			if len(lines) == 0 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d connection(s) refused for lack of slots in %s", len(lines), pg.LogPath),
				Proposed: "max_connections above the sum of the pools (see postgres/max-connections with -answer pools=N), or smaller pools",
				Why: "each line is a client that got an error instead of a connection: the pools " +
					"together asked for more than the server accepts. Under load it reads as " +
					"failures of the application, at the load where the pools grow",
			}}
		})
}

// postgresSettings are the server settings a comparison publishes, in
// this order: the version, the connections, memory, WAL, then the rest.
var postgresSettings = []string{
	"server_version", "max_connections", "superuser_reserved_connections",
	"shared_buffers", "work_mem", "max_wal_size", "checkpoint_timeout",
	"autovacuum", "ssl", "password_encryption",
}
