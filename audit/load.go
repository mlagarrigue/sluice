package audit

import (
	"encoding/csv"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Symptoms of a load step put on the setting that explains them, after
// linux/listen-overflows: new connections too many for the requests
// served, and a PostgreSQL server whose connections were all taken.

// A PGSession is one row of pg_stat_activity: what the session was doing
// when it was read, and what kind of process it is.
type PGSession struct {
	State       string
	BackendType string // empty on a server that does not report it
}

// parsePGActivity reads psql's CSV of pg_stat_activity: a header naming
// state, backend_type optional. Each header starts a reading, so several
// readings taken during a load may be appended to one file.
func parsePGActivity(text string) ([][]PGSession, error) {
	r := csv.NewReader(strings.NewReader(text))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("not psql's CSV output: %w", err)
	}
	var out [][]PGSession
	state, kind := -1, -1
	for _, row := range rows {
		if i := slices.Index(row, "state"); i >= 0 {
			state, kind = i, slices.Index(row, "backend_type")
			out = append(out, nil)
			continue
		}
		if state < 0 {
			return nil, errors.New("no header naming a state column: want psql --csv output of pg_stat_activity")
		}
		if state >= len(row) {
			return nil, fmt.Errorf("a row of %d fields, the state is field %d", len(row), state+1)
		}
		s := PGSession{State: row[state]}
		if kind >= 0 && kind < len(row) {
			s.BackendType = row[kind]
		}
		out[len(out)-1] = append(out[len(out)-1], s)
	}
	if len(out) == 0 {
		return nil, errors.New("empty: want psql --csv output of pg_stat_activity")
	}
	return out, nil
}

// clients counts a reading's client sessions: the client backends where
// the server says which they are, else the rows with a state, which
// background processes do not have.
func clients(reading []PGSession, state string) int {
	n := 0
	for _, s := range reading {
		client := s.BackendType == "client backend" || (s.BackendType == "" && s.State != "")
		if client && (state == "" || s.State == state) {
			n++
		}
	}
	return n
}

// fewRequestsPerConnection is the bound below which a load step's new
// connections are a cost of their own: under it, one request in a few
// pays a handshake.
const fewRequestsPerConnection = 32

func init() {
	Register(Rule{
		Name:  "linux/reconnections",
		Needs: Selection{Components: {"linux"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   connBudgetDoc,
		Check: func(t *Target, ans Answers) []Finding {
			if t.Counters == nil {
				return nil
			}
			requests, err := strconv.ParseInt(ans.Get("requests"), 10, 64)
			opens := t.Counters.Delta["TcpPassiveOpens"]
			if err != nil || requests <= 0 || opens <= 0 || requests/opens >= fewRequestsPerConnection {
				return nil
			}
			current := fmt.Sprintf("%d requests over %d new connections (TcpPassiveOpens), %d per connection",
				requests, opens, requests/opens)
			var proposed, why string
			switch {
			case t.App != nil && t.App.httpstream() && atoi(t.App.Flags["max-requests-per-conn"]) > 0 &&
				atoi(t.App.Flags["max-requests-per-conn"]) < 1024:
				current += "; the server closes each connection after -max-requests-per-conn " + t.App.Flags["max-requests-per-conn"]
				proposed = "-max-requests-per-conn 0 (the package's 1,024), see app/max-requests-per-conn"
				why = "the server's own budget closes the connections: each client reconnects every few requests"
			case t.Proxy != nil && len(t.Proxy.keepaliveGaps()) > 0:
				current += "; the proxy closes its connections to the application: " + strings.Join(t.Proxy.keepaliveGaps(), "; ")
				proposed = "keep-alive towards the application, see proxy/upstream-keepalive"
				why = "the proxy opens a connection to the application for each request it forwards"
			default:
				current += "; neither the server's flags nor a proxy read closes them"
				proposed = "clients that keep their connections: keep-alive in the load generator or the HTTP client, no Connection: close"
				why = "the clients open a connection per request, or close it after a few"
			}
			return []Finding{{
				Current:  current,
				Proposed: proposed,
				Why: why + ": every new connection costs a TCP handshake, and a TLS one when the hop is " +
					"encrypted, before its first request — a round trip or two of latency on those " +
					"requests, and a socket in TIME_WAIT after them",
			}}
		},
	})

	Register(Rule{
		Name:  "postgres/connections-saturated",
		Needs: Selection{Components: {"postgres"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW",
		Check: func(t *Target, _ Answers) []Finding {
			pg := t.Postgres
			if pg == nil || pg.Activity == nil {
				return nil
			}
			limit, ok := pg.integer("max_connections")
			if !ok {
				return nil
			}
			reserved, _ := pg.integer("superuser_reserved_connections")
			more, _ := pg.integer("reserved_connections")
			open := limit - reserved - more
			peak := 0
			for _, reading := range pg.Activity {
				peak = max(peak, clients(reading, ""))
			}
			if int64(peak) < open {
				return nil
			}
			proposed := "max_connections above what the pools open together, or smaller pools"
			if t.App != nil {
				proposed += fmt.Sprintf(" (the study's server opens -db-conns %s and one for its gateway)", t.App.Flags["db-conns"])
			}
			return []Finding{{
				Current: fmt.Sprintf("%d client connections at the peak of %s, for %d max_connections less %d reserved",
					peak, pg.ActivityPath, limit, reserved+more),
				Proposed: proposed,
				Why: "every slot a client may take was taken: the next connection is refused (\"too many " +
					"clients\"), and a pool that grows under load fails at the load where it grows",
			}}
		},
	})

	Register(Rule{
		Name:  "postgres/idle-in-transaction",
		Needs: Selection{Components: {"postgres"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT",
		Check: func(t *Target, _ Answers) []Finding {
			pg := t.Postgres
			if pg == nil || pg.Activity == nil {
				return nil
			}
			peak, of := 0, 0
			for _, reading := range pg.Activity {
				if n := clients(reading, "idle in transaction"); n > peak {
					peak, of = n, clients(reading, "")
				}
			}
			if peak < 5 || peak*10 < of {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d of %d client sessions idle in transaction at once in %s", peak, of, pg.ActivityPath),
				Proposed: "the code that opens a transaction ends it before waiting on anything else; idle_in_transaction_session_timeout as a bound",
				Why: "a session idle in a transaction holds its connection, its locks and the oldest " +
					"snapshot: the pool has that many connections fewer, and vacuum cannot remove the " +
					"rows that snapshot may still see",
			}}
		},
	})
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
