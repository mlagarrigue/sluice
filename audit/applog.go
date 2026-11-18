package audit

import (
	"fmt"
	"regexp"
	"strings"
)

// What the application said about itself while it ran. A server that runs
// out of file descriptors cannot accept a connection, and the only trace it
// leaves is its own log: the kernel counts nothing, and the client sees a
// connection that hangs until it times out. Every stack prints the errno one
// way or the other — its text, "too many open files", or its name, EMFILE —
// so the lines are found by those, not by a format of one stack's.

// errnoLines returns the lines of the application's log that name an errno
// by its text (any case) or by its name, but for those holding unless.
func errnoLines(t *Target, text, name, unless string) []string {
	var out []string
	for line := range strings.SplitSeq(t.AppLog, "\n") {
		lower := strings.ToLower(line)
		if unless != "" && strings.Contains(lower, unless) {
			continue
		}
		if strings.Contains(lower, text) || strings.Contains(line, name) {
			out = append(out, line)
		}
	}
	return out
}

// init registers app/out-of-descriptors.
func init() {
	Register(Rule{
		Name:  "app/out-of-descriptors",
		Needs: Selection{Components: {"app"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://man7.org/linux/man-pages/man2/accept.2.html",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			// ENFILE first: its text contains EMFILE's, and its cause is the
			// system's table, not the process's limit.
			if lines := errnoLines(t, "too many open files in system", "ENFILE", ""); len(lines) > 0 {
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) naming ENFILE (\"too many open files in system\") in %s, the first: %s", len(lines), t.AppLogPath, cite(lines[0])),
					Proposed: "fs.file-max raised (sysctl), the open files of the whole system",
					Why: "the system's table of open files is full: every process fails to open " +
						"one, the server's accept first among them, and a connection waits in " +
						"the listen queue until its client gives up",
				})
			}
			if lines := errnoLines(t, "too many open files", "EMFILE", "too many open files in system"); len(lines) > 0 {
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) naming EMFILE (\"too many open files\") in %s, the first: %s", len(lines), t.AppLogPath, cite(lines[0])),
					Proposed: "the server's nofile limit raised above the connections it is to hold, see linux/nofile and container/nofile",
					Why: "a connection is a file: at the process's nofile limit, accept fails " +
						"with EMFILE, the server backs off and retries, and the connections " +
						"waiting meanwhile see a latency of seconds, or a timeout, with no " +
						"error of their own",
				})
			}
			return out
		},
	})
}

// cite keeps a quoted log line to a length a report can carry.
func cite(line string) string {
	line = strings.TrimSpace(line)
	if len(line) > 200 {
		line = line[:200] + "…"
	}
	return fmt.Sprintf("%q", line)
}

// accessLine is a line of an access log, one a server writes per request:
// the request line in quotes, then the status — the Common Log Format's
// `"GET /x HTTP/1.1" 200` and uvicorn's alike. uvicornAccess is uvicorn's
// own shape (its LOGGING_CONFIG, `%(levelprefix)s %(client_addr)s -
// "%(request_line)s" %(status_code)s`), which names the setting to change.
var (
	accessLine    = regexp.MustCompile(`"(?:GET|HEAD|POST|PUT|PATCH|DELETE|OPTIONS) \S+ HTTP/[0-9.]+" [1-5][0-9]{2}\b`)
	uvicornAccess = regexp.MustCompile(`^(?:TRACE|DEBUG|INFO|WARNING|ERROR|CRITICAL):\s+\S+ - "`)
)

// init registers app/access-log.
func init() {
	Register(Rule{
		Name:  "app/access-log",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://uvicorn.dev/settings/#logging",
		Check: func(t *Target, _ Answers) []Finding {
			var n, uvicorn int
			var first string
			for line := range strings.SplitSeq(t.AppLog, "\n") {
				if !accessLine.MatchString(line) {
					continue
				}
				if n == 0 {
					first = line
				}
				n++
				if uvicornAccess.MatchString(line) {
					uvicorn++
				}
			}
			if n == 0 {
				return nil
			}
			proposed := "the access log off in production, or sampled: every request otherwise costs a formatted line written out"
			if uvicorn == n {
				proposed = "access_log=False in uvicorn.Config, or --no-access-log on uvicorn's command line"
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d access line(s), one per request, in %s, the first: %s", n, t.AppLogPath, cite(first)),
				Proposed: proposed,
				Why: "the server formats and writes a line for every request it answers: under load that is a " +
					"write per request on the hot path, and under a container's log driver a file that grows " +
					"by the request (study §3.3, the stacks' own ceilings)",
			}}
		},
	})
}

// poolDry is what each contender's pool writes when a request waited for a
// connection past its timeout, as its source words it, and the settings it
// is put on. pgx and asyncpg write no words of their own (a context
// deadline, a TimeoutError): pg_stat_activity tells their saturation
// (postgres/connections-saturated), the log does not.
var poolDry = []struct{ stack, words, settings string }{
	// HikariPool.createTimeoutException.
	{
		"HikariCP", "Connection is not available, request timed out after",
		"spring.datasource.hikari.maximum-pool-size (10 by default) against the database's max_connections, or connection-timeout (30 s)",
	},
	// PoolingDataSource.RentAsync.
	{
		"Npgsql", "The connection pool has been exhausted",
		"Max Pool Size (100 by default) in the connection string against the database's max_connections, or Timeout",
	},
	// pg-pool's connect, under connectionTimeoutMillis.
	{
		"node-postgres", "timeout exceeded when trying to connect",
		"the Pool's max (10 by default) against the database's max_connections, or connectionTimeoutMillis",
	},
	// sqlx_core::Error::PoolTimedOut.
	{
		"sqlx", "pool timed out while waiting for an open connection",
		"PgPoolOptions::max_connections (10 by default) against the database's max_connections, or acquire_timeout (30 s)",
	},
	// sqlalchemy.pool.QueuePool._do_get.
	{
		"SQLAlchemy", "QueuePool limit of size",
		"create_engine's pool_size and max_overflow (5 and 10 by default) against the database's max_connections, or pool_timeout (30 s)",
	},
}

// init registers app/pool-exhausted.
func init() {
	Register(Rule{
		Name:  "app/pool-exhausted",
		Needs: Selection{Components: {"app"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			for _, p := range poolDry {
				var n int
				var first string
				for line := range strings.SplitSeq(t.AppLog, "\n") {
					if strings.Contains(line, p.words) {
						if n == 0 {
							first = line
						}
						n++
					}
				}
				if n == 0 {
					continue
				}
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) of %s's pool run dry in %s, the first: %s", n, p.stack, t.AppLogPath, cite(first)),
					Proposed: p.settings,
					Why: "a request waited for a database connection longer than the pool allows and failed: " +
						"the pool is smaller than the load needs, or the database holds its connections too long; " +
						"raising it past max_connections only moves the refusal to the database",
				})
			}
			return out
		},
	})
}

// pgCancelled is what the PostgreSQL server sends back when it cancels a
// statement or ends a session on a timeout, as ProcessInterrupts words it
// (src/backend/tcop/postgres.c), which every driver copies into the error
// the application logs; and the setting each is put on.
var pgCancelled = []struct{ words, setting string }{
	{
		"canceling statement due to statement timeout",
		"statement_timeout, against the slowest statement the load runs: raise it for that statement, or make the statement cheaper",
	},
	{
		"canceling statement due to lock timeout",
		"lock_timeout, and the transaction that held the lock that long: shorten it, or order the locks",
	},
	{
		"canceling statement due to user request",
		"the client's own deadline (the request's context, the driver's command timeout), shorter than the statement: the cancellation is the client's, the server only obeyed",
	},
	{
		"terminating connection due to idle-in-transaction timeout",
		"idle_in_transaction_session_timeout, and the code that kept a transaction open while it waited on something else",
	},
	{
		"terminating connection due to transaction timeout",
		"transaction_timeout, against the longest transaction the load runs",
	},
	{
		"terminating connection due to idle-session timeout",
		"idle_session_timeout, above the pool's idle lifetime: the pool should close an idle connection before the server does",
	},
}

// init registers app/statement-cancelled.
func init() {
	Register(Rule{
		Name:  "app/statement-cancelled",
		Needs: Selection{Components: {"app"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-STATEMENT",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			for _, c := range pgCancelled {
				var n int
				var first string
				for line := range strings.SplitSeq(t.AppLog, "\n") {
					if strings.Contains(line, c.words) {
						if n == 0 {
							first = line
						}
						n++
					}
				}
				if n == 0 {
					continue
				}
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) of PostgreSQL %q in %s, the first: %s", n, c.words, t.AppLogPath, cite(first)),
					Proposed: c.setting,
					Why: "each is a request that failed on a bound, not on its data: under load the bound is " +
						"reached by the queue in front of the statement as much as by the statement itself",
				})
			}
			return out
		},
	})
}
