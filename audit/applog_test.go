package audit

import (
	"strings"
	"testing"
)

// The application's log names the descriptor it ran out of: EMFILE, the
// process's own limit, in the forms the stacks print it — sluice's warn line,
// net/http's Accept error, the errno's name alone — and ENFILE, the system's
// table, which is another setting. A log with neither reports nothing.
func TestTheApplicationLogNamesTheDescriptorsRunOut(t *testing.T) {
	log := `time=2026-11-13T09:00:00Z level=INFO msg=listening addr=:8443
time=2026-11-13T09:00:05Z level=WARN msg="httpstream: accepting failed; retrying with backoff until it succeeds" addr=[::]:8443 err="accept tcp [::]:8443: accept4: too many open files"
2026/11/13 09:00:06 http: Accept error: accept tcp [::]:8080: accept4: too many open files; retrying in 5ms
Error: accept EMFILE
`
	target, err := Open(Spec{AppLog: seedFile(t, "server.log", log)})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var got []Finding
	for _, f := range findings {
		if f.Rule == "app/out-of-descriptors" {
			got = append(got, f)
		}
	}
	if len(got) != 1 || !strings.HasPrefix(got[0].Current, "3 line(s) naming EMFILE") || !strings.Contains(got[0].Proposed, "nofile") {
		t.Fatalf("app/out-of-descriptors said %+v, want one finding: the three EMFILE lines, towards nofile", got)
	}

	system, err := Open(Spec{AppLog: seedFile(t, "server.log", "OSError: [Errno 23] Too many open files in system\n")})
	if err != nil {
		t.Fatal(err)
	}
	fs, _ := Run(Selection{Sources: {"logs"}}, nil, system)
	if len(fs) != 1 || !strings.Contains(fs[0].Current, "ENFILE") || !strings.Contains(fs[0].Proposed, "fs.file-max") {
		t.Errorf("an ENFILE line reported %+v, want one finding towards fs.file-max and none towards nofile", fs)
	}

	quiet, err := Open(Spec{AppLog: seedFile(t, "server.log", "time=2026-11-13T09:00:00Z level=INFO msg=listening addr=:8443\n")})
	if err != nil {
		t.Fatal(err)
	}
	if fs, _ := Run(Selection{Sources: {"logs"}}, nil, quiet); len(fs) != 0 {
		t.Errorf("a quiet application log was reported: %v", fs)
	}
}

// TestAnAccessLogIsReadInTheApplicationLog: uvicorn's access lines name
// uvicorn's setting; a Common Log Format line gets the general advice; a
// log with no access line, its startup and an error, reports nothing.
func TestAnAccessLogIsReadInTheApplicationLog(t *testing.T) {
	for _, tc := range []struct{ name, log, want string }{
		{"uvicorn", "INFO:     Started server process [1]\n" +
			"INFO:     172.18.0.3:51234 - \"GET /orders/12 HTTP/1.1\" 200 OK\n" +
			"INFO:     172.18.0.3:51236 - \"POST /orders HTTP/1.1\" 200 OK\n", "access_log=False"},
		{"common log format", `10.0.0.7 - - [15/Nov/2026:10:00:00 +0000] "GET /static HTTP/1.1" 200 1043` + "\n", "the access log off in production"},
		{"none", "time=2026-11-15T10:00:00Z level=INFO msg=listening addr=:8443\n" +
			"time=2026-11-15T10:00:01Z level=ERROR msg=\"GET /orders/1 failed\" status=503\n", ""},
	} {
		target, err := Open(Spec{AppLog: seedFile(t, "server.log", tc.log)})
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		var got []Finding
		for _, f := range findings {
			if f.Rule == "app/access-log" {
				got = append(got, f)
			}
		}
		switch {
		case tc.want == "" && len(got) != 0:
			t.Errorf("%s: %v, want nothing", tc.name, got)
		case tc.want != "" && (len(got) != 1 || !strings.Contains(got[0].Proposed, tc.want)):
			t.Errorf("%s: %v, want one finding proposing %q", tc.name, got, tc.want)
		}
	}
}

// TestAPoolRunDryIsReadInTheApplicationLog: one line of each stack's pool
// run dry, as its driver words it, names that stack; a log without them,
// a startup and an access line, reports nothing.
func TestAPoolRunDryIsReadInTheApplicationLog(t *testing.T) {
	lines := map[string]string{
		"HikariCP":      "java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30005ms (total=10, active=10, idle=0, waiting=187)",
		"Npgsql":        "Npgsql.NpgsqlException (0x80004005): The connection pool has been exhausted, either raise 'Max Pool Size' (currently 100) or 'Timeout' (currently 15 seconds) in your connection string.",
		"node-postgres": "Error: timeout exceeded when trying to connect",
		"sqlx":          "Error: pool timed out while waiting for an open connection",
		"SQLAlchemy":    "sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection timed out, timeout 30.00",
	}
	for stack, line := range lines {
		target, err := Open(Spec{AppLog: seedFile(t, "server.log", "starting\n"+line+"\n")})
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		var got []Finding
		for _, f := range findings {
			if f.Rule == "app/pool-exhausted" {
				got = append(got, f)
			}
		}
		if len(got) != 1 || !strings.Contains(got[0].Current, stack+"'s pool run dry") || !strings.Contains(got[0].Proposed, "max_connections") {
			t.Errorf("%s: %v, want one finding naming it", stack, got)
		}
	}
	target, err := Open(Spec{AppLog: seedFile(t, "server.log", "INFO:     Started server process [1]\nINFO:     10.0.0.1:5000 - \"GET /orders/1 HTTP/1.1\" 200 OK\n")})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Rule == "app/pool-exhausted" {
			t.Errorf("a log without a pool run dry: %v", f)
		}
	}
}

// TestPostgreSQLTimeoutsAreReadInTheApplicationLog: a line for each of the
// server's cancellations, as a driver logs it, names its setting; a log
// without them reports nothing.
func TestPostgreSQLTimeoutsAreReadInTheApplicationLog(t *testing.T) {
	for _, tc := range []struct{ line, setting string }{
		{`ERROR: canceling statement due to statement timeout (SQLSTATE 57014)`, "statement_timeout"},
		{`org.postgresql.util.PSQLException: ERROR: canceling statement due to lock timeout`, "lock_timeout"},
		{`level=ERROR msg="list failed" err="postgres: 57014: canceling statement due to user request"`, "the client's own deadline"},
		{`FATAL: terminating connection due to idle-in-transaction timeout`, "idle_in_transaction_session_timeout"},
		{`FATAL: terminating connection due to transaction timeout`, "transaction_timeout"},
		{`asyncpg.exceptions.IdleSessionTimeoutError: terminating connection due to idle-session timeout`, "idle_session_timeout"},
	} {
		target, err := Open(Spec{AppLog: seedFile(t, "server.log", "starting\n"+tc.line+"\n")})
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		var got []Finding
		for _, f := range findings {
			if f.Rule == "app/statement-cancelled" {
				got = append(got, f)
			}
		}
		if len(got) != 1 || !strings.Contains(got[0].Proposed, tc.setting) {
			t.Errorf("%s: %v, want one finding naming %s", tc.line, got, tc.setting)
		}
	}
	target, err := Open(Spec{AppLog: seedFile(t, "server.log", "level=INFO msg=listening\nERROR: duplicate key value violates unique constraint\n")})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Rule == "app/statement-cancelled" {
			t.Errorf("a log without a cancellation: %v", f)
		}
	}
}
