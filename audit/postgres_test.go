package audit

import (
	"slices"
	"strings"
	"testing"
)

// A PostgreSQL server is read from the files a caller saved, so these
// tests seed them: the server worth reporting is the one on its defaults,
// opened to the network on trust, which nobody should run to see a report.

const defaultSettings = `name,setting,unit
server_version,17.11,
max_connections,100,
superuser_reserved_connections,3,
shared_buffers,16384,8kB
work_mem,4096,kB
max_wal_size,1024,MB
checkpoint_timeout,300,s
autovacuum,off,
ssl,off,
password_encryption,md5,
`

const tunedSettings = `name,setting,unit
server_version,17.11,
max_connections,200,
superuser_reserved_connections,3,
shared_buffers,524288,8kB
work_mem,16384,kB
max_wal_size,8192,MB
checkpoint_timeout,3600,s
autovacuum,on,
ssl,on,
password_encryption,scram-sha-256,
`

const openHBA = `# TYPE  DATABASE  USER  ADDRESS  METHOD
local   all       all            trust
host    all       all   127.0.0.1/32  trust
host    all       all   192.168.0.0 255.255.255.0 \
        md5
include_if_exists extra.conf
host    all       all   all       password
`

const closedHBA = `local   all   all                peer
hostssl all   all   10.0.0.0/8   scram-sha-256
`

const busyLog = `2026-11-12 10:00:00 UTC [42] LOG:  checkpoints are occurring too frequently (9 seconds apart)
2026-11-12 10:00:01 UTC [43] FATAL:  sorry, too many clients already
2026-11-12 10:00:02 UTC [44] FATAL:  remaining connection slots are reserved for roles with the SUPERUSER attribute
`

var postgresRules = []string{
	"postgres/max-connections", "postgres/shared-buffers", "postgres/autovacuum",
	"postgres/password-encryption", "postgres/ssl", "postgres/hba-trust",
	"postgres/hba-md5", "postgres/checkpoints", "postgres/too-many-clients",
}

func postgresFindings(t *testing.T, settings, hba, log string, answers Answers) []Finding {
	t.Helper()
	target, err := Open(Spec{
		PGSettings: seedFile(t, "settings.csv", settings),
		PGHBA:      seedFile(t, "pg_hba.conf", hba),
		PGLog:      seedFile(t, "postgresql.log", log),
	})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"postgres"}}, answers, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Current == "" || f.Proposed == "" || f.Why == "" || f.Doc == "" || f.Goal == "" {
			t.Errorf("a finding of %s is missing one of its five fields: %+v", f.Rule, f)
		}
	}
	return findings
}

func TestAServerOnItsDefaultsIsReadSettingBySetting(t *testing.T) {
	var got []string
	for _, f := range postgresFindings(t, defaultSettings, openHBA, busyLog, Answers{"pools": "120"}) {
		got = append(got, f.Rule)
	}
	for _, want := range postgresRules {
		if !slices.Contains(got, want) {
			t.Errorf("%s stayed silent on the default server: %v", want, got)
		}
	}
}

func TestATunedServerIsSilent(t *testing.T) {
	if got := postgresFindings(t, tunedSettings, closedHBA, "LOG:  database system is ready\n", Answers{"pools": "120"}); len(got) != 0 {
		t.Errorf("a server set for its load was reported: %+v", got)
	}
}

// TestTheConnectionsCountTheReservedSlots: 100 connections with 3 reserved
// leave 97 for clients, and 95 asked for plus five a person opens is one
// too many. Unknown pools say nothing: the sum is read nowhere.
func TestTheConnectionsCountTheReservedSlots(t *testing.T) {
	for pools, want := range map[string]bool{"92": false, "93": true, "unknown": false} {
		var fired bool
		for _, f := range postgresFindings(t, defaultSettings, closedHBA, "", Answers{"pools": pools}) {
			fired = fired || f.Rule == "postgres/max-connections"
		}
		if fired != want {
			t.Errorf("pools=%s: postgres/max-connections fired = %v, want %v", pools, fired, want)
		}
	}
}

// TestTheHBARulesAreReadWhereTheyAre: the line numbers a finding names are
// the file's, a rule continued over two lines and an address with a
// separate mask included.
func TestTheHBARulesAreReadWhereTheyAre(t *testing.T) {
	byRule := map[string][]string{}
	for _, f := range postgresFindings(t, defaultSettings, openHBA, "", nil) {
		byRule[f.Rule] = append(byRule[f.Rule], f.Current)
	}
	trust := strings.Join(byRule["postgres/hba-trust"], " | ")
	if !strings.Contains(trust, "over the network") || !strings.Contains(trust, "line 3") ||
		!strings.Contains(trust, "Unix socket") || !strings.Contains(trust, "line 2") {
		t.Errorf("postgres/hba-trust said %q, want line 3 over the network and line 2 on the socket", trust)
	}
	if md5 := strings.Join(byRule["postgres/hba-md5"], ""); !strings.Contains(md5, "4 (md5), 7 (password)") {
		t.Errorf("postgres/hba-md5 said %q, want lines 4 (md5) and 7 (password)", md5)
	}
	// ssl off with remote rules is said whatever the exposure answer; line
	// 3 is loopback and not one of them.
	if ssl := strings.Join(byRule["postgres/ssl"], ""); !strings.HasSuffix(ssl, "(line 4, 7)") {
		t.Errorf("postgres/ssl said %q, want the remote rules 4 and 7", ssl)
	}
}

func TestSSLOffOnLoopbackAloneIsAnExposureQuestion(t *testing.T) {
	loopbackOnly := "local all all peer\nhost all all 127.0.0.1/32 scram-sha-256\n"
	for exposure, want := range map[string]bool{"private": false, "internet": true} {
		var fired bool
		for _, f := range postgresFindings(t, defaultSettings, loopbackOnly, "", Answers{"exposure": exposure}) {
			fired = fired || f.Rule == "postgres/ssl"
		}
		if fired != want {
			t.Errorf("exposure=%s: postgres/ssl fired = %v, want %v", exposure, fired, want)
		}
	}
}

func TestSettingsMustBePsqlsCSV(t *testing.T) {
	for name, body := range map[string]string{
		"empty":     "",
		"no header": "max_connections,100\n",
		"not CSV":   "name,setting\n\"unterminated\n",
	} {
		if _, err := Open(Spec{PGSettings: seedFile(t, "s.csv", body)}); err == nil {
			t.Errorf("%s: a file that is not pg_settings was read", name)
		}
	}
}

func TestTheSystemTablePublishesThePostgresSettings(t *testing.T) {
	target, err := Open(Spec{PGSettings: seedFile(t, "s.csv", strings.Replace(tunedSettings, "ssl,on,\n", "", 1))})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range System(target) {
		got[s.Name] = s.Value
	}
	for name, want := range map[string]string{
		"postgres max_connections": "200",
		"postgres shared_buffers":  "524288 8kB",
		"postgres server_version":  "17.11",
		"postgres ssl":             "absent",
	} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
}
