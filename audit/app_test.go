package audit

import (
	"slices"
	"strings"
	"testing"
)

// R as the study starts it: the contract's variables, and the server with
// no flag but the database's. Each test seeds one difference from it.
const appR = `PGHOST=127.0.0.1
PGUSER=bench
PGPASSWORD=secret
STUDY_DB_POOL=8
/usr/local/bin/server -db-name bench
`

func appFindings(t *testing.T, start string, settings string, sel Selection, ans Answers) []Finding {
	t.Helper()
	spec := Spec{AppArgs: seedFile(t, "start.txt", start)}
	if settings != "" {
		spec.PGSettings = seedFile(t, "settings.csv", settings)
	}
	target, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(sel, ans, target)
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

func TestTheReferenceConfigurationIsSilent(t *testing.T) {
	if got := appFindings(t, appR, tunedSettings, Selection{Components: {"app"}}, Answers{"exposure": "internet"}); len(got) != 0 {
		t.Errorf("R was reported: %+v", got)
	}
}

func TestSeededDeparturesFromRAreReported(t *testing.T) {
	for _, c := range []struct {
		name, start, settings string
		ans                   Answers
		rule, says            string
	}{
		{
			"a connection closed every 100 requests", appR + "-transport httpstream-h2 -max-requests-per-conn=100\n", "", nil,
			"app/max-requests-per-conn", "-max-requests-per-conn 100 on httpstream-h2",
		},
		{
			"a remote database, no password", "PGHOST=10.0.0.5\nPGUSER=bench\nserver\n", "", nil,
			"app/db-in-clear", "-db 10.0.0.5:5432",
		},
		{
			"TLS off on the internet", appR + "-tls=false\n", "",
			Answers{"exposure": "internet"},
			"app/tls-off", "-tls=false",
		},
		{
			"more connections than the server's slots", appR + "-db-conns 97\n", defaultSettings, nil,
			"app/db-conns", "98 in all, against 100",
		},
		{
			"session tickets in a comparison", "STUDY_TLS_RESUME=on\n" + appR, "", nil,
			"app/tls-resume", "-tls-resume",
		},
	} {
		got := appFindings(t, c.start, c.settings, Selection{Components: {"app"}}, c.ans)
		if len(got) != 1 || got[0].Rule != c.rule || !strings.Contains(got[0].Current, c.says) {
			t.Errorf("%s: %+v, want %s saying %q", c.name, got, c.rule, c.says)
		}
	}
	// Outside a comparison, resuming sessions is the production's choice.
	got := appFindings(t, "STUDY_TLS_RESUME=on\n"+appR, "", Selection{Components: {"app"}, Profile: {"production"}}, nil)
	if len(got) != 0 {
		t.Errorf("tls-resume reported under the production profile: %+v", got)
	}
	got = appFindings(t, "PGHOST=10.0.0.5\nPGUSER=bench\nserver\n", "", Selection{Components: {"app"}}, nil)
	if len(got) != 1 || !strings.Contains(got[0].Current, "trust") {
		t.Errorf("no password on a remote database is not named: %+v", got)
	}
}

// TestTheFlagsWinOverTheContract: the environment first, the command line
// over it, as the server reads them; a flag the table does not know is
// refused rather than skipped.
func TestTheFlagsWinOverTheContract(t *testing.T) {
	a, err := parseApp("STUDY_PROTOCOL=h3\nSTUDY_DB_POOL=4\nserver -db-conns 16 --gateway-eager -db=x:1\n")
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{"transport": "httpstream-h3", "db-conns": "16", "gateway-eager": "true", "db": "x:1", "tls": "true"} {
		if a.Flags[k] != want {
			t.Errorf("-%s = %q, want %q", k, a.Flags[k], want)
		}
	}
	for name, start := range map[string]string{
		"unknown flag":         "server -workers 4\n",
		"value missing":        "server -db-conns\n",
		"not a flag":           "server bench\n",
		"outside the contract": "STUDY_PROTOCOL=h4\n",
	} {
		if _, err := parseApp(start); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}

func TestTheSystemTablePublishesTheServersFlags(t *testing.T) {
	target, err := Open(Spec{AppArgs: seedFile(t, "start.txt", appR+"-gateway-within 500us\n")})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	var names []string
	for _, s := range System(target) {
		got[s.Name] = s.Value
		names = append(names, s.Name)
	}
	for name, want := range map[string]string{"app transport": "nethttp-h1", "app db-conns": "8", "app gateway-within": "500µs", "app tls-resume": "false"} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
	if slices.ContainsFunc(names, func(n string) bool { return strings.Contains(n, "password") }) {
		t.Errorf("the password is in the system table: %v", names)
	}
}
