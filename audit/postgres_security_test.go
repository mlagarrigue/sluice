package audit

import (
	"strings"
	"testing"
)

func rolesFindings(t *testing.T, roles string) []Finding {
	t.Helper()
	target, err := Open(Spec{PGRoles: seedFile(t, "roles.csv", roles)})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"postgres"}, Goals: {"security"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// TestASecondSuperuserThatLogsInIsReported: postgres alone is the image's
// default; a second superuser that logs in is one password more whose
// theft is the server; a superuser that cannot log in is a group.
func TestASecondSuperuserThatLogsInIsReported(t *testing.T) {
	const header = "rolname,rolsuper,rolcanlogin\n"
	one := header + "postgres,t,t\napp,f,t\nadmins,t,f\n"
	if got := rolesFindings(t, one); len(got) != 0 {
		t.Errorf("one superuser that logs in was reported: %+v", got)
	}
	two := header + "postgres,t,t\napp,t,t\n"
	got := rolesFindings(t, two)
	if len(got) != 1 || got[0].Rule != "postgres/superusers" || !strings.Contains(got[0].Current, "postgres, app") {
		t.Errorf("two superusers that log in: %+v", got)
	}
}

func TestRolesMustBePsqlsCSV(t *testing.T) {
	for name, body := range map[string]string{"empty": "", "no columns": "rolname,rolsuper\npostgres,t\n"} {
		if _, err := Open(Spec{PGRoles: seedFile(t, "roles.csv", body)}); err == nil {
			t.Errorf("%s: a file that is not pg_roles was read", name)
		}
	}
}

func cveFindings(t *testing.T, version, hba string) []Finding {
	t.Helper()
	spec := Spec{PGSettings: seedFile(t, "settings.csv", "name,setting,unit\nserver_version,"+version+",\n")}
	if hba != "" {
		spec.PGHBA = seedFile(t, "pg_hba.conf", hba)
	}
	target, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"postgres"}, CVEs: {"yes"}, Sources: {"config"}, Goals: {"security"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []Finding
	for _, f := range findings {
		if f.Rule == "postgres/cve" {
			out = append(out, f)
		}
	}
	return out
}

// TestCVE202123214IsJudgedOnTheVersionAndPgHBA: the flaw is in 14.0 and
// fixed in 14.1; it is reachable only through cert authentication, or
// trust with a clientcert requirement.
func TestCVE202123214IsJudgedOnTheVersionAndPgHBA(t *testing.T) {
	const certHBA = "local all all peer\nhostssl all all 0.0.0.0/0 cert\n"
	const trustCert = "hostssl all all 10.0.0.0/8 trust clientcert=verify-full\n"
	const scram = "hostssl all all 10.0.0.0/8 scram-sha-256\n"
	for _, c := range []struct {
		name, version, hba, verdict, where string
	}{
		{"cert on 14.0", "14.0 (Debian 14.0-1.pgdg110+1)", certHBA, VerdictApplies, "line 2"},
		{"trust with clientcert on 9.6.23", "9.6.23", trustCert, VerdictApplies, "line 1"},
		{"scram on 13.4", "13.4", scram, VerdictAbsent, ""},
		{"no pg_hba.conf on 12.8", "12.8", "", VerdictUndetermined, ""},
		{"9.5, never listed", "9.5.25", certHBA, VerdictUndetermined, ""},
		{"fixed in 14.1", "14.1", certHBA, "", ""},
		{"fixed in 9.6.24", "9.6.24", certHBA, "", ""},
		{"a later major", "17.2", certHBA, "", ""},
	} {
		got := cveFindings(t, c.version, c.hba)
		switch {
		case c.verdict == "" && len(got) != 0:
			t.Errorf("%s: reported on a fixed release: %+v", c.name, got)
		case c.verdict == "":
		case len(got) != 1 || got[0].Verdict != c.verdict || !strings.Contains(got[0].Why, c.where) ||
			!strings.Contains(got[0].Doc, "CVE-2021-23214"):
			t.Errorf("%s: %+v, want the verdict %q naming %q", c.name, got, c.verdict, c.where)
		}
	}
}

func TestAVersionNotReadIsSaidWhenCVEsAreAsked(t *testing.T) {
	target, err := Open(Spec{PGSettings: seedFile(t, "s.csv", "name,setting,unit\nmax_connections,100,\n")})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := Run(Selection{Components: {"postgres"}, CVEs: {"yes"}, Goals: {"security"}}, nil, target)
	if len(got) != 1 || got[0].Verdict != VerdictUndetermined {
		t.Errorf("no server_version, CVEs asked: %+v", got)
	}
}

func TestServerVersionNum(t *testing.T) {
	for in, want := range map[string]int{"14.0": 140000, "17.11": 170011, "9.6.24": 90624, "10.19 (Ubuntu)": 100019} {
		pg := &Postgres{Settings: map[string]PGSetting{"server_version": {Value: in}}}
		if got, ok := pg.versionNum(); !ok || got != want {
			t.Errorf("%q: %d, want %d", in, got, want)
		}
	}
	pg := &Postgres{Settings: map[string]PGSetting{"server_version_num": {Value: "160004"}, "server_version": {Value: "x"}}}
	if got, _ := pg.versionNum(); got != 160004 || versionName(got) != "16.4" || versionName(90624) != "9.6.24" {
		t.Errorf("server_version_num first: %d %s", got, versionName(got))
	}
}
