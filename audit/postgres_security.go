package audit

import (
	"encoding/csv"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

// A PGRole is one row of pg_authid or pg_roles: a role, whether it is a
// superuser, and whether it may log in.
type PGRole struct {
	Name     string
	Super    bool
	CanLogin bool
}

// parsePGRoles reads psql's CSV of pg_authid or pg_roles: a header naming
// rolname, rolsuper and rolcanlogin. A file without them is refused rather
// than read as a server with no roles.
func parsePGRoles(text string) ([]PGRole, error) {
	rows, err := csv.NewReader(strings.NewReader(text)).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("not psql's CSV output: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("empty: want psql --csv output of pg_roles")
	}
	name, super, login := slices.Index(rows[0], "rolname"), slices.Index(rows[0], "rolsuper"), slices.Index(rows[0], "rolcanlogin")
	if name < 0 || super < 0 || login < 0 {
		return nil, fmt.Errorf("the header %q names no rolname, rolsuper and rolcanlogin columns", rows[0])
	}
	out := make([]PGRole, 0, len(rows)-1)
	for _, r := range rows[1:] {
		out = append(out, PGRole{Name: r[name], Super: r[super] == "t" || r[super] == "true", CanLogin: r[login] == "t" || r[login] == "true"})
	}
	return out, nil
}

// versionNum returns the server's version as server_version_num counts it
// — 140001 for 14.1, 90624 for 9.6.24 — from that setting, or from
// server_version when only it was saved.
func (pg *Postgres) versionNum() (int, bool) {
	if n, ok := pg.integer("server_version_num"); ok && n >= 0 && n <= math.MaxInt32 {
		return int(n), true
	}
	s, ok := pg.setting("server_version")
	if !ok {
		return 0, false
	}
	v, _, _ := strings.Cut(strings.TrimSpace(s.Value), " ") // "14.0 (Debian 14.0-1.pgdg110+1)"
	var parts [3]int
	f := strings.Split(v, ".")
	for i := 0; i < len(f) && i < 3; i++ {
		n, err := strconv.Atoi(f[i])
		if err != nil {
			return 0, false
		}
		parts[i] = n
	}
	if parts[0] >= 10 {
		return parts[0]*10000 + parts[1], true
	}
	return parts[0]*10000 + parts[1]*100 + parts[2], true
}

// major is a version number's major release: 14 for 140001, 906 for 90624.
func major(num int) int {
	if num >= 100000 {
		return num / 10000
	}
	return num / 100
}

// pgAdvisory is a PostgreSQL vulnerability whose reach depends on what the
// server was set to, judged from the version and the setting read.
type pgAdvisory struct {
	ID      string
	Summary string
	// Fixed is the first fixed release of each affected major, as
	// server_version_num counts them. A major newer than all of them was
	// never affected; one older is out of support and was not listed.
	Fixed map[int]int
	// Exposed returns what in the configuration makes the flaw reachable,
	// empty when nothing does; known is false when the part it reads was
	// not given.
	Exposed func(*Postgres) (where string, known bool)
}

// pgAdvisories are the PostgreSQL advisories the audit judges. Each is
// read from its page on postgresql.org, its affected configurations
// included: a version alone says a flaw is present, not that it is
// reachable.
var pgAdvisories = []pgAdvisory{{
	ID: "CVE-2021-23214",
	Summary: "a man in the middle can inject SQL before the TLS handshake ends, " +
		"on a server using cert authentication, or trust with a clientcert requirement",
	Fixed: map[int]int{14: 140001, 13: 130005, 12: 120009, 11: 110014, 10: 100019, 906: 90624},
	Exposed: func(pg *Postgres) (string, bool) {
		if pg.HBAPath == "" {
			return "", false
		}
		var lines []string
		for _, l := range pg.HBA {
			clientcert := slices.ContainsFunc(l.Options, func(o string) bool { return strings.HasPrefix(o, "clientcert=") })
			if l.Method == "cert" || (l.Method == "trust" && clientcert) {
				lines = append(lines, strconv.Itoa(l.Line))
			}
		}
		if len(lines) == 0 {
			return "", true
		}
		return pg.HBAPath + ", line " + strings.Join(lines, ", "), true
	},
}}

func init() {
	Register(Rule{
		Name:  "postgres/superusers",
		Needs: Selection{Components: {"postgres"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   "https://www.postgresql.org/docs/current/role-attributes.html",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Postgres == nil || t.Postgres.RolesPath == "" {
				return nil
			}
			var supers []string
			for _, r := range t.Postgres.Roles {
				if r.Super && r.CanLogin {
					supers = append(supers, r.Name)
				}
			}
			if len(supers) <= 1 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d superusers that log in, in %s: %s", len(supers), t.Postgres.RolesPath, strings.Join(supers, ", ")),
				Proposed: "one superuser, for administration; the applications' roles without SUPERUSER, granted what they use",
				Why: "a superuser bypasses every permission check, row security included, and can " +
					"run programs on the server's machine (COPY … PROGRAM): each one that logs in " +
					"is a password whose theft is the whole server",
			}}
		},
	})

	Register(Rule{
		Name:  "postgres/cve",
		Needs: Selection{Components: {"postgres"}, Goals: {"security"}, Sources: {"config"}, CVEs: {"yes"}},
		Doc:   "https://www.postgresql.org/support/security/",
		Check: func(t *Target, _ Answers) []Finding {
			pg := t.Postgres
			if pg == nil || pg.Settings == nil {
				return nil
			}
			num, ok := pg.versionNum()
			if !ok {
				return []Finding{{
					Current:  "the server's version was not read: pg_settings holds no server_version",
					Proposed: "save pg_settings whole, server_version_num included",
					Verdict:  VerdictUndetermined,
					Why: "a run that asked for CVEs and could not judge them must say so: a report " +
						"silent about vulnerabilities reads as a report that found none",
				}}
			}
			var out []Finding
			for _, adv := range pgAdvisories {
				if f, ok := judge(adv, pg, num); ok {
					out = append(out, f)
				}
			}
			return out
		},
	})
}

// judge says what one advisory means for this server: nothing for a
// release it was fixed in or never touched, a verdict otherwise.
func judge(adv pgAdvisory, pg *Postgres, num int) (Finding, bool) {
	doc := "https://www.postgresql.org/support/security/" + adv.ID + "/"
	m := major(num)
	fixed, listed := adv.Fixed[m]
	switch {
	case listed && num >= fixed:
		return Finding{}, false
	case !listed && num > slices.Max(slices.Collect(maps.Values(adv.Fixed))):
		return Finding{}, false // a major released after the fix
	}
	version := strconv.Itoa(num)
	if s, ok := pg.setting("server_version"); ok {
		version = s.Value
	}
	current := adv.ID + " on PostgreSQL " + version + ": " + adv.Summary
	proposed := "a supported major: this one is older than the advisory lists"
	if listed {
		proposed = "the latest minor release of this major, " + versionName(fixed) + " or later"
	}
	where, known := adv.Exposed(pg)
	f := Finding{Current: current, Proposed: proposed, Doc: doc}
	switch {
	case !listed:
		f.Verdict = VerdictUndetermined
		f.Why = "the major is out of support and the advisory does not list it: whether it is fixed is not published"
	case !known:
		f.Verdict = VerdictUndetermined
		f.Why = "the release is affected; whether the flaw is reachable depends on pg_hba.conf, which was not given"
	case where == "":
		f.Verdict = VerdictAbsent
		f.Why = "the release is affected, but nothing in pg_hba.conf uses what the flaw needs; " +
			"a rule added later would expose it, so the upgrade stays due"
	default:
		f.Verdict = VerdictApplies
		f.Why = "the release is affected and the configuration reaches the flaw: " + where
	}
	return f, true
}

// versionName writes a server_version_num as a release: 14.1, 9.6.24.
func versionName(num int) string {
	if num >= 100000 {
		return fmt.Sprintf("%d.%d", num/10000, num%10000)
	}
	return fmt.Sprintf("%d.%d.%d", num/10000, num/100%100, num%100)
}
