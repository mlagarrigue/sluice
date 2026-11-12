package audit

import (
	"slices"
	"strings"
	"testing"
)

func finding(rule, current, verdict string) Finding {
	return Finding{Rule: rule, Current: current, Verdict: verdict, Goal: "security", Doc: "https://example.invalid/" + rule}
}

// TestTwoReportsAreCompared: one finding went, one came, one moved its
// value, one stayed; two advisories of one rule stay two findings.
func TestTwoReportsAreCompared(t *testing.T) {
	was := Report{
		Findings: []Finding{
			finding("linux/somaxconn", "proc/sys/net/core/somaxconn = 128 connections", ""),
			finding("postgres/ssl", "ssl = off", ""),
			finding("deps/cve", "GO-2024-0001 in example.com/m v1.0.0", VerdictApplies),
			finding("deps/cve", "GO-2024-0002 in example.com/m v1.0.0", VerdictApplies),
		},
		System: []Setting{{"net.core.somaxconn", "128"}, {"kernel", "6.1"}},
	}
	is := Report{
		Findings: []Finding{
			finding("linux/somaxconn", "proc/sys/net/core/somaxconn = 512 connections", ""),
			finding("deps/cve", "GO-2024-0002 in example.com/m v1.0.0", VerdictApplies),
			finding("postgres/hba-trust", "trust over the network in pg_hba.conf, line 3", ""),
		},
		System: []Setting{{"net.core.somaxconn", "512"}, {"kernel", "6.1"}, {"host smt active", "1"}},
	}
	d := Compare(was, is)
	rules := func(fs []Finding) (out []string) {
		for _, f := range fs {
			out = append(out, f.Rule+" "+f.Current)
		}
		return out
	}
	if got := rules(d.Appeared); len(got) != 1 || !strings.HasPrefix(got[0], "postgres/hba-trust") {
		t.Errorf("appeared %v", got)
	}
	if got := rules(d.Resolved); len(got) != 2 || !slices.Contains(got, "postgres/ssl ssl = off") ||
		!slices.Contains(got, "deps/cve GO-2024-0001 in example.com/m v1.0.0") {
		t.Errorf("resolved %v", got)
	}
	if len(d.Modified) != 1 || !strings.Contains(d.Modified[0].Was.Current, "128") || !strings.Contains(d.Modified[0].Is.Current, "512") {
		t.Errorf("modified %+v", d.Modified)
	}
	if d.Unchanged != 1 {
		t.Errorf("unchanged %d, want the second advisory", d.Unchanged)
	}
	want := []SettingChange{{"net.core.somaxconn", "128", "512"}, {"host smt active", "absent", "1"}}
	if !slices.Equal(d.System, want) {
		t.Errorf("system %+v, want %+v", d.System, want)
	}
}

func TestTheGateTripsOnWhatIsNew(t *testing.T) {
	applies := finding("postgres/cve", "CVE-2021-23214 on PostgreSQL 14.0: …", VerdictApplies)
	absent := finding("postgres/cve", "CVE-2021-23214 on PostgreSQL 14.0: …", VerdictAbsent)
	setting := finding("postgres/ssl", "ssl = off", "")
	for _, c := range []struct {
		name    string
		gates   []string
		was, is []Finding
		tripped int
	}{
		{"a new advisory that applies", []string{"cve"}, nil, []Finding{applies}, 1},
		{"the same advisory, already there", []string{"cve"}, []Finding{applies}, []Finding{applies}, 0},
		{"an advisory that does not apply", []string{"cve"}, nil, []Finding{absent}, 0},
		{"an advisory that came to apply", []string{"cve"}, []Finding{absent}, []Finding{applies}, 1},
		{"a new setting under cve", []string{"cve"}, nil, []Finding{setting}, 0},
		{"a new setting under security", []string{"security"}, nil, []Finding{setting}, 1},
		{"anything new under new", []string{"new"}, nil, []Finding{setting, absent}, 2},
	} {
		d := Compare(Report{Findings: c.was}, Report{Findings: c.is})
		got, err := Gate(c.gates, c.is, &d)
		if err != nil || len(got) != c.tripped {
			t.Errorf("%s: %v %v, want %d", c.name, got, err, c.tripped)
		}
	}
	if got, _ := Gate([]string{"cve"}, []Finding{applies, setting}, nil); len(got) != 1 {
		t.Errorf("without a diff, the whole report counts: %v", got)
	}
	if _, err := Gate([]string{"warnings"}, nil, nil); err == nil {
		t.Error("an unknown gate was accepted")
	}
}
