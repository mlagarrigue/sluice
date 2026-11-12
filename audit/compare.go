package audit

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Two reports of the same target, taken at two moments: what appeared,
// what went, what changed. A finding is the same finding from one report
// to the next when its rule, its documentation and its text match once the
// numbers in the text are set aside — "somaxconn = 128" and "somaxconn =
// 4096" are one setting that moved, not one finding resolved and another
// raised. An advisory's identifier is kept whole: two CVEs of one rule are
// two findings.

// Report is the JSON form of a run: the findings always, and the system
// table when the comparative profile asked for it.
type Report struct {
	Findings []Finding `json:"findings"`
	System   []Setting `json:"system,omitempty"`
}

// Diff is what changed between two reports.
type Diff struct {
	Appeared []Finding  `json:"appeared"`
	Resolved []Finding  `json:"resolved"`
	Modified []Modified `json:"modified"`
	// Unchanged counts the findings found again as they were.
	Unchanged int `json:"unchanged"`
	// System holds the lines of the system table whose value moved, or
	// that one report has and the other has not ("absent").
	System []SettingChange `json:"system,omitempty"`
}

// Modified is one finding found in both reports with another value.
type Modified struct {
	Was Finding `json:"was"`
	Is  Finding `json:"is"`
}

// SettingChange is one line of the system table that moved.
type SettingChange struct {
	Name string `json:"name"`
	Was  string `json:"was"`
	Is   string `json:"is"`
}

var (
	advisoryID = regexp.MustCompile(`\b(CVE|GO|GHSA)-[0-9A-Za-z-]+`)
	numbers    = regexp.MustCompile(`\d+`)
)

// identity is what a finding is called from one report to the next: its
// rule, its documentation, and its text with every number but an
// advisory's identifier set aside.
func identity(f *Finding) string {
	ids := advisoryID.FindAllString(f.Current, -1)
	text := numbers.ReplaceAllString(advisoryID.ReplaceAllString(f.Current, "\x01"), "#")
	return f.Rule + "\x00" + f.Doc + "\x00" + text + "\x00" + strings.Join(ids, ",")
}

// Compare says what changed from was to is.
func Compare(was, is Report) Diff {
	d := Diff{Appeared: []Finding{}, Resolved: []Finding{}, Modified: []Modified{}}
	pending := map[string][]Finding{}
	for i := range was.Findings {
		k := identity(&was.Findings[i])
		pending[k] = append(pending[k], was.Findings[i])
	}
	for i := range is.Findings {
		f := is.Findings[i]
		k := identity(&f)
		olds := pending[k]
		if len(olds) == 0 {
			d.Appeared = append(d.Appeared, f)
			continue
		}
		// The same text first, so that a finding repeated with two values
		// pairs each value with itself.
		i := slices.IndexFunc(olds, func(o Finding) bool { return o.Current == f.Current && o.Verdict == f.Verdict })
		if i >= 0 {
			d.Unchanged++
		} else {
			i = 0
			d.Modified = append(d.Modified, Modified{Was: olds[0], Is: f})
		}
		pending[k] = slices.Delete(olds, i, i+1)
	}
	for i := range was.Findings {
		k := identity(&was.Findings[i])
		if len(pending[k]) > 0 {
			d.Resolved = append(d.Resolved, pending[k][0])
			pending[k] = pending[k][1:]
		}
	}
	d.System = compareSystem(was.System, is.System)
	return d
}

func compareSystem(was, is []Setting) []SettingChange {
	old := map[string]string{}
	for _, s := range was {
		old[s.Name] = s.Value
	}
	var out []SettingChange
	seen := map[string]bool{}
	for _, s := range is {
		seen[s.Name] = true
		v, ok := old[s.Name]
		if !ok {
			v = "absent"
		}
		if v != s.Value {
			out = append(out, SettingChange{Name: s.Name, Was: v, Is: s.Value})
		}
	}
	for _, s := range was {
		if !seen[s.Name] {
			out = append(out, SettingChange{Name: s.Name, Was: s.Value, Is: "absent"})
		}
	}
	return out
}

// Gates are what a run may fail on: any new finding, a new advisory that
// applies, a new security finding.
var Gates = []string{"new", "cve", "security"}

// Gate returns why a run fails under gates, one line per finding that
// trips one, empty when it passes. With a diff, only what is new trips
// it — a finding that appeared, or an advisory whose verdict became
// "applies"; without one, every finding of the report counts as new.
func Gate(gates []string, findings []Finding, d *Diff) ([]string, error) {
	for _, g := range gates {
		if !slices.Contains(Gates, g) {
			return nil, fmt.Errorf("-fail-on %q: want one of %s", g, strings.Join(Gates, ", "))
		}
	}
	fresh := findings
	if d != nil {
		fresh = slices.Clone(d.Appeared)
		for i := range d.Modified {
			m := &d.Modified[i]
			if m.Is.Verdict == VerdictApplies && m.Was.Verdict != VerdictApplies {
				fresh = append(fresh, m.Is)
			}
		}
	}
	var out []string
	for _, f := range fresh {
		switch {
		case slices.Contains(gates, "new"):
			out = append(out, "new: "+f.Rule+": "+f.Current)
		case slices.Contains(gates, "cve") && f.Verdict == VerdictApplies:
			out = append(out, "cve: "+f.Rule+": "+f.Current)
		case slices.Contains(gates, "security") && f.Goal == "security":
			out = append(out, "security: "+f.Rule+": "+f.Current)
		}
	}
	return out, nil
}
