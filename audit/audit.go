// Package audit reads what a machine, an image and a binary were set to,
// and says what each setting costs — never changing any of them.
//
// Everything is parameterised by independent axes, each holding several
// values at once: a run asks for the components it cares about, the goals
// it pursues, the profile it reports under, whether CVEs are judged, the
// sources it may read and the output it wants. Every combination is valid,
// and a rule runs only when the selection covers what it declares. There
// is no mode: "security only, no CVE, on this binary" is a selection like
// any other, and a new rule adds values to the axes rather than a branch
// to the program.
//
// A recommendation always carries five things — the value found, the value
// proposed, the goal it serves, the reason in plain words, and a link to
// the official documentation. The last one is not a courtesy: a rule
// without it cannot be checked by whoever reads the report, so
// [ValidateRules] refuses it and a test holds that.
package audit

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// An Axis is one dimension of a run's parameters. Axes are independent:
// naming values on one says nothing about another.
type Axis string

const (
	// Components is what is audited: the pieces of the stack a selection
	// points at. More values arrive with the rules that read them.
	Components Axis = "components"

	// Goals is what the audit is for. A rule serves exactly one.
	Goals Axis = "goals"

	// Profile is how the report reads: Comparative means every machine of
	// a comparison must be set the same way, and the report is the table
	// the study publishes; Production means the report is advice for one
	// deployment.
	Profile Axis = "profile"

	// CVEs says whether known vulnerabilities are judged. Judging them
	// costs a database and a call path analysis, which a run may not want.
	CVEs Axis = "cve"

	// Sources is what the audit may read. Nothing is read that a
	// selection has not allowed.
	Sources Axis = "sources"

	// Output is the shape of the report.
	Output Axis = "output"
)

// The values of each axis. A rule declares the ones it needs, and
// [Selection.Validate] refuses anything that is not here: a typo in a
// value would otherwise silently select nothing, and an audit that found
// nothing because it ran nothing is the worst report of all.
var axisValues = map[Axis][]string{
	Components: {"gobuild", "linux", "docker", "go", "postgres", "proxy", "firewall", "host", "app"},
	Goals:      {"performance", "security", "reliability"},
	Profile:    {"comparative", "production"},
	CVEs:       {"yes", "no"},
	Sources:    {"binary", "config", "logs", "tools", "network"},
	Output:     {"text", "json"},
}

// Axes returns the axes in a fixed order: a report's own order must not
// depend on a map's iteration.
func Axes() []Axis {
	out := slices.Collect(maps.Keys(axisValues))
	slices.Sort(out)
	return out
}

// Values of an axis, in the order they are declared.
func Values(a Axis) []string { return slices.Clone(axisValues[a]) }

// A Selection is what a run asked for: per axis, one or more values. An
// axis left out means every value of it — a run that says nothing about
// the output wants whatever the default output is, not nothing.
type Selection map[Axis][]string

// Validate refuses an unknown axis or value, and an axis named with no
// value at all, which is the one way to ask for nothing by accident.
func (s Selection) Validate() error {
	var errs []error
	for _, a := range slices.Sorted(maps.Keys(s)) {
		known, ok := axisValues[a]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown axis %q", a))
			continue
		}
		if len(s[a]) == 0 {
			errs = append(errs, fmt.Errorf("axis %q names no value: leave it out to mean every value", a))
		}
		for _, v := range s[a] {
			if !slices.Contains(known, v) {
				errs = append(errs, fmt.Errorf("axis %q: unknown value %q, want one of %s", a, v, strings.Join(known, ", ")))
			}
		}
	}
	return errors.Join(errs...)
}

// Has reports whether the selection covers a value: an axis left out
// covers every value of it.
func (s Selection) Has(a Axis, v string) bool {
	chosen, named := s[a]
	if !named {
		return slices.Contains(axisValues[a], v)
	}
	return slices.Contains(chosen, v)
}

// Covers reports whether the selection admits a rule's needs: for every
// axis the rule names, the selection must hold at least one of the values
// the rule names. A rule that needs "security" does not run under a
// selection of "performance" alone, and one that needs a source it was
// not given does not read it.
func (s Selection) Covers(needs Selection) bool {
	for a, wanted := range needs {
		if !slices.ContainsFunc(wanted, func(v string) bool { return s.Has(a, v) }) {
			return false
		}
	}
	return true
}

// A Question is what reading cannot settle: the role of the machine, how
// exposed it is. Every question has a default, so a run in CI answers
// nothing and still gets a report; the interactive form asks.
type Question struct {
	Key     string
	Ask     string
	Default string
	Choices []string
}

// Questions are the ones the rules may read.
var Questions = []Question{
	{Key: "role", Ask: "what does this machine do?", Default: "server", Choices: []string{"server", "workstation", "build"}},
	{Key: "exposure", Ask: "is it reachable from the internet?", Default: "private", Choices: []string{"internet", "private"}},
}

// Answers holds what was answered; a key left out is the question's
// default, so a zero Answers is the non-interactive run.
type Answers map[string]string

// Get returns the answer, or the question's default. An unknown key is a
// programming fault and says so rather than returning an empty string
// that a rule would read as an answer.
func (a Answers) Get(key string) string {
	if v, ok := a[key]; ok {
		return v
	}
	for _, q := range Questions {
		if q.Key == key {
			return q.Default
		}
	}
	panic("audit: no question named " + key)
}

// A Finding is one recommendation, with the five things every
// recommendation carries.
type Finding struct {
	Rule     string `json:"rule"`
	Current  string `json:"current"`
	Proposed string `json:"proposed"`
	Goal     string `json:"goal"`
	Why      string `json:"why"`
	Doc      string `json:"doc"`
}

// A Rule reads a target and says what it would change. Needs are the axis
// values it requires: a rule of the "gobuild" component serving
// "performance" and reading the "binary" source declares exactly that.
type Rule struct {
	Name  string
	Needs Selection
	Doc   string // the official documentation; a rule without it is refused
	Check func(*Target, Answers) []Finding
}

// rules is the registry, in registration order so that a report's order
// is the one the rules were written in.
var rules []Rule

// Register adds a rule. It is called from the init of each family's file.
func Register(r Rule) { rules = append(rules, r) }

// Rules returns the registry.
func Rules() []Rule { return slices.Clone(rules) }

// ValidateRules checks what no reviewer should have to: that every rule
// names a goal, declares needs the axes know, and links to the
// documentation a reader would check it against.
func ValidateRules() error { return validate(rules) }

func validate(rules []Rule) error {
	var errs []error
	seen := map[string]bool{}
	for _, r := range rules {
		switch {
		case r.Name == "":
			errs = append(errs, errors.New("a rule has no name"))
			continue
		case seen[r.Name]:
			errs = append(errs, fmt.Errorf("rule %q is registered twice", r.Name))
		}
		seen[r.Name] = true
		if strings.TrimSpace(r.Doc) == "" {
			errs = append(errs, fmt.Errorf("rule %q links to no documentation", r.Name))
		} else if !strings.HasPrefix(r.Doc, "https://") {
			errs = append(errs, fmt.Errorf("rule %q: %q is not a link", r.Name, r.Doc))
		}
		if r.Check == nil {
			errs = append(errs, fmt.Errorf("rule %q checks nothing", r.Name))
		}
		if err := r.Needs.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("rule %q: %w", r.Name, err))
		}
		if len(r.Needs[Goals]) != 1 {
			errs = append(errs, fmt.Errorf("rule %q serves %d goals, want exactly one", r.Name, len(r.Needs[Goals])))
		}
	}
	return errors.Join(errs...)
}

// Run applies the rules a selection covers and returns their findings. It
// reads nothing the selection did not allow: a rule declares its source,
// and a rule whose source is not selected does not run.
func Run(sel Selection, answers Answers, t *Target) ([]Finding, error) {
	if err := sel.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateRules(); err != nil {
		return nil, err
	}
	var out []Finding
	for _, r := range rules {
		if !sel.Covers(r.Needs) {
			continue
		}
		for _, f := range r.Check(t, answers) {
			f.Rule, f.Doc = r.Name, r.Doc
			if f.Goal == "" {
				f.Goal = r.Needs[Goals][0]
			}
			out = append(out, f)
		}
	}
	return out, nil
}
