package audit

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// TestEveryRuleLinksItsDocumentation: the registry as it stands, and then
// the faults the validation exists to refuse. A rule whose reason nobody
// can check against a published document is an opinion with a command-line
// flag, so this is a test rather than a review habit.
func TestEveryRuleLinksItsDocumentation(t *testing.T) {
	if err := ValidateRules(); err != nil {
		t.Fatalf("the registered rules: %v", err)
	}
	if len(Rules()) == 0 {
		t.Fatal("no rule is registered, so the test above checked nothing")
	}

	ok := Rule{
		Name:  "x/ok",
		Needs: Selection{Components: {"gobuild"}, Goals: {"security"}, Sources: {"binary"}},
		Doc:   "https://example.invalid/doc",
		Check: func(*Target, Answers) []Finding { return nil },
	}
	faults := []struct {
		name string
		rule Rule
		want string
	}{
		{"no documentation", with(ok, func(r *Rule) { r.Doc = "" }), "links to no documentation"},
		{"not a link", with(ok, func(r *Rule) { r.Doc = "see the manual" }), "is not a link"},
		{"no check", with(ok, func(r *Rule) { r.Check = nil }), "checks nothing"},
		{"no name", with(ok, func(r *Rule) { r.Name = "" }), "has no name"},
		{"two goals", with(ok, func(r *Rule) { r.Needs[Goals] = []string{"security", "performance"} }), "serves 2 goals"},
		{"unknown value", with(ok, func(r *Rule) { r.Needs[Components] = []string{"nope"} }), "unknown value"},
	}
	for _, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			err := validate([]Rule{f.rule})
			if err == nil || !strings.Contains(err.Error(), f.want) {
				t.Errorf("validate: %v, want an error saying %q", err, f.want)
			}
		})
	}
	t.Run("twice", func(t *testing.T) {
		if err := validate([]Rule{ok, ok}); err == nil || !strings.Contains(err.Error(), "registered twice") {
			t.Errorf("the same rule twice: %v", err)
		}
	})
}

// with copies a rule and applies a fault to the copy: the needs are a map,
// so a shallow copy would let one case edit another's.
func with(r Rule, fault func(*Rule)) Rule {
	needs := Selection{}
	for a, v := range r.Needs {
		needs[a] = slices.Clone(v)
	}
	r.Needs = needs
	fault(&r)
	return r
}

// TestEveryCombinationIsValidAndSelectsOnlyWhatItAsked walks the whole
// product of the axes — one value per axis, every combination — and
// checks the two properties the parameters exist for: nothing is refused,
// and nothing runs that the selection did not ask for. A mode hidden in a
// combination would show up here as a rule running without its axis.
func TestEveryCombinationIsValidAndSelectsOnlyWhatItAsked(t *testing.T) {
	axes := Axes()
	byName := map[string]Rule{}
	for _, r := range Rules() {
		byName[r.Name] = r
	}

	// The product of the axes, one value each: 1,080 combinations as the
	// axes stand, walked whole. Past a few thousand this becomes every
	// pair instead; it is not there yet, and the whole product is the
	// stronger check.
	total := 1
	for _, a := range axes {
		total *= len(Values(a))
	}
	if total > 5000 {
		t.Fatalf("the product reached %d combinations: walk the pairs instead", total)
	}

	target := &Target{} // no binary: the rules must still be selected or not
	seen := map[string]bool{}
	var walk func(i int, sel Selection)
	walk = func(i int, sel Selection) {
		if i == len(axes) {
			if err := sel.Validate(); err != nil {
				t.Fatalf("%v: %v", sel, err)
			}
			findings, err := Run(sel, nil, target)
			if err != nil {
				t.Fatalf("%v: %v", sel, err)
			}
			for _, f := range findings {
				r, ok := byName[f.Rule]
				if !ok {
					t.Fatalf("%v: a finding of no registered rule: %q", sel, f.Rule)
				}
				if !sel.Covers(r.Needs) {
					t.Fatalf("%v: rule %q ran without its needs %v", sel, r.Name, r.Needs)
				}
				if f.Goal != sel[Goals][0] {
					t.Fatalf("%v: a %s finding under the goal %s", sel, f.Goal, sel[Goals][0])
				}
				seen[f.Rule] = true
			}
			return
		}
		for _, v := range Values(axes[i]) {
			sel[axes[i]] = []string{v}
			walk(i+1, sel)
		}
		delete(sel, axes[i])
	}
	walk(0, Selection{})

	// An axis left out means all of its values, which is what a bare run
	// asks for: every rule of the registry must then be reachable.
	findings, err := Run(Selection{}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) == 0 {
		t.Error("a selection naming no axis selected no rule at all")
	}
}

// TestSelectionRefusesWhatWouldSelectNothing: an axis named with no value
// is the one way to ask for nothing by accident, and a typo in a value is
// the other.
func TestSelectionRefusesWhatWouldSelectNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		sel  Selection
		want string
	}{
		{"empty axis", Selection{Goals: {}}, "names no value"},
		{"unknown axis", Selection{Axis("colours"): {"blue"}}, "unknown axis"},
		{"unknown value", Selection{Goals: {"speed"}}, "unknown value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.sel.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate: %v, want an error saying %q", err, tc.want)
			}
		})
	}
}

// TestAnswersFallBackOnTheDefault: a run that answers nothing is the run
// in CI, and it must still have an answer to read.
func TestAnswersFallBackOnTheDefault(t *testing.T) {
	var none Answers
	if got := none.Get("role"); got != "server" {
		t.Errorf("role with nothing answered = %q, want the question's default", got)
	}
	if got := (Answers{"role": "workstation"}).Get("role"); got != "workstation" {
		t.Errorf("role answered = %q", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("an unknown question returned an answer instead of saying it does not exist")
		}
	}()
	_ = none.Get("colour")
}

// TestBuildSettingsAreReadFromTheBinary builds the same program twice —
// once on the defaults, once hardened — and checks that the rules fire on
// the first and not on the second. The second binary is the mutation: the
// rule is proved by a build where it must stay silent, not by reading the
// report of the first.
func TestBuildSettingsAreReadFromTheBinary(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain to build with")
	}
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module probe\n\ngo 1.26.0\n")
	write("main.go", "package main\n\nfunc main() {}\n")

	build := func(out string, env ...string) string {
		t.Helper()
		path := filepath.Join(dir, out)
		args := []string{"build", "-o", path}
		if out == "hardened" {
			args = append(args, "-trimpath")
		}
		cmd := exec.Command("go", append(args, ".")...) //nolint:gosec // G204: the arguments are this test's own
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), env...)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %v: %v\n%s", args, err, b)
		}
		return path
	}

	lax := build("lax", "CGO_ENABLED=1", "GOAMD64=v1", "GOFLAGS=")
	hardened := build("hardened", "CGO_ENABLED=0", "GOAMD64=v3", "GOFLAGS=")

	rules := func(path string) []string {
		t.Helper()
		target, err := OpenBinary(path)
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(Selection{}, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, f := range findings {
			if f.Current == "" || f.Proposed == "" || f.Why == "" || f.Doc == "" || f.Goal == "" {
				t.Errorf("%s: a finding of %s is missing one of its five fields: %+v", path, f.Rule, f)
			}
			names = append(names, f.Rule)
		}
		return names
	}

	onLax, onHardened := rules(lax), rules(hardened)

	loose := []string{"gobuild/trimpath", "gobuild/cgo"}
	if runtime.GOARCH == "amd64" {
		loose = append(loose, "gobuild/goamd64")
	}
	for _, name := range loose {
		if !slices.Contains(onLax, name) {
			t.Errorf("%s did not fire on the lax build: %v", name, onLax)
		}
		if slices.Contains(onHardened, name) {
			t.Errorf("%s fired on the hardened build: %v", name, onHardened)
		}
	}
	// Neither build has a profile or a revision: a module in a temporary
	// directory is not a checkout, and the rules must say so on both.
	for _, name := range []string{"gobuild/pgo", "gobuild/vcs"} {
		if !slices.Contains(onLax, name) || !slices.Contains(onHardened, name) {
			t.Errorf("%s is silent on a build that has neither: %v / %v", name, onLax, onHardened)
		}
	}
	// And the toolchain of this test is not older than the floor.
	if slices.Contains(onHardened, "gobuild/toolchain") {
		t.Errorf("the toolchain rule fired on a build by the toolchain running this test: %v", onHardened)
	}

	if _, err := OpenBinary(filepath.Join(dir, "main.go")); err == nil {
		t.Error("a file that is not a Go binary was read as one")
	}
}
