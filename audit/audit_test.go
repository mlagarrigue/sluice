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

	// A target that really holds something: walking the combinations
	// against an empty one would check the properties below on zero
	// findings, which is a green on nothing. The module is left out so
	// that no combination calls an external tool a thousand times; the
	// advisory rules have their own test.
	dir := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":     "module probe\n\ngo 1.26.0\n",
		"main.go":    "package main\n\nfunc main() {}\n",
		"Dockerfile": "FROM golang:latest\nENV API_TOKEN=s3cret\nRUN true\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bin := filepath.Join(dir, "probe")
	build(t, dir, bin)
	target, err := Open(Spec{Binary: bin, Dockerfile: filepath.Join(dir, "Dockerfile")})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	combinations := 0
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
			combinations++
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

	// Pointed at nothing, every rule stays silent: a rule that reports on
	// a source it was not given reports on something it never read. Two
	// of them used to, and were caught by reading a run's output rather
	// than by this test, which is why the assertion is here now.
	findings, err := Run(Selection{}, nil, &Target{})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Errorf("a target holding nothing produced %d findings: %+v", len(findings), findings)
	}
	if combinations != total {
		t.Errorf("%d combinations walked, want the whole product of %d", combinations, total)
	}
	// The walk must have exercised the families the target carries, or the
	// properties above were checked on an empty report. By family rather
	// than by rule: a rule is allowed to stay silent on a healthy target,
	// and gobuild/toolchain does — this binary was built by the toolchain
	// running the test.
	families := map[string]bool{}
	for name := range seen {
		family, _, _ := strings.Cut(name, "/")
		families[family] = true
	}
	for _, want := range []string{"gobuild", "docker"} {
		if !families[want] {
			t.Errorf("no rule of %s produced anything in any of the %d combinations", want, total)
		}
	}
	// And every registered rule is reachable: the selection made of a
	// rule's own needs must cover it, or the rule can never run at all.
	for name, r := range byName {
		if !r.Needs.Covers(r.Needs) {
			t.Errorf("%s declares needs no selection can cover: %v", name, r.Needs)
		}
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

	lax := filepath.Join(dir, "lax")
	build(t, dir, lax, "CGO_ENABLED=1", "GOAMD64=v1", "GOFLAGS=")
	hardened := filepath.Join(dir, "hardened")
	build(t, dir, hardened, "-trimpath", "CGO_ENABLED=0", "GOAMD64=v3", "GOFLAGS=")

	rules := func(path string) []string {
		t.Helper()
		target, err := Open(Spec{Binary: path})
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

	if _, err := Open(Spec{Binary: filepath.Join(dir, "main.go")}); err == nil {
		t.Error("a file that is not a Go binary was read as one")
	}
}

// build compiles the module in dir to out. An argument starting with a
// dash goes to go build, anything else is an environment variable: the
// settings a build fixes are exactly what these tests vary.
func build(t *testing.T, dir, out string, args ...string) {
	t.Helper()
	flags := []string{"build", "-o", out}
	var env []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			continue
		}
		env = append(env, a)
	}
	cmd := exec.Command("go", append(flags, ".")...) //nolint:gosec // G204: the arguments are this test's own
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %v: %v\n%s", flags, err, b)
	}
}
