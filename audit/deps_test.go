package audit

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The three verdicts are judged from govulncheck's own stream, so they are
// checked against recorded streams: a test that called the real tool would
// depend on a database that changes daily, and would say nothing about the
// mapping, which is what this module decides. The call itself is checked
// once, below, when the tool is installed.

func TestVerdictsFollowWhatTheAnalysisProved(t *testing.T) {
	const applies = `{"osv":{"schema_version":"1.3.1","id":"GO-2024-0001","summary":"Something in a parser","aliases":["CVE-2024-0001"]}}
{"finding":{"osv":"GO-2024-0001","fixed_version":"v1.2.3","trace":[{"module":"example.com/x","version":"v1.2.0","package":"example.com/x/parse","function":"Parse"}]}}`

	const undetermined = `{"osv":{"id":"GO-2024-0002","summary":"Something in a codec"}}
{"finding":{"osv":"GO-2024-0002","fixed_version":"v2.0.0","trace":[{"module":"example.com/y","version":"v1.0.0","package":"example.com/y/codec"}]}}`

	const absent = `{"osv":{"id":"GO-2024-0003","summary":"Something in a module nobody calls"}}
{"finding":{"osv":"GO-2024-0003","fixed_version":"v3.1.0","trace":[{"module":"example.com/z","version":"v3.0.0"}]}}`

	const noTrace = `{"osv":{"id":"GO-2024-0004","summary":"Reported with nothing to judge it by"}}
{"finding":{"osv":"GO-2024-0004"}}`

	for _, tc := range []struct {
		name    string
		stream  string
		verdict string
		because string
	}{
		{"a function is reached", applies, VerdictApplies, "example.com/x/parse.Parse"},
		{"a package is linked", undetermined, VerdictUndetermined, "no call path"},
		{"only required", absent, VerdictAbsent, "nothing of it is linked"},
		{"no trace at all", noTrace, VerdictUndetermined, "no trace to judge it by"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeVulns([]byte(tc.stream))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("%d findings, want 1: %+v", len(got), got)
			}
			if got[0].Verdict != tc.verdict {
				t.Errorf("verdict %q, want %q", got[0].Verdict, tc.verdict)
			}
			if !strings.Contains(got[0].Because, tc.because) {
				t.Errorf("the reason %q does not say %q", got[0].Because, tc.because)
			}
			if got[0].Verdict != VerdictApplies && got[0].Verdict != VerdictUndetermined && got[0].Verdict != VerdictAbsent {
				t.Errorf("verdict %q is outside the closed list", got[0].Verdict)
			}
		})
	}

	// The same advisory reported at three levels is one line, at the
	// strongest verdict: govulncheck emits a finding per level it reached,
	// and judging one thing three ways leaves the reader to pick.
	threeLevels := strings.Join([]string{
		`{"osv":{"id":"GO-2024-0005","summary":"One advisory, three traces"}}`,
		`{"finding":{"osv":"GO-2024-0005","fixed_version":"v1.1.0","trace":[{"module":"example.com/w","version":"v1.0.0"}]}}`,
		`{"finding":{"osv":"GO-2024-0005","fixed_version":"v1.1.0","trace":[{"module":"example.com/w","version":"v1.0.0","package":"example.com/w/p"}]}}`,
		`{"finding":{"osv":"GO-2024-0005","fixed_version":"v1.1.0","trace":[{"module":"example.com/w","version":"v1.0.0","package":"example.com/w/p","function":"Do"}]}}`,
	}, "\n")
	judged, err := judgeVulns([]byte(threeLevels))
	if err != nil {
		t.Fatal(err)
	}
	if len(judged) != 1 || judged[0].Verdict != VerdictApplies {
		t.Fatalf("%+v: want one line at %q", judged, VerdictApplies)
	}
	if !strings.Contains(judged[0].Because, "example.com/w/p.Do") {
		t.Errorf("the strongest verdict lost what proved it: %q", judged[0].Because)
	}

	// An advisory the stream loaded but never reported is not a finding:
	// the database holds hundreds of them, and a report that listed every
	// one as "does not apply" would bury the three that matter.
	loaded, err := judgeVulns([]byte(`{"osv":{"id":"GO-2024-0009","summary":"Loaded, never reported"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Errorf("an advisory with no finding was reported: %+v", loaded)
	}
}

// TestEveryCVEFindingCarriesItsVerdictAndAdvisory: the rule turns each
// judged advisory into a recommendation whose five fields and verdict are
// all there, and whose link is the advisory's own page.
func TestEveryCVEFindingCarriesItsVerdictAndAdvisory(t *testing.T) {
	if _, err := exec.LookPath("govulncheck"); err != nil {
		t.Skip("govulncheck is not installed")
	}
	// This module itself: it requires nothing, so whatever is reported is
	// the standard library's, and the verdict must still be one of three.
	target, err := Open(Spec{Module: "."})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"go"}, Goals: {"security"}, Sources: {"tools"}, CVEs: {"yes"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Rule != "deps/cve" {
			t.Errorf("a rule other than deps/cve ran: %s", f.Rule)
		}
		if !slices.Contains([]string{VerdictApplies, VerdictUndetermined, VerdictAbsent}, f.Verdict) {
			t.Errorf("%s: verdict %q is outside the closed list", f.Current, f.Verdict)
		}
		if f.Why == "" {
			t.Errorf("%s: a verdict with nothing behind it", f.Current)
		}
		if !strings.HasPrefix(f.Doc, "https://pkg.go.dev/vuln/GO-") {
			t.Errorf("%s: the link %q is not the advisory's own page", f.Current, f.Doc)
		}
	}
	// And the same selection without the tools source reads nothing at all.
	quiet, err := Run(Selection{Components: {"go"}, Goals: {"security"}, Sources: {"config"}, CVEs: {"yes"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range quiet {
		if f.Rule == "deps/cve" {
			t.Error("the advisories were read although the tools source was not selected")
		}
	}
}

// TestMissingGovulncheckIsSaid: a machine without the tool must not
// produce a report that reads as "no vulnerability".
func TestMissingGovulncheckIsSaid(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	target, err := Open(Spec{Module: "."})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"go"}, Goals: {"security"}, Sources: {"tools"}, CVEs: {"yes"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Verdict != VerdictUndetermined ||
		!strings.Contains(findings[0].Current, "not read") {
		t.Fatalf("with no govulncheck on the path: %+v", findings)
	}
}

// TestGoSumIsRequiredWhenSomethingIsRequired: a module with requirements
// and no sums is a build that takes what it is served.
func TestGoSumIsRequiredWhenSomethingIsRequired(t *testing.T) {
	sel := Selection{Components: {"go"}, Goals: {"security"}, Sources: {"config"}}
	fires := func(t *testing.T, gomod, gosum string) bool {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o600); err != nil {
			t.Fatal(err)
		}
		if gosum != "" {
			if err := os.WriteFile(filepath.Join(dir, "go.sum"), []byte(gosum), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		target, err := Open(Spec{Module: dir})
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(sel, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range findings {
			if f.Rule == "deps/gosum" {
				return true
			}
		}
		return false
	}
	const withRequire = "module probe\n\ngo 1.26.0\n\nrequire example.com/x v1.0.0\n"
	if !fires(t, withRequire, "") {
		t.Error("a module that requires something without go.sum passed")
	}
	if fires(t, withRequire, "example.com/x v1.0.0 h1:…\n") {
		t.Error("a module with its sums was reported anyway")
	}
	if fires(t, "module probe\n\ngo 1.26.0\n", "") {
		t.Error("a module that requires nothing was asked for sums")
	}
	// The word in a comment is not a directive: this module's own go.mod
	// says "requires" in its header, and a textual search read it as a
	// dependency.
	if fires(t, "// nothing it requires may widen the graph\nmodule probe\n\ngo 1.26.0\n", "") {
		t.Error("the word require in a comment was read as a directive")
	}
	if !fires(t, "module probe\n\ngo 1.26.0\n\nrequire (\n\texample.com/x v1.0.0\n)\n", "") {
		t.Error("a require block was not read as a directive")
	}
}
