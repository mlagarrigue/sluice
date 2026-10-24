package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// What the build was given: the module's requirements, and the known
// vulnerabilities among them.
//
// The vulnerabilities are read by calling govulncheck rather than by
// reimplementing it. The database, the call-graph analysis and the
// advisory format are the Go team's, maintained daily; a second
// implementation here would be a second thing to be wrong, and wrong
// quietly. That is why "tools" is a value of the sources axis: a run that
// does not allow it gets no verdict rather than a guess.
//
// Each advisory is given one of three verdicts, and never any other:
//
//   - applies — the analysis reached the vulnerable function from this
//     module's own code. The trace names it.
//   - undetermined — the vulnerable package is linked, but no call path to
//     the vulnerable symbol was proved. It may be reachable through
//     reflection or a path the analysis cannot follow; a reader has to
//     decide, and the report says what is known.
//   - does not apply — the module is required, and nothing of it is
//     linked or called.
//
// A verdict with nothing behind it would be worse than none, so each one
// says what decided it.
const (
	VerdictApplies      = "applies"
	VerdictUndetermined = "undetermined"
	VerdictAbsent       = "does not apply"
)

// vulnFinding is one advisory as govulncheck reported it, already judged.
type vulnFinding struct {
	ID       string
	Summary  string
	Fixed    string
	Module   string
	Version  string
	Verdict  string
	Because  string
	Function string
}

// govulncheckJSON is the shape this reads of govulncheck's stream: the
// advisories it loaded, and the findings it reached. Everything else of
// the protocol is skipped on purpose — a field added upstream must not
// break the reading.
type govulncheckJSON struct {
	OSV *struct {
		ID       string   `json:"id"`
		Summary  string   `json:"summary"`
		Aliases  []string `json:"aliases"`
		Affected []struct {
			Package struct {
				Name string `json:"name"`
			} `json:"package"`
		} `json:"affected"`
	} `json:"osv"`
	Finding *struct {
		OSV          string `json:"osv"`
		FixedVersion string `json:"fixed_version"`
		Trace        []struct {
			Module   string `json:"module"`
			Version  string `json:"version"`
			Package  string `json:"package"`
			Function string `json:"function"`
		} `json:"trace"`
	} `json:"finding"`
}

// vulnerabilities runs govulncheck on a module and judges what it found.
// A missing govulncheck is not a failure of the audit: it is a source the
// machine does not have, and it says so.
func vulnerabilities(dir string) ([]vulnFinding, error) {
	bin, err := exec.LookPath("govulncheck")
	if err != nil {
		return nil, fmt.Errorf("govulncheck is not installed (go install golang.org/x/vuln/cmd/govulncheck@latest): %w", err)
	}
	cmd := exec.Command(bin, "-json", "./...") //nolint:gosec // G204: the binary is looked up, the arguments are fixed
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("govulncheck in %s: %w: %s", dir, err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("govulncheck in %s: %w", dir, err)
	}
	return judgeVulns(out)
}

// judgeVulns reads the stream and applies the three verdicts. It is apart
// from the call so that a recorded stream can be judged in a test without
// a network or a database.
func judgeVulns(stream []byte) ([]vulnFinding, error) {
	type osvEntry struct {
		summary string
		aliases []string
	}
	advisories := map[string]osvEntry{}
	var out []vulnFinding

	dec := json.NewDecoder(strings.NewReader(string(stream)))
	for {
		var msg govulncheckJSON
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("govulncheck's output: %w", err)
		}
		switch {
		case msg.OSV != nil:
			advisories[msg.OSV.ID] = osvEntry{summary: msg.OSV.Summary, aliases: msg.OSV.Aliases}
		case msg.Finding != nil:
			f := msg.Finding
			v := vulnFinding{ID: f.OSV, Fixed: f.FixedVersion}
			if a, ok := advisories[f.OSV]; ok {
				v.Summary = a.summary
				if len(a.aliases) > 0 {
					v.Summary += " (" + strings.Join(a.aliases, ", ") + ")"
				}
			}
			// The trace runs from the vulnerable symbol outwards; its first
			// frame is the most precise thing the analysis proved.
			if len(f.Trace) > 0 {
				top := f.Trace[0]
				v.Module, v.Version, v.Function = top.Module, top.Version, top.Function
				switch {
				case top.Function != "":
					v.Verdict = VerdictApplies
					v.Because = "the analysis reached " + top.Package + "." + top.Function
				case top.Package != "":
					v.Verdict = VerdictUndetermined
					v.Because = "the package " + top.Package + " is linked, but no call path to the vulnerable symbol was proved"
				default:
					v.Verdict = VerdictAbsent
					v.Because = "the module is required, and nothing of it is linked or called"
				}
			}
			if v.Verdict == "" {
				v.Verdict = VerdictUndetermined
				v.Because = "govulncheck reported the advisory with no trace to judge it by"
			}
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b vulnFinding) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// requiresSomething reports whether a go.mod has a require directive. The
// line is read, not the file searched: this module's own go.mod says the
// word "requires" in a comment, and a textual search called it a module
// with dependencies — found by reading a run's output, which is the whole
// point of reading one.
func requiresSomething(gomod string) bool {
	for line := range strings.SplitSeq(gomod, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "require" || line == "require(" ||
			strings.HasPrefix(line, "require ") || strings.HasPrefix(line, "require(") {
			return true
		}
	}
	return false
}

func init() {
	Register(Rule{
		Name: "deps/cve",
		Needs: Selection{
			Components: {"go"},
			Goals:      {"security"},
			Sources:    {"tools"},
			CVEs:       {"yes"},
		},
		Doc: "https://go.dev/doc/security/vuln/",
		Check: func(t *Target, _ Answers) []Finding {
			if t.ModuleDir == "" {
				return nil
			}
			found, err := vulnerabilities(t.ModuleDir)
			if err != nil {
				return []Finding{{
					Current:  "the advisories were not read: " + err.Error(),
					Proposed: "install govulncheck, or drop the tools source from the selection",
					Verdict:  VerdictUndetermined,
					Why: "a run that asked for CVEs and could not read them must say so: a report " +
						"silent about vulnerabilities reads as a report that found none",
				}}
			}
			var out []Finding
			for i := range found {
				v := &found[i]
				proposed := "no fix is published yet"
				if v.Fixed != "" {
					proposed = "upgrade " + v.Module + " to " + v.Fixed
				}
				current := v.ID
				if v.Module != "" {
					current += " in " + v.Module + " " + v.Version
				}
				if v.Summary != "" {
					current += ": " + v.Summary
				}
				out = append(out, Finding{
					Current:  current,
					Proposed: proposed,
					Verdict:  v.Verdict,
					Why:      v.Because,
					Doc:      "https://pkg.go.dev/vuln/" + v.ID,
				})
			}
			return out
		},
	})

	Register(Rule{
		Name: "deps/gosum",
		Needs: Selection{
			Components: {"go"},
			Goals:      {"security"},
			Sources:    {"config"},
		},
		Doc: "https://go.dev/ref/mod#go-sum-files",
		Check: func(t *Target, _ Answers) []Finding {
			if t.ModuleDir == "" {
				return nil
			}
			mod, err := os.ReadFile(filepath.Join(t.ModuleDir, "go.mod")) //nolint:gosec // G304: the directory was opened by Open
			if err != nil {
				return nil
			}
			if !requiresSomething(string(mod)) {
				return nil // nothing to check sums of
			}
			if _, err := os.Stat(filepath.Join(t.ModuleDir, "go.sum")); err == nil {
				return nil
			}
			return []Finding{{
				Current:  "go.mod requires modules and there is no go.sum",
				Proposed: "go mod tidy, and commit go.sum",
				Why: "without the sums a build takes whatever the proxy serves: nothing ties the " +
					"source that was reviewed to the source that is compiled, and a replaced " +
					"module version goes unnoticed",
			}}
		},
	})
}
