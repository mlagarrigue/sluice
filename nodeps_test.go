package sluice_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// A short list of production dependencies is the project's founding
// constraint — the standard library plus golang.org/x/crypto, for the
// ChaCha20-Poly1305 cipher crypto/tls negotiates but does not export, and
// golang.org/x/text, for the NFKC normalisation SASLprep needs (ADR 0002) —
// and CI checks it two ways:
// go.mod requiring nothing outside that allowlist, and `go list -deps ./...`
// finding no package from anywhere else in the graph.
//
// This test is the third way, and it exists because of the second module. The
// comparative benchmarks in benchmarks/ pin pgx, gin, echo, chi and fiber —
// fifty-odd modules of transitive graph — and the whole arrangement rests on
// that graph never crossing into this one. CI catches the crossing on push;
// this catches it on `go test`, which is where someone actually is when they
// make it.
//
// golang.org/x/sys is on the list because x/crypto brings it (CPU feature
// detection for the assembly), not because anything here imports it.
func TestGoModRequiresOnlyTheAllowlist(t *testing.T) {
	allowed := map[string]bool{
		"golang.org/x/crypto": true,
		"golang.org/x/sys":    true,
		"golang.org/x/text":   true,
	}
	seen := map[string]bool{}
	for _, r := range goModRequires(t) {
		seen[r.mod] = true
		if !allowed[r.mod] {
			t.Errorf("go.mod:%d requires %q.\n"+
				"the recorded dependencies are golang.org/x/crypto (and the x/sys it brings) "+
				"and golang.org/x/text. "+
				"If this came from measuring against another library, the measurement "+
				"belongs in the comparison module in benchmarks/, which depends on this one and is "+
				"never depended on by it.", r.line, r.mod)
		}
	}
	if !seen["golang.org/x/crypto"] {
		t.Error("go.mod no longer requires golang.org/x/crypto; quic needs its ChaCha20-Poly1305")
	}
}

// The root package's doc is the first page pkg.go.dev shows, and it states
// the dependency graph: every module go.mod requires directly must be named
// there. It named golang.org/x/crypto alone for a release after golang.org/x/text
// had joined, beside the test above that listed both.
func TestPackageDocNamesEveryDirectDependency(t *testing.T) {
	src, err := os.ReadFile("sluice.go")
	if err != nil {
		t.Fatal(err)
	}
	doc, _, ok := strings.Cut(string(src), "\npackage sluice")
	if !ok {
		t.Fatal("sluice.go has no package clause")
	}
	for _, r := range goModRequires(t) {
		if !r.indirect && !strings.Contains(doc, r.mod) {
			t.Errorf("go.mod:%d requires %s, which the package doc in sluice.go does not name", r.line, r.mod)
		}
	}
}

// requirement is a module go.mod requires.
type requirement struct {
	line     int
	mod      string
	indirect bool
}

// goModRequires reads the require directives of go.mod, single-line and
// block alike.
func goModRequires(t *testing.T) []requirement {
	t.Helper()
	src, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	var out []requirement
	inBlock := false
	lineNo := 0
	for line := range strings.Lines(string(src)) {
		lineNo++
		f := strings.Fields(line)
		switch {
		case len(f) == 0:
			continue
		case f[0] == "require" && len(f) == 2 && f[1] == "(":
			inBlock = true
			continue
		case f[0] == ")":
			inBlock = false
			continue
		case f[0] == "require":
			f = f[1:]
		case !inBlock:
			continue
		}
		out = append(out, requirement{line: lineNo, mod: f[0], indirect: strings.Contains(line, "// indirect")})
	}
	return out
}

// The nested modules — the comparison module (benchmarks/) and the interop
// module (interop/) — have to keep pointing at this tree by a replace
// directive rather than at a published version. Without it the comparison
// silently measures whatever was last tagged, and the interop test silently
// proves a quic-go handshake against it, which is not what the person running
// either is trying to find out. A missing module is a failure, not a skip: a
// skip would turn green the day one of them moved away.
func TestNestedModulesReplaceThisOne(t *testing.T) {
	for _, dir := range []string{"benchmarks", "interop"} {
		src, err := os.ReadFile(dir + "/go.mod") //nolint:gosec // G304: the two paths are the literals above
		if err != nil {
			t.Errorf("no %s module here: %v", dir, err)
			continue
		}
		if !strings.Contains(string(src), "replace github.com/mlagarrigue/sluice => ../") {
			t.Errorf("%s/go.mod no longer replaces the root module with this tree, "+
				"so it is exercising a published version rather than the working one", dir)
		}
	}
}

// The same toolchain setting rewrites the `go` line when a dependency needs a
// newer one — pgx pulled it from 1.24 to 1.25.0 in one silent step while the
// benchmarks module was being wired, and a golang.org/x/crypto bump past
// the newest release that still builds on the floor would do the same. That matters because
// CI builds this module on the oldest version it promises, so a bumped line
// turns into a build failure on a runner rather than on the machine that
// caused it.
//
// Compared as major.minor: `go mod tidy` writes the patch level (1.26.0) once
// a dependency's own go line carries one, and the CI matrix names releases
// without it.
func TestGoModVersionMatchesTheOldestCIBuild(t *testing.T) {
	gomod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	declared := regexp.MustCompile(`(?m)^go (\S+)`).FindSubmatch(gomod)
	if declared == nil {
		t.Fatal("go.mod declares no go version")
	}

	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Skipf("no CI workflow to compare against: %v", err)
	}
	matrix := regexp.MustCompile(`go: \['([^']+)'`).FindSubmatch(workflow)
	if matrix == nil {
		// A failure, not a skip: a skip here would turn green forever the day
		// the workflow is reshaped, which is exactly when this check matters.
		t.Fatal("could not find the go version matrix in .github/workflows/ci.yml — " +
			"if the workflow changed shape, update this test's regexp with it")
	}

	floor := majorMinor(string(declared[1]))
	if want := majorMinor(string(matrix[1])); floor != want {
		t.Errorf("go.mod declares go %s but CI builds the oldest supported version as %s.\n"+
			"If the bump was deliberate, move the CI matrix with it. If it appeared on its own, "+
			"a dependency that needs a newer Go was resolved — golang.org/x/crypto raises its go line "+
			"with each Go release — and the choice between pinning it and dropping the older Go is a decision, "+
			"not a side effect.", floor, want)
	}

	// The format and lint jobs pin a release by hand (their linters are built
	// with it); 'stable' legs are the other end of the range and are not
	// floors. Every literal pin is the floor or the matrix lies about it.
	for _, pin := range regexp.MustCompile(`go-version: '(\d[^']*)'`).FindAllSubmatch(workflow, -1) {
		if got := majorMinor(string(pin[1])); got != floor {
			t.Errorf("ci.yml pins go-version %s in a job while the floor is %s; move them together", got, floor)
		}
	}

	// The floor is published for contributors; the sentence must name the
	// same release the build enforces, or the policy is only stated.
	contributing, err := os.ReadFile("CONTRIBUTING.md")
	if err != nil {
		t.Fatalf("reading CONTRIBUTING.md: %v", err)
	}
	if want := "Go " + floor + " today"; !strings.Contains(string(contributing), want) {
		t.Errorf("CONTRIBUTING.md does not say %q under \"Releases and versions\"; the published floor must follow go.mod", want)
	}
	started, err := os.ReadFile("docs/guide/getting-started.md")
	if err != nil {
		t.Fatalf("reading docs/guide/getting-started.md: %v", err)
	}
	if want := "Go " + floor + " or newer"; !strings.Contains(string(started), want) {
		t.Errorf("docs/guide/getting-started.md does not say %q next to go get; the published floor must follow go.mod", want)
	}
}

func majorMinor(v string) string {
	if i := strings.Index(v, "."); i >= 0 {
		if j := strings.Index(v[i+1:], "."); j >= 0 {
			return v[:i+1+j]
		}
	}
	return v
}

// ciPromises lists what the repository's rules promise CI runs, which no
// other test would notice going missing: the latest stable toolchain beside
// the floor, the UDP code on Windows and macOS (test-udp), and the stage
// budget's absolute ceiling, which TestStageBudget skips under -race and so
// holds only in a step without it. It returns one message per broken
// promise.
func ciPromises(workflow string) []string {
	var broken []string
	job := func(name string) string {
		i := strings.Index(workflow, "\n  "+name+":\n")
		if i < 0 {
			return ""
		}
		rest := workflow[i+len(name)+4:]
		if j := regexp.MustCompile(`\n {2}[a-z][a-z0-9-]*:\n`).FindStringIndex(rest); j != nil {
			rest = rest[:j[0]]
		}
		return rest
	}
	if !regexp.MustCompile(`(?m)^\s+go: \[[^\]]*'stable'`).MatchString(job("test")) {
		broken = append(broken, "the test job no longer runs the latest stable Go beside the floor")
	}
	udp := job("test-udp")
	for _, platform := range []string{"windows-latest", "macos-latest"} {
		if !regexp.MustCompile(`(?m)^\s+os: \[[^\]]*` + platform).MatchString(udp) {
			broken = append(broken, "test-udp no longer runs on "+platform)
		}
	}
	budget := false
	for line := range strings.SplitSeq(job("test"), "\n") {
		if strings.Contains(line, "run:") && strings.Contains(line, "TestStageBudget") && !strings.Contains(line, "-race") {
			budget = true
		}
	}
	if !budget {
		broken = append(broken, "no step of the test job runs TestStageBudget without -race, where its ceiling is the only one played")
	}
	return broken
}

// The rules promise CI three things nothing else checks. Each promise is
// held here, and each is cut from a copy of the workflow to prove the check
// sees it go: a check that stays green on a broken workflow is no check.
func TestCIKeepsItsPromises(t *testing.T) {
	src, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("reading the CI workflow: %v", err)
	}
	workflow := string(src)
	for _, msg := range ciPromises(workflow) {
		t.Error(msg)
	}
	for _, cut := range []struct{ old, new string }{
		{"go: ['1.26', 'stable']", "go: ['1.26']"},
		{"os: [windows-latest, macos-latest,", "os: [windows-latest,"},
		{"os: [windows-latest, macos-latest,", "os: [macos-latest,"},
		{"run: go test -run 'TestStageBudget", "run: go test -race -run 'TestStageBudget"},
	} {
		if !strings.Contains(workflow, cut.old) {
			t.Errorf("the workflow no longer contains %q: update this test's amputations with it", cut.old)
			continue
		}
		if len(ciPromises(strings.Replace(workflow, cut.old, cut.new, 1))) == 0 {
			t.Errorf("replacing %q with %q broke a promise and nothing noticed", cut.old, cut.new)
		}
	}
}
