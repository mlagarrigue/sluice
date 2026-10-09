package sluice_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// One production dependency is the project's founding constraint — the
// standard library plus golang.org/x/crypto, for the ChaCha20-Poly1305 cipher
// crypto/tls negotiates but does not export — and CI checks it two ways:
// go.mod requiring nothing outside that allowlist, and `go list -deps ./...`
// finding no package from anywhere else in the graph.
//
// This test is the third way, and it exists because of the second module. The
// comparative benchmarks on the benchmarks branch pin pgx, gin, echo, chi and fiber —
// fifty-odd modules of transitive graph — and the whole arrangement rests on
// that graph never crossing into this one. CI catches the crossing on push;
// this catches it on `go test`, which is where someone actually is when they
// make it.
//
// golang.org/x/sys is on the list because x/crypto brings it (CPU feature
// detection for the assembly), not because anything here imports it.
func TestGoModRequiresOnlyTheAllowlist(t *testing.T) {
	src, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	allowed := map[string]bool{
		"golang.org/x/crypto": true,
		"golang.org/x/sys":    true,
	}
	seen := map[string]bool{}
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
		mod := f[0]
		seen[mod] = true
		if !allowed[mod] {
			t.Errorf("go.mod:%d requires %q.\n"+
				"the recorded dependencies are golang.org/x/crypto (and the x/sys it brings). "+
				"If this came from measuring against another library, the measurement "+
				"belongs in the comparison module on the benchmarks branch, which depends on this one and is "+
				"never depended on by it.", lineNo, mod)
		}
	}
	if !seen["golang.org/x/crypto"] {
		t.Error("go.mod no longer requires golang.org/x/crypto; quic needs its ChaCha20-Poly1305")
	}
}

// The comparison module (benchmarks/, on the benchmarks branch) has to keep
// pointing at this tree by a replace directive rather than at a published version. Without it the comparison
// silently measures whatever was last tagged, which is not what the person
// running it is trying to find out.
func TestBenchmarksModuleReplacesThisOne(t *testing.T) {
	src, err := os.ReadFile("benchmarks/go.mod")
	if err != nil {
		t.Skipf("no benchmarks module here: %v", err)
	}
	if !strings.Contains(string(src), "replace github.com/mlagarrigue/sluice => ../") {
		t.Error("benchmarks/go.mod no longer replaces the root module with this tree, " +
			"so it is measuring a published version rather than the working one")
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

	if got, want := majorMinor(string(declared[1])), majorMinor(string(matrix[1])); got != want {
		t.Errorf("go.mod declares go %s but CI builds the oldest supported version as %s.\n"+
			"If the bump was deliberate, move the CI matrix with it. If it appeared on its own, "+
			"a dependency that needs a newer Go was resolved — golang.org/x/crypto raises its go line "+
			"with each Go release — and the choice between pinning it and dropping the older Go is a decision, "+
			"not a side effect.", got, want)
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
