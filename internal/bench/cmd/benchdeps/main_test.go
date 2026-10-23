package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const module = "example.com/m"

// The package below spreads what a benchmark reaches over files with
// different imports, the shapes the gate has to read right.
var files = map[string]string{
	"helpers_test.go": `package bench

import "example.com/m/x"

func viaX() { x.Do() }
`,
	"types_test.go": `package bench

import (
	"fmt"

	"example.com/m/y"
	zz "example.com/m/z"
)

type T struct{}

func (T) m() { y.Do() }

type G[K any] struct{}

func (g *G[K]) m() { zz.Do(); fmt.Println() }
`,
	"bench_test.go": `package bench

import "testing"

func local() int { return 1 }

var shared = local()

func BenchmarkLocal(b *testing.B)  { _ = local() + shared }
func BenchmarkHelper(b *testing.B) { viaX() }
func BenchmarkMethod(b *testing.B) { var t T; t.m() }
func BenchmarkGeneric(b *testing.B) { var g G[int]; g.m() }
`,
	"direct_test.go": `package bench

import (
	"testing"

	"example.com/m"
	"example.com/m/x"
	"example.com/other"
)

func BenchmarkDirect(b *testing.B) { m.Do(); other.Do() }
func BenchmarkUnused(b *testing.B) { _ = 1 }
var _ = x.Do
`,
}

func TestReach(t *testing.T) {
	dir := t.TempDir()
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		benchmark string
		want      []string
	}{
		// Helpers of its own file only: no package of the module, the
		// shape of BenchmarkSeqStages.
		{"BenchmarkLocal", nil},
		// A helper in a file that imports what the benchmark's file does not.
		{"BenchmarkHelper", []string{"example.com/m/x"}},
		// A method of a type the benchmark names, declared in another file.
		{"BenchmarkMethod", []string{"example.com/m/y"}},
		// The same with a generic receiver and a renamed import; fmt is not
		// the module's.
		{"BenchmarkGeneric", []string{"example.com/m/z"}},
		// The module's root package; another module's is not printed.
		{"BenchmarkDirect", []string{"example.com/m"}},
		// What its file imports for another declaration is not reached.
		{"BenchmarkUnused", nil},
	} {
		got, err := p.reach(tc.benchmark, module)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s reaches %q, want %q", tc.benchmark, got, tc.want)
		}
	}
	if _, err := p.reach("BenchmarkMissing", module); err == nil {
		t.Error("a benchmark the package does not declare was accepted")
	}
}

// The case that made the gate read the code: BenchmarkSeqStages builds its
// stages from helpers of its own file and reaches nothing of the module, so a
// diff to database/postgres alone (1a71025) must not measure it; the
// connector's read loop reaches the connector.
func TestReachInternalBench(t *testing.T) {
	p, err := load("../..")
	if err != nil {
		t.Fatal(err)
	}
	const sluice = "github.com/mlagarrigue/sluice"
	for _, name := range []string{"BenchmarkSeqStages", "BenchmarkSelectiveFilter", "BenchmarkWebHandwritten"} {
		got, err := p.reach(name, sluice)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("%s reaches %q, want nothing of the module", name, got)
		}
	}
	got, err := p.reach("BenchmarkPGReadRows", sluice)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got, sluice+"/database/postgres") {
		t.Errorf("BenchmarkPGReadRows reaches %q, want database/postgres among them", got)
	}
}
