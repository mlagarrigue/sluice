package sluice_test

import (
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// experimentalSymbols is the list scripts/check-api.sh reads: the exported
// symbols of stable packages that are experimental themselves, in apidiff's
// spelling (`net/httpstream.(*Server).ServeQUIC`).
const experimentalSymbols = "scripts/api-experimental.txt"

// A stable package may carry a few experimental symbols — the HTTP/3 entry
// points of net/httpstream take net/quic's types, and net/quic is
// experimental (ADR 0021). Each one says so in a paragraph of its
// documentation that starts with "Experimental:", and scripts/check-api.sh
// exempts exactly the ones scripts/api-experimental.txt lists from the v1
// promise (ADR 0020). This test holds the list and the documentation to
// each other, both ways: a listed symbol that lost the paragraph would be
// exempt without saying so, and a marked symbol missing from the list would
// claim an exemption the script does not give.
func TestExperimentalSymbolsMatchTheirList(t *testing.T) {
	module, docs := modulePackages(t)
	b, err := os.ReadFile(experimentalSymbols)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for line := range strings.SplitSeq(string(b), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			listed = append(listed, line)
		}
	}
	var marked []string
	for path, synopsis := range docs {
		if strings.Contains(strings.ToLower(synopsis), "experiment") {
			continue // the whole package is outside the promise
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(path, module), "/")
		marked = append(marked, markedSymbols(t, rel)...)
	}
	slices.Sort(listed)
	slices.Sort(marked)
	if !slices.Equal(listed, marked) {
		t.Errorf("%s lists %q;\nthe symbols whose documentation has an \"Experimental:\" paragraph are %q", experimentalSymbols, listed, marked)
	}
	if len(marked) == 0 {
		t.Error("no symbol is marked experimental: the HTTP/3 entry points of net/httpstream should be")
	}
}

// markedSymbols returns, in apidiff's spelling, the exported symbols of the
// package in dir whose documentation has a paragraph starting with
// "Experimental:".
func markedSymbols(t *testing.T, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	matches, err := filepath.Glob(filepath.Join(filepath.FromSlash(dir+"/"), "*.go"))
	if dir == "" {
		matches, err = filepath.Glob("*.go")
	}
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, m := range matches {
		if strings.HasSuffix(m, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, m, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		return nil
	}
	p, err := doc.NewFromFiles(fset, files, dir)
	if err != nil {
		t.Fatal(err)
	}
	isMarked := func(text string) bool {
		for para := range strings.SplitSeq(text, "\n\n") {
			if strings.HasPrefix(strings.TrimSpace(para), "Experimental:") {
				return true
			}
		}
		return false
	}
	var out []string
	add := func(name string) { out = append(out, dir+"."+name) }
	for _, f := range p.Funcs {
		if isMarked(f.Doc) {
			add(f.Name)
		}
	}
	for _, typ := range p.Types {
		if isMarked(typ.Doc) {
			add(typ.Name)
		}
		for _, f := range typ.Funcs {
			if isMarked(f.Doc) {
				add(f.Name)
			}
		}
		for _, m := range typ.Methods {
			if !isMarked(m.Doc) {
				continue
			}
			if strings.HasPrefix(m.Recv, "*") {
				add("(*" + typ.Name + ")." + m.Name)
			} else {
				add(typ.Name + "." + m.Name)
			}
		}
	}
	return out
}
