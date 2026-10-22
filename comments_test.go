package sluice_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The comments of the code are read by people who have the tree and nothing
// else, and three ways they went wrong were each found by a review reading
// every file: a declaration's doc that names another declaration, a comment
// that sends the reader to a working document the tree does not hold, and
// one that tells what the code used to do instead of what it does. A script
// finds all three; these tests are that script.

// goSource is one parsed Go file of the tree.
type goSource struct {
	path      string
	fset      *token.FileSet
	file      *ast.File
	test      bool // a _test.go file
	nested    bool // in a nested module: benchmarks/, interop/, comparison/
	generated bool
}

// goSources parses every Go file of the tree but testdata and the working
// material, comments included.
func goSources(t *testing.T) []goSource {
	t.Helper()
	var out []goSource
	err := filepath.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != "." && (strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		top := strings.SplitN(filepath.ToSlash(p), "/", 2)[0]
		out = append(out, goSource{
			path:      p,
			fset:      fset,
			file:      f,
			test:      strings.HasSuffix(p, "_test.go"),
			nested:    top == "benchmarks" || top == "interop" || top == "comparison",
			generated: ast.IsGenerated(f),
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 100 {
		t.Fatalf("parsed %d Go files; the walk is not reaching the tree", len(out))
	}
	return out
}

// The doc comment of an exported declaration begins with the name it
// documents — after an article, or with the parentheses of a call — the
// convention go doc and pkg.go.dev read. One that begins with another name
// documents the wrong thing: pgwire's MaxMessage carried SetMaxMessage's
// doc, and SetMaxMessage none.
func TestDocComments(t *testing.T) {
	for _, src := range goSources(t) {
		if src.test || src.nested || src.generated {
			continue
		}
		check := func(name string, doc *ast.CommentGroup, pos token.Pos) {
			if doc == nil || !ast.IsExported(name) {
				return
			}
			words := strings.Fields(doc.Text())
			if len(words) == 0 {
				return
			}
			first := words[0]
			if (first == "A" || first == "An" || first == "The") && len(words) > 1 {
				first = words[1]
			}
			first = strings.TrimRight(strings.TrimSuffix(strings.TrimSuffix(first, "'s"), "()"), ".,:;")
			if first != name && first != "Deprecated" {
				t.Errorf("%s: the doc of %s begins with %q", src.fset.Position(pos), name, words[0])
			}
		}
		for _, decl := range src.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				check(d.Name.Name, d.Doc, d.Pos())
			case *ast.GenDecl:
				if len(d.Specs) != 1 {
					continue // a grouped declaration's doc speaks for the group
				}
				switch s := d.Specs[0].(type) {
				case *ast.TypeSpec:
					check(s.Name.Name, d.Doc, d.Pos())
				case *ast.ValueSpec:
					if len(s.Names) == 1 {
						check(s.Names[0].Name, d.Doc, d.Pos())
					}
				}
			}
		}
	}
}

// processMarker is a working document named in a comment — the task
// description a change was made from, the list of work left, a review's
// report, the design notes, a user's report. None of them is in the
// repository, so the comment sends its reader nowhere. The RFCs are "the
// RFC"; a queue is a queue.
var processMarker = regexp.MustCompile(`(?i)\b(the brief|brief's|backlog|the audit|audit pass|audit's|the spec|field report|field-report)\b`)

// historyMarker is a production comment telling what the code used to do.
// The rule it carries is stated in the present; what it replaced is in the
// history and in the regression test. "Is used to", "never used to" and
// ", used to" are the verb's other sense.
var historyMarker = regexp.MustCompile(`(?i)\bused to\b|\bformerly\b|\bbefore removal\b`)

var usedToOtherSense = regexp.MustCompile(`(?i)\b(is|are|be|was|were|being|never|not|only)\s+used to\b|,\s*used to\b`)

func TestCommentMarkers(t *testing.T) {
	for _, src := range goSources(t) {
		if src.generated || strings.HasPrefix(filepath.ToSlash(src.path), "comparison/") {
			continue
		}
		for _, group := range src.file.Comments {
			for _, c := range group.List {
				pos := src.fset.Position(c.Pos())
				if m := processMarker.FindString(c.Text); m != "" {
					t.Errorf("%s: the comment cites %q, which is not in the repository", pos, m)
				}
				if src.test || src.nested {
					continue
				}
				if historyMarker.MatchString(c.Text) && !usedToOtherSense.MatchString(c.Text) {
					t.Errorf("%s: the comment tells the code's history; state the rule it holds: %s", pos, strings.TrimSpace(c.Text))
				}
			}
		}
	}
}

// adrCitation is a decision record named by number.
var adrCitation = regexp.MustCompile(`\bADR (\d{4})\b`)

// Every "ADR NNNN" in a comment or a document names a record that exists. It
// cannot tell a record cited for the wrong decision from the right one —
// that is a reader's judgment — but a number off the end of the list, or one
// left behind by a renumbering, it can.
func TestADRCitations(t *testing.T) {
	files, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	exists := map[string]bool{}
	for _, f := range files {
		exists[filepath.Base(f)[:4]] = true
	}
	cite := func(where, text string) {
		for _, m := range adrCitation.FindAllStringSubmatch(text, -1) {
			if !exists[m[1]] {
				t.Errorf("%s: cites ADR %s, which docs/design/adr/ does not hold", where, m[1])
			}
		}
	}
	for _, src := range goSources(t) {
		for _, group := range src.file.Comments {
			cite(src.fset.Position(group.Pos()).String(), group.Text())
		}
	}
	for _, f := range markdownFiles(t) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		cite(f, string(b))
	}
}
