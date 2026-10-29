package sluice_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"
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
			if doc == nil {
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
			if !ast.IsExported(name) {
				// An unexported declaration's doc is free in its form, but
				// not in the name it opens on: one that spells the name
				// exported is the doc of a symbol unexported since, and
				// sends a reader to look for an API that is not there.
				if first != name && strings.EqualFold(first, name) {
					t.Errorf("%s: the doc of %s begins with the exported %q", src.fset.Position(pos), name, first)
				}
				return
			}
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

// A declaration's doc comment is its own: when a declaration is inserted
// between a comment and the function it documents, go doc gives the new
// declaration both texts glued together — maxIdleFloor carried
// actOnDeadlines' first sentence, and actOnDeadlines lost it. The shape is
// a comment that does not open on the declaration's name and, further down,
// starts a line with it right after a full stop: the lines above that one
// speak of something else. Every declaration is read, exported or not,
// outside tests.
func TestDocCommentsAreNotGlued(t *testing.T) {
	for _, src := range goSources(t) {
		if src.test || src.generated {
			continue
		}
		check := func(name string, doc *ast.CommentGroup) {
			if doc == nil || len(doc.List) < 2 {
				return
			}
			if first, _, _ := strings.Cut(strings.TrimPrefix(doc.List[0].Text, "// "), " "); first == name {
				return
			}
			for i := 1; i < len(doc.List); i++ {
				line := strings.TrimPrefix(doc.List[i].Text, "// ")
				above := strings.TrimSpace(strings.TrimPrefix(doc.List[i-1].Text, "//"))
				if above == "" || !strings.HasSuffix(above, ".") {
					continue // a new paragraph, or the sentence above goes on
				}
				if first, _, _ := strings.Cut(line, " "); first == name {
					t.Errorf("%s: the doc of %s opens on line %d; the %d lines above it belong to another declaration",
						src.fset.Position(doc.Pos()), name, i+1, i)
					return
				}
			}
		}
		for _, decl := range src.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				check(d.Name.Name, d.Doc)
			case *ast.GenDecl:
				if len(d.Specs) != 1 {
					continue
				}
				switch s := d.Specs[0].(type) {
				case *ast.TypeSpec:
					check(s.Name.Name, d.Doc)
				case *ast.ValueSpec:
					if len(s.Names) == 1 {
						check(s.Names[0].Name, d.Doc)
					}
				}
			}
		}
	}
}

// A declaration's doc comment does not open on the name of another
// declaration of its file: that is a comment a later insertion separated
// from its own declaration, which go doc then shows on the wrong name while
// the right one goes bare. TestDocCommentsAreNotGlued finds the comment
// glued on top of another; this finds the one left on the wrong side.
//
// Only a first word in lower case is read as a name: an unexported
// identifier is not an English word, where "Frame types" or "Error codes"
// opening a block's comment are both a sentence and the name of a type or a
// method.
func TestDocCommentsStayOnTheirDeclaration(t *testing.T) {
	for _, src := range goSources(t) {
		if src.test || src.generated {
			continue
		}
		decls := src.file.Decls
		declared := map[string]bool{}
		names := make([][]string, len(decls))
		docs := make([]*ast.CommentGroup, len(decls))
		for i, decl := range decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				names[i], docs[i] = []string{d.Name.Name}, d.Doc
			case *ast.GenDecl:
				docs[i] = d.Doc
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						names[i] = append(names[i], s.Name.Name)
					case *ast.ValueSpec:
						for _, n := range s.Names {
							names[i] = append(names[i], n.Name)
						}
					}
				}
			}
			for _, n := range names[i] {
				declared[n] = true
			}
		}
		for i, doc := range docs {
			if doc == nil {
				continue
			}
			words := strings.Fields(doc.Text())
			if len(words) == 0 {
				continue
			}
			first := strings.TrimRight(words[0], ".,:;")
			if first == "" || !unicode.IsLower(rune(first[0])) || !declared[first] || slices.Contains(names[i], first) {
				continue
			}
			t.Errorf("%s: the doc of %s opens on %s, another declaration of this file: put it back on its own",
				src.fset.Position(doc.Pos()), strings.Join(names[i], ", "), first)
		}
	}
}

// processMarker is a working document named in a comment — the task
// description a change was made from, the list of work left, a review's
// report, the design notes, a user's report. None of them is in the
// repository, so the comment sends its reader nowhere. The RFCs are "the
// RFC"; a queue is a queue.
var processMarker = regexp.MustCompile(`(?i)\b(the brief|brief's|backlog|the audit|audit pass|audit's|the spec|field report|field-report)\b`)

// processMarkerInAudit is the same list for the module under audit/, minus
// the three phrases that name, there, the program that module is: a reader
// of those comments can open the code they point at. The word's other
// sense — a review whose notes live outside the tree — stays out, under the
// name that spells it.
var processMarkerInAudit = regexp.MustCompile(`(?i)\b(the brief|brief's|backlog|audit pass|the spec|field report|field-report)\b`)

// historyMarker is a production comment telling what the code used to do.
// The rule it carries is stated in the present; what it replaced is in the
// history and in the regression test. "Is used to", "never used to" and
// ", used to" are the verb's other sense. A measurement that justifies a
// choice compares it with the alternative it names, not with "the previous
// shape".
var historyMarker = regexp.MustCompile(`(?i)\bused to\b|\bformerly\b|\bbefore removal\b|\bprevious shape\b|\btried and reverted\b|\buntil this existed\b|\bturned out not to\b|\bbecame the (zero|default)\b`)

// datedHistory is a production comment that dates what the code did, or
// when a choice was made: "until 2026-10-21 the term was zero", "decided
// 2026-10-16". The commit carries the date and the regression test the
// failure; the comment states the rule. "Since 2000-01-01", an epoch, is
// not a history.
var datedHistory = regexp.MustCompile(`(?i)\b(until|before|decided|measured)\s+(on\s+)?20\d{2}-\d{2}-\d{2}\b`)

var usedToOtherSense = regexp.MustCompile(`(?i)\b(is|are|be|was|were|being|never|not|only)\s+used to\b|,\s*used to\b`)

func TestCommentMarkers(t *testing.T) {
	for _, src := range goSources(t) {
		if src.generated || strings.HasPrefix(filepath.ToSlash(src.path), "comparison/") {
			continue
		}
		for _, group := range src.file.Comments {
			// Read over the whole group: "until" and its date are as often
			// on two lines as on one.
			if !src.test && !src.nested {
				if m := datedHistory.FindString(group.Text()); m != "" {
					t.Errorf("%s: the comment dates the code's history (%q); state the rule it holds",
						src.fset.Position(group.Pos()), strings.Join(strings.Fields(m), " "))
				}
			}
			for _, c := range group.List {
				pos := src.fset.Position(c.Pos())
				marker := processMarker
				if strings.HasPrefix(filepath.ToSlash(src.path), "audit/") {
					marker = processMarkerInAudit
				}
				if m := marker.FindString(c.Text); m != "" {
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

// maxUnplannedLines is the longest function a reader is left to read with no
// map. Past it, a function's doc comment states its plan: the steps it runs,
// in order, as a numbered list or a sentence that says "in this order" —
// or, for a dispatch loop, "one case per" what it dispatches on.
const maxUnplannedLines = 150

var readingPlan = regexp.MustCompile(`(?m)^\s*1\. |\bin this order\b|\bone case per\b`)

// A function longer than maxUnplannedLines lines carries a reading plan in
// its doc comment, so that its length is navigable rather than read top to
// bottom. Splitting it is the other way out; the plan is the floor.
func TestLongFunctionsHaveAReadingPlan(t *testing.T) {
	for _, src := range goSources(t) {
		if src.generated || src.test || src.nested {
			continue
		}
		for _, decl := range src.file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			start, end := src.fset.Position(fd.Pos()), src.fset.Position(fd.End())
			n := end.Line - start.Line + 1
			if n <= maxUnplannedLines {
				continue
			}
			if fd.Doc == nil || !readingPlan.MatchString(fd.Doc.Text()) {
				t.Errorf("%s: %s runs %d lines with no reading plan in its doc comment (a numbered list, or \"in this order\")",
					start, fd.Name.Name, n)
			}
		}
	}
}
