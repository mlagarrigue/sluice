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
				// Nor one that opens on a neighbour of its name, an
				// identifier sharing its first letters: appendChunk's doc
				// read "appendChunked writes…", the name it had before.
				if first != name && identLike(first) {
					n := min(len(first), len(name), 4)
					if first[:n] == name[:n] {
						t.Errorf("%s: the doc of %s begins with %q, another name", src.fset.Position(pos), name, first)
					}
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
// shape", "what it replaced" or "the earlier" figure. The forms are narrow
// on purpose: "replaced" and "earlier" alone are the present tense of many
// rules ("a broken one is replaced", "the earlier frames").
var historyMarker = regexp.MustCompile(`(?i)\bused to\b|\bformerly\b|\bbefore removal\b|\bprevious shape\b|\btried and reverted\b|\buntil this existed\b|\bturned out not to\b|\bbecame the (zero|default)\b|\bit replaced (a|an|the|on|in)\b|\bthis was an? \w+ before\b|\bearlier ~?\d`)

// datedHistory is a production comment that dates what the code did, or
// when a choice was made: "until 2026-10-21 the term was zero", "decided
// 2026-10-16". The commit carries the date and the regression test the
// failure; the comment states the rule. "Since 2000-01-01", an epoch, is
// not a history.
var datedHistory = regexp.MustCompile(`(?i)\b(until|before|decided|measured)\s+(on\s+)?20\d{2}-\d{2}-\d{2}\b`)

// identLike reports a word written as a Go identifier of more than one
// part: lower case first, an upper case letter after ("appendChunked"), so
// that the prose a doc opens on ("append", "records") is not one.
func identLike(w string) bool {
	if w == "" || !unicode.IsLower(rune(w[0])) {
		return false
	}
	return strings.ContainsFunc(w[1:], unicode.IsUpper)
}

// deferredMarker is a production comment that puts its decision off — "to
// revisit", "revisit when" — where the condition it holds under is what the
// reader needs: "the choice holds while no measurement shows…". Work left
// goes to an issue, not to the code.
var deferredMarker = regexp.MustCompile(`(?i)\brevisit(ed|ing)?\b`)

// runnerCase is a QUIC Interop Runner test case named in a production
// comment: the comment then tells the incident the case caught — which
// peer, which pass — where the code needs the rule it keeps. The incident
// is the commit's, or ADR 0019's. Only the names that are nothing but a
// case are listed; "handshake" or "retry" are words of the protocol.
var runnerCase = regexp.MustCompile(`\b(amplificationlimit|connectionmigration|handshakecorruption|handshakeloss|rebind-addr|rebind-port|transfercorruption|transferloss|zerortt|longrtt)\b`)

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
				if m := runnerCase.FindString(c.Text); m != "" {
					t.Errorf("%s: the comment names the interop runner's %s case; state the rule it keeps: %s", pos, m, strings.TrimSpace(c.Text))
				}
				if deferredMarker.MatchString(c.Text) {
					t.Errorf("%s: the comment puts its decision off; state the condition it holds under: %s", pos, strings.TrimSpace(c.Text))
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

// commentLink is a doc link: [Name], [Name.Member], [pkg.Name] or
// [pkg.Name.Member], a pointer star allowed, not preceded by what would make
// it an index expression ("buf[i]") or a slice type ("[]byte").
var commentLink = regexp.MustCompile(`(?:^|[^\w\]\)])\[\*?([A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2})\]`)

// commentCamel is a word written as a Go identifier of more than one part,
// lower case first ("appendChunked"), or a qualified one ("w.cfg.limit").
var commentCamel = regexp.MustCompile(`\b[a-z][a-z0-9]*[A-Z]\w*\b`)

// commentWordsOutsideGo are the camelCase words of the tree's comments
// that name nothing of this tree, each with what it is. A word that is a
// typo of an identifier, or an old name of one, is not one of them.
var commentWordsOutsideGo = map[string]string{
	"keepAliveTimeout": "Node.js http.Server", "headersTimeout": "Node.js http.Server",
	"requestTimeout": "Node.js http.Server", "createServer": "Node.js http",
	"maxRequestsPerSocket": "Node.js http.Server", "connectionTimeout": "Node.js and Java pools",
	"createTimeoutException": "HikariCP", "connectionTimeoutMillis": "node-postgres",
	"pqSendSome": "libpq", "pgSQL": "PL/pgSQL",
	"sha224WithRSAEncryption": "RFC 4055", "signatureAlgorithm": "RFC 5280",
	"hashAlgorithm": "RFC 4055", "digitalSignature": "RFC 5280 key usage",
	"kPersistentCongestionThreshold": "RFC 9002", "kGranularity": "RFC 9002",
	"maxUselessRecords": "crypto/tls, unexported", "kB": "a unit",
	"saleQty": "a field of the documentation's example model",
	"macOS":   "an operating system", "gRPC": "a protocol",
}

// TestCommentsNameIdentifiersThatExist reads the comments of the
// production code for the identifiers they name — a doc link, a word
// written as an identifier — and checks each against the identifiers the
// tree declares or uses. A comment that names a function under a name it
// never had ("NewSealer and NewOpener build…", "[Opener.Open]", "minSaltLen
// is") sends its reader looking for what does not exist; nothing caught
// those three until a review did.
func TestCommentsNameIdentifiersThatExist(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := strings.Cut(string(mod), "\n")
	module := strings.TrimSpace(strings.TrimPrefix(first, "module"))
	sources := goSources(t)
	idents, packages, outside := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, src := range sources {
		packages[src.file.Name.Name] = true
		for _, im := range src.file.Imports {
			if p := strings.Trim(im.Path.Value, `"`); !strings.HasPrefix(p, module) {
				outside[p[strings.LastIndex(p, "/")+1:]] = true
			}
		}
		ast.Inspect(src.file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				idents[id.Name] = true
			}
			return true
		})
	}
	for _, src := range sources {
		if src.test || src.generated || src.nested {
			continue
		}
		imports := map[string]bool{} // package name → whether it is the tree's
		for _, im := range src.file.Imports {
			p := strings.Trim(im.Path.Value, `"`)
			name := p[strings.LastIndex(p, "/")+1:]
			if im.Name != nil {
				name = im.Name.Name
			}
			imports[name] = strings.HasPrefix(p, module)
		}
		for _, group := range src.file.Comments {
			for _, c := range group.List {
				text := strings.TrimPrefix(c.Text, "//")
				if strings.HasPrefix(text, "\t") || strings.HasPrefix(text, "go:") || strings.HasPrefix(text, "nolint") {
					continue // a code block of the doc, or a directive
				}
				pos := src.fset.Position(c.Pos())
				for _, m := range commentLink.FindAllStringSubmatch(text, -1) {
					parts := strings.Split(m[1], ".")
					if len(parts) > 1 {
						tree, imported := imports[parts[0]]
						switch {
						case imported && !tree, !imported && outside[parts[0]]:
							continue // the standard library's, or another module's
						case imported || packages[parts[0]] && !idents[parts[0]]:
							parts = parts[1:]
						}
					}
					if len(parts) == 1 && !unicode.IsUpper(rune(parts[0][0])) {
						continue // [i], [n]: a word in brackets, not a link
					}
					for _, p := range parts {
						if !idents[p] {
							t.Errorf("%s: the link [%s] names %s, which the tree does not declare", pos, m[1], p)
							break
						}
					}
				}
				for _, loc := range commentCamel.FindAllStringIndex(text, -1) {
					w := text[loc[0]:loc[1]]
					if strings.HasPrefix(text[loc[1]:], "*") {
						continue // lastValidated*: the fields that share the prefix
					}
					if idents[w] || idents[strings.TrimSuffix(w, "s")] || commentWordsOutsideGo[w] != "" {
						continue
					}
					t.Errorf("%s: the comment names %s, which the tree does not declare", pos, w)
				}
			}
		}
		for _, d := range src.file.Decls {
			name, doc := declName(d)
			if doc == nil || name == "" {
				continue
			}
			first, _, _ := strings.Cut(strings.TrimSpace(doc.Text()), " ")
			first = strings.TrimSuffix(strings.TrimRight(first, ",.:;"), "()")
			if first != name && isCamel(first) && !idents[first] {
				t.Errorf("%s: the doc of %s opens on %s, which the tree does not declare", src.fset.Position(doc.Pos()), name, first)
			}
		}
	}
}

// declName is the name a declaration's doc comment is about, and that doc.
func declName(d ast.Decl) (string, *ast.CommentGroup) {
	switch d := d.(type) {
	case *ast.FuncDecl:
		return d.Name.Name, d.Doc
	case *ast.GenDecl:
		if d.Lparen.IsValid() || len(d.Specs) != 1 {
			return "", nil
		}
		switch s := d.Specs[0].(type) {
		case *ast.TypeSpec:
			return s.Name.Name, d.Doc
		case *ast.ValueSpec:
			if len(s.Names) == 1 {
				return s.Names[0].Name, d.Doc
			}
		}
	}
	return "", nil
}

// isCamel reports a word of two humps or more: "NewSealer", "minSaltLen".
func isCamel(w string) bool {
	if len(w) < 2 || !unicode.IsLetter(rune(w[0])) {
		return false
	}
	humps := 0
	for i, r := range w {
		if i > 0 && unicode.IsUpper(r) && unicode.IsLower(rune(w[i-1])) {
			humps++
		}
	}
	return humps > 0
}
