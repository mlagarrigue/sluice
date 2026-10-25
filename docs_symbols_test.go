package sluice_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// module is the import path every package of this repository starts with.
const module = "github.com/mlagarrigue/sluice"

// citation is one `pkg.Name`, `pkg.Type.Member` or `Type.Member` a page
// names in prose.
type citation struct {
	file string
	line int
	path string // import path of pkg; empty for an unqualified Type.Member
	name string // "Name" or "Type.Member"
}

// codeSpan is an inline code span; qualified is a module identifier inside
// one, optionally followed by a member; member is a Type.Member written
// without its package, as a guide does once the package is the page's subject.
var (
	codeSpan  = regexp.MustCompile("`([^`\n]+)`")
	qualified = regexp.MustCompile(`(?:^|[^\w.])([a-z][a-z0-9]*)\.([A-Z]\w*)(?:\.([A-Z]\w*))?`)
	member    = regexp.MustCompile(`(?:^|[^\w.])([A-Z]\w*)\.([A-Z]\w*)`)
)

// proseCitations returns the module symbols README.md and the guides name in
// inline code, outside the fenced blocks TestGuideCodeBlocksTypeCheck already
// compiles.
func proseCitations(t *testing.T) []citation {
	t.Helper()
	packages := importablePackages(t)
	files, err := filepath.Glob("docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	var out []citation
	for _, f := range append([]string{"README.md"}, files...) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		fenced := false
		for i, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if fenced {
				continue
			}
			for _, span := range codeSpan.FindAllStringSubmatch(line, -1) {
				for _, m := range qualified.FindAllStringSubmatch(span[1], -1) {
					path, ok := packages[m[1]]
					if !ok || !strings.HasPrefix(path, module) {
						continue
					}
					name := m[2]
					if m[3] != "" {
						name += "." + m[3]
					}
					out = append(out, citation{f, i + 1, path, name})
				}
				for _, m := range member.FindAllStringSubmatch(span[1], -1) {
					out = append(out, citation{f, i + 1, "", m[1] + "." + m[2]})
				}
			}
		}
	}
	return out
}

// Every module symbol a guide or the README names in prose is exported by the
// tree: a renamed function, a removed field, a method that moved to another
// type is a sentence that sends the reader to something that is not there,
// and the compiler never reads the sentence. The code blocks are held by
// TestGuideCodeBlocksTypeCheck; this holds the words around them. A
// Type.Member written without its package is checked against every module
// type of that name, and passes over a name no module type has: that one is
// the standard library's, or a placeholder.
func TestGuideSymbolsAreExported(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the module from source")
	}
	cited := proseCitations(t)
	if len(cited) < 100 {
		t.Fatalf("found %d cited symbols; the extraction changed shape", len(cited))
	}
	imp := importer.ForCompiler(token.NewFileSet(), "source", nil)
	loaded := map[string]*types.Package{}
	load := func(path string) *types.Package {
		if p, ok := loaded[path]; ok {
			return p
		}
		p, err := imp.Import(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		loaded[path] = p
		return p
	}
	var all []string
	for _, path := range importablePackages(t) {
		if strings.HasPrefix(path, module) {
			all = append(all, path)
		}
	}
	for _, c := range cited {
		if c.path != "" {
			if pkg := load(c.path); !resolves(pkg, c.name) {
				t.Errorf("%s:%d: %s.%s is not exported by %s", c.file, c.line, pkg.Name(), c.name, c.path)
			}
			continue
		}
		typ, _, _ := strings.Cut(c.name, ".")
		var owners []string
		found := false
		for _, path := range all {
			pkg := load(path)
			if _, ok := pkg.Scope().Lookup(typ).(*types.TypeName); !ok {
				continue
			}
			owners = append(owners, pkg.Name())
			found = found || resolves(pkg, c.name)
		}
		if len(owners) > 0 && !found {
			t.Errorf("%s:%d: no %s.%s in %s", c.file, c.line, typ, c.name[len(typ)+1:], strings.Join(owners, ", "))
		}
	}
}

// resolves reports whether name — "Name" or "Type.Member" — is an exported
// object of pkg, the member a field or a method of the type or its pointer.
func resolves(pkg *types.Package, name string) bool {
	head, member, _ := strings.Cut(name, ".")
	obj := pkg.Scope().Lookup(head)
	if obj == nil || !obj.Exported() {
		return false
	}
	if member == "" {
		return true
	}
	tn, ok := obj.(*types.TypeName)
	if !ok {
		return false
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(tn.Type()), true, pkg, member)
	return m != nil && m.Exported()
}

// The guides follow main, and say so while the last release lacks what they
// cite: README.md and the getting-started guide name that release in one
// sentence, "the last one published, vX.Y.Z". The sentence must name the
// latest tag — a release cut without touching it leaves the reader told to
// install main against an older version than the one go get now serves — and
// must go once every cited symbol exists at that tag.
func TestGuidesNameTheReleaseTheyOutrun(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the module from source")
	}
	out, err := exec.Command("git", "describe", "--tags", "--abbrev=0").Output()
	if err != nil {
		t.Skipf("no tag reachable: %v", err)
	}
	tag := strings.TrimSpace(string(out))

	exports := map[string]map[string]bool{}
	var missing []citation
	for _, c := range proseCitations(t) {
		if c.path == "" {
			continue // no package to look the type up in at the tag
		}
		dir := strings.TrimPrefix(strings.TrimPrefix(c.path, module), "/")
		if dir == "" {
			dir = "."
		}
		names, ok := exports[dir]
		if !ok {
			names = exportsAt(t, tag, dir)
			exports[dir] = names
		}
		if !names[c.name] {
			missing = append(missing, c)
		}
	}

	sentence := regexp.MustCompile(`the last one published, (v\d+\.\d+\.\d+)`)
	for _, f := range []string{"README.md", "docs/guide/getting-started.md"} {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		m := sentence.FindSubmatch(b)
		switch {
		case len(missing) > 0 && m == nil:
			t.Errorf("%s: %d cited symbols are missing at %s (first: %s.%s, %s:%d) and the page does not say it follows main",
				f, len(missing), tag, filepath.Base(missing[0].path), missing[0].name, missing[0].file, missing[0].line)
		case len(m) > 1 && string(m[1]) != tag:
			t.Errorf("%s names %s as the last release; the latest tag is %s", f, m[1], tag)
		case len(missing) == 0 && m != nil:
			t.Errorf("%s: every cited symbol exists at %s, so the guides no longer need to point at main", f, tag)
		}
	}
	t.Logf("%d cited symbols are missing at %s", len(missing), tag)
}

// exportsAt returns what the package in dir exported at tag: its exported
// top-level names, and "Type.Member" for the exported methods and struct
// fields of its exported types. Read from git, so no checkout is needed.
//
// The root package is "<tag>:" and not "<tag>:.", which git refuses as an
// object name. Read the wrong way it returned nothing and every symbol of
// the root package counted as missing — harmless while the guides said
// they followed main, since the caller only asks whether anything is
// missing, and wrong the moment they stopped (v0.3.0).
func exportsAt(t *testing.T, tag, dir string) map[string]bool {
	t.Helper()
	tree := tag + ":"
	if dir != "." {
		tree += dir
	}
	ls, err := exec.Command("git", "ls-tree", "--name-only", tree).Output() //nolint:gosec // G204: a tag and a directory of this repository
	if err != nil {
		return map[string]bool{} // the package did not exist
	}
	names := map[string]bool{}
	fset := token.NewFileSet()
	for name := range strings.FieldsSeq(string(ls)) {
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := exec.Command("git", "show", tag+":"+filepath.ToSlash(filepath.Join(dir, name))).Output() //nolint:gosec // G204: a tag and a file of this repository
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s:%s/%s: %v", tag, dir, name, err)
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					names[d.Name.Name] = true
					continue
				}
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				switch r := recv.(type) {
				case *ast.IndexExpr:
					recv = r.X
				case *ast.IndexListExpr:
					recv = r.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					names[id.Name+"."+d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.ValueSpec:
						for _, n := range s.Names {
							names[n.Name] = true
						}
					case *ast.TypeSpec:
						names[s.Name.Name] = true
						if st, ok := s.Type.(*ast.StructType); ok {
							for _, field := range st.Fields.List {
								for _, n := range field.Names {
									names[s.Name.Name+"."+n.Name] = true
								}
							}
						}
					}
				}
			}
		}
	}
	return names
}
