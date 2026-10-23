// Command benchdeps prints, for each benchmark named, the packages of the
// module its code reaches: one line per benchmark, its name then the import
// paths, sorted.
//
//	go run ./internal/bench/cmd/benchdeps -dir internal/bench -module github.com/mlagarrigue/sluice BenchmarkScan BenchmarkSeqStages
//
// The regression gate measures a benchmark only when the diff touches what it
// measures, because a change anywhere in the test binary moves where every
// other function lands, and the sign test reads that constant layout effect
// as a regression. The gate first read the imports of the file declaring the
// benchmark, and a file that imported nothing of the module was given every
// package internal/bench imports: BenchmarkSeqStages, whose stages are
// helpers of its own file and reach no package of the module, was measured
// on a diff to database/postgres alone and declared SLOWER (+4.9%, 1 win of
// 11, 1a71025). This reads the code instead: the benchmark's body, every
// package-level declaration of its package that body names, theirs in turn,
// and the methods of every type reached; a selector on an import of the
// declaring file is a package reached.
//
// The walk is by name, without type checking: a local variable that shadows
// a package-level name pulls that declaration in too. That errs on the side
// of measuring, which is the safe side for a gate.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

func main() {
	dir := flag.String("dir", ".", "directory of the package holding the benchmarks")
	module := flag.String("module", "", "module path; only its packages are printed")
	flag.Parse()
	if *module == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: benchdeps -dir <package dir> -module <module path> <Benchmark>...")
		os.Exit(2)
	}
	pkg, err := load(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchdeps:", err)
		os.Exit(1)
	}
	for _, name := range flag.Args() {
		deps, err := pkg.reach(name, *module)
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchdeps:", err)
			os.Exit(1)
		}
		fmt.Println(strings.Join(append([]string{name}, deps...), " "))
	}
}

// decl is one package-level declaration and the imports of its file.
type decl struct {
	node    ast.Node
	imports map[string]string // local name → import path
}

// pkg is the package-level declarations of one directory, tests included,
// by name; a method is filed under its receiver's type name as well, so
// reaching a type reaches its methods.
type pkg struct {
	decls map[string][]decl
}

func load(dir string) (*pkg, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	p := &pkg{decls: map[string][]decl{}}
	fset := token.NewFileSet()
	for _, file := range files {
		f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		imports := map[string]string{}
		for _, spec := range f.Imports {
			ip, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			name := path.Base(ip)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			imports[name] = ip
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				name := d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					name = receiverType(d.Recv.List[0].Type)
				}
				p.decls[name] = append(p.decls[name], decl{d, imports})
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						p.decls[s.Name.Name] = append(p.decls[s.Name.Name], decl{s, imports})
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if n.Name == "_" {
								continue // var _ = …: no code can name it
							}
							p.decls[n.Name] = append(p.decls[n.Name], decl{s, imports})
						}
					}
				}
			}
		}
	}
	return p, nil
}

// receiverType is the type name of a method's receiver: T, *T, T[K] or *T[K].
func receiverType(e ast.Expr) string {
	for {
		switch t := e.(type) {
		case *ast.StarExpr:
			e = t.X
		case *ast.IndexExpr:
			e = t.X
		case *ast.IndexListExpr:
			e = t.X
		case *ast.Ident:
			return t.Name
		default:
			return ""
		}
	}
}

// reach walks from the benchmark's declaration and returns the import paths
// under module that the code it reaches selects from.
func (p *pkg) reach(benchmark, module string) ([]string, error) {
	if _, ok := p.decls[benchmark]; !ok {
		return nil, fmt.Errorf("%s: no such declaration", benchmark)
	}
	seen := map[string]bool{benchmark: true}
	queue := []string{benchmark}
	found := map[string]bool{}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		for _, d := range p.decls[name] {
			ast.Inspect(d.node, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					if x, ok := n.X.(*ast.Ident); ok {
						if ip, ok := d.imports[x.Name]; ok {
							if ip == module || strings.HasPrefix(ip, module+"/") {
								found[ip] = true
							}
							return false
						}
					}
				case *ast.Ident:
					if _, ok := p.decls[n.Name]; ok && !seen[n.Name] {
						seen[n.Name] = true
						queue = append(queue, n.Name)
					}
				}
				return true
			})
		}
	}
	out := make([]string, 0, len(found))
	for ip := range found {
		out = append(out, ip)
	}
	slices.Sort(out)
	return out, nil
}
