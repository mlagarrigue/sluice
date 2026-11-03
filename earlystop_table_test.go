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

// earlyStopElsewhere names the operators whose early stop is held by a test of
// their own rather than by the table of TestOperatorsPropagateEarlyStop, with
// that test: an operator that reads ahead, fills a batch before yielding, or
// reads two streams needs an assertion the table's two-batch allowance cannot
// make.
var earlyStopElsewhere = map[string]string{
	"join.Merge":                "TestMergeJoinByEarlyStopEveryBranch",
	"join.Interval":             "TestIntervalJoinEarlyStopReleasesBoth",
	"join.ZipLongest":           "TestZipLongestEarlyStopReleasesBoth",
	"parallel.Async":            "TestAsyncEarlyStopReleasesProducer",
	"parallel.Unordered":        "TestParallelUnorderedStopDuringTailDrain",
	"parallel.OrderedWithState": "TestParallelInitPropagatesEarlyStop",
	"window.CoalesceWithin":     "TestCoalesceWithinEarlyStopReleasesProducer",
}

// The architecture's S10 says adding an operator without adding it to the
// early-stop table is an omission the test makes visible. This is what makes
// it visible: every exported function of the operator packages that takes a
// Stream and returns one is called in the table, or named above with the test
// that holds its early stop instead — and that test exists.
func TestEveryOperatorHasAnEarlyStopTest(t *testing.T) {
	table, err := os.ReadFile("earlystop_test.go")
	if err != nil {
		t.Fatal(err)
	}
	isStream := func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.IndexExpr:
			e = x.X
		case *ast.IndexListExpr:
			e = x.X
		default:
			return false
		}
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name == "Stream"
		case *ast.SelectorExpr:
			return x.Sel.Name == "Stream"
		}
		return false
	}
	takesAndReturns := func(ft *ast.FuncType) bool {
		in, out := false, false
		for _, f := range ft.Params.List {
			in = in || isStream(f.Type)
		}
		if ft.Results != nil {
			for _, f := range ft.Results.List {
				out = out || isStream(f.Type)
			}
		}
		return in && out
	}
	found, seen := 0, map[string]bool{}
	for _, dir := range []string{".", "join", "window", "parallel", "distinct", "probe", "pushdown"} {
		pkgName := filepath.Base(dir)
		if dir == "." {
			pkgName = "sluice"
		}
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		var tests strings.Builder
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				src, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
				if err != nil {
					t.Fatal(err)
				}
				tests.Write(src)
			}
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Recv != nil || !fn.Name.IsExported() || !takesAndReturns(fn.Type) {
					continue
				}
				found++
				op := pkgName + "." + fn.Name.Name
				seen[op] = true
				if regexp.MustCompile(`\b` + regexp.QuoteMeta(op) + `\(`).Match(table) {
					continue
				}
				other, ok := earlyStopElsewhere[op]
				if !ok {
					t.Errorf("%s takes a Stream and returns one, and neither the early-stop table nor earlyStopElsewhere names it", op)
					continue
				}
				if !strings.Contains(tests.String(), "func "+other+"(") {
					t.Errorf("%s: earlyStopElsewhere names %s, which %s's tests do not define", op, other, dir)
				}
			}
		}
	}
	for op := range earlyStopElsewhere {
		if !seen[op] {
			t.Errorf("earlyStopElsewhere names %s, which is not an operator any more", op)
		}
	}
	if found < 20 {
		t.Fatalf("found %d operators; the walk changed shape", found)
	}
}
