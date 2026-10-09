package sluice

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/internal/batch"
)

// sentinels is every sentinel this package declares. TestTryKnowsEverySentinel
// fails if a declaration is missing from it.
var sentinels = []error{
	ErrOverflow,
	ErrUnsorted,
	ErrSplitStalled,
	ErrSplitDrained,
}

// Every sentinel converts; the error comes back as raised.
func TestTryConvertsSentinels(t *testing.T) {
	for _, s := range sentinels {
		t.Run(s.Error(), func(t *testing.T) {
			err := Try(func() { panic(s) })
			if !errors.Is(err, s) {
				t.Errorf("Try returned %v, want %v", err, s)
			}
		})
	}
}

// A panic that is not a sentinel is a bug and must cross Try untouched — a
// Try that absorbed it would be the silent-wrongness failure through the exit.
func TestTryRepanicsUserBugs(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"string panic", "nil map write"},
		{"user error", errors.New("user error, not a sentinel")},
		{"non-error value", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != tt.value {
					t.Errorf("recovered %v, want the original %v", r, tt.value)
				}
			}()
			_ = Try(func() { panic(tt.value) })
			t.Error("Try absorbed a non-sentinel panic")
		})
	}
}

func TestTryNoPanicReturnsNil(t *testing.T) {
	if err := Try(func() {}); err != nil {
		t.Errorf("got %v, want nil", err)
	}
}

func TestTryNilConsumePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("nil consume: no panic")
		}
	}()
	_ = Try(nil)
}

// The drift guard behind the sentinels list: every package-level `var ErrX =
// errors.New(...)` in the package source must be in the list, and nothing
// else. A sentinel added to the package without updating Try fails here, by
// name, instead of silently crossing the boundary as a re-raised panic.
func TestTryKnowsEverySentinel(t *testing.T) {
	byName := map[string]error{
		"ErrOverflow":     ErrOverflow,
		"ErrUnsorted":     ErrUnsorted,
		"ErrSplitStalled": ErrSplitStalled,
		"ErrSplitDrained": ErrSplitDrained,
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var declared []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for _, name := range vs.Names {
					if strings.HasPrefix(name.Name, "Err") && ast.IsExported(name.Name) {
						declared = append(declared, name.Name)
					}
				}
			}
		}
	}
	slices.Sort(declared)

	for _, name := range declared {
		s, known := byName[name]
		if !known {
			t.Errorf("%s is declared in the package but unknown to this test — add it here", name)
			continue
		}
		var marker *batch.Sentinel
		if !errors.As(s, &marker) {
			t.Errorf("%s is not declared through batch.NewSentinel, so Try will not convert it", name)
		}
	}
	if len(declared) != len(byName) {
		t.Errorf("package declares %d sentinels %v, this test knows %d", len(declared), declared, len(byName))
	}
}
