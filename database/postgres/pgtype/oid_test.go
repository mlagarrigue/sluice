package pgtype

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// The two blocks of oid.go say they are in OID order, which is what lets a
// reader find a number by scanning; a constant added at the end of a block
// breaks it silently. Read from the source, since constants have no order
// at run time.
func TestOIDBlocksAreInOIDOrder(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "oid.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	blocks := 0
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		blocks++
		var prev uint64
		var prevName string
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			lit, ok := vs.Values[0].(*ast.BasicLit)
			if !ok {
				t.Fatalf("%s is not a literal", vs.Names[0].Name)
			}
			v, err := strconv.ParseUint(lit.Value, 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			if v <= prev && prevName != "" {
				t.Errorf("%s (%d) comes after %s (%d)", vs.Names[0].Name, v, prevName, prev)
			}
			prev, prevName = v, vs.Names[0].Name
		}
	}
	if blocks != 2 {
		t.Errorf("oid.go has %d const blocks, want the scalar and the array one", blocks)
	}
}
