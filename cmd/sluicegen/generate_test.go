package main

import (
	"bytes"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The drift guard, and the reason the generated file is committed at all:
// generated code that lives only on the machine that ran the generator is
// code nobody reviews and nothing compiles until it is too late. Regenerating
// and comparing makes a generator change that nobody re-ran fail the build.
func TestCommittedOutputIsCurrent(t *testing.T) {
	for _, tc := range []struct{ typ, src, out string }{
		{"Order", "../../database/postgres/internal/entity/order.go", "../../database/postgres/internal/entity/order_hydrate.go"},
		{"Product", "../../database/postgres/internal/entity/product.go", "../../database/postgres/internal/entity/product_hydrate.go"},
	} {
		in, err := os.ReadFile(tc.src)
		if err != nil {
			t.Fatal(err)
		}
		generated, err := Generate(in, filepath.Base(tc.src), tc.typ)
		if err != nil {
			t.Fatalf("Generate(%s) returned %v", tc.typ, err)
		}
		committed, err := os.ReadFile(tc.out)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(generated, committed) {
			t.Errorf("%s is out of date — run go generate ./...", tc.out)
		}
	}
}

// The mappings beyond the plain scalars: every text type a string can hold,
// bytes copied out of the row buffer, an exact decimal, and a date selected
// by the tag. The committed Product fixture compiles them; this asserts what
// they render to.
func TestGeneratedMappings(t *testing.T) {
	src := []byte(`package fixture

import (
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

type Thing struct {
	Code  string           ` + "`db:\"code\"`" + `
	Blob  []byte           ` + "`db:\"blob\"`" + `
	Price postgres.Numeric ` + "`db:\"price\"`" + `
	Day   time.Time        ` + "`db:\"day,date\"`" + `
	At    time.Time        ` + "`db:\"at\"`" + `
}
`)
	out, err := Generate(src, "fixture.go", "Thing")
	if err != nil {
		t.Fatalf("Generate returned %v", err)
	}
	got := string(out)
	for _, want := range []string{
		"postgres.OIDBPChar", "postgres.OIDName",
		"e.Blob = slices.Clone(b)",
		"postgres.DecodeNumeric(b)", "postgres.OIDNumeric",
		`case "day":`, "postgres.DecodeDate(b)", "f.TypeOID != postgres.OIDDate {",
		"f.TypeOID != postgres.OIDTimestampTZ && f.TypeOID != postgres.OIDTimestamp {",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q\n%s", want, got)
		}
	}
	if strings.Contains(got, `"day,date"`) {
		t.Error("the tag option leaked into the column name")
	}
}

func TestDateOptionRefusals(t *testing.T) {
	for name, field := range map[string]string{
		"not a time.Time": "N int64 `db:\"n,date\"`",
		"unknown option":  "N int64 `db:\"n,bogus\"`",
	} {
		_, err := Generate([]byte("package p\ntype T struct{ "+field+" }\n"), "p.go", "T")
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// What the generator makes of each field shape, asserted on the output rather
// than on internals: the tag renames a column, its absence takes the
// snake_case of the field, a pointer becomes a nullable branch, "-" opts out,
// and an unexported field is not a column.
func TestGeneratedShapes(t *testing.T) {
	src := []byte(`package fixture

import "time"

type Thing struct {
	ID        int64     ` + "`db:\"id\"`" + `
	TotalCost float64
	CustomerID int64
	Note      *string   ` + "`db:\"note\"`" + `
	Skipped   int64     ` + "`db:\"-\"`" + `
	hidden    int64
	At        time.Time ` + "`db:\"at\"`" + `
}
`)
	out, err := Generate(src, "fixture.go", "Thing")
	if err != nil {
		t.Fatalf("Generate returned %v", err)
	}
	got := string(out)

	tests := []struct {
		name  string
		want  string
		found bool
	}{
		{"the tag names the column", `case "id":`, true},
		{"no tag means snake_case", `case "total_cost":`, true},
		{"an acronym stays whole", `case "customer_id":`, true},
		{"a pointer field is nullable", "e.Note = &v", true},
		{`db:"-" opts out`, "Skipped", false},
		{"an unexported field is not a column", "hidden", false},
		{"the type OID is checked", "postgres.OIDInt8", true},
		{"a text column accepts varchar", "postgres.OIDVarchar", true},
		{"the loop is row-major", "for i := range rows.Len()", true},
		// Two same-named result columns — an ordinary join — used to bind
		// last-wins with no error: silent wrong data chosen by the SELECT's
		// column order. The binder must refuse a column it already bound.
		{"a doubled column is refused at bind time", "appears more than once in the result", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if strings.Contains(got, tt.want) != tt.found {
				t.Errorf("output contains %q = %v, want %v\n%s", tt.want, !tt.found, tt.found, got)
			}
		})
	}
}

// A Go type the generator does not understand is refused at generation time.
// Guessing at runtime is how a connector ends up storing something plausible
// and wrong.
func TestUnknownTypeIsRefused(t *testing.T) {
	src := []byte(`package fixture

type Thing struct {
	Weird map[string]int ` + "`db:\"weird\"`" + `
}
`)
	_, err := Generate(src, "fixture.go", "Thing")
	if err == nil {
		t.Fatal("a field of an unmapped type was accepted")
	}
	if !strings.Contains(err.Error(), "no decoder") {
		t.Errorf("error = %v, want it to name the missing decoder", err)
	}
}

// The refusal names the type as written. It used to render anything but an
// identifier, selector or array as "?", and an array with a named length as
// "[]byte" — which matched the bytea decoder for a field it cannot fill.
func TestUnknownTypeIsNamedInTheError(t *testing.T) {
	for _, typ := range []string{"map[string]int", "[N]byte", "chan int", "func()", "struct{}"} {
		src := []byte("package fixture\n\nconst N = 4\n\ntype Thing struct {\n\tF " + typ + " `db:\"f\"`\n}\n")
		_, err := Generate(src, "fixture.go", "Thing")
		if err == nil {
			t.Errorf("%s: accepted", typ)
			continue
		}
		if !strings.Contains(err.Error(), strconv.Quote(typ)) {
			t.Errorf("%s: error = %v, want the type named", typ, err)
		}
	}
}

func TestGenerateErrors(t *testing.T) {
	tests := []struct {
		name string
		src  string
		typ  string
		want string
	}{
		{"missing type", "package p\n", "Absent", "no struct type"},
		{"not a struct", "package p\ntype Absent int\n", "Absent", "no struct type"},
		{"no usable fields", "package p\ntype T struct{ hidden int }\n", "T", "no hydratable"},
		{"embedded field", "package p\ntype E struct{ X int64 }\ntype T struct{ E }\n", "T", "embedded"},
		{"unparsable", "package\n", "T", "parsing"},
		// Two fields on one column render duplicate cases in the binder's
		// switch. The compiler rejects that while pointing at a DO NOT EDIT
		// file, with nothing to say which fields collided — so the generator
		// says it instead, and names both.
		// **T used to be read as *T, and the generated `e.F = &v` did not
		// compile against it.
		{"pointer to pointer", "package p\ntype T struct{ ID **int64 }\n", "T", "pointer to a pointer"},
		{"pointer to pointer, other type", "package p\nimport \"time\"\ntype T struct{ At **time.Time }\n", "T", "T.At"},
		{
			"colliding columns",
			"package p\ntype T struct{\n\tUserID int64\n\tOwner int64 `db:\"user_id\"`\n}\n",
			"T", "both resolve to column",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Generate([]byte(tt.src), "fixture.go", tt.typ)
			if err == nil {
				t.Fatal("no error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestSnakeCase(t *testing.T) {
	tests := []struct{ in, want string }{
		{"ID", "id"},
		{"Total", "total"},
		{"TotalCost", "total_cost"},
		{"CustomerID", "customer_id"},
		{"HTTPStatus", "http_status"},
		{"CreatedAt", "created_at"},
		{"A", "a"},
	}
	for _, tt := range tests {
		if got := snakeCase(tt.in); got != tt.want {
			t.Errorf("snakeCase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// The db tag follows reflect.StructTag's grammar, because that is the grammar
// every other tool reading the same tag uses: a tag the compiler's vet and
// encoding/json accept means the same thing here.
func TestColumnNameFromTag(t *testing.T) {
	tests := []struct {
		tag, want string
		skip      bool
	}{
		{"`db:\"id\"`", "id", false},
		{"`json:\"x\" db:\"id\"`", "id", false},
		{"`json:\"x\"   db:\"id\"`", "id", false},
		{"`db:\"id\" json:\"x\"`", "id", false},
		{"`db:\"a\\\"b\"`", `a"b`, false},
		{"`json:\"x\"`", "field_name", false},
		{"`db:\"-\"`", "", true},
		{"`db:\"\"`", "field_name", false},
		{"``", "field_name", false},
		{"`malformed`", "field_name", false},
		{"`json:x db:\"id\"`", "field_name", false},
		{"`db:\"id`", "field_name", false},
		{"`json:\"x\" junk db:\"id\"`", "field_name", false},
		{`"db:\"id\""`, "id", false},
	}
	for _, tt := range tests {
		got, skip := columnName("FieldName", &ast.BasicLit{Kind: token.STRING, Value: tt.tag})
		if got != tt.want || skip != tt.skip {
			t.Errorf("columnName(%s) = %q, %v; want %q, %v", tt.tag, got, skip, tt.want, tt.skip)
		}
	}
	if got, _ := columnName("FieldName", nil); got != "field_name" {
		t.Errorf("columnName without a tag = %q", got)
	}
}

// run is thin, and it is the part a user actually invokes: the flags, the
// GOFILE go:generate sets, and the default output name. Untested glue that
// writes files is still glue that writes files.
func TestRun(t *testing.T) {
	src := "package fixture\n\ntype Thing struct {\n\tID int64 `db:\"id\"`\n}\n"

	t.Run("writes the default output name", func(t *testing.T) {
		dir := t.TempDir()
		in := filepath.Join(dir, "thing.go")
		if err := os.WriteFile(in, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := run("Thing", in, ""); err != nil {
			t.Fatalf("run returned %v", err)
		}
		out := filepath.Join(dir, "thing_hydrate.go")
		b, err := os.ReadFile(out) //nolint:gosec // G304: a path this test just built in its own temp dir
		if err != nil {
			t.Fatalf("the default output %s was not written: %v", out, err)
		}
		if !strings.Contains(string(b), "func HydrateThing(") {
			t.Errorf("the output does not hold the hydrator:\n%s", b)
		}
	})

	t.Run("takes the input from GOFILE", func(t *testing.T) {
		dir := t.TempDir()
		in := filepath.Join(dir, "thing.go")
		if err := os.WriteFile(in, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GOFILE", in)
		out := filepath.Join(dir, "custom.go")
		if err := run("Thing", "", out); err != nil {
			t.Fatalf("run returned %v", err)
		}
		if _, err := os.Stat(out); err != nil {
			t.Errorf("the named output was not written: %v", err)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		t.Setenv("GOFILE", "")
		tests := []struct {
			name         string
			typ, in, out string
			want         string
		}{
			{"no type", "", "x.go", "", "-type is required"},
			{"no input and no GOFILE", "Thing", "", "", "-in is required"},
			{"unreadable input", "Thing", "/nonexistent/x.go", "", "no such file"},
			{"unknown type in a real file", "Absent", "generate.go", "", "no struct type"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := run(tt.typ, tt.in, tt.out)
				if err == nil {
					t.Fatal("no error")
				}
				if !strings.Contains(err.Error(), tt.want) {
					t.Errorf("error = %v, want it to mention %q", err, tt.want)
				}
			})
		}
	})
}
