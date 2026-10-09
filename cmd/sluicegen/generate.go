// Command sluicegen generates hydration code: the loop that turns a batch of
// decoded PostgreSQL rows into a slice of a Go struct.
//
// It exists because reflection costs on every field of every row, even
// written carefully: the gap is measured by BenchmarkHydrateReflect against
// BenchmarkHydrateRowMajor in postgres/hydrate_test.go, and the figure lives
// in docs/design/architecture.md, "Hydration". Generated code is how a caller gets the
// hand-written figure without writing it per entity and keeping it in step
// with the schema.
//
// The same measurement corrected what to generate. That section argued for columnar
// decoding — one pass per column, then assemble — and that shape measured
// *slower* than the row-major loop it was meant to beat, by 11% at a thousand
// rows and 43% at one. So this generator emits row-major code.
//
//	//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type Order
//
// Two properties of the generated code are worth more than its speed.
//
// **Columns bind by name, never by position.** A SELECT whose columns are
// reordered would otherwise write each value into a different field of the
// same type — total into id, both bigint — with no error anywhere. The
// generated binder resolves names against the RowDescription the server sent
// and fails on a column it cannot find.
//
// **Types are checked against the schema at bind time.** A column altered from
// int4 to bigint in a migration is caught when the result opens, not by
// decoding eight bytes as four somewhere in the middle of a batch.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"unicode"
)

// column is one field of the entity, resolved to how it is decoded.
type column struct {
	Field      string   // Go field name
	Name       string   // column name in SQL
	Decode     string   // expression decoding `b` into the field's type
	OIDs       []string // acceptable type OIDs, by their constant names
	Nullable   bool     // the Go field is a pointer, so NULL is expressible
	GoType     string   // the Go type, for the nullable branch
	Infallible bool     // Decode cannot fail, so no error branch is rendered
}

// generatedFormat is stamped into every generated file's header. It names
// the contract between the generator and the postgres symbols the generated
// code calls: it changes when the generated code's shape changes, so a file
// produced by an older generator can be told apart from one that merely
// needs regenerating.
const generatedFormat = 1

// entity is what the template renders.
type entity struct {
	Package string
	Type    string
	Columns []column
	Format  int
}

// typeMapping says how each supported Go type is decoded and which PostgreSQL
// types may legitimately feed it.
//
// A type absent from this table is a compile-time refusal rather than a
// runtime surprise: a field this generator does not understand means the
// entity and the schema disagree about something, and guessing is how a
// connector ends up storing a struct's address in a text column.
var typeMapping = map[string]struct {
	decode string
	oids   []string
	// infallible marks a decode expression that cannot fail — a plain
	// conversion — so the generator emits no error branch for it: any text
	// of any length is a valid Go string, and an `if err != nil` on an err
	// that is always nil is dead code in a DO NOT EDIT file.
	infallible bool
}{
	"int64":   {decode: "postgres.DecodeInt8(b)", oids: []string{"postgres.OIDInt8"}},
	"int32":   {decode: "postgres.DecodeInt4(b)", oids: []string{"postgres.OIDInt4"}},
	"int16":   {decode: "postgres.DecodeInt2(b)", oids: []string{"postgres.OIDInt2"}},
	"float64": {decode: "postgres.DecodeFloat8(b)", oids: []string{"postgres.OIDFloat8"}},
	"float32": {decode: "postgres.DecodeFloat4(b)", oids: []string{"postgres.OIDFloat4"}},
	"bool":    {decode: "postgres.DecodeBool(b)", oids: []string{"postgres.OIDBool"}},
	// text, varchar, char(n) and name all travel as their characters in
	// binary format; char(n) keeps its blank padding, as the server stores it.
	"string": {decode: "string(b)", oids: []string{"postgres.OIDText", "postgres.OIDVarchar", "postgres.OIDBPChar", "postgres.OIDName"}, infallible: true},
	// The row's bytes are the reader's buffer, reused for the next batch, so
	// the field gets its own copy.
	"[]byte":           {decode: "slices.Clone(b)", oids: []string{"postgres.OIDBytea"}, infallible: true},
	"postgres.Numeric": {decode: "postgres.DecodeNumeric(b)", oids: []string{"postgres.OIDNumeric"}},
	"time.Time":        {decode: "postgres.DecodeTimestampTZ(b)", oids: []string{"postgres.OIDTimestampTZ", "postgres.OIDTimestamp"}},
	"[16]byte":         {decode: "postgres.DecodeUUID(b)", oids: []string{"postgres.OIDUUID"}},
}

// Generate produces the hydration source for one struct type found in src.
//
// src is the Go source of a single file, filename names it in errors, and
// typeName is the struct to hydrate; the output's package clause is the
// input's own. Returning the bytes rather than writing them is what makes this
// testable against a golden file, which is the only way a generator's output
// stays reviewed.
func Generate(src []byte, filename, typeName string) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", filename, err)
	}

	st, err := findStruct(file, typeName)
	if err != nil {
		return nil, err
	}

	e := entity{Package: file.Name.Name, Type: typeName, Format: generatedFormat}
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			return nil, fmt.Errorf("%s: embedded fields are not supported — name the columns explicitly", typeName)
		}
		for _, name := range f.Names {
			if !name.IsExported() {
				continue // an unexported field is not part of the row
			}
			col, err := columnFor(typeName, name.Name, f)
			if err != nil {
				return nil, err
			}
			if col == nil {
				continue // explicitly skipped with `db:"-"`
			}
			e.Columns = append(e.Columns, *col)
		}
	}
	if len(e.Columns) == 0 {
		return nil, fmt.Errorf("%s has no hydratable exported fields", typeName)
	}
	// Two fields resolving to one column would render duplicate switch cases
	// in the binder — a compile error pointing into a DO NOT EDIT file with no
	// hint of which fields collided. Refused here, naming both.
	byColumn := make(map[string]string, len(e.Columns))
	for _, c := range e.Columns {
		if prev, dup := byColumn[c.Name]; dup {
			return nil, fmt.Errorf("%s: fields %s and %s both resolve to column %q — rename one with a db tag",
				typeName, prev, c.Field, c.Name)
		}
		byColumn[c.Name] = c.Field
	}

	var buf bytes.Buffer
	if err := hydrateTemplate.Execute(&buf, e); err != nil {
		return nil, fmt.Errorf("rendering %s: %w", typeName, err)
	}
	// Formatting here rather than trusting the template: a generator whose
	// output has to be tidied by hand is a generator nobody runs twice.
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("formatting the generated %s hydrator: %w\n%s", typeName, err, buf.String())
	}
	return out, nil
}

func findStruct(file *ast.File, typeName string) (*ast.StructType, error) {
	var found *ast.StructType
	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != typeName {
			return true
		}
		if st, ok := ts.Type.(*ast.StructType); ok {
			found = st
		}
		return false
	})
	if found == nil {
		return nil, fmt.Errorf("no struct type %q in the file", typeName)
	}
	return found, nil
}

// columnFor resolves one field to a column, or nil when the field opts out.
func columnFor(typeName, fieldName string, f *ast.Field) (*column, error) {
	name, skip := columnName(fieldName, f.Tag)
	if skip {
		return nil, nil
	}

	// One pointer is how a field says its column may be NULL. A second has
	// no meaning the generated code could give it — `e.F = &v` would not even
	// compile against **T — so it is refused here, where the message can name
	// the field, rather than in a DO NOT EDIT file.
	if star, ok := f.Type.(*ast.StarExpr); ok {
		if _, multi := star.X.(*ast.StarExpr); multi {
			return nil, fmt.Errorf("%s.%s: a pointer to a pointer is not supported — a nullable column is a single pointer, *T",
				typeName, fieldName)
		}
	}
	goType, nullable := typeExpr(f.Type)
	m, ok := typeMapping[goType]
	if !ok {
		return nil, fmt.Errorf("%s.%s: no decoder for Go type %q — add one to the generator rather than guessing at runtime",
			typeName, fieldName, goType)
	}
	col := &column{
		Field:      fieldName,
		Name:       name,
		Decode:     m.decode,
		OIDs:       m.oids,
		Nullable:   nullable,
		GoType:     goType,
		Infallible: m.infallible,
	}
	switch opt := tagOption(f.Tag); opt {
	case "":
	case "date":
		// date is four bytes of days where timestamp and timestamptz are
		// eight of microseconds, so it needs its own decoder. Chosen here
		// from the tag rather than at bind time from the OID: a per-row
		// branch between the two measured +12% on the generated hydrator
		// for every time.Time field, date or not.
		if goType != "time.Time" {
			return nil, fmt.Errorf("%s.%s: the date option needs a time.Time field, not %s", typeName, fieldName, goType)
		}
		col.Decode, col.OIDs = dateMapping.decode, dateMapping.oids
	default:
		return nil, fmt.Errorf("%s.%s: unknown db tag option %q", typeName, fieldName, opt)
	}
	return col, nil
}

// dateMapping is time.Time read from a date column, selected with the `date`
// tag option: `db:"released,date"`.
var dateMapping = struct {
	decode string
	oids   []string
}{decode: "postgres.DecodeDate(b)", oids: []string{"postgres.OIDDate"}}

// tagOption returns what follows the column name in the `db` tag, if anything.
func tagOption(tag *ast.BasicLit) string {
	if tag == nil {
		return ""
	}
	unquoted, err := strconv.Unquote(tag.Value)
	if err != nil {
		return ""
	}
	v, _ := reflect.StructTag(unquoted).Lookup("db")
	_, opt, _ := strings.Cut(v, ",")
	return opt
}

// columnName reads the `db` tag, falling back to the snake_case of the field
// name — the convention PostgreSQL schemas overwhelmingly use, and one a
// caller overrides per field rather than fighting globally.
func columnName(fieldName string, tag *ast.BasicLit) (name string, skip bool) {
	if tag != nil {
		if unquoted, err := strconv.Unquote(tag.Value); err == nil {
			// reflect.StructTag is a string type with a parser attached; no
			// reflect.Type is built. Its grammar is the one vet and every
			// other tag reader apply, so the same tag means the same here.
			if v, ok := reflect.StructTag(unquoted).Lookup("db"); ok {
				v, _, _ = strings.Cut(v, ",") // options follow the name; see tagOption
				if v == "-" {
					return "", true
				}
				if v != "" {
					return v, false
				}
			}
		}
	}
	return snakeCase(fieldName), false
}

// typeExpr renders a field's type and reports whether it is a pointer, which
// is how the generated code expresses a nullable column.
func typeExpr(e ast.Expr) (goType string, nullable bool) {
	if star, ok := e.(*ast.StarExpr); ok {
		t, _ := typeExpr(star.X)
		return t, true
	}
	// ExprString renders any type expression as written, so a field the
	// mapping does not cover is named in the error — a map or a [N]byte with
	// a named length used to come out as "?" or as "[]byte", the latter
	// matching the bytea decoder for a type it cannot assign to.
	return types.ExprString(e), false
}

// snakeCase turns FieldName into field_name, and CustomerID into customer_id
// rather than customer_i_d.
func snakeCase(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) {
			prevLower := unicode.IsLower(runes[i-1])
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if prevLower || nextLower {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// The import block below is fixed, and that is a property of the output
// rather than an oversight: generated code infers every value's type with
// `:=`, so it never names time.Time or any other type a field might have. A
// decoder added to typeMapping that does name a type would have to extend it
// — and would fail the build immediately, since the generated file is
// compiled. The two groups are separated the way gofumpt wants them, so the
// generated file passes the project's formatter untouched.
var hydrateTemplate = template.Must(template.New("hydrate").Parse(`// Code generated by sluicegen (format {{.Format}}). DO NOT EDIT.

package {{.Package}}

import (
	"fmt"
	"slices"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// {{.Type}}Plan binds {{.Type}}'s fields to the columns of one result.
//
// Binding happens once per result rather than once per row, and it is where
// two mistakes are caught that would otherwise be silent: a column that moved
// (columns are matched by name, never by position) and a column whose type
// changed under the entity.
type {{.Type}}Plan struct {
{{- range .Columns}}
	col{{.Field}} int
{{- end}}
}

// Bind{{.Type}} resolves the columns {{.Type}} needs in the result described
// by fields.
func Bind{{.Type}}(fields []postgres.Field) ({{.Type}}Plan, error) {
	var p {{.Type}}Plan
{{range .Columns}}	p.col{{.Field}} = -1
{{end}}	for i, f := range fields {
		switch f.Name {
{{- range .Columns}}
		case {{printf "%q" .Name}}:
			if p.col{{.Field}} >= 0 {
				// Two columns of this name in one result — an ordinary join
				// does it. Last-wins binding would pick one silently, and
				// which one is a property of the SELECT's column order:
				// wrong data with no error. Alias one side instead.
				return p, fmt.Errorf("column %q appears more than once in the result and {{$.Type}}.{{.Field}} cannot choose between them — alias one in the query", f.Name)
			}
			if {{range $i, $oid := .OIDs}}{{if $i}} && {{end}}f.TypeOID != {{$oid}}{{end}} {
				return p, fmt.Errorf("column %q is OID %d, which {{$.Type}}.{{.Field}} cannot hold", f.Name, f.TypeOID)
			}
			p.col{{.Field}} = i
{{- end}}
		}
	}
{{range .Columns}}	if p.col{{.Field}} < 0 {
		return p, fmt.Errorf("the result has no column %q for {{$.Type}}.{{.Field}}", {{printf "%q" .Name}})
	}
{{end}}	return p, nil
}

// Hydrate{{.Type}} appends one batch of rows to dst.
//
// The loop is row-major, which measurement chose over the columnar shape
// §8.6 originally specified: 31.2 ns/row against 34.7, and 26.3 against 37.6
// on a batch of one (measured, see docs/benchmarks.md).
// On an error dst is returned as it stood before this batch — capacity and
// prior contents intact — so a caller that logs and moves on keeps its reuse
// buffer rather than having it replaced by nil.
func Hydrate{{.Type}}(dst []{{.Type}}, rows *postgres.Rows, p {{.Type}}Plan) ([]{{.Type}}, error) {
	before := len(dst)
	// Reserved up front with amortized growth: the batch's row count is
	// known, and slices.Grow both skips append's per-batch reallocation on a
	// fresh buffer and gives a buffer accumulating batch after batch the
	// headroom an exact-fit reservation would deny it.
	dst = slices.Grow(dst, rows.Len())
	for i := range rows.Len() {
		var e {{.Type}}
{{range .Columns}}		{
			b, isNull := rows.Value(i, p.col{{.Field}})
{{- if .Nullable}}
			if !isNull {
{{- if .Infallible}}
				v := {{.Decode}}
{{- else}}
				v, err := {{.Decode}}
				if err != nil {
					return dst[:before], fmt.Errorf("row %d, column %q: %w", i, {{printf "%q" .Name}}, err)
				}
{{- end}}
				e.{{.Field}} = &v
			}
{{- else}}
			if isNull {
				return dst[:before], fmt.Errorf("row %d: column %q is NULL and {{$.Type}}.{{.Field}} cannot hold it", i, {{printf "%q" .Name}})
			}
{{- if .Infallible}}
			e.{{.Field}} = {{.Decode}}
{{- else}}
			v, err := {{.Decode}}
			if err != nil {
				return dst[:before], fmt.Errorf("row %d, column %q: %w", i, {{printf "%q" .Name}}, err)
			}
			e.{{.Field}} = v
{{- end}}
{{- end}}
		}
{{end}}		dst = append(dst, e)
	}
	return dst, nil
}
`))
