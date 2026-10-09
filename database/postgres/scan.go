package postgres

import (
	"fmt"
	"slices"
	"time"
)

// Column scanners: one column of a whole batch, decoded in one pass.
//
// They exist to extract a column, which is a thing a batched pipeline wants
// often — the keys of one result becoming the `= ANY($1)` parameter of the
// next is the shape this whole package is built around.
//
// **They are not the fast way to hydrate structs, and §8.6's claim that they
// would be is measured false.** That section argues that a monomorphic loop
// over a thousand values of one type beats a row-major loop alternating
// decoders per field. On this workload it does not: assembling five-column
// rows costs 34.7 ns/row through scanners against 31.2 ns/row row-major, and
// 37.6 against 26.3 on a batch of one. The likely reason is that the scanners
// make two passes — into scratch arrays, then out again to build the structs —
// and the extra memory traffic costs more than the branch predictability
// buys, since a row-major loop over five repeating fields is a pattern the
// processor predicts perfectly well.
//
// What the architecture's "Hydration" section is right about is the other half: reflection costs measurably
// more than generated code, which should simply emit row-major code. The
// figure lives in docs/benchmarks.md, measured by
// BenchmarkHydrateReflect against BenchmarkHydrateRowMajor (hydrate_test.go).
//
// Each scanner appends to dst so a caller reuses one slice per column across
// every batch, which is what keeps a scan allocation-free after the first.
// That reuse is why an error still returns dst rather than nil: a nil would
// discard the caller's buffer — and its capacity — over one bad batch, which
// is exactly the allocation the append contract exists to avoid. The returned
// slice holds the rows decoded before the failure; nothing partial is ever
// appended, so none of its content is garbage.
//
// A NULL is an error here. A column that can be NULL is a different column,
// and a scanner that silently produced a zero would turn a missing value into
// a real one — use [ScanNulls] alongside to get the mask, or read the column
// as bytes. Generated code knows which columns are NOT NULL and calls
// accordingly; hand-written code has to decide, which is the point at which it
// should.

// # The column's type is checked, once per call
//
// A scanner reads bytes by width, and width is a poor witness: a text column
// whose values happen to be eight bytes long scans as int8 and decodes into
// numbers nobody stored. So each scanner compares the column's type OID from
// the RowDescription against the types it reads, before the first row:
//
//   - A built-in type (OID below 16384, PostgreSQL's FirstNormalObjectId) that
//     the scanner does not read is an error naming both types.
//   - A user-defined type or a domain (OID 16384 and above) is accepted, and
//     the per-value width checks are what stand guard. That is deliberate: a
//     domain over int8 is sent as int8 and scans as int8, and its OID is
//     assigned per database, so no fixed list could name it.
//   - A [Rows] built with [NewRows] carries no description, so there is
//     nothing to compare and the scan goes ahead on width alone.

// firstNormalObjectID is PostgreSQL's FirstNormalObjectId: every OID below it
// is assigned at initdb and means the same type in every database.
const firstNormalObjectID = 16384

// checkColumn refuses a built-in column type the scanner does not read.
func checkColumn(rows *Rows, col int, scanner string, accepts ...uint32) error {
	if col < 0 || col >= len(rows.desc) {
		return nil // no description to check against
	}
	got := rows.desc[col].TypeOID
	if got >= firstNormalObjectID {
		return nil
	}
	if slices.Contains(accepts, got) {
		return nil
	}
	return fmt.Errorf("%w: column %d (%q) has type %s, and %s reads %s",
		ErrCodec, col, rows.desc[col].Name, oidName(got), scanner, oidName(accepts[0]))
}

// oidName renders a type OID for an error message.
func oidName(oid uint32) string {
	name := map[uint32]string{
		OIDBool: "bool", OIDBytea: "bytea", OIDInt8: "int8", OIDInt2: "int2",
		OIDInt4: "int4", OIDText: "text", OIDFloat4: "float4", OIDFloat8: "float8",
		OIDVarchar: "varchar", OIDDate: "date", OIDTimestamp: "timestamp",
		OIDTimestampTZ: "timestamptz", OIDUUID: "uuid", OIDNumeric: "numeric",
		OIDJSON: "json", OIDJSONB: "jsonb", oidBpchar: "bpchar", oidNameType: "name",
	}[oid]
	if name == "" {
		return fmt.Sprintf("OID %d", oid)
	}
	return fmt.Sprintf("%s (OID %d)", name, oid)
}

// The text-like built-ins whose binary form is the string's bytes.
const (
	oidNameType uint32 = 19
	oidUnknown  uint32 = 705
	oidBpchar   uint32 = 1042
)

// ErrNullValue reports a NULL where a scanner was asked for a value.
var ErrNullValue = fmt.Errorf("%w: NULL in a column scanned as not-null", ErrCodec)

// ScanNulls appends the NULL mask of a column: one bool per row.
func ScanNulls(dst []bool, rows *Rows, col int) []bool {
	for i := range rows.Len() {
		_, isNull := rows.Value(i, col)
		dst = append(dst, isNull)
	}
	return dst
}

// ScanInt8 appends column col of every row, decoded as int8 (int64).
func ScanInt8(dst []int64, rows *Rows, col int) ([]int64, error) {
	if err := checkColumn(rows, col, "ScanInt8", OIDInt8); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		v, err := DecodeInt8(b)
		if err != nil {
			return dst, fmt.Errorf("row %d, column %d: %w", i, col, err)
		}
		dst = append(dst, v)
	}
	return dst, nil
}

// ScanInt4 appends column col of every row, decoded as int4 (int32).
func ScanInt4(dst []int32, rows *Rows, col int) ([]int32, error) {
	if err := checkColumn(rows, col, "ScanInt4", OIDInt4); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		v, err := DecodeInt4(b)
		if err != nil {
			return dst, fmt.Errorf("row %d, column %d: %w", i, col, err)
		}
		dst = append(dst, v)
	}
	return dst, nil
}

// ScanFloat8 appends column col of every row, decoded as float8 (float64).
func ScanFloat8(dst []float64, rows *Rows, col int) ([]float64, error) {
	if err := checkColumn(rows, col, "ScanFloat8", OIDFloat8); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		v, err := DecodeFloat8(b)
		if err != nil {
			return dst, fmt.Errorf("row %d, column %d: %w", i, col, err)
		}
		dst = append(dst, v)
	}
	return dst, nil
}

// ScanBool appends column col of every row, decoded as bool.
func ScanBool(dst []bool, rows *Rows, col int) ([]bool, error) {
	if err := checkColumn(rows, col, "ScanBool", OIDBool); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		v, err := DecodeBool(b)
		if err != nil {
			return dst, fmt.Errorf("row %d, column %d: %w", i, col, err)
		}
		dst = append(dst, v)
	}
	return dst, nil
}

// ScanText appends column col of every row as strings. It reads text,
// varchar, bpchar, name and json columns — the built-ins whose binary form is
// the string's own bytes.
//
// Each string is a copy: a string is immutable in Go and the batch's buffer is
// not, so a borrowed one would change under its holder at the next batch.
// That copy is the one allocation hydration cannot avoid, and it is per value
// rather than per row.
func ScanText(dst []string, rows *Rows, col int) ([]string, error) {
	if err := checkColumn(rows, col, "ScanText", OIDText, OIDVarchar, oidBpchar, oidNameType, OIDJSON, oidUnknown); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		dst = append(dst, string(b))
	}
	return dst, nil
}

// ScanTimestampTZ appends column col of every row as UTC times.
func ScanTimestampTZ(dst []time.Time, rows *Rows, col int) ([]time.Time, error) {
	if err := checkColumn(rows, col, "ScanTimestampTZ", OIDTimestampTZ); err != nil {
		return dst, err
	}
	for i := range rows.Len() {
		b, isNull := rows.Value(i, col)
		if isNull {
			return dst, fmt.Errorf("%w: row %d, column %d", ErrNullValue, i, col)
		}
		v, err := DecodeTimestampTZ(b)
		if err != nil {
			return dst, fmt.Errorf("row %d, column %d: %w", i, col, err)
		}
		dst = append(dst, v)
	}
	return dst, nil
}
