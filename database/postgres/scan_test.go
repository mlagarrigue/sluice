package postgres

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// Every column scanner, held to the same three promises.
//
// They were not all held to any: ScanInt4 had never been executed at all, and
// three others had only their happy path. A scanner is the shape a caller
// reaches for when the keys of one result become the `= ANY($1)` parameter of
// the next — the loop this package is built around — so "it compiles" is a
// thin guarantee for a public function.
//
// The promises: the values come out in row order; a NULL is refused rather
// than turned into a zero, because a missing key silently becoming key 0 is
// exactly the merge this package refuses everywhere else; a value of the
// wrong width is refused rather than read past; and an error hands the
// caller's buffer back rather than nil, because the buffer is the whole point
// of the append contract.
func TestScanners(t *testing.T) {
	// One row of every type, then a second row that is NULL throughout.
	at := time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)
	values := [][]byte{
		AppendInt8(nil, -1234567890123),
		AppendInt4(nil, -1234567),
		AppendFloat8(nil, -0.125),
		AppendBool(nil, true),
		AppendText(nil, "héllo"),
		AppendTimestampTZ(nil, at),
	}

	rows := NewRows(len(values), 4)
	if err := rows.Append(dataRow(values...)); err != nil {
		t.Fatal(err)
	}
	if err := rows.Append(dataRow(make([][]byte, len(values))...)); err != nil {
		t.Fatal(err)
	}

	t.Run("values come out in row order", func(t *testing.T) {
		one := NewRows(len(values), 4)
		if err := one.Append(dataRow(values...)); err != nil {
			t.Fatal(err)
		}
		if got, err := ScanInt8(nil, one, 0); err != nil || len(got) != 1 || got[0] != -1234567890123 {
			t.Errorf("ScanInt8 = %v, %v", got, err)
		}
		if got, err := ScanInt4(nil, one, 1); err != nil || len(got) != 1 || got[0] != -1234567 {
			t.Errorf("ScanInt4 = %v, %v", got, err)
		}
		if got, err := ScanFloat8(nil, one, 2); err != nil || len(got) != 1 || got[0] != -0.125 {
			t.Errorf("ScanFloat8 = %v, %v", got, err)
		}
		if got, err := ScanBool(nil, one, 3); err != nil || len(got) != 1 || !got[0] {
			t.Errorf("ScanBool = %v, %v", got, err)
		}
		if got, err := ScanText(nil, one, 4); err != nil || len(got) != 1 || got[0] != "héllo" {
			t.Errorf("ScanText = %v, %v", got, err)
		}
		got, err := ScanTimestampTZ(nil, one, 5)
		if err != nil || len(got) != 1 || !got[0].Equal(at) {
			t.Errorf("ScanTimestampTZ = %v, %v", got, err)
		}
	})

	// A NULL has nowhere to go in a slice of values, so it is an error rather
	// than a zero. A key that came back as 0 because it was absent would join
	// against whatever row 0 is.
	t.Run("a NULL is refused rather than zeroed", func(t *testing.T) {
		scanners := map[string]func() error{
			"ScanInt8":        func() error { _, err := ScanInt8(nil, rows, 0); return err },
			"ScanInt4":        func() error { _, err := ScanInt4(nil, rows, 1); return err },
			"ScanFloat8":      func() error { _, err := ScanFloat8(nil, rows, 2); return err },
			"ScanBool":        func() error { _, err := ScanBool(nil, rows, 3); return err },
			"ScanText":        func() error { _, err := ScanText(nil, rows, 4); return err },
			"ScanTimestampTZ": func() error { _, err := ScanTimestampTZ(nil, rows, 5); return err },
		}
		for name, scan := range scanners {
			if err := scan(); !errors.Is(err, ErrNullValue) {
				t.Errorf("%s over a column holding a NULL = %v, want ErrNullValue", name, err)
			}
		}
	})

	// A value of the wrong width means the stream and the description
	// disagree, and reading it anyway is how one bad row makes every later
	// value plausible nonsense.
	t.Run("a value of the wrong width is refused", func(t *testing.T) {
		narrow := NewRows(1, 4)
		if err := narrow.Append(dataRow([]byte{1, 2, 3})); err != nil {
			t.Fatal(err)
		}
		scanners := map[string]func() error{
			"ScanInt8":        func() error { _, err := ScanInt8(nil, narrow, 0); return err },
			"ScanInt4":        func() error { _, err := ScanInt4(nil, narrow, 0); return err },
			"ScanFloat8":      func() error { _, err := ScanFloat8(nil, narrow, 0); return err },
			"ScanBool":        func() error { _, err := ScanBool(nil, narrow, 0); return err },
			"ScanTimestampTZ": func() error { _, err := ScanTimestampTZ(nil, narrow, 0); return err },
		}
		for name, scan := range scanners {
			if err := scan(); !errors.Is(err, ErrCodec) {
				t.Errorf("%s over three bytes = %v, want ErrCodec", name, err)
			}
		}
		// ScanText takes any width by definition, so it has no such refusal
		// to make — its absence from the table above is the point, not an
		// omission.
		if got, err := ScanText(nil, narrow, 0); err != nil || len(got) != 1 {
			t.Errorf("ScanText over three bytes = %v, %v; want it accepted", got, err)
		}
	})

	// An error must not cost the caller its buffer. The scanners exist so one
	// slice per column is reused across every batch; a nil return on a bad
	// batch would throw that capacity away and reintroduce the allocation the
	// append contract removes. What came out before the failure stays: it is
	// real decoded values, never a partial append.
	t.Run("an error returns the caller's buffer", func(t *testing.T) {
		dst := make([]int64, 0, 8)
		dst = append(dst, 42)
		got, err := ScanInt8(dst, rows, 0) // row 0 decodes, row 1 is NULL
		if !errors.Is(err, ErrNullValue) {
			t.Fatalf("ScanInt8 over a NULL = %v, want ErrNullValue", err)
		}
		if len(got) == 0 || &got[0] != &dst[0] {
			t.Fatalf("ScanInt8 returned a new slice on error, want the caller's buffer back")
		}
		if len(got) != 2 || got[0] != 42 || got[1] != -1234567890123 {
			t.Errorf("buffer after a failed scan = %v, want [42 -1234567890123]", got)
		}
	})

	// ScanNulls is the other half: it reports which rows are missing rather
	// than refusing them, which is what a caller uses to decide before
	// scanning a column that may hold one.
	t.Run("ScanNulls reports rather than refuses", func(t *testing.T) {
		got := ScanNulls(nil, rows, 0)
		if len(got) != 2 || got[0] || !got[1] {
			t.Errorf("ScanNulls = %v, want [false true]", got)
		}
	})

	// A float column carrying values a caller cannot compare with == still has
	// to survive the scan.
	t.Run("NaN survives a float scan", func(t *testing.T) {
		one := NewRows(1, 4)
		if err := one.Append(dataRow(AppendFloat8(nil, math.NaN()))); err != nil {
			t.Fatal(err)
		}
		got, err := ScanFloat8(nil, one, 0)
		if err != nil || len(got) != 1 || !math.IsNaN(got[0]) {
			t.Errorf("ScanFloat8 over NaN = %v, %v", got, err)
		}
	})
}

// described builds a one-column accumulator whose description says oid, with
// one row holding value.
func described(t *testing.T, oid uint32, value []byte) *Rows {
	t.Helper()
	rows := NewRows(1, 4)
	rows.Init([]Field{{Name: "c", TypeOID: oid}}, 4)
	if err := rows.Append(dataRow(value)); err != nil {
		t.Fatal(err)
	}
	return rows
}

// A text column whose value happens to be eight bytes long used to scan as
// int8 and decode into a number nobody stored. A built-in type the scanner
// does not read is refused, naming both types.
func TestScannersRefuseABuiltinTypeMismatch(t *testing.T) {
	text8 := described(t, OIDText, []byte("abcdefgh"))
	got, err := ScanInt8(nil, text8, 0)
	if !errors.Is(err, ErrCodec) || len(got) != 0 {
		t.Fatalf("ScanInt8 over a text column = %v, %v; want ErrCodec and nothing decoded", got, err)
	}
	for _, name := range []string{"text (OID 25)", "int8 (OID 20)", "ScanInt8"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}

	at := AppendTimestampTZ(nil, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	for name, scan := range map[string]func() error{
		"ScanInt4 over int8":             func() error { _, err := ScanInt4(nil, described(t, OIDInt8, AppendInt4(nil, 1)), 0); return err },
		"ScanFloat8 over int8":           func() error { _, err := ScanFloat8(nil, described(t, OIDInt8, AppendInt8(nil, 1)), 0); return err },
		"ScanBool over bytea":            func() error { _, err := ScanBool(nil, described(t, OIDBytea, []byte{1}), 0); return err },
		"ScanText over int8":             func() error { _, err := ScanText(nil, described(t, OIDInt8, AppendInt8(nil, 1)), 0); return err },
		"ScanTimestampTZ over timestamp": func() error { _, err := ScanTimestampTZ(nil, described(t, OIDTimestamp, at), 0); return err },
		"ScanTimestampTZ over float8":    func() error { _, err := ScanTimestampTZ(nil, described(t, OIDFloat8, at), 0); return err },
		"ScanInt8 over a zero OID":       func() error { _, err := ScanInt8(nil, described(t, 0, AppendInt8(nil, 1)), 0); return err },
	} {
		if err := scan(); !errors.Is(err, ErrCodec) {
			t.Errorf("%s = %v, want ErrCodec", name, err)
		}
	}
}

// The matching built-ins scan, and so does any type at or above
// FirstNormalObjectId: a domain over int8 is sent as int8, its OID is
// assigned per database, and the width checks still guard it.
func TestScannersAcceptMatchingAndUserTypes(t *testing.T) {
	if got, err := ScanInt8(nil, described(t, OIDInt8, AppendInt8(nil, 42)), 0); err != nil || got[0] != 42 {
		t.Errorf("ScanInt8 over int8 = %v, %v", got, err)
	}
	for _, oid := range []uint32{OIDText, OIDVarchar, 1042, 19, OIDJSON} {
		if got, err := ScanText(nil, described(t, oid, []byte("v")), 0); err != nil || got[0] != "v" {
			t.Errorf("ScanText over OID %d = %v, %v", oid, got, err)
		}
	}
	const domainOverInt8 = 16384
	if got, err := ScanInt8(nil, described(t, domainOverInt8, AppendInt8(nil, 7)), 0); err != nil || got[0] != 7 {
		t.Errorf("ScanInt8 over a user type = %v, %v", got, err)
	}
	// Accepted is not unchecked: the width still has to be right.
	if _, err := ScanInt8(nil, described(t, 99999, []byte("short")), 0); !errors.Is(err, ErrCodec) {
		t.Errorf("ScanInt8 over a user type of the wrong width = %v, want ErrCodec", err)
	}
}
