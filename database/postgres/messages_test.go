package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// errBody builds an ErrorResponse body: (type byte, C string) pairs then a
// zero terminator.
func errBody(pairs ...string) []byte {
	if len(pairs)%2 != 0 {
		// A helper that indexed past its input would fail inside the loop with
		// a message about the loop rather than about the call.
		panic("errBody: pairs must come in twos")
	}
	var b []byte
	for i := 0; i+1 < len(pairs); i += 2 {
		b = append(b, pairs[i][0])
		b = append(b, pairs[i+1]...)
		b = append(b, 0)
	}
	return append(b, 0)
}

func TestParseError(t *testing.T) {
	body := errBody(
		"S", "ERROR",
		"C", "23505",
		"M", "duplicate key value violates unique constraint",
		"D", "Key (id)=(7) already exists.",
		"n", "orders_pkey",
		"t", "orders",
	)
	e, err := ParseError(body)
	if err != nil {
		t.Fatalf("ParseError returned %v", err)
	}
	if e.Code != "23505" {
		t.Errorf("Code = %q, want %q — the SQLSTATE is the only stable part", e.Code, "23505")
	}
	if e.Severity != "ERROR" || e.Table != "orders" || e.Constraint != "orders_pkey" {
		t.Errorf("fields lost: %+v", e)
	}
	if !strings.Contains(e.Error(), "23505") {
		t.Errorf("Error() = %q, want the SQLSTATE in it", e.Error())
	}
}

// Every field the protocol defines lands in its own place. Position first: it
// is what points a syntax error at its token, and without it the caller is
// left counting characters in the message.
func TestParseErrorAllFields(t *testing.T) {
	body := errBody(
		"S", "ERROR",
		"C", "42601",
		"M", "syntax error at or near \"FORM\"",
		"P", "10",
		"p", "3",
		"q", "SELECT 1 FORM t",
		"d", "int8",
		"F", "scan.l",
		"L", "1176",
		"R", "scanner_yyerror",
	)
	e, err := ParseError(body)
	if err != nil {
		t.Fatalf("ParseError returned %v", err)
	}
	want := Error{
		Severity: "ERROR", Code: "42601", Message: "syntax error at or near \"FORM\"",
		Position: 10, InternalPosition: 3, InternalQuery: "SELECT 1 FORM t",
		DataType: "int8", File: "scan.l", Line: "1176", Routine: "scanner_yyerror",
	}
	if *e != want {
		t.Errorf("parsed %+v\nwant   %+v", *e, want)
	}
}

// S is translated by the server's lc_messages; V, when present, is the same
// severity untranslated, and is what Severity reports whichever comes first.
func TestParseErrorPrefersNonLocalisedSeverity(t *testing.T) {
	for _, body := range [][]byte{
		errBody("S", "ERREUR", "V", "ERROR", "C", "23505"),
		errBody("V", "ERROR", "S", "ERREUR", "C", "23505"),
	} {
		e, err := ParseError(body)
		if err != nil {
			t.Fatalf("ParseError returned %v", err)
		}
		if e.Severity != "ERROR" {
			t.Errorf("Severity = %q, want the untranslated %q", e.Severity, "ERROR")
		}
	}
	// A pre-9.6 server sends S alone, and that is what there is to report.
	e, err := ParseError(errBody("S", "ERREUR", "C", "23505"))
	if err != nil || e.Severity != "ERREUR" {
		t.Errorf("without V: Severity = %q, %v; want S kept", e.Severity, err)
	}
}

// A field type this package has never heard of must be skipped, not refused:
// the protocol reserves the right to add them, and failing on one turns a
// server upgrade into an outage.
func TestParseErrorSkipsUnknownFields(t *testing.T) {
	body := errBody("S", "ERROR", "Z", "something new", "C", "42P01")
	e, err := ParseError(body)
	if err != nil {
		t.Fatalf("ParseError returned %v", err)
	}
	if e.Code != "42P01" {
		t.Errorf("Code = %q — an unknown field derailed the parse", e.Code)
	}
}

func TestParseErrorMalformed(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"no terminator", []byte("SERROR\x00")},
		{"unterminated value", []byte{'S', 'E', 'R', 'R'}},
		// A position that is not one would read as Position 0, "none sent".
		{"non-numeric position", errBody("S", "ERROR", "P", "ten")},
		{"zero position", errBody("S", "ERROR", "P", "0")},
		{"negative internal position", errBody("S", "ERROR", "p", "-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseError(tt.body); !errors.Is(err, ErrProtocol) {
				t.Errorf("got %v, want ErrProtocol", err)
			}
		})
	}
}

// rowDesc builds a RowDescription body for columns of the given names and type
// OIDs.
func rowDesc(names []string, oids []uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(names)))
	for i, n := range names {
		b = append(b, n...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, 0)       // table OID
		b = binary.BigEndian.AppendUint16(b, 0)       // column number
		b = binary.BigEndian.AppendUint32(b, oids[i]) // type OID
		b = binary.BigEndian.AppendUint16(b, 8)       // type size
		b = binary.BigEndian.AppendUint32(b, 0)       // type modifier
		b = binary.BigEndian.AppendUint16(b, uint16(pgwire.FormatBinary))
	}
	return b
}

func TestParseRowDescription(t *testing.T) {
	body := rowDesc([]string{"id", "total"}, []uint32{20, 1700})
	fields, err := ParseRowDescription(nil, body)
	if err != nil {
		t.Fatalf("ParseRowDescription returned %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("got %d fields, want 2", len(fields))
	}
	if fields[0].Name != "id" || fields[0].TypeOID != 20 {
		t.Errorf("field 0 = %+v", fields[0])
	}
	if fields[1].Name != "total" || fields[1].Format != pgwire.FormatBinary {
		t.Errorf("field 1 = %+v", fields[1])
	}
}

func TestParseRowDescriptionTruncated(t *testing.T) {
	full := rowDesc([]string{"id"}, []uint32{20})
	for _, n := range []int{0, 1, 3, len(full) - 1} {
		if _, err := ParseRowDescription(nil, full[:n]); !errors.Is(err, ErrProtocol) {
			t.Errorf("truncated to %d bytes: got %v, want ErrProtocol", n, err)
		}
	}
}

// dataRow builds a DataRow body. A nil value is NULL.
func dataRow(values ...[]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			b = binary.BigEndian.AppendUint32(b, ^uint32(0)) // -1
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	return b
}

func TestRowsAppend(t *testing.T) {
	r := newRows(3, 16, 0)
	if err := r.appendRow(dataRow([]byte("a"), nil, []byte("ccc"))); err != nil {
		t.Fatalf("Append returned %v", err)
	}
	if err := r.appendRow(dataRow([]byte(""), []byte("b"), nil)); err != nil {
		t.Fatalf("Append returned %v", err)
	}
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}

	// NULL and the empty value must stay distinguishable: the protocol keeps
	// them apart, and merging them turns a missing value into an empty one
	// with no way back.
	tests := []struct {
		row, col int
		want     string
		isNull   bool
	}{
		{0, 0, "a", false},
		{0, 1, "", true},
		{0, 2, "ccc", false},
		{1, 0, "", false},
		{1, 1, "b", false},
		{1, 2, "", true},
	}
	for _, tt := range tests {
		got, isNull := r.Value(tt.row, tt.col)
		if isNull != tt.isNull {
			t.Errorf("(%d,%d) isNull = %v, want %v", tt.row, tt.col, isNull, tt.isNull)
		}
		if !isNull && string(got) != tt.want {
			t.Errorf("(%d,%d) = %q, want %q", tt.row, tt.col, got, tt.want)
		}
	}
}

// Reset keeps the storage and empties the content, which is what makes a batch
// cost no allocation after the first.
func TestRowsResetReusesStorage(t *testing.T) {
	r := newRows(1, 8, 0)
	for range 8 {
		if err := r.appendRow(dataRow([]byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	capBefore := cap(r.buf)
	r.reset()
	if r.Len() != 0 {
		t.Errorf("Len after Reset = %d, want 0", r.Len())
	}
	for range 8 {
		if err := r.appendRow(dataRow([]byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	if cap(r.buf) != capBefore {
		t.Errorf("buffer regrew from %d to %d — Reset did not keep its storage", capBefore, cap(r.buf))
	}
	if got, _ := r.Value(7, 0); string(got) != "value" {
		t.Errorf("after refill (7,0) = %q", got)
	}
}

// A row wider or narrower than the description means the stream is being read
// at the wrong offset: every value after it would be plausible nonsense, so it
// must fail rather than be interpreted.
func TestRowsRejectsWrongWidth(t *testing.T) {
	r := newRows(2, 8, 0)
	if err := r.appendRow(dataRow([]byte("only one"))); !errors.Is(err, ErrProtocol) {
		t.Errorf("got %v, want ErrProtocol", err)
	}
	if err := r.appendRow(dataRow([]byte("a"), []byte("b"), []byte("c"))); !errors.Is(err, ErrProtocol) {
		t.Errorf("got %v, want ErrProtocol", err)
	}
}

func TestRowsRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"no field count", []byte{0}},
		{"field without length", append(binary.BigEndian.AppendUint16(nil, 1), 0, 0)},
		{"length past the body", func() []byte {
			b := binary.BigEndian.AppendUint16(nil, 1)
			b = binary.BigEndian.AppendUint32(b, 99)
			return append(b, 'x')
		}()},
		{"trailing bytes", append(dataRow([]byte("a")), 'x')},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRows(1, 8, 0)
			if err := r.appendRow(tt.body); !errors.Is(err, ErrProtocol) {
				t.Errorf("got %v, want ErrProtocol", err)
			}
		})
	}
}

// The row limit is S1 applied to a result set: a query returning more than the
// caller budgeted must fail rather than grow until the process dies.
func TestRowsBounded(t *testing.T) {
	r := newRows(1, 2, 0)
	for range 2 {
		if err := r.appendRow(dataRow([]byte("x"))); err != nil {
			t.Fatal(err)
		}
	}
	if !r.full() {
		t.Error("Full is false at the limit")
	}
	if err := r.appendRow(dataRow([]byte("x"))); !errors.Is(err, ErrTooManyRows) {
		t.Errorf("got %v, want ErrTooManyRows", err)
	}
}

// The byte bound is a threshold, not a refusal: the row that takes the values
// past it is accepted and the batch reports itself full, even when that row
// is wider than the bound on its own. Only the row limit refuses.
func TestRowsFullAtTheByteBound(t *testing.T) {
	r := newRows(1, 100, 32)
	value := make([]byte, 20)
	if err := r.appendRow(dataRow(value)); err != nil {
		t.Fatalf("the first row: %v", err)
	}
	if r.full() {
		t.Fatal("Full with 20 of 32 bytes held")
	}
	if err := r.appendRow(dataRow(value)); err != nil {
		t.Fatalf("the row that crosses the bound returned %v, want it accepted", err)
	}
	if !r.full() || !r.fullOnBytes() {
		t.Errorf("Full = %v, fullOnBytes = %v with 40 of 32 bytes held, want both", r.full(), r.fullOnBytes())
	}
	if got := r.largestRow(); got != 20 {
		t.Errorf("largestRow = %d, want 20", got)
	}
	r.reset()
	if err := r.appendRow(dataRow(make([]byte, 40))); err != nil {
		t.Fatalf("a row wider than the bound, alone in its batch: %v", err)
	}
	if !r.fullOnBytes() {
		t.Error("a row wider than the bound does not fill its batch")
	}

	unbounded := newRows(1, 100, 0)
	for range 3 {
		if err := unbounded.appendRow(dataRow(value)); err != nil {
			t.Fatalf("with no byte bound: %v", err)
		}
	}
	if unbounded.full() {
		t.Error("Full with no byte bound and 3 of 100 rows")
	}
}

// The frontend builders must produce what the reader reads back, with the
// framing this package computes rather than the caller.
func TestWriterExtendedQuerySequence(t *testing.T) {
	var out bytes.Buffer
	wr := pgwire.NewWriter(&out)

	wr.Parse("", "SELECT id FROM orders WHERE id = ANY($1)", []uint32{1016})
	wr.BindBinary("", "", [][]byte{[]byte("\x00\x01"), nil})
	wr.Describe('P', "")
	wr.Execute("", 0)
	wr.Sync()
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}

	rd := pgwire.NewReader(bytes.NewReader(out.Bytes()))
	var types []byte
	for {
		m, err := rd.Next()
		if err != nil {
			break
		}
		types = append(types, m.Type)
	}
	want := []byte{pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendSync}
	if !slices.Equal(types, want) {
		t.Errorf("message types = %q, want %q", types, want)
	}
}

// A nil parameter is NULL on the wire, which is -1 rather than a zero length:
// the distinction the whole type system above rests on.
func TestBindBinaryEncodesNull(t *testing.T) {
	var out bytes.Buffer
	wr := pgwire.NewWriter(&out)
	wr.BindBinary("", "", [][]byte{nil, {}})
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}

	rd := pgwire.NewReader(bytes.NewReader(out.Bytes()))
	m, err := rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	// portal(1) stmt(1) formats(2+2) paramCount(2) then the two parameters.
	body := m.Body[8:]
	if got := binary.BigEndian.Uint32(body[:4]); got != ^uint32(0) {
		t.Errorf("nil parameter encoded as length %d, want -1 (NULL)", int32(got))
	}
	if got := binary.BigEndian.Uint32(body[4:8]); got != 0 {
		t.Errorf("empty parameter encoded as length %d, want 0", int32(got))
	}
}

// Asking for a field that does not exist is a programming error, and it panics
// like a slice index does — returning a zero value instead would let a typo in
// a generated hydration function read as a legitimate NULL.
//
// The panic carries the indices as a value rather than a formatted sentence,
// so whoever recovers one can read them. This test pins that shape.
func TestRowsValueOutOfRangePanics(t *testing.T) {
	r := newRows(2, 4, 0)
	if err := r.appendRow(dataRow(AppendInt8(nil, 1), AppendInt8(nil, 2))); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		row, col int
	}{
		{"a row past the end", 1, 0},
		{"a negative row", -1, 0},
		{"a column past the width", 0, 2},
		{"a negative column", 0, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				got := recover()
				if got == nil {
					t.Fatalf("Value(%d,%d) did not panic", tc.row, tc.col)
				}
				e, ok := got.(IndexOutOfRange)
				if !ok {
					t.Fatalf("panicked with %T (%v), want IndexOutOfRange", got, got)
				}
				if e.Row != tc.row || e.Col != tc.col {
					t.Errorf("panic carries (%d,%d), want (%d,%d)", e.Row, e.Col, tc.row, tc.col)
				}
				if e.Rows != 1 || e.Fields != 2 {
					t.Errorf("panic reports %d rows of %d fields, want 1 of 2", e.Rows, e.Fields)
				}
				// It must read as something when printed, which is how it
				// will usually be met — a panic value reaches a log through
				// its String method, not its Error one.
				for name, text := range map[string]string{"Error": e.Error(), "String": e.String()} {
					if !strings.Contains(text, "out of range") {
						t.Errorf("%s() = %q", name, text)
					}
					if !strings.Contains(text, strconv.Itoa(tc.row)) {
						t.Errorf("%s() = %q, want it to carry the row", name, text)
					}
				}
			}()
			r.Value(tc.row, tc.col)
		})
	}

	// And the in-range case still answers rather than panicking.
	if v, isNull := r.Value(0, 1); isNull || len(v) != 8 {
		t.Errorf("Value(0,1) = %x, null=%v; want eight bytes", v, isNull)
	}
}

// NULL is folded into the offsets — a negative end means absent — so the field
// *after* a NULL has to find its start through that marker. Getting it wrong
// reads a value from the wrong offset, which is a plausible wrong answer
// rather than an error.
func TestRowsNullDoesNotShiftTheFieldAfterIt(t *testing.T) {
	rows := newRows(4, 8, 0)

	// A value, a NULL, an empty value, a value. The empty one matters: it and
	// the NULL both have end == start, and only the marker tells them apart.
	if err := rows.appendRow(dataRow([]byte("first"), nil, []byte(""), []byte("last"))); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		col    int
		want   string
		isNull bool
	}{
		{0, "first", false},
		{1, "", true},
		{2, "", false},
		{3, "last", false},
	} {
		got, isNull := rows.Value(0, tc.col)
		if isNull != tc.isNull {
			t.Errorf("column %d: isNull = %v, want %v", tc.col, isNull, tc.isNull)
		}
		if !tc.isNull && string(got) != tc.want {
			t.Errorf("column %d: %q, want %q", tc.col, got, tc.want)
		}
	}
}

// Two NULLs in a row, and a value after them: the start has to walk back
// through a marker that is itself a marker.
func TestRowsConsecutiveNulls(t *testing.T) {
	rows := newRows(4, 8, 0)

	if err := rows.appendRow(dataRow([]byte("x"), nil, nil, []byte("y"))); err != nil {
		t.Fatal(err)
	}
	if v, isNull := rows.Value(0, 3); isNull || string(v) != "y" {
		t.Errorf("the value after two NULLs is %q (null=%v), want \"y\"", v, isNull)
	}
	for _, col := range []int{1, 2} {
		if _, isNull := rows.Value(0, col); !isNull {
			t.Errorf("column %d should be NULL", col)
		}
	}
}

// A NULL at the very start of a row: there is no previous field to walk back
// to, and the row before it ended somewhere.
func TestRowsNullAtTheStartOfASecondRow(t *testing.T) {
	rows := newRows(2, 8, 0)

	if err := rows.appendRow(dataRow([]byte("one"), []byte("two"))); err != nil {
		t.Fatal(err)
	}
	if err := rows.appendRow(dataRow(nil, []byte("four"))); err != nil {
		t.Fatal(err)
	}
	if _, isNull := rows.Value(1, 0); !isNull {
		t.Error("the first field of the second row should be NULL")
	}
	if v, isNull := rows.Value(1, 1); isNull || string(v) != "four" {
		t.Errorf("the field after a leading NULL is %q (null=%v), want \"four\"", v, isNull)
	}
	// And the first row is untouched.
	if v, _ := rows.Value(0, 1); string(v) != "two" {
		t.Errorf("the previous row now reads %q", v)
	}
}

// The offsets in Rows are int32 and they address a whole batch, not one
// message. The reader's message limit bounds a single DataRow, so a batch of
// wide values can cross the int32 range with every row individually legal —
// and a wrapped offset comes back negative, which Value reads as NULL. Values
// turning into NULLs with no error anywhere is the failure this refuses.
func TestRowsRefuseABatchPastTheOffsetRange(t *testing.T) {
	saved := maxRowBytes
	maxRowBytes = 32 // the same arithmetic, without allocating two gigabytes
	defer func() { maxRowBytes = saved }()

	r := newRows(1, 100, 0)
	value := make([]byte, 20)
	if err := r.appendRow(dataRow(value)); err != nil {
		t.Fatalf("the first row did not fit under the bound: %v", err)
	}
	err := r.appendRow(dataRow(value))
	if !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("Append past the offset range returned %v, want ErrTooManyRows", err)
	}
	// The batch keeps what it had: the refusal is not a reset.
	if r.Len() != 1 {
		t.Errorf("the accumulator holds %d rows, want the 1 it had accepted", r.Len())
	}
	if b, isNull := r.Value(0, 0); isNull || len(b) != 20 {
		t.Errorf("the accepted row reads back as (%d bytes, null=%v)", len(b), isNull)
	}
}

// A RowDescription with bytes after its last column is being read at the
// wrong offset, and every field above it is then plausible nonsense —
// the same refusal Rows.appendRow makes for a DataRow.
func TestParseRowDescriptionRefusesTrailingBytes(t *testing.T) {
	body := append(rowDesc([]string{"id"}, []uint32{OIDInt8}), 0xff)
	if _, err := ParseRowDescription(nil, body); !errors.Is(err, ErrProtocol) {
		t.Fatalf("ParseRowDescription accepted a trailing byte: %v", err)
	}
}

// The byte bound of a Rows is the caller's code, not data: a negative one, or
// one past what the offsets address, is a programming error and panics rather
// than becoming the ceiling.
func TestRowsPanicsOnAnOutOfRangeByteBound(t *testing.T) {
	for _, n := range []int{-1, maxRowBytes + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("newRows with a byte bound of %d did not panic", n)
				}
			}()
			newRows(1, 4, n)
		}()
	}
}

// BenchmarkRowsAppend is what decoding a result's rows into a batch costs:
// 1 000 DataRow bodies of three columns — two bigints and a label of about
// twenty bytes, the shape of the comparison's bench_orders — appended to
// one accumulator, reused from round to round as a connection reuses it.
func BenchmarkRowsAppend(b *testing.B) {
	const n = 1000
	bodies := make([][]byte, n)
	for i := range bodies {
		bodies[i] = dataRow(AppendInt8(nil, int64(i)), AppendInt8(nil, int64(i)*7), []byte("order label number 0"))
	}
	r := newRows(3, n, 0)
	b.ReportAllocs()
	for b.Loop() {
		r.reset()
		for _, body := range bodies {
			if err := r.appendRow(body); err != nil {
				b.Fatal(err)
			}
		}
	}
}
