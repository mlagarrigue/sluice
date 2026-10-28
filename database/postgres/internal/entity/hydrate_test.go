package entity_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/internal/entity"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// replay is a backend that has already answered: it drops what the client
// sends and hands back one scripted result. The tests below reach a
// *postgres.Rows the way every caller does, through a query, so what they
// hydrate is the connection's own accumulator, reused from batch to batch.
type replay struct{ *bytes.Reader }

func (replay) Write(p []byte) (int, error) { return len(p), nil }
func (replay) Close() error                { return nil }

// eachBatch runs a query against a backend whose result has the given
// columns and arrives as the given batches of DataRow bodies, and calls fn
// with each batch's rows, valid only for the call. Every batch but the last
// is as long as the first: the server cuts a result at the row limit the
// client asks for, and the first batch's length is that limit.
func eachBatch(tb testing.TB, fields []postgres.Field, batches [][][]byte, fn func(rows *postgres.Rows)) {
	tb.Helper()
	var script []byte
	msg := func(typ byte, body []byte) {
		script = append(script, typ)
		script = binary.BigEndian.AppendUint32(script, uint32(4+len(body)))
		script = append(script, body...)
	}
	msg('1', nil) // ParseComplete
	msg('2', nil) // BindComplete
	msg('T', rowDescription(fields))
	total := 0
	for i, batch := range batches {
		if i < len(batches)-1 && len(batch) != len(batches[0]) {
			tb.Fatalf("batch %d holds %d rows, and only the last may be shorter than the first's %d", i, len(batch), len(batches[0]))
		}
		for _, row := range batch {
			msg('D', row)
		}
		total += len(batch)
		if i < len(batches)-1 {
			msg('s', nil) // PortalSuspended
		}
	}
	msg('C', fmt.Appendf(nil, "SELECT %d\x00", total))
	msg('Z', []byte("I"))

	conn := postgres.NewConn(replay{bytes.NewReader(script)}, postgres.Config{})
	src := conn.Query(tb.Context(), "SELECT", nil, postgres.QueryConfig{
		BatchRows: len(batches[0]),
		Fetch:     postgres.FetchBatches,
	})
	for b := range src.Stream() {
		if b.Len() > 0 {
			fn(b.Items[0].Rows())
		}
	}
	if err := src.Err(); err != nil {
		tb.Fatalf("the scripted query failed: %v", err)
	}
}

// rowDescription builds a RowDescription body for fields, every column in
// binary.
func rowDescription(fields []postgres.Field) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(fields)))
	for _, f := range fields {
		b = append(b, f.Name...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, 0) // table OID
		b = binary.BigEndian.AppendUint16(b, 0) // column number
		b = binary.BigEndian.AppendUint32(b, f.TypeOID)
		b = binary.BigEndian.AppendUint16(b, 0) // type size
		b = binary.BigEndian.AppendUint32(b, 0) // type modifier
		b = binary.BigEndian.AppendUint16(b, 1) // binary
	}
	return b
}

// dataRow builds a DataRow body; a nil value is NULL.
func dataRow(values ...[]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			b = binary.BigEndian.AppendUint32(b, ^uint32(0))
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	return b
}

// orderFields describes the result Order expects, in an order deliberately
// different from the struct's: columns bind by name, so this must work.
func orderFields() []postgres.Field {
	return []postgres.Field{
		{Name: "created_at", TypeOID: pgtype.OIDTimestampTZ},
		{Name: "total_amount", TypeOID: pgtype.OIDFloat8},
		{Name: "id", TypeOID: pgtype.OIDInt8},
		{Name: "note", TypeOID: pgtype.OIDText},
		{Name: "is_active", TypeOID: pgtype.OIDBool},
		{Name: "label", TypeOID: pgtype.OIDVarchar},
	}
}

func orderRow(id int64, total float64, at time.Time, note *string, active bool, label string) []byte {
	var noteBytes []byte
	if note != nil {
		noteBytes = []byte(*note)
	}
	return dataRow(
		pgtype.AppendTimestampTZ(nil, at),
		pgtype.AppendFloat8(nil, total),
		pgtype.AppendInt8(nil, id),
		noteBytes,
		pgtype.AppendBool(nil, active),
		[]byte(label),
	)
}

func TestGeneratedHydrator(t *testing.T) {
	plan, err := entity.BindOrder(orderFields())
	if err != nil {
		t.Fatalf("BindOrder returned %v", err)
	}

	at := time.Date(2024, 5, 4, 10, 0, 0, 0, time.UTC)
	note := "urgent"
	var orders []entity.Order
	eachBatch(t, orderFields(), [][][]byte{{
		orderRow(1, 12.5, at, &note, true, "first"),
		orderRow(2, 0, at, nil, false, "second"),
	}}, func(rows *postgres.Rows) {
		if orders, err = entity.HydrateOrder(orders, rows, plan); err != nil {
			t.Fatalf("HydrateOrder returned %v", err)
		}
	})
	if len(orders) != 2 {
		t.Fatalf("got %d orders, want 2", len(orders))
	}

	// The columns were in a different order from the struct's fields, so every
	// value landing where it belongs is the by-name binding working.
	o := orders[0]
	if o.ID != 1 || o.Total != 12.5 || o.Label != "first" || !o.Active {
		t.Errorf("order 0 = %+v", o)
	}
	if !o.CreatedAt.Equal(at) {
		t.Errorf("order 0 CreatedAt = %v, want %v", o.CreatedAt, at)
	}
	if o.Note == nil || *o.Note != "urgent" {
		t.Errorf("order 0 Note = %v, want a pointer to \"urgent\"", o.Note)
	}
	// A NULL in a nullable column is a nil pointer, not an empty string: the
	// distinction the whole codec layer keeps.
	if orders[1].Note != nil {
		t.Errorf("order 1 Note = %v, want nil for a NULL", *orders[1].Note)
	}
	// The opted-out field is untouched by hydration.
	if orders[1].Computed != 0 {
		t.Errorf(`a db:"-" field was written: %d`, orders[1].Computed)
	}
}

// The first silent failure the generator exists to prevent: a column whose
// type changed under the entity. Two bigint columns swapped would decode
// perfectly and mean something else, so the type is checked when the result
// opens.
func TestBindRejectsWrongType(t *testing.T) {
	fields := orderFields()
	for i := range fields {
		if fields[i].Name == "id" {
			fields[i].TypeOID = pgtype.OIDInt4 // altered from bigint in a migration
		}
	}
	_, err := entity.BindOrder(fields)
	if err == nil {
		t.Fatal("a column of the wrong type was accepted")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("error = %v, want it to name the column", err)
	}
}

// The second: a column that is simply not there. Failing at bind time names
// it; failing later would name an index.
func TestBindRejectsMissingColumn(t *testing.T) {
	var fields []postgres.Field
	for _, f := range orderFields() {
		if f.Name == "label" {
			continue
		}
		fields = append(fields, f)
	}
	_, err := entity.BindOrder(fields)
	if err == nil {
		t.Fatal("a result missing a column was accepted")
	}
	if !strings.Contains(err.Error(), "label") {
		t.Errorf("error = %v, want it to name the missing column", err)
	}
}

// The third silent failure: two result columns with the same name, which an
// ordinary join produces for free. Last-wins binding would pick whichever the
// SELECT listed last — wrong data chosen by column order, with no error — so
// the binder refuses and tells the caller to alias.
func TestBindRejectsADuplicateColumn(t *testing.T) {
	fields := append(orderFields(), postgres.Field{Name: "id", TypeOID: pgtype.OIDInt8})
	_, err := entity.BindOrder(fields)
	if err == nil {
		t.Fatal("a result carrying two \"id\" columns was accepted")
	}
	if !strings.Contains(err.Error(), "id") || !strings.Contains(err.Error(), "more than once") {
		t.Errorf("error = %v, want it to name the doubled column", err)
	}
}

// A NULL in a column the struct cannot express is an error, not a zero value.
func TestHydrateRejectsUnexpectedNull(t *testing.T) {
	plan, err := entity.BindOrder(orderFields())
	if err != nil {
		t.Fatal(err)
	}
	// A NULL id, which Order declares as a plain int64.
	body := dataRow(
		pgtype.AppendTimestampTZ(nil, time.Now()),
		pgtype.AppendFloat8(nil, 1),
		nil,
		nil,
		pgtype.AppendBool(nil, true),
		[]byte("x"),
	)
	eachBatch(t, orderFields(), [][][]byte{{body}}, func(rows *postgres.Rows) {
		_, err = entity.HydrateOrder(nil, rows, plan)
	})
	if err == nil {
		t.Fatal("a NULL in a non-nullable column was accepted")
	} else if !strings.Contains(err.Error(), "id") {
		t.Errorf("error = %v, want it to name the column", err)
	}
}

// A hydration that fails must hand the caller's buffer back, not nil.
//
// The calling convention is dst = Hydrate(dst, ...) for reuse across batches,
// so returning nil on the first bad row replaced the caller's slice — capacity
// and all — and took the rows appended before the failure with it. A pipeline
// that logs a bad batch and carries on then reallocates from zero, silently.
//
// What comes back is the buffer as it stood before this batch: all or nothing
// per batch, which is what makes a partial batch impossible to mistake for a
// whole one.
func TestHydrateKeepsTheCallersBufferWhenARowFails(t *testing.T) {
	plan, err := entity.BindOrder(orderFields())
	if err != nil {
		t.Fatal(err)
	}

	// One good batch, so the caller has a buffer worth keeping, then a second
	// whose second row is unhydratable.
	at := time.Now().UTC()
	bad := dataRow(
		pgtype.AppendTimestampTZ(nil, at),
		pgtype.AppendFloat8(nil, 30),
		nil, // a NULL id, which Order cannot hold
		nil,
		pgtype.AppendBool(nil, true),
		[]byte("c"),
	)
	var dst []entity.Order
	var before, capBefore int
	batch := 0
	eachBatch(t, orderFields(), [][][]byte{
		{orderRow(1, 10, at, nil, true, "a"), orderRow(2, 15, at, nil, true, "b")},
		{orderRow(3, 20, at, nil, true, "c"), bad},
	}, func(rows *postgres.Rows) {
		batch++
		if batch == 1 {
			if dst, err = entity.HydrateOrder(dst, rows, plan); err != nil {
				t.Fatal(err)
			}
			if len(dst) != 2 {
				t.Fatalf("the first batch produced %d orders, want 2", len(dst))
			}
			before, capBefore = len(dst), cap(dst)
			return
		}
		dst, err = entity.HydrateOrder(dst, rows, plan)
	})
	if batch != 2 {
		t.Fatalf("the query delivered %d batches, want 2", batch)
	}
	if err == nil {
		t.Fatal("the unhydratable row was accepted")
	}
	if dst == nil {
		t.Fatal("the caller's buffer came back nil, taking the earlier batch with it")
	}
	if len(dst) != before {
		t.Errorf("len = %d, want the %d the caller had: a half-hydrated batch was left behind", len(dst), before)
	}
	if dst[0].ID != 1 {
		t.Errorf("the first batch's order was overwritten: %+v", dst[0])
	}
	if cap(dst) < capBefore {
		t.Errorf("cap = %d, want at least the %d already claimed", cap(dst), capBefore)
	}
}

// Generated hydration must be allocation-free after the first batch, apart
// from the strings it copies out — the same economy as everything else here.
func BenchmarkGeneratedHydrate(b *testing.B) {
	plan, err := entity.BindOrder(orderFields())
	if err != nil {
		b.Fatal(err)
	}
	const n = 1024
	at := time.Now().UTC()
	note := "n"
	bodies := make([][]byte, n)
	for i := range n {
		bodies[i] = orderRow(int64(i), float64(i), at, &note, i%2 == 0, "label")
	}

	dst := make([]entity.Order, 0, n)
	eachBatch(b, orderFields(), [][][]byte{bodies}, func(rows *postgres.Rows) {
		b.ReportAllocs()
		for b.Loop() {
			dst, err = entity.HydrateOrder(dst[:0], rows, plan)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
}

// productFields describes the result Product expects, with released as oid.
func productFields(released uint32) []postgres.Field {
	return []postgres.Field{
		{Name: "sku", TypeOID: pgtype.OIDBPChar},
		{Name: "owner", TypeOID: pgtype.OIDName},
		{Name: "image", TypeOID: pgtype.OIDBytea},
		{Name: "thumbnail", TypeOID: pgtype.OIDBytea},
		{Name: "price", TypeOID: pgtype.OIDNumeric},
		{Name: "released", TypeOID: released},
		{Name: "weight", TypeOID: pgtype.OIDFloat8},
	}
}

// The mappings Order does not exercise, end to end through the committed
// generated code: char(n) and name into string, bytea copied out of the row
// buffer, numeric kept exact, a date column through the `date` option, and
// a Null[T] field for the nullable column that takes no pointer.
func TestGeneratedProductHydrator(t *testing.T) {
	plan, err := entity.BindProduct(productFields(pgtype.OIDDate))
	if err != nil {
		t.Fatalf("BindProduct returned %v", err)
	}
	price, err := pgtype.ParseNumeric("19.90")
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	first := [][]byte{
		dataRow(
			[]byte("AB-1  "), []byte("postgres"), []byte{1, 2, 3}, nil,
			pgtype.AppendNumeric(nil, price), pgtype.AppendDate(nil, day),
			pgtype.AppendFloat8(nil, 1.25),
		),
		dataRow(
			[]byte("AB-2  "), []byte("postgres"), []byte{1, 2, 3}, nil,
			pgtype.AppendNumeric(nil, price), pgtype.AppendDate(nil, day),
			nil,
		),
	}
	// The second batch overwrites the connection's row buffer, which is what
	// the bytea check below needs.
	second := [][]byte{dataRow(
		[]byte("ZZ-9  "), []byte("other"), []byte{9, 9, 9}, []byte{7},
		pgtype.AppendNumeric(nil, price), pgtype.AppendDate(nil, day), nil,
	)}

	var got []entity.Product
	var p entity.Product
	batch := 0
	eachBatch(t, productFields(pgtype.OIDDate), [][][]byte{first, second}, func(rows *postgres.Rows) {
		batch++
		if batch > 1 {
			// The bytes are the field's own: the row buffer is reused for
			// the next batch, and a []byte that aliased it would have
			// changed under the caller by now.
			if string(p.Image) != "\x01\x02\x03" {
				t.Errorf("Image changed with the row buffer: %v", p.Image)
			}
			return
		}
		if got, err = entity.HydrateProduct(nil, rows, plan); err != nil {
			t.Fatalf("HydrateProduct returned %v", err)
		}
		p = got[0]
	})
	if batch != 2 {
		t.Fatalf("the query delivered %d batches, want 2", batch)
	}
	if p.SKU != "AB-1  " || p.Owner != "postgres" {
		t.Errorf("text fields = %q, %q", p.SKU, p.Owner)
	}
	if p.Thumbnail != nil {
		t.Errorf("Thumbnail = %v, want nil for a NULL", p.Thumbnail)
	}
	if p.Price.String() != "19.90" {
		t.Errorf("Price = %s, want 19.90 with its scale", p.Price)
	}
	if !p.Released.Equal(day) {
		t.Errorf("Released = %v, want %v", p.Released, day)
	}
	// A Null[T] field: present as a valid value, absent as the zero Null —
	// never a zero value that reads as present.
	if want := (pgtype.Null[float64]{Value: 1.25, Valid: true}); p.Weight != want {
		t.Errorf("Weight = %+v, want %+v", p.Weight, want)
	}
	if got[1].Weight.Valid {
		t.Errorf("Weight of a NULL row = %+v, want an invalid Null", got[1].Weight)
	}

	// A timestamp column is refused for the date field: the bytes differ.
	if _, err := entity.BindProduct(productFields(pgtype.OIDTimestampTZ)); err == nil {
		t.Error("a timestamptz column bound to a date field")
	}
}

// The reason Null[T] exists beside *T: a present value costs no allocation,
// where a pointer field costs one per row. Measured here as a difference, so
// the allocations the other fields make (two strings, one bytea) cancel out.
func TestNullFieldDoesNotAllocate(t *testing.T) {
	plan, err := entity.BindProduct(productFields(pgtype.OIDDate))
	if err != nil {
		t.Fatal(err)
	}
	price, _ := pgtype.ParseNumeric("1")
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	dst := make([]entity.Product, 0, 1)
	allocs := func(weight []byte) float64 {
		var n float64
		eachBatch(t, productFields(pgtype.OIDDate), [][][]byte{{dataRow(
			[]byte("A"), []byte("o"), []byte{1}, nil,
			pgtype.AppendNumeric(nil, price), pgtype.AppendDate(nil, day), weight,
		)}}, func(rows *postgres.Rows) {
			n = testing.AllocsPerRun(100, func() {
				if _, err := entity.HydrateProduct(dst[:0], rows, plan); err != nil {
					t.Fatal(err)
				}
			})
		})
		return n
	}
	if extra := allocs(pgtype.AppendFloat8(nil, 1)) - allocs(nil); extra != 0 {
		t.Errorf("a present Null[float64] costs %v allocations per row, want 0", extra)
	}
}
