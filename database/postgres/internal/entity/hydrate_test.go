package entity_test

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/internal/entity"
)

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
		{Name: "created_at", TypeOID: postgres.OIDTimestampTZ},
		{Name: "total_amount", TypeOID: postgres.OIDFloat8},
		{Name: "id", TypeOID: postgres.OIDInt8},
		{Name: "note", TypeOID: postgres.OIDText},
		{Name: "is_active", TypeOID: postgres.OIDBool},
		{Name: "label", TypeOID: postgres.OIDVarchar},
	}
}

func orderRow(id int64, total float64, at time.Time, note *string, active bool, label string) []byte {
	var noteBytes []byte
	if note != nil {
		noteBytes = []byte(*note)
	}
	return dataRow(
		postgres.AppendTimestampTZ(nil, at),
		postgres.AppendFloat8(nil, total),
		postgres.AppendInt8(nil, id),
		noteBytes,
		postgres.AppendBool(nil, active),
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
	rows := postgres.NewRows(6, 8)
	if err := rows.Append(orderRow(1, 12.5, at, &note, true, "first")); err != nil {
		t.Fatal(err)
	}
	if err := rows.Append(orderRow(2, 0, at, nil, false, "second")); err != nil {
		t.Fatal(err)
	}

	orders, err := entity.HydrateOrder(nil, rows, plan)
	if err != nil {
		t.Fatalf("HydrateOrder returned %v", err)
	}
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
			fields[i].TypeOID = postgres.OIDInt4 // altered from bigint in a migration
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
	fields := append(orderFields(), postgres.Field{Name: "id", TypeOID: postgres.OIDInt8})
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
	rows := postgres.NewRows(6, 4)
	// A NULL id, which Order declares as a plain int64.
	body := dataRow(
		postgres.AppendTimestampTZ(nil, time.Now()),
		postgres.AppendFloat8(nil, 1),
		nil,
		nil,
		postgres.AppendBool(nil, true),
		[]byte("x"),
	)
	if err := rows.Append(body); err != nil {
		t.Fatal(err)
	}
	if _, err := entity.HydrateOrder(nil, rows, plan); err == nil {
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

	// One good batch, so the caller has a buffer worth keeping.
	rows := postgres.NewRows(6, 4)
	if err := rows.Append(orderRow(1, 10, time.Now().UTC(), nil, true, "a")); err != nil {
		t.Fatal(err)
	}
	dst, err := entity.HydrateOrder(nil, rows, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(dst) != 1 {
		t.Fatalf("the first batch produced %d orders, want 1", len(dst))
	}
	before, capBefore := len(dst), cap(dst)

	// A second batch whose second row is unhydratable.
	rows.Reset()
	if err := rows.Append(orderRow(2, 20, time.Now().UTC(), nil, true, "b")); err != nil {
		t.Fatal(err)
	}
	bad := dataRow(
		postgres.AppendTimestampTZ(nil, time.Now().UTC()),
		postgres.AppendFloat8(nil, 30),
		nil, // a NULL id, which Order cannot hold
		nil,
		postgres.AppendBool(nil, true),
		[]byte("c"),
	)
	if err := rows.Append(bad); err != nil {
		t.Fatal(err)
	}

	dst, err = entity.HydrateOrder(dst, rows, plan)
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
	rows := postgres.NewRows(6, n)
	for i := range n {
		if err := rows.Append(orderRow(int64(i), float64(i), at, &note, i%2 == 0, "label")); err != nil {
			b.Fatal(err)
		}
	}

	dst := make([]entity.Order, 0, n)
	b.ReportAllocs()
	for b.Loop() {
		dst, err = entity.HydrateOrder(dst[:0], rows, plan)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
}

// The mappings Order does not exercise, end to end through the committed
// generated code: char(n) and name into string, bytea copied out of the row
// buffer, numeric kept exact, and a date column through the `date` option.
func TestGeneratedProductHydrator(t *testing.T) {
	plan, err := entity.BindProduct([]postgres.Field{
		{Name: "sku", TypeOID: postgres.OIDBPChar},
		{Name: "owner", TypeOID: postgres.OIDName},
		{Name: "image", TypeOID: postgres.OIDBytea},
		{Name: "thumbnail", TypeOID: postgres.OIDBytea},
		{Name: "price", TypeOID: postgres.OIDNumeric},
		{Name: "released", TypeOID: postgres.OIDDate},
	})
	if err != nil {
		t.Fatalf("BindProduct returned %v", err)
	}
	price, err := postgres.ParseNumeric("19.90")
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	rows := postgres.NewRows(6, 4)
	if err := rows.Append(dataRow(
		[]byte("AB-1  "), []byte("postgres"), []byte{1, 2, 3}, nil,
		postgres.AppendNumeric(nil, price), postgres.AppendDate(nil, day),
	)); err != nil {
		t.Fatal(err)
	}
	got, err := entity.HydrateProduct(nil, rows, plan)
	if err != nil {
		t.Fatalf("HydrateProduct returned %v", err)
	}
	p := got[0]
	if p.SKU != "AB-1  " || p.Owner != "postgres" {
		t.Errorf("text fields = %q, %q", p.SKU, p.Owner)
	}
	if string(p.Image) != "\x01\x02\x03" || p.Thumbnail != nil {
		t.Errorf("bytea fields = %v, %v", p.Image, p.Thumbnail)
	}
	if p.Price.String() != "19.90" {
		t.Errorf("Price = %s, want 19.90 with its scale", p.Price)
	}
	if !p.Released.Equal(day) {
		t.Errorf("Released = %v, want %v", p.Released, day)
	}

	// The bytes are the field's own: the row buffer is reused for the next
	// batch, and a []byte that aliased it would change under the caller.
	rows.Reset()
	if err := rows.Append(dataRow(
		[]byte("ZZ-9  "), []byte("other"), []byte{9, 9, 9}, []byte{7},
		postgres.AppendNumeric(nil, price), postgres.AppendDate(nil, day),
	)); err != nil {
		t.Fatal(err)
	}
	if string(p.Image) != "\x01\x02\x03" {
		t.Errorf("Image changed with the row buffer: %v", p.Image)
	}

	// A timestamp column is refused for the date field: the bytes differ.
	if _, err := entity.BindProduct([]postgres.Field{
		{Name: "sku", TypeOID: postgres.OIDText},
		{Name: "owner", TypeOID: postgres.OIDText},
		{Name: "image", TypeOID: postgres.OIDBytea},
		{Name: "thumbnail", TypeOID: postgres.OIDBytea},
		{Name: "price", TypeOID: postgres.OIDNumeric},
		{Name: "released", TypeOID: postgres.OIDTimestampTZ},
	}); err == nil {
		t.Error("a timestamptz column bound to a date field")
	}
}
