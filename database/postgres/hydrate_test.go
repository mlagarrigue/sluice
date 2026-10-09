package postgres

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// §8.6 asserts that reflection "works row by row and field by field, which
// breaks vectorization", and concludes that hydration must be generated. That
// is a performance claim, and this file measures it before anything is
// generated on the strength of it — if the gap turns out to be small, the
// spec is what needs correcting, not the code that needs writing.
//
// Three hydrations of the same entity from the same batch:
//
//   - reflection, the shape `database/sql` and every ORM take — with a
//     per-type plan and typed setters, so it pays for reflection and not for
//     boxing (the boxed variant is measured beside it, separately);
//   - row-major, a hand-written loop calling Value and a decoder per field —
//     what a person writes without a generator;
//   - columnar, one pass per column with [ScanInt8] and friends, then one
//     pass to assemble — the shape §8.6 argues for and the one a generator
//     would emit.

type order struct {
	ID      int64
	Total   float64
	Name    string
	Active  bool
	Created time.Time
}

// orderRows builds a batch of n rows matching order's five columns.
func orderRows(n int) *Rows {
	rows := NewRows(5, n)
	for i := range n {
		body := dataRow(
			AppendInt8(nil, int64(i)),
			AppendFloat8(nil, float64(i)*1.5),
			fmt.Appendf(nil, "order-%d", i),
			AppendBool(nil, i%2 == 0),
			AppendTimestampTZ(nil, time.Unix(int64(i), 0).UTC()),
		)
		if err := rows.Append(body); err != nil {
			panic(err)
		}
	}
	return rows
}

// orderKinds is what a reflection-based mapper learns once per type and
// caches: how each field is set. Building it per row would measure the cache
// a real mapper keeps, not reflection.
var orderKinds = func() []reflect.Kind {
	t := reflect.TypeFor[order]()
	kinds := make([]reflect.Kind, t.NumField())
	for i := range kinds {
		kinds[i] = t.Field(i).Type.Kind()
	}
	return kinds
}()

// hydrateReflect is the shape to beat, written the way a careful mapper
// writes it: the field kinds come from a per-type plan, every value is set
// through its typed setter, and nothing is boxed into an interface. What is
// left is the cost reflection itself adds — Value.Index, Value.Field and a
// setter per field — against the same decoders and the same one string
// allocation per row as the hand-written loop.
func hydrateReflect(dst []order, rows *Rows) ([]order, error) {
	n := rows.Len()
	dst = slices.Grow(dst[:0], n)[:n]
	sv := reflect.ValueOf(dst) // once per batch: elements are addressable
	for i := range n {
		ev := sv.Index(i)
		for col, kind := range orderKinds {
			b, isNull := rows.Value(i, col)
			if isNull {
				return nil, ErrNullValue
			}
			f := ev.Field(col)
			switch kind {
			case reflect.Int64:
				x, err := DecodeInt8(b)
				if err != nil {
					return nil, err
				}
				f.SetInt(x)
			case reflect.Float64:
				x, err := DecodeFloat8(b)
				if err != nil {
					return nil, err
				}
				f.SetFloat(x)
			case reflect.String:
				f.SetString(string(b))
			case reflect.Bool:
				x, err := DecodeBool(b)
				if err != nil {
					return nil, err
				}
				f.SetBool(x)
			case reflect.Struct: // time.Time, the only struct field
				x, err := DecodeTimestampTZ(b)
				if err != nil {
					return nil, err
				}
				// Through a pointer: Interface on a pointer does not box,
				// where Set(reflect.ValueOf(x)) allocates per value.
				*f.Addr().Interface().(*time.Time) = x
			}
		}
	}
	return dst, nil
}

// hydrateReflectBoxed is the reflection this file first measured, kept so the
// difference stays visible: a type switch on f.Interface() per field, and a
// reflect.ValueOf per time.Time. Both box a value per field, and the boxing —
// not reflection — was most of the figure once quoted against generated code.
func hydrateReflectBoxed(dst []order, rows *Rows) ([]order, error) {
	dst = dst[:0]
	for i := range rows.Len() {
		var o order
		v := reflect.ValueOf(&o).Elem()
		for col := range rows.Fields() {
			b, isNull := rows.Value(i, col)
			if isNull {
				return nil, ErrNullValue
			}
			f := v.Field(col)
			switch f.Interface().(type) {
			case int64:
				x, err := DecodeInt8(b)
				if err != nil {
					return nil, err
				}
				f.SetInt(x)
			case float64:
				x, err := DecodeFloat8(b)
				if err != nil {
					return nil, err
				}
				f.SetFloat(x)
			case string:
				f.SetString(string(b))
			case bool:
				x, err := DecodeBool(b)
				if err != nil {
					return nil, err
				}
				f.SetBool(x)
			case time.Time:
				x, err := DecodeTimestampTZ(b)
				if err != nil {
					return nil, err
				}
				f.Set(reflect.ValueOf(x))
			}
		}
		dst = append(dst, o)
	}
	return dst, nil
}

// hydrateRowMajor is the same work without reflection: still one row at a
// time, still a different decoder per field.
func hydrateRowMajor(dst []order, rows *Rows) ([]order, error) {
	dst = dst[:0]
	for i := range rows.Len() {
		var o order
		b, isNull := rows.Value(i, 0)
		if isNull {
			return nil, ErrNullValue
		}
		id, err := DecodeInt8(b)
		if err != nil {
			return nil, err
		}
		o.ID = id

		b, _ = rows.Value(i, 1)
		total, err := DecodeFloat8(b)
		if err != nil {
			return nil, err
		}
		o.Total = total

		b, _ = rows.Value(i, 2)
		o.Name = string(b)

		b, _ = rows.Value(i, 3)
		active, err := DecodeBool(b)
		if err != nil {
			return nil, err
		}
		o.Active = active

		b, _ = rows.Value(i, 4)
		created, err := DecodeTimestampTZ(b)
		if err != nil {
			return nil, err
		}
		o.Created = created

		dst = append(dst, o)
	}
	return dst, nil
}

// columnarScratch is the per-column storage generated code would hold and
// reuse across batches.
type columnarScratch struct {
	ids     []int64
	totals  []float64
	names   []string
	actives []bool
	created []time.Time
}

// hydrateColumnar is what a generator would emit: one monomorphic pass per
// column, then one pass to assemble the structs.
func hydrateColumnar(dst []order, rows *Rows, s *columnarScratch) ([]order, error) {
	var err error
	if s.ids, err = ScanInt8(s.ids[:0], rows, 0); err != nil {
		return nil, err
	}
	if s.totals, err = ScanFloat8(s.totals[:0], rows, 1); err != nil {
		return nil, err
	}
	if s.names, err = ScanText(s.names[:0], rows, 2); err != nil {
		return nil, err
	}
	if s.actives, err = ScanBool(s.actives[:0], rows, 3); err != nil {
		return nil, err
	}
	if s.created, err = ScanTimestampTZ(s.created[:0], rows, 4); err != nil {
		return nil, err
	}

	dst = dst[:0]
	for i := range rows.Len() {
		dst = append(dst, order{
			ID:      s.ids[i],
			Total:   s.totals[i],
			Name:    s.names[i],
			Active:  s.actives[i],
			Created: s.created[i],
		})
	}
	return dst, nil
}

// All three must agree, or the benchmark below compares different work.
func TestHydrationsAgree(t *testing.T) {
	rows := orderRows(64)

	byReflect, err := hydrateReflect(nil, rows)
	if err != nil {
		t.Fatalf("reflect: %v", err)
	}
	byBoxed, err := hydrateReflectBoxed(nil, rows)
	if err != nil {
		t.Fatalf("reflect, boxed: %v", err)
	}
	byRow, err := hydrateRowMajor(nil, rows)
	if err != nil {
		t.Fatalf("row-major: %v", err)
	}
	byCol, err := hydrateColumnar(nil, rows, &columnarScratch{})
	if err != nil {
		t.Fatalf("columnar: %v", err)
	}

	if len(byReflect) != 64 || len(byBoxed) != 64 || len(byRow) != 64 || len(byCol) != 64 {
		t.Fatalf("lengths differ: %d, %d, %d, %d", len(byReflect), len(byBoxed), len(byRow), len(byCol))
	}
	for i := range byReflect {
		if byReflect[i] != byRow[i] || byBoxed[i] != byRow[i] || byRow[i] != byCol[i] {
			t.Fatalf("row %d differs:\n reflect %+v\n row-major %+v\n columnar %+v",
				i, byReflect[i], byRow[i], byCol[i])
		}
	}
	// And the values are the ones the batch was built from — both parities of
	// the bool, so a scanner returning a constant would not pass.
	if byCol[7].ID != 7 || byCol[7].Name != "order-7" || byCol[7].Active {
		t.Errorf("row 7 = %+v, want ID 7, name order-7, inactive", byCol[7])
	}
	if byCol[8].ID != 8 || !byCol[8].Active {
		t.Errorf("row 8 = %+v, want ID 8, active", byCol[8])
	}
	if want := time.Unix(8, 0).UTC(); !byCol[8].Created.Equal(want) {
		t.Errorf("row 8 Created = %v, want %v", byCol[8].Created, want)
	}
}

// A NULL where a scanner was asked for a value is an error, not a zero: a
// silent zero would turn a missing value into a real one.
// Scanners reuse the caller's slice, which is what makes hydration
// allocation-free after the first batch.
func TestScannersReuseSlices(t *testing.T) {
	rows := orderRows(128)
	s := &columnarScratch{}
	if _, err := hydrateColumnar(nil, rows, s); err != nil {
		t.Fatal(err)
	}
	capBefore := cap(s.ids)
	for range 3 {
		if _, err := hydrateColumnar(nil, rows, s); err != nil {
			t.Fatal(err)
		}
	}
	if cap(s.ids) != capBefore {
		t.Errorf("column storage regrew from %d to %d", capBefore, cap(s.ids))
	}
}

const hydrateBatch = 1024

func BenchmarkHydrateReflect(b *testing.B) {
	rows := orderRows(hydrateBatch)
	dst := make([]order, 0, hydrateBatch)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateReflect(dst, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*hydrateBatch), "ns/row")
}

func BenchmarkHydrateReflectBoxed(b *testing.B) {
	rows := orderRows(hydrateBatch)
	dst := make([]order, 0, hydrateBatch)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateReflectBoxed(dst, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*hydrateBatch), "ns/row")
}

func BenchmarkHydrateRowMajor(b *testing.B) {
	rows := orderRows(hydrateBatch)
	dst := make([]order, 0, hydrateBatch)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateRowMajor(dst, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*hydrateBatch), "ns/row")
}

func BenchmarkHydrateColumnar(b *testing.B) {
	rows := orderRows(hydrateBatch)
	dst := make([]order, 0, hydrateBatch)
	s := &columnarScratch{}
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateColumnar(dst, rows, s)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*hydrateBatch), "ns/row")
}

// The same three over a batch of one, which is what the web vertical's
// request path looks like: the columnar form's per-column setup has nothing to
// amortize there, and the figure says whether that matters.
func BenchmarkHydrateColumnarBatchOne(b *testing.B) {
	rows := orderRows(1)
	dst := make([]order, 0, 1)
	s := &columnarScratch{}
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateColumnar(dst, rows, s)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/row")
}

func BenchmarkHydrateRowMajorBatchOne(b *testing.B) {
	rows := orderRows(1)
	dst := make([]order, 0, 1)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = hydrateRowMajor(dst, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/row")
}

// Row.Rows is the bridge from a Query's batch of Row views to the scanners and
// generated hydration functions, which take the whole *Rows: every view of one
// batch reaches the same Rows, and a view retained past its query panics the
// same way Value does rather than reading the next result's bytes.
func TestRowReachesItsRows(t *testing.T) {
	rows := NewRows(1, 4)
	for i := range 3 {
		if err := rows.Append(dataRow(AppendInt8(nil, int64(i)))); err != nil {
			t.Fatal(err)
		}
	}
	c := &Conn{}
	var got *Rows
	c.emitRows(rows, nil, func(b sluice.Batch[Row]) bool {
		got = b.Items[0].Rows()
		for _, r := range b.Items {
			if r.Rows() != got {
				t.Fatal("two rows of one batch view different Rows")
			}
		}
		dst, err := ScanInt8(nil, got, 0)
		if err != nil || len(dst) != 3 || dst[2] != 2 {
			t.Fatalf("ScanInt8 over Row.Rows(): %v %v", dst, err)
		}
		return true
	})
	if got != rows {
		t.Fatal("Row.Rows() is not the batch's Rows")
	}
	// Retained past the query: the accumulator moved on.
	stale := Row{rows: rows, idx: 0, gen: rows.gen - 1}
	defer func() {
		if r := recover(); r != ErrRowRetained { //nolint:errorlint // a panic value, compared by identity
			t.Fatalf("a retained Row's Rows() panicked with %v, want ErrRowRetained", r)
		}
	}()
	_ = stale.Rows()
}
