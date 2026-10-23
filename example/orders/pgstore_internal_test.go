package orders

import (
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// Decoding a row allocates nothing. The hand-written decoder used to build a
// table of four closures per row, five allocations in all, which in a loop
// over a batch is per-element cost the batch model exists to remove.
func TestDecodeLineDoesNotAllocate(t *testing.T) {
	cols := [][]byte{
		pgtype.AppendInt8(nil, 7),
		pgtype.AppendInt4(nil, 2),
		pgtype.AppendInt8(nil, 5),
		pgtype.AppendInt8(nil, 1250),
	}
	value := func(col int) ([]byte, bool) { return cols[col], false }

	l, err := decodeLine(value)
	if err != nil {
		t.Fatal(err)
	}
	if l != (Line{OrderID: 7, Index: 2, SaleQty: 5, Price: 1250}) {
		t.Fatalf("decoded %+v", l)
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = decodeLine(value) }); n != 0 {
		t.Errorf("decodeLine allocates %v times per row, want 0", n)
	}

	null := func(col int) ([]byte, bool) { return cols[col], col == 2 }
	if _, err := decodeLine(null); err == nil {
		t.Error("a NULL sale_qty decoded without an error")
	}
}
