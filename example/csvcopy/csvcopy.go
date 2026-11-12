// Package csvcopy loads a CSV file into PostgreSQL with no other system in
// between: the file is read a batch of records at a time by csvstream, each
// record is typed by the core's operators, and each batch leaves in one
// binary COPY call.
//
// The file is "id,customer,total" with a header line, the total in cents:
//
//	id,customer,total
//	1,ACME,1250
//	2,Globex,99000
//
// In plain words: a million lines become a thousand COPY calls of a thousand
// rows, in one transaction, and a line that cannot be read rolls the whole
// load back — the table holds the file or nothing of it.
//
// The guide: docs/guide/files.md.
package csvcopy

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/encoding/csvstream"
)

// Order is one line of the file, typed.
type Order struct {
	ID       int64
	Customer string
	Cents    int64
}

// row is a typed record, or why the record could not be typed: Convert cannot
// fail, so the error travels with the element to the one place that stops
// the load.
type row struct {
	order  Order
	record int
	err    error
}

// Copy is the statement Load runs; the table is the caller's to create.
const Copy = "COPY orders (id, customer, total_cents) FROM STDIN WITH (FORMAT BINARY)"

// Load copies the orders of r into the orders table and reports how many
// rows it wrote. It runs in one transaction: on any error — a record
// csvstream cannot read, a field that is not a number, a row the server
// refuses — the copy is aborted and nothing of r is left in the table.
func Load(ctx context.Context, conn *postgres.Conn, r io.Reader) (int64, error) {
	src := csvstream.Read(r, csvstream.ReadConfig{FieldsPerRecord: 3})
	n := 0
	typed := sluice.Convert(sluice.Drop(src.Stream(), 1), func(rec []string) row {
		n++
		return parse(rec, n+1) // the header is record 1
	})

	// RowsPerTx past any file this example loads: one transaction, so an
	// abort leaves nothing behind. A loader of unbounded files would choose a
	// size and resume after the last committed batch instead.
	cp, err := conn.CopyFrom(ctx, Copy, postgres.CopyConfig{RowsPerTx: 1 << 40})
	if err != nil {
		return 0, err
	}
	var tuples, scratch []byte
	var loadErr error
	for b := range typed {
		tuples = tuples[:0]
		for _, r := range b.Items {
			if r.err != nil {
				loadErr = fmt.Errorf("record %d: %w", r.record, r.err)
				break
			}
			tuples = postgres.AppendTupleHeader(tuples, 3)
			scratch = pgtype.AppendInt8(scratch[:0], r.order.ID)
			tuples = postgres.AppendField(tuples, scratch)
			scratch = pgtype.AppendText(scratch[:0], r.order.Customer)
			tuples = postgres.AppendField(tuples, scratch)
			scratch = pgtype.AppendInt8(scratch[:0], r.order.Cents)
			tuples = postgres.AppendField(tuples, scratch)
		}
		if loadErr != nil {
			break
		}
		if loadErr = cp.WriteTuples(tuples, len(b.Items)); loadErr != nil {
			break
		}
	}
	if loadErr == nil {
		loadErr = src.Err()
	}
	if loadErr != nil {
		_ = cp.Abort(loadErr.Error())
		return 0, loadErr
	}
	if err := cp.Close(ctx); err != nil {
		return 0, err
	}
	return cp.Rows(), nil
}

func parse(rec []string, record int) row {
	id, err := strconv.ParseInt(rec[0], 10, 64)
	if err != nil {
		return row{record: record, err: fmt.Errorf("id: %w", err)}
	}
	cents, err := strconv.ParseInt(rec[2], 10, 64)
	if err != nil {
		return row{record: record, err: fmt.Errorf("total: %w", err)}
	}
	return row{order: Order{ID: id, Customer: strings.TrimSpace(rec[1]), Cents: cents}, record: record}
}
