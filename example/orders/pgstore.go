package orders

import (
	"context"
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// PGStore is [Store] backed by the connector, and it is the point where the
// library's two halves meet: the gateway gathers concurrent requests into a
// batch, and the batch becomes one `= ANY($1)` rather than sixty-four queries.
//
// Until this existed, that seam was a sentence in a comment. It is compiled
// now, which is the difference between a claim and a check — running it needs
// a server, but a signature that stopped matching, a codec that changed shape
// or a query helper that was renamed would break the build.
//
// # One connection is enough, and that is not a coincidence
//
// A [postgres.Conn] is one conversation and is not safe for concurrent use.
// The gateway runs its pipeline on a single goroutine, so a store built for it
// needs exactly one connection and no pool — the batching that makes the
// queries efficient is the same property that makes the concurrency question
// disappear. A service wanting several pipelines gives each its own gateway
// and its own connection, which is a pool with the policy left where the
// application can see it.
type PGStore struct {
	conn *postgres.Conn

	// Buffers reused across every batch, which is what the batch model is for.
	orderIDs []int64
	indexes  []int32
}

// NewPGStore returns a store over an established connection.
func NewPGStore(conn *postgres.Conn) *PGStore { return &PGStore{conn: conn} }

// loadSQL pairs the two key arrays positionally, so a batch of sixty-four
// (order, line) pairs travels as two parameters and is matched in one pass.
//
// The alternative — sixty-four `OR (order_id = $n AND line_index = $m)` terms —
// is a different query text per batch size, which defeats the statement cache
// and grows the parse time with the batch. `unnest` of two arrays is one
// statement whatever the batch holds.
const loadSQL = `
SELECT o.order_id, o.line_index, o.sale_qty, o.price
FROM order_lines o
JOIN unnest($1::bigint[], $2::int[]) AS k(order_id, line_index)
  ON o.order_id = k.order_id AND o.line_index = k.line_index`

// LoadLines reads every requested line in one round trip.
func (s *PGStore) LoadLines(ctx context.Context, keys []LineKey) (map[LineKey]Line, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.orderIDs = s.orderIDs[:0]
	s.indexes = s.indexes[:0]
	for _, k := range keys {
		s.orderIDs = append(s.orderIDs, k.OrderID)
		s.indexes = append(s.indexes, int32(k.Index)) //nolint:gosec // G115: a line index within a batch
	}

	src := s.conn.Query(ctx, loadSQL,
		[][]byte{
			postgres.AppendInt8Array(nil, s.orderIDs),
			postgres.AppendInt4Array(nil, s.indexes),
		},
		postgres.QueryConfig{
			BatchRows: len(keys),
			ParamOIDs: []uint32{postgres.OIDInt8Array, postgres.OIDInt4Array},
		})

	out := make(map[LineKey]Line, len(keys))
	var decodeErr error
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			line, err := decodeLine(row.Value)
			if err != nil {
				decodeErr = err
				return false
			}
			out[LineKey{OrderID: line.OrderID, Index: line.Index}] = line
		}
		return true
	})
	if decodeErr != nil {
		return nil, decodeErr
	}
	// Consume, then check: a query that failed to start produced no row to
	// carry an error, and one that failed at the end produced all of them.
	if err := src.Err(); err != nil {
		return nil, fmt.Errorf("loading %d lines: %w", len(keys), err)
	}
	return out, nil
}

// decodeLine is what a generated hydrator would replace: `sluicegen` emits this
// loop, bound by column name and with the type OIDs checked when the result
// opens. Written by hand here so the example stays one package.
//
// It is straight-line code on purpose. A table of per-column closures reads
// well and cost five allocations a row — the closures capture l, so they and
// the table escape — which in a loop over a thousand-row batch is the cost
// the batch exists to remove. value is the row's Value method, which is all
// decoding needs and lets a test supply a row without a connection.
func decodeLine(value func(col int) ([]byte, bool)) (Line, error) {
	var l Line
	var err error
	if l.OrderID, err = int8Column(value, 0, "order_id"); err != nil {
		return l, err
	}
	b, isNull := value(1)
	if isNull {
		return l, nullColumn("line_index")
	}
	idx, err := postgres.DecodeInt4(b)
	if err != nil {
		return l, fmt.Errorf("column %q: %w", "line_index", err)
	}
	l.Index = int(idx)
	if l.SaleQty, err = int8Column(value, 2, "sale_qty"); err != nil {
		return l, err
	}
	if l.Price, err = int8Column(value, 3, "price"); err != nil {
		return l, err
	}
	return l, nil
}

func int8Column(value func(int) ([]byte, bool), col int, name string) (int64, error) {
	b, isNull := value(col)
	if isNull {
		return 0, nullColumn(name)
	}
	v, err := postgres.DecodeInt8(b)
	if err != nil {
		return 0, fmt.Errorf("column %q: %w", name, err)
	}
	return v, nil
}

func nullColumn(name string) error {
	return fmt.Errorf("column %q is NULL and orders.Line cannot hold it", name)
}

// saveSQL updates every amended line in one statement, the same way the read
// reads them: the batch is the unit, not the row.
const saveSQL = `
UPDATE order_lines o
SET sale_qty = k.sale_qty
FROM unnest($1::bigint[], $2::int[], $3::bigint[]) AS k(order_id, line_index, sale_qty)
WHERE o.order_id = k.order_id AND o.line_index = k.line_index`

// SaveLines writes every amended line in one round trip.
func (s *PGStore) SaveLines(ctx context.Context, lines []Line) error {
	if len(lines) == 0 {
		return nil
	}
	s.orderIDs = s.orderIDs[:0]
	s.indexes = s.indexes[:0]
	qty := make([]int64, 0, len(lines))
	for _, l := range lines {
		s.orderIDs = append(s.orderIDs, l.OrderID)
		s.indexes = append(s.indexes, int32(l.Index)) //nolint:gosec // G115: a line index within a batch
		qty = append(qty, l.SaleQty)
	}

	src := s.conn.Query(ctx, saveSQL,
		[][]byte{
			postgres.AppendInt8Array(nil, s.orderIDs),
			postgres.AppendInt4Array(nil, s.indexes),
			postgres.AppendInt8Array(nil, qty),
		},
		postgres.QueryConfig{
			BatchRows: 1,
			ParamOIDs: []uint32{postgres.OIDInt8Array, postgres.OIDInt4Array, postgres.OIDInt8Array},
		})

	// An UPDATE returns no rows; consuming the stream is what runs it.
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	if err := src.Err(); err != nil {
		return fmt.Errorf("saving %d lines: %w", len(lines), err)
	}
	return nil
}
