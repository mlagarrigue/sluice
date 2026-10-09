package orders_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/internal/pgfake"
	"github.com/mlagarrigue/sluice/example/orders"
)

// memTable is order_lines held in memory, answering PGStore's two statements
// through a fake backend: the SELECT by decoding its key arrays, the UPDATE by
// applying its three. What runs is PGStore's real protocol path — parameters
// encoded, rows decoded, the batch-sized portal — with no server behind it.
type memTable struct {
	mu    sync.Mutex
	lines map[orders.LineKey]orders.Line
	fail  string // a SQL substring whose next statement fails
	// nullCol, when ≥ 0, makes that column NULL in every row returned.
	nullCol int
}

func (m *memTable) exec(t *testing.T) func(string, [][]byte, map[string]string) pgfake.Result {
	t.Helper()
	return func(sql string, params [][]byte, _ map[string]string) pgfake.Result {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.fail != "" && strings.Contains(sql, m.fail) {
			m.fail = ""
			return pgfake.Result{Err: "simulated failure"}
		}
		switch {
		case strings.Contains(sql, "SELECT"):
			ids, err := postgres.DecodeInt8Array(nil, params[0])
			if err != nil {
				t.Errorf("order ids: %v", err)
			}
			idx, err := postgres.DecodeInt4Array(nil, params[1])
			if err != nil {
				t.Errorf("line indexes: %v", err)
			}
			res := pgfake.Result{
				Columns: []string{"order_id", "line_index", "sale_qty", "price"},
				OIDs:    []uint32{postgres.OIDInt8, postgres.OIDInt4, postgres.OIDInt8, postgres.OIDInt8},
			}
			for i := range ids {
				l, ok := m.lines[orders.LineKey{OrderID: ids[i], Index: int(idx[i])}]
				if !ok {
					continue
				}
				row := [][]byte{
					postgres.AppendInt8(nil, l.OrderID),
					postgres.AppendInt4(nil, int32(l.Index)), //nolint:gosec // a test line index
					postgres.AppendInt8(nil, l.SaleQty),
					postgres.AppendInt8(nil, l.Price),
				}
				if m.nullCol >= 0 {
					row[m.nullCol] = nil
				}
				res.Rows = append(res.Rows, row)
			}
			return res
		case strings.Contains(sql, "UPDATE"):
			ids, _ := postgres.DecodeInt8Array(nil, params[0])
			idx, _ := postgres.DecodeInt4Array(nil, params[1])
			qty, _ := postgres.DecodeInt8Array(nil, params[2])
			for i := range ids {
				k := orders.LineKey{OrderID: ids[i], Index: int(idx[i])}
				if l, ok := m.lines[k]; ok {
					l.SaleQty = qty[i]
					m.lines[k] = l
				}
			}
			return pgfake.Result{Tag: "UPDATE " + strconv.Itoa(len(ids))}
		}
		return pgfake.Result{}
	}
}

func newMemTable() *memTable {
	return &memTable{nullCol: -1, lines: map[orders.LineKey]orders.Line{
		{OrderID: 1, Index: 0}: {OrderID: 1, Index: 0, SaleQty: 5, Price: 1000},
		{OrderID: 1, Index: 1}: {OrderID: 1, Index: 1, SaleQty: 3, Price: 500},
		{OrderID: 2, Index: 0}: {OrderID: 2, Index: 0, SaleQty: 7, Price: 2000},
	}}
}

func pgStore(t *testing.T, m *memTable) *orders.PGStore {
	t.Helper()
	return orders.NewPGStore((&pgfake.Backend{Exec: m.exec(t)}).Conn(t))
}

func TestPGStoreLoadLines(t *testing.T) {
	m := newMemTable()
	s := pgStore(t, m)
	ctx := t.Context()

	got, err := s.LoadLines(ctx, []orders.LineKey{{OrderID: 1, Index: 0}, {OrderID: 2, Index: 0}, {OrderID: 9, Index: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("%d lines, want 2 (the missing key is absent): %+v", len(got), got)
	}
	if l := got[orders.LineKey{OrderID: 2, Index: 0}]; l.SaleQty != 7 || l.Price != 2000 {
		t.Errorf("line 2/0 = %+v", l)
	}

	if got, err := s.LoadLines(ctx, nil); got != nil || err != nil {
		t.Errorf("no keys: %v, %v; want nothing and no round trip", got, err)
	}
}

func TestPGStoreLoadLinesFailures(t *testing.T) {
	keys := []orders.LineKey{{OrderID: 1, Index: 0}, {OrderID: 1, Index: 1}}
	for _, tc := range []struct {
		name string
		set  func(*memTable)
		want string
	}{
		{"the query fails", func(m *memTable) { m.fail = "SELECT" }, "loading 2 lines"},
		{"order_id is NULL", func(m *memTable) { m.nullCol = 0 }, `"order_id" is NULL`},
		{"line_index is NULL", func(m *memTable) { m.nullCol = 1 }, `"line_index" is NULL`},
		{"sale_qty is NULL", func(m *memTable) { m.nullCol = 2 }, `"sale_qty" is NULL`},
		{"price is NULL", func(m *memTable) { m.nullCol = 3 }, `"price" is NULL`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMemTable()
			tc.set(m)
			s := pgStore(t, m)
			_, err := s.LoadLines(t.Context(), keys)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one mentioning %s", err, tc.want)
			}
			// The connection is still usable for the next batch.
			m.mu.Lock()
			m.nullCol = -1
			m.mu.Unlock()
			if got, err := s.LoadLines(t.Context(), keys); err != nil || len(got) != 2 {
				t.Errorf("after the failure: %v, %v", got, err)
			}
		})
	}
}

func TestPGStoreSaveLines(t *testing.T) {
	m := newMemTable()
	s := pgStore(t, m)
	ctx := t.Context()

	if err := s.SaveLines(ctx, nil); err != nil {
		t.Errorf("no lines: %v", err)
	}
	if err := s.SaveLines(ctx, []orders.Line{{OrderID: 1, Index: 1, SaleQty: 9}}); err != nil {
		t.Fatal(err)
	}
	if got := m.lines[orders.LineKey{OrderID: 1, Index: 1}].SaleQty; got != 9 {
		t.Errorf("sale_qty = %d after the save, want 9", got)
	}

	m.fail = "UPDATE"
	if err := s.SaveLines(ctx, []orders.Line{{OrderID: 1, Index: 1, SaleQty: 4}}); err == nil || !strings.Contains(err.Error(), "saving 1 lines") {
		t.Errorf("err = %v, want the save's failure", err)
	}
}

// The whole vertical over PGStore: concurrent callers, one gateway, the real
// connector's protocol path, and what the table holds afterwards.
func TestVerticalOverPGStore(t *testing.T) {
	m := newMemTable()
	svc := orders.New(pgStore(t, m), 16, time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i, a := range []orders.Amendment{
		{OrderID: 1, Index: 0, SaleQty: 6},
		{OrderID: 2, Index: 0, SaleQty: 8},
	} {
		wg.Go(func() {
			res, err := svc.Apply(ctx, a)
			if err != nil || !res.Applied {
				t.Errorf("amendment %d: %+v, %v", i, res, err)
			}
		})
	}
	wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lines[orders.LineKey{OrderID: 1, Index: 0}].SaleQty != 6 || m.lines[orders.LineKey{OrderID: 2, Index: 0}].SaleQty != 8 {
		t.Errorf("table after the vertical: %+v", m.lines)
	}
}
