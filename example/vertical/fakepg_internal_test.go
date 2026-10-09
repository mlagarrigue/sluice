package vertical

import (
	"strings"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/internal/pgfake"
)

// fakeRow is one order_lines row as the fake backend stores it. nullSale
// makes sale_qty NULL, for the decode-failure path.
type fakeRow struct {
	tenant   string
	line     Line
	nullSale bool
}

// fakePG runs the vertical's statements against rows held in memory, under
// a row-level-security policy it enforces itself: a row exists only for the
// app.tenant_id the pipeline set, exactly as the real policy in
// vertical_test.go decides.
type fakePG struct {
	rows []fakeRow

	mu     sync.Mutex
	failOn map[string]int // SQL substring → how many statements to fail
}

func (f *fakePG) fail(sqlPart string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOn == nil {
		f.failOn = map[string]int{}
	}
	f.failOn[sqlPart]++
}

func (f *fakePG) conn(t *testing.T) *postgres.Conn {
	t.Helper()
	return (&pgfake.Backend{Exec: f.exec}).Conn(t)
}

func (f *fakePG) exec(sql string, _ [][]byte, settings map[string]string) pgfake.Result {
	f.mu.Lock()
	for k, n := range f.failOn {
		if n > 0 && strings.Contains(sql, k) {
			f.failOn[k] = n - 1
			f.mu.Unlock()
			return pgfake.Result{Err: "simulated failure"}
		}
	}
	f.mu.Unlock()
	if !strings.Contains(sql, "order_lines") {
		return pgfake.Result{}
	}
	res := pgfake.Result{
		Columns: []string{"order_id", "line_index", "sale_qty", "price"},
		OIDs:    []uint32{postgres.OIDInt8, postgres.OIDInt4, postgres.OIDInt8, postgres.OIDInt8},
	}
	tenant := settings["app.tenant_id"]
	for _, r := range f.rows {
		if r.tenant != tenant {
			continue
		}
		sale := postgres.AppendInt8(nil, r.line.SaleQty)
		if r.nullSale {
			sale = nil
		}
		res.Rows = append(res.Rows, [][]byte{
			postgres.AppendInt8(nil, r.line.OrderID),
			postgres.AppendInt4(nil, int32(r.line.Index)), //nolint:gosec // a test line index
			sale,
			postgres.AppendInt8(nil, r.line.Price),
		})
	}
	return res
}
