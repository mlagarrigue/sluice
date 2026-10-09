package orders_test

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/orders"
)

// The §1.1 vertical, end to end, against a server that can disagree.
//
// Everything else about this example runs on an in-memory store, and the claim
// that a store backed by the connector satisfies the same interface has until
// now been checked by the compiler alone — the comment on
// TestPGStoreSatisfiesStore says so, and says why: running it needs a server.
//
// This runs it. Concurrent callers reach one gateway, the gateway batches them
// into one pipeline pass, the pipeline reads and writes through the real
// connector, and the assertion is what PostgreSQL actually holds afterwards.
// That is the whole architecture in one test, and the only one where every
// layer is the real one.
//
//	SLUICE_PG=127.0.0.1:5432 SLUICE_PG_USER=sluice SLUICE_PG_DB=sluice \
//	  SLUICE_PG_PASSWORD=secret go test ./example/orders/ -run TestIntegration
func TestIntegrationVerticalAgainstPostgres(t *testing.T) {
	conn := dialOrders(t)

	const ddl = `CREATE TEMP TABLE order_lines (
		order_id   bigint  NOT NULL,
		line_index int     NOT NULL,
		sale_qty   bigint  NOT NULL,
		price      bigint  NOT NULL,
		PRIMARY KEY (order_id, line_index))`
	if err := execOrders(t, conn, ddl); err != nil {
		t.Fatalf("creating the table: %v", err)
	}

	const n = 32
	// One INSERT carrying every line, because that is the shape this project
	// argues for even in its own fixtures.
	insert := "INSERT INTO order_lines (order_id, line_index, sale_qty, price) " +
		"SELECT 1, i, 1, 100 + i FROM generate_series(0, " + strconv.Itoa(n-1) + ") AS i"
	if err := execOrders(t, conn, insert); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	store := orders.NewPGStore(conn)
	svc := orders.New(store, n, 50*time.Millisecond)
	defer func() { _ = svc.Close() }()

	// Every caller arrives on its own goroutine, which is the whole point: the
	// gateway is what turns them into one batch, and the connector is what
	// turns that batch into one round trip.
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Go(func() {
			got, err := svc.Apply(t.Context(), orders.Amendment{
				OrderID: 1, Index: i, SaleQty: int64(i + 1),
			})
			if err != nil {
				errs <- fmt.Errorf("request %d: %w", i, err)
				return
			}
			if !got.Applied {
				errs <- fmt.Errorf("request %d was not applied: %+v", i, got)
				return
			}
			if got.Line.SaleQty != int64(i+1) {
				errs <- fmt.Errorf("request %d returned qty %d, want %d", i, got.Line.SaleQty, i+1)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The database is the assertion. Everything above could agree with itself
	// and still have written nothing.
	src := conn.Query(t.Context(),
		"SELECT line_index, sale_qty FROM order_lines WHERE order_id = 1 ORDER BY line_index",
		nil, postgres.QueryConfig{BatchRows: 16})

	seen := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			idx, _ := postgres.DecodeInt4(v)
			v, _ = row.Value(1)
			qty, _ := postgres.DecodeInt8(v)
			if want := int64(idx) + 1; qty != want {
				t.Errorf("line %d holds qty %d, want %d", idx, qty, want)
			}
			seen++
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if seen != n {
		t.Errorf("read back %d lines, want %d", seen, n)
	}
}

// A line the amendment names and the table does not: the diagnostic must come
// back from real SQL rather than from a fixture that agreed to be missing.
func TestIntegrationMissingLineIsCriticalFromRealSQL(t *testing.T) {
	conn := dialOrders(t)

	const ddl = `CREATE TEMP TABLE order_lines (
		order_id bigint NOT NULL, line_index int NOT NULL,
		sale_qty bigint NOT NULL, price bigint NOT NULL,
		PRIMARY KEY (order_id, line_index))`
	if err := execOrders(t, conn, ddl); err != nil {
		t.Fatal(err)
	}

	svc := orders.New(orders.NewPGStore(conn), 8, 20*time.Millisecond)
	defer func() { _ = svc.Close() }()

	got, err := svc.Apply(t.Context(), orders.Amendment{OrderID: 99, Index: 0, SaleQty: 5})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got.Applied {
		t.Fatal("an amendment to a line that does not exist was applied")
	}
	if len(got.Diagnostics) == 0 {
		t.Fatal("no diagnostic came back for a missing line")
	}
}

func dialOrders(t *testing.T) *postgres.Conn {
	t.Helper()
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		t.Skip("SLUICE_PG is not set: no server to talk to")
	}
	cfg := postgres.StartupConfig{
		User:     envOr("SLUICE_PG_USER", "postgres"),
		Password: os.Getenv("SLUICE_PG_PASSWORD"),
	}
	cfg.Database = envOr("SLUICE_PG_DB", cfg.User)

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	if err := nc.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := postgres.Startup(nc, cfg)
	if err != nil {
		t.Fatalf("connecting as %s: %v", cfg.User, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// execOrders runs a statement for its effect, draining whatever it returns.
func execOrders(t *testing.T, conn *postgres.Conn, sql string) error {
	t.Helper()
	src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{BatchRows: 4})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	return src.Err()
}
