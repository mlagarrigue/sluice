package postgres_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// What a savepoint per element costs, which §0.3 names and nobody had measured.
//
// The brief's warning: each savepoint is a subtransaction, PostgreSQL caches a
// fixed number of them per backend, and past that the cache overflows and
// visibility checks start going to shared storage. With a batch size of
// sixty-four and one savepoint per principal, a batch sits exactly on the edge.
//
// The trap is not where it looks. Establishing savepoints costs the backend
// that establishes them a little; what the overflow actually breaks is **every
// other backend** that has to decide whether rows written by that transaction
// are visible. A single-connection benchmark would measure the cheap half and
// report that savepoints are fine.
//
// So both are measured: the cost to the writer, and the cost to a reader while
// the writer holds an overflowed transaction open.

func savepointFixture(t *testing.T, conn *postgres.Conn) string {
	t.Helper()
	const table = "sluice_savepoint_test"
	for _, s := range []string{
		`DROP TABLE IF EXISTS ` + table,
		`CREATE TABLE ` + table + ` (id bigint PRIMARY KEY, n bigint NOT NULL)`,
		`INSERT INTO ` + table + ` SELECT g, g FROM generate_series(1, 5000) g`,
	} {
		if err := conn.Exec(t.Context(), s); err != nil {
			t.Fatalf("fixture %q: %v", s, err)
		}
	}
	t.Cleanup(func() { _ = conn.Exec(t.Context(), `DROP TABLE IF EXISTS `+table) })
	return table
}

// second opens another connection, so a reader can be measured while a writer
// holds a transaction open.
func second(t *testing.T) *postgres.Conn {
	t.Helper()
	addr, cfg := pgTarget(t)
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := postgres.Startup(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestIntegrationSavepointCost(t *testing.T) {
	if os.Getenv("SLUICE_LATENCY") == "" {
		t.Skip("set SLUICE_LATENCY=1 to measure what savepoints cost")
	}
	writer := dial(t)
	table := savepointFixture(t, writer)
	reader := second(t)
	ctx := t.Context()

	const rounds = 30

	t.Log("writer: establishing N savepoints and one update each, per transaction")
	for _, n := range []int{1, 8, 32, 64, 128, 256} {
		start := time.Now()
		for r := range rounds {
			tx, err := writer.Begin(ctx, postgres.TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				if err := tx.Savepoint(ctx, fmt.Sprintf("sp_%d", i)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Exec(ctx, fmt.Sprintf(
					`UPDATE %s SET n = n + 1 WHERE id = %d`, table, (r*n+i)%5000+1)); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		per := time.Since(start) / rounds
		t.Logf("  savepoints=%3d  per transaction=%-12v  per savepoint=%v", n, per, per/time.Duration(n))
	}

	t.Log("writer: the same savepoints in one round trip")
	for _, n := range []int{1, 8, 32, 64, 128, 256} {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("sp_%d", i)
		}
		start := time.Now()
		for range rounds {
			tx, err := writer.Begin(ctx, postgres.TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Savepoints(ctx, names...); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		per := time.Since(start) / rounds
		t.Logf("  pipelined savepoints=%3d  per transaction=%-12v  per savepoint=%v",
			n, per, per/time.Duration(n))
	}

	t.Log("reader: scanning the table while the writer holds an open transaction with N subtransactions")
	for _, n := range []int{1, 8, 32, 64, 128, 256} {
		tx, err := writer.Begin(ctx, postgres.TxConfig{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range n {
			if err := tx.Savepoint(ctx, fmt.Sprintf("sp_%d", i)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Exec(ctx, fmt.Sprintf(
				`UPDATE %s SET n = n + 1 WHERE id = %d`, table, i%5000+1)); err != nil {
				t.Fatal(err)
			}
		}

		// The reader now has to decide, for every row the writer touched,
		// whether the subtransaction that wrote it committed. That is the
		// lookup the cache exists to avoid.
		start := time.Now()
		for range rounds {
			var seen int
			src := reader.Query(ctx, `SELECT id, n FROM `+table, nil,
				postgres.QueryConfig{BatchRows: 1024, AllRows: true})
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				seen += b.Len()
				return true
			})
			if err := src.Err(); err != nil {
				t.Fatal(err)
			}
			if seen != 5000 {
				t.Fatalf("reader saw %d rows, want 5000", seen)
			}
		}
		per := time.Since(start) / rounds

		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		t.Logf("  writer subtransactions=%3d  reader scan=%v", n, per)
	}
}
