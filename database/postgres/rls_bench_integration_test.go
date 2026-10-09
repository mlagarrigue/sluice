package postgres_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// The number §0.3 says decides whether batching survives multi-tenancy: what a
// batch costs as the number of distinct principals in it grows.
//
// The design partitions a batch by principal and issues one query per distinct
// one. On a single-goroutine connection those are sequential, so K principals
// cost K round trips of latency borne by every caller in the batch. This
// measures the curve.
//
// **The round trip here is a loopback round trip**, which is the smallest one
// that exists. A real deployment pays more — often 500 µs against the tens of
// microseconds below — so read the *shape* of the curve rather than its
// absolute values, and scale it by your own round trip.
func TestIntegrationRLSPartitionCost(t *testing.T) {
	if os.Getenv("SLUICE_LATENCY") == "" {
		t.Skip("set SLUICE_LATENCY=1 to measure the partitioning cost")
	}
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	const batch = 64
	const rounds = 200

	for _, principals := range []int{1, 2, 4, 8, 16, 32, 64} {
		perPrincipal := batch / principals
		start := time.Now()

		for range rounds {
			tx, err := conn.Begin(ctx, postgres.TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			for p := range principals {
				tenant := fmt.Sprintf("tenant-%02d", p)
				if err := tx.SetLocal(ctx, "app.tenant_id", tenant); err != nil {
					t.Fatal(err)
				}
				src := tx.Query(ctx, `SELECT id FROM `+table, nil,
					postgres.QueryConfig{BatchRows: 64})
				src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
				if err := src.Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}

		elapsed := time.Since(start) / rounds
		t.Logf("principals=%2d  per-principal calls=%2d  batch latency=%-12v  per call=%v",
			principals, perPrincipal, elapsed, elapsed/batch)
	}
}
