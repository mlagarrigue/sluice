package postgres_test

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// A pipeline must return exactly what the sequential form returns, or the round
// trip it saves is bought with a different answer.
func TestIntegrationPipelineMatchesSequential(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	tenants := []string{"acme", "globex", "nobody", "acme"}
	parts := make([]postgres.Part, len(tenants))
	for i, tenant := range tenants {
		parts[i] = postgres.Part{
			Setting: "app.tenant_id", Value: tenant,
			SQL: `SELECT id FROM ` + table + ` ORDER BY id`,
		}
	}

	// Sequential: one transaction, one SET LOCAL and one query per tenant.
	want := make([][]int64, len(tenants))
	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i, tenant := range tenants {
		if err := tx.SetLocal(ctx, "app.tenant_id", tenant); err != nil {
			t.Fatal(err)
		}
		want[i] = readIDs(t, tx.Query(ctx, parts[i].SQL, nil, postgres.QueryConfig{BatchRows: 64}))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Pipelined: the same thing in one round trip.
	got := make([][]int64, len(tenants))
	tx, err = conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // the read is the assertion

	err = tx.Pipeline(ctx, parts, postgres.QueryConfig{BatchRows: 64},
		func(i int, b sluice.Batch[postgres.Row]) bool {
			for _, r := range b.Items {
				v, isNull := r.Value(0)
				if isNull {
					t.Error("a NULL id")
					continue
				}
				id, err := postgres.DecodeInt8(v)
				if err != nil {
					t.Errorf("decoding: %v", err)
					continue
				}
				got[i] = append(got[i], id)
			}
			return true
		})
	if err != nil {
		t.Fatalf("Pipeline returned %v", err)
	}

	for i, tenant := range tenants {
		if !slices.Equal(got[i], want[i]) {
			t.Errorf("part %d (%s): pipelined %v, sequential %v", i, tenant, got[i], want[i])
		}
	}
	// And the security boundary still holds through the pipeline: acme sees
	// two rows, globex one, nobody none — with no tenant predicate in the SQL.
	if len(got[0]) != 2 || len(got[1]) != 1 || len(got[2]) != 0 {
		t.Errorf("row-level security did not survive the pipeline: %v", got)
	}
}

// A failing part takes every later part with it, because the backend discards
// to the next Sync and there is one Sync. The error must say which, so a caller
// holding waiting callers knows who was answered.
func TestIntegrationPipelineReportsWhatNeverRan(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	parts := []postgres.Part{
		{Setting: "app.tenant_id", Value: "acme", SQL: `SELECT id FROM ` + table},
		{Setting: "app.tenant_id", Value: "acme", SQL: `SELECT id FROM no_such_table_here`},
		{Setting: "app.tenant_id", Value: "globex", SQL: `SELECT id FROM ` + table},
	}

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // the error is the assertion

	served := map[int]bool{}
	err = tx.Pipeline(ctx, parts, postgres.QueryConfig{},
		func(i int, _ sluice.Batch[postgres.Row]) bool { served[i] = true; return true })

	if !errors.Is(err, postgres.ErrPipelineAborted) {
		t.Fatalf("err = %v, want it to wrap ErrPipelineAborted", err)
	}
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) {
		t.Errorf("err = %v, want the server's error to survive", err)
	}
	if !served[0] {
		t.Error("part 0 ran before the failure and its rows were not delivered")
	}
	if served[2] {
		t.Error("part 2 was reported as served, but the backend discarded it")
	}
	// The connection survives: the Sync put it back at a known position.
	if conn.Err() != nil {
		t.Fatalf("the connection was broken by a pipeline error: %v", conn.Err())
	}
}

// The measurement that decides §0.3, now with both forms side by side.
func TestIntegrationPipelineCost(t *testing.T) {
	if os.Getenv("SLUICE_LATENCY") == "" {
		t.Skip("set SLUICE_LATENCY=1 to measure the pipelined partitioning cost")
	}
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	const batch = 64
	const rounds = 200

	for _, principals := range []int{1, 4, 16, 64} {
		parts := make([]postgres.Part, principals)
		for i := range parts {
			parts[i] = postgres.Part{
				Setting: "app.tenant_id", Value: fmt.Sprintf("tenant-%02d", i),
				SQL: `SELECT id FROM ` + table,
			}
		}

		start := time.Now()
		for range rounds {
			tx, err := conn.Begin(ctx, postgres.TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Pipeline(ctx, parts, postgres.QueryConfig{},
				func(int, sluice.Batch[postgres.Row]) bool { return true }); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(start) / rounds
		t.Logf("principals=%2d  batch latency=%-12v  per call=%v",
			principals, elapsed, elapsed/batch)
	}
}
