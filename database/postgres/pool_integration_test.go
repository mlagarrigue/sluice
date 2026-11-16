package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// backendPID is the server process behind the connection a query runs on.
func backendPID(t *testing.T, src sluice.Source[postgres.Row]) int32 {
	t.Helper()
	var pid int32
	err := src.Consume(func(b sluice.Batch[postgres.Row]) bool {
		v, _ := b.Items[0].Value(0)
		var err error
		if pid, err = pgtype.DecodeInt4(v); err != nil {
			t.Fatal(err)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// terminate ends a server process the way an administrator, a failover or
// an idle-session timeout does, and waits until it is gone.
func terminate(t *testing.T, admin *postgres.Conn, pid int32) {
	t.Helper()
	pidArg := []postgres.Param{postgres.Arg(pgtype.Int4Codec(), pid)}
	if _, err := admin.ExecParams(t.Context(), "SELECT pg_terminate_backend($1)", pidArg); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		tag, err := admin.ExecParams(t.Context(), "SELECT 1 FROM pg_stat_activity WHERE pid = $1", pidArg)
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := tag.RowsAffected(); n == 0 {
			return
		}
	}
	t.Fatalf("backend %d still running", pid)
}

// Against a real server: the pool's idle connection is terminated, the call
// that finds it fails, and the next is served by a new process — where a
// connection kept by hand fails every call after.
func TestIntegrationPoolReplacesATerminatedConnection(t *testing.T) {
	addr, cfg := pgTarget(t)
	p := postgres.NewPool(1, func(ctx context.Context) (*postgres.Conn, error) {
		return postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	})
	defer func() { _ = p.Close() }()
	admin := dial(t)
	const pidQuery = "SELECT pg_backend_pid()"

	first := backendPID(t, p.Query(t.Context(), pidQuery, nil, postgres.QueryConfig{}))
	terminate(t, admin, first)
	if _, err := p.ExecParams(t.Context(), "SELECT 1", nil); err == nil {
		t.Fatal("a call on a terminated connection succeeded: the probe did not reach it")
	}
	if second := backendPID(t, p.Query(t.Context(), pidQuery, nil, postgres.QueryConfig{})); second == first {
		t.Errorf("served by backend %d again, want a new process", second)
	}

	// The control: the same end on a connection nobody replaces.
	kept := dial(t)
	terminate(t, admin, backendPID(t, kept.Query(t.Context(), pidQuery, nil, postgres.QueryConfig{})))
	for i := range 2 {
		if _, err := kept.ExecParams(t.Context(), "SELECT 1", nil); err == nil {
			t.Errorf("call %d on a kept, terminated connection succeeded", i+1)
		}
	}
}

// A pool of eight connections for queries that run side by side, each call
// lent one; a broken connection is replaced on the next call.
func ExampleNewPool() {
	ctx := context.Background()
	pool := postgres.NewPool(8, func(ctx context.Context) (*postgres.Conn, error) {
		return postgres.Connect(ctx, postgres.DialTCP("db.internal:5432"), postgres.Config{User: "app", Database: "shop"})
	})
	defer func() { _ = pool.Close() }()

	page := pool.Query(ctx, "SELECT id, total FROM orders WHERE id > $1 ORDER BY id LIMIT 100",
		[]postgres.Param{postgres.Arg(pgtype.Int8Codec(), int64(1000))}, postgres.QueryConfig{})
	_ = page.Consume(func(sluice.Batch[postgres.Row]) bool { return true })
}
