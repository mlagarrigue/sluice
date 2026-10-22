package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

// Enough parameter bytes going out and result bytes coming back that both
// sides' socket buffers fill before the server has read the last part: 16 MiB
// out, 51 MB back. The client's buffers are shrunk, 16 KiB to send and
// 256 KiB to receive, so the threshold no longer rides on the kernel's socket
// autotuning (64 parts of 16 KiB answering 200 KB hung on one machine and not
// on another), and the server's own buffers, which autotuning grows to a few
// megabytes at most, hold a fraction of either direction.
//
// The receive buffer stays well above the loopback's maximum segment, about
// 64 KiB. CI reaches the server through a proxy on 127.0.0.1, and a 16 KiB
// receive buffer there stalled the connection itself: the client waiting in a
// read, the server in a write, the bytes between them held by the proxy,
// whichever way the pipeline read. The direct address passed in 0.3 s.
//
// A pipeline that reads while it sends finishes in well under a second; one
// that flushes first never returns, so it runs in a goroutine and its socket
// is closed to release it.
func TestIntegrationPipelineLargeVolumeFinishes(t *testing.T) {
	addr, cfg := pgTarget(t)
	ctx := t.Context()
	// The socket itself, closed to release a blocked pipeline: Conn.Close
	// would flush a Terminate into the same full buffer and block with it.
	var raw net.Conn
	dialSmall := func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		tc := c.(*net.TCPConn)
		if err := tc.SetReadBuffer(256 << 10); err != nil {
			return nil, errors.Join(err, c.Close())
		}
		if err := tc.SetWriteBuffer(16 << 10); err != nil {
			return nil, errors.Join(err, c.Close())
		}
		raw = c
		return c, nil
	}
	conn, err := postgres.Connect(ctx, dialSmall, cfg)
	if err != nil {
		t.Fatalf("connecting to %s as %s: %v", addr, cfg.User, err)
	}
	defer func() { _ = conn.Close() }()

	const parts = 256
	param := bytes.Repeat([]byte("p"), 64<<10)
	ps := make([]postgres.Part, parts)
	for i := range ps {
		ps[i] = postgres.Part{
			SQL:    `SELECT repeat('r', 200000), length($1)`,
			Params: []postgres.Param{{OID: postgres.OIDText, Value: param}},
		}
	}

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}

	answered := 0
	done := make(chan error, 1)
	go func() {
		done <- tx.Pipeline(ctx, ps, postgres.QueryConfig{}, func(i int, b sluice.Batch[postgres.Row]) bool {
			for _, r := range b.Items {
				v, _ := r.Value(0)
				if len(v) != 200000 {
					t.Errorf("part %d: result is %d bytes, want 200000", i, len(v))
				}
				answered++
			}
			return true
		})
	}()
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		_ = raw.Close()
		<-done
		t.Fatalf("Pipeline still blocked after 10 s with %d of %d parts answered: "+
			"send and read do not overlap", answered, parts)
	}
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if answered != parts {
		t.Fatalf("%d rows answered, want %d", answered, parts)
	}
	_ = tx.Rollback()
}
