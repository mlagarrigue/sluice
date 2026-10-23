package pgfake_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/example/internal/pgfake"
)

// rows answers a SELECT with n one-column rows.
func rows(n int) func(string, [][]byte, map[string]string) pgfake.Result {
	return func(sql string, _ [][]byte, _ map[string]string) pgfake.Result {
		if !strings.HasPrefix(sql, "SELECT") {
			return pgfake.Result{}
		}
		res := pgfake.Result{Columns: []string{"n"}, OIDs: []uint32{pgtype.OIDInt8}}
		for i := range n {
			res.Rows = append(res.Rows, [][]byte{pgtype.AppendInt8(nil, int64(i))})
		}
		return res
	}
}

// A result of exactly BatchRows rows is the trap QueryConfig.BatchRows
// documents: the server suspends at the limit without knowing nothing
// follows, and the client sends another Execute to learn it. A fake that
// completed at once would let the examples pass over a path no server takes.
func TestExactBatchSuspends(t *testing.T) {
	for _, tc := range []struct {
		batch    int
		suspends bool
	}{
		{batch: 3, suspends: true},
		{batch: 4, suspends: false},
	} {
		b := &pgfake.Backend{Exec: rows(3)}
		conn := b.Conn(t)
		src := conn.Query(t.Context(), "SELECT n FROM t", nil, postgres.QueryConfig{BatchRows: tc.batch})
		got := 0
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			got += len(batch.Items)
			return true
		})
		if err := src.Err(); err != nil || got != 3 {
			t.Fatalf("BatchRows %d: %d rows, err %v; want 3 and none", tc.batch, got, err)
		}
		if n := b.Executes(); (n > 1) != tc.suspends {
			t.Errorf("BatchRows %d over 3 rows: %d Executes, want more than one: %v", tc.batch, n, tc.suspends)
		}
		// Whatever was asked for ahead has been read: the connection is in step.
		if tag, err := conn.ExecParams(t.Context(), "SELECT n FROM t", nil); err != nil || tag != "SELECT 3" {
			t.Errorf("BatchRows %d, next statement: %q, %v; want SELECT 3", tc.batch, tag, err)
		}
	}
}

// In an aborted block the server refuses every statement but those that
// leave it, and answers COMMIT with the tag ROLLBACK and no error.
func TestAbortedBlock(t *testing.T) {
	b := &pgfake.Backend{Exec: func(sql string, _ [][]byte, _ map[string]string) pgfake.Result {
		if strings.HasPrefix(sql, "INSERT") {
			return pgfake.Result{Err: "duplicate key", Code: "23505"}
		}
		return pgfake.Result{}
	}}
	conn := b.Conn(t)
	ctx := t.Context()
	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var pgErr *postgres.Error
	if err := tx.Exec(ctx, "INSERT INTO t VALUES (1)"); !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("failing INSERT: %v; want the simulated SQLSTATE 23505", err)
	}
	if _, err := tx.ExecParams(ctx, "UPDATE t SET n = 2", nil); !errors.As(err, &pgErr) || pgErr.Code != "25P02" {
		t.Errorf("statement in the aborted block: %v; want 25P02", err)
	}
	if slices.Contains(b.Statements(), "UPDATE t SET n = 2") {
		t.Errorf("the aborted block ran a statement: %q", b.Statements())
	}
	tag, err := tx.ExecParams(ctx, "COMMIT", nil)
	if tag != "ROLLBACK" || !errors.Is(err, postgres.ErrTxCommitRollback) {
		t.Fatalf("COMMIT in the aborted block: %q, %v; want ROLLBACK and ErrTxCommitRollback", tag, err)
	}
	if tag, err := conn.ExecParams(ctx, "UPDATE t SET n = 2", nil); err != nil || tag != "UPDATE 0" {
		t.Errorf("after the block: %q, %v; want UPDATE 0", tag, err)
	}
}

func TestErrorCodeDefault(t *testing.T) {
	conn := (&pgfake.Backend{Exec: func(string, [][]byte, map[string]string) pgfake.Result {
		return pgfake.Result{Err: "boom"}
	}}).Conn(t)
	var pgErr *postgres.Error
	if err := conn.Exec(t.Context(), "SELECT 1"); !errors.As(err, &pgErr) || pgErr.Code != "XX000" {
		t.Errorf("error without a code: %v; want XX000", err)
	}
}
