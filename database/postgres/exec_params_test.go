package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// extendedCycle is what the server answers to an ExecParams that returns no
// rows: one chunk, because FetchAll sends the Sync with the query.
func extendedCycle(tag string, status string) []byte {
	var b []byte
	b = append(b, beSimple(pgwire.BackendParseComplete)...)
	b = append(b, beSimple(pgwire.BackendBindComplete)...)
	b = append(b, beSimple(pgwire.BackendNoData)...)
	b = append(b, beCommandComplete(tag)...)
	return append(b, frame(pgwire.BackendReadyForQuery, status)...)
}

func TestCommandTagRowsAffected(t *testing.T) {
	for _, tc := range []struct {
		tag  CommandTag
		n    int64
		have bool
	}{
		{"UPDATE 3", 3, true},
		{"DELETE 0", 0, true},
		{"INSERT 0 5", 5, true},
		{"INSERT 16384 1", 1, true}, // an oid from a server that still sets one
		{"MERGE 2", 2, true},
		{"SELECT 10", 10, true},
		{"MOVE 1", 1, true},
		{"FETCH 4", 4, true},
		{"COPY 7", 7, true},
		{"UPDATE 9223372036854775807", 9223372036854775807, true},

		{"SET", 0, false},
		{"CREATE TABLE", 0, false},
		{"BEGIN", 0, false},
		{"", 0, false},
		{"UPDATE", 0, false},
		{"UPDATE ", 0, false},
		{"INSERT 5", 0, false},
		{"UPDATE -1", 0, false},
		{"UPDATE +1", 0, false},
		{"UPDATE 3 4", 0, false},
		{"UPDATE 9223372036854775808", 0, false},
		{"CREATE TABLE 3", 0, false}, // a count only where the protocol puts one
		{"update 3", 0, false},
	} {
		n, ok := tc.tag.RowsAffected()
		if n != tc.n || ok != tc.have {
			t.Errorf("CommandTag(%q).RowsAffected() = %d, %v; want %d, %v", tc.tag, n, ok, tc.n, tc.have)
		}
	}
}

// The values travel as bound parameters, the tag comes back whole, and it
// costs one round trip.
func TestExecParamsReturnsTheTag(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{extendedCycle("UPDATE 3", "I")}}
	conn := NewConn(srv)

	const hostile = "acme'; DROP TABLE orders; --"
	tag, err := conn.ExecParams(t.Context(), "UPDATE orders SET shipped = true WHERE customer = $1",
		[]Param{{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, hostile)}})
	if err != nil {
		t.Fatal(err)
	}
	if tag != "UPDATE 3" {
		t.Errorf("tag = %q, want %q", tag, "UPDATE 3")
	}
	if n, ok := tag.RowsAffected(); n != 3 || !ok {
		t.Errorf("RowsAffected() = %d, %v; want 3, true", n, ok)
	}
	for _, stmt := range clientSQL(t, srv) {
		if strings.Contains(stmt, "acme") {
			t.Fatalf("the value reached the wire as SQL text: %q", stmt)
		}
	}
	if p := bindParams(t, srv); len(p) != 1 || len(p[0]) != 1 || string(p[0][0]) != hostile {
		t.Errorf("Bind carried %q, want the one value", p)
	}
	if srv.writes != 1 {
		t.Errorf("ExecParams flushed %d times, want one round trip", srv.writes)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection was broken: %v", conn.Err())
	}
}

// Rows the statement returns are dropped, and the tag still counts them.
func TestExecParamsDropsRowsAndKeepsTheCount(t *testing.T) {
	conn := NewConn(&fakeServer{responses: setConfigCycle()})
	tag, err := conn.ExecParams(t.Context(), "SELECT set_config($1, $2, false)",
		[]Param{{Value: pgtype.AppendText(nil, "a.b")}, {Value: pgtype.AppendText(nil, "c")}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "SELECT 1" || n != 1 || !ok {
		t.Errorf("tag = %q, RowsAffected() = %d, %v; want SELECT 1, 1, true", tag, n, ok)
	}
}

// A statement that failed completed nothing: the tag is empty, the server's
// error comes back, and the connection stays usable.
func TestExecParamsErrorLeavesTheTagEmpty(t *testing.T) {
	failed := append(beErrorResponse("23505", "duplicate key"), beReadyForQuery()...)
	conn := NewConn(&fakeServer{responses: [][]byte{failed, extendedCycle("INSERT 0 1", "I")}})
	ctx := t.Context()

	tag, err := conn.ExecParams(ctx, "INSERT INTO t VALUES ($1)", []Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 1)}})
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("err = %v, want the server's 23505", err)
	}
	if tag != "" {
		t.Errorf("tag = %q after a failure, want empty", tag)
	}
	tag, err = conn.ExecParams(ctx, "INSERT INTO t VALUES ($1)", []Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 2)}})
	if err != nil || tag != "INSERT 0 1" {
		t.Fatalf("the next statement returned %q, %v", tag, err)
	}
}

// The COMMIT rule of Exec holds on this door too: in an aborted block the
// server answers "ROLLBACK" and no error, and nil would report writes that
// never happened.
func TestExecParamsCommitInAbortedBlockReportsRollback(t *testing.T) {
	conn := NewConn(abortedBlock(extendedCycle("ROLLBACK", "I")))
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(ctx, "SELECT nope"); err == nil {
		t.Fatal("the failing statement reported success")
	}
	tag, err := tx.ExecParams(ctx, "COMMIT", nil)
	if !errors.Is(err, ErrTxCommitRollback) {
		t.Fatalf("Tx.ExecParams(COMMIT) in an aborted block returned %v, want ErrTxCommitRollback", err)
	}
	if tag != "ROLLBACK" {
		t.Errorf("tag = %q, want ROLLBACK", tag)
	}
	// The server left the block: the Tx must not run the next statement in
	// autocommit.
	if _, err := tx.ExecParams(ctx, "SELECT 1", nil); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Tx.ExecParams after a SQL-level COMMIT returned %v, want ErrTxDone", err)
	}
}

// A ROLLBACK through ExecParams completes with the same tag and did what it
// was asked: nil, and the tag says what happened.
func TestExecParamsRollbackInAbortedBlockSucceeds(t *testing.T) {
	conn := NewConn(abortedBlock(extendedCycle("ROLLBACK", "I")))
	ctx := t.Context()
	_ = conn.Exec(ctx, "BEGIN")
	_ = conn.Exec(ctx, "SELECT nope")
	tag, err := conn.ExecParams(ctx, "ROLLBACK", nil)
	if err != nil || tag != "ROLLBACK" {
		t.Errorf("ExecParams(ROLLBACK) in an aborted block = %q, %v; want ROLLBACK, nil", tag, err)
	}
}

// A refused Tx sends nothing.
func TestTxExecParamsAfterEndIsRefused(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	before := srv.sent.Len()
	if _, err := tx.ExecParams(ctx, "UPDATE t SET x = $1", []Param{{Value: pgtype.AppendInt8(nil, 1)}}); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Tx.ExecParams after Commit returned %v, want ErrTxDone", err)
	}
	if srv.sent.Len() != before {
		t.Error("bytes went to the server for a finished transaction")
	}
}
