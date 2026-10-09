package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// clientSQL returns every statement the client sent, whether as a simple Query
// message or as the Parse of an extended-query sequence. It is what lets a
// test assert that a value never travelled as SQL text.
func clientSQL(t *testing.T, srv *fakeServer) []string {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var out []string
	for {
		m, err := rd.Next()
		if err != nil {
			return out
		}
		switch m.Type {
		case pgwire.FrontendQuery:
			s, _, _ := cstring(m.Body)
			out = append(out, s)
		case pgwire.FrontendParse:
			_, rest, ok := cstring(m.Body) // statement name
			if !ok {
				continue
			}
			s, _, _ := cstring(rest)
			out = append(out, s)
		}
	}
}

// cmdCycle is one simple statement answered and acknowledged.
func cmdCycle(tag string) []byte {
	return append(beCommandComplete(tag), beReadyForQuery()...)
}

// setConfigCycle is what the server answers to `SELECT set_config($1,$2,true)`:
// an extended-query sequence returning one row nobody reads. One chunk, not
// two: execParams runs under AllRows, so the Sync travels with the query and
// a real backend delivers everything up to and including ReadyForQuery as one
// answer.
func setConfigCycle() [][]byte {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"set_config"}, []uint32{OIDText})...)
	first = append(first, beDataRow([]byte("x"))...)
	first = append(first, beCommandComplete("SELECT 1")...)
	first = append(first, beReadyForQuery()...)
	return [][]byte{first}
}

func TestBeginCommit(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatalf("Begin returned %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit returned %v", err)
	}
	if got, want := clientSQL(t, srv), []string{"BEGIN", "COMMIT"}; !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q", got, want)
	}
}

// The shape the documentation tells callers to copy has to actually work: a
// deferred Rollback beside a Commit on the success path must send nothing.
func TestRollbackAfterCommitIsANoOp(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback after Commit returned %v, want nil", err)
	}
	if got := clientSQL(t, srv); len(got) != 2 {
		t.Errorf("statements = %q: the no-op rollback spoke to the server", got)
	}
}

func TestBeginOptionsReachTheStatement(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts TxConfig
		want string
	}{
		{"default", TxConfig{}, "BEGIN"},
		{"read committed", TxConfig{Isolation: IsolationReadCommitted}, "BEGIN ISOLATION LEVEL READ COMMITTED"},
		{"repeatable read", TxConfig{Isolation: IsolationRepeatableRead}, "BEGIN ISOLATION LEVEL REPEATABLE READ"},
		{"serializable", TxConfig{Isolation: IsolationSerializable}, "BEGIN ISOLATION LEVEL SERIALIZABLE"},
		{"read only", TxConfig{ReadOnly: true}, "BEGIN READ ONLY"},
		{"both", TxConfig{Isolation: IsolationSerializable, ReadOnly: true}, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("ROLLBACK")}}
			conn := NewConn(srv)
			tx, err := conn.Begin(t.Context(), tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback() //nolint:errcheck // the assertion is on what was sent
			if got := clientSQL(t, srv); len(got) == 0 || got[0] != tc.want {
				t.Errorf("BEGIN statement = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBeginRejectsAnUnknownIsolationLevel(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)
	if _, err := conn.Begin(t.Context(), TxConfig{Isolation: IsolationLevel(9)}); err == nil {
		t.Fatal("Begin accepted an isolation level that does not exist")
	}
	if srv.sent.Len() != 0 {
		t.Errorf("% x went to the server for a transaction that could not be described", srv.sent.Bytes())
	}
}

// A nested BEGIN is a warning and a no-op on the server, which makes the next
// COMMIT close somebody else's transaction. It must be refused here.
func TestBeginRefusesASecondTransaction(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("ROLLBACK")}}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Begin(t.Context(), TxConfig{}); !errors.Is(err, ErrTxOpen) {
		t.Fatalf("the second Begin returned %v, want ErrTxOpen", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// And the connection is usable for a transaction again afterwards.
	srv.responses = append(srv.responses, cmdCycle("BEGIN"))
	if _, err := conn.Begin(t.Context(), TxConfig{}); err != nil {
		t.Errorf("Begin after the first transaction closed returned %v", err)
	}
}

// A copy opens its own transaction, so it has to agree with Begin about
// whether one is already open.
func TestCopyRefusesToNestInsideATransaction(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN")}}
	conn := NewConn(srv)
	if _, err := conn.Begin(t.Context(), TxConfig{}); err != nil {
		t.Fatal(err)
	}

	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); !errors.Is(err, ErrTxOpen) {
		t.Fatalf("WriteTuples inside an open transaction returned %v, want ErrTxOpen", err)
	}
}

// And the converse: a copy's transaction blocks Begin, so the copy's COMMIT
// cannot be closed by someone else's.
func TestBeginRefusedWhileACopyHoldsATransaction(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Begin(t.Context(), TxConfig{}); !errors.Is(err, ErrTxOpen) {
		t.Fatalf("Begin during a copy returned %v, want ErrTxOpen", err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.inTx {
		t.Error("the copy's transaction was not released on Close")
	}
}

// The security assertion of this file. A tenant identifier is an attacker-
// controlled string by construction — it comes from a token — so it must never
// appear in the SQL the client sends.
func TestSetLocalNeverPutsTheValueIntoSQL(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}

	const hostile = "acme'; SET LOCAL ROLE postgres; --"
	// The error is deliberately not asserted on here. What is on trial is what
	// left the client, and it left the client whether or not the scripted
	// server liked it — an assertion that depends on the call succeeding would
	// pass for the wrong reason the day the transport changes.
	_ = tx.SetLocal(t.Context(), "app.tenant_id", hostile)

	for _, stmt := range clientSQL(t, srv) {
		if strings.Contains(stmt, "acme") || strings.Contains(stmt, "postgres") {
			t.Fatalf("the value reached the wire as SQL text: %q", stmt)
		}
	}
	// It must still have travelled — as a bound parameter.
	if !bytes.Contains(srv.sent.Bytes(), []byte(hostile)) {
		t.Error("the value never reached the server at all")
	}
	// And through set_config, not SET.
	var sawSetConfig bool
	for _, stmt := range clientSQL(t, srv) {
		if strings.Contains(stmt, "set_config") {
			sawSetConfig = true
		}
		if strings.HasPrefix(stmt, "SET ") || strings.HasPrefix(stmt, "SET LOCAL") {
			t.Errorf("a SET statement was assembled: %q", stmt)
		}
	}
	if !sawSetConfig {
		t.Error("no set_config call was sent")
	}
}

// The setting *name* is concatenated, so it is validated. A name built from a
// request is a caller reaching a parameter they were not meant to reach.
func TestSetLocalRejectsHostileNames(t *testing.T) {
	for _, name := range []string{
		"", "app.tenant_id; DROP TABLE orders", "app tenant", "APP.TENANT",
		"1app", "app..id", "app.", ".id", "app.tenant_id'", strings.Repeat("a", 64),
	} {
		t.Run(name, func(t *testing.T) {
			srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN")}}
			conn := NewConn(srv)
			tx, err := conn.Begin(t.Context(), TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			before := srv.sent.Len()
			if err := tx.SetLocal(t.Context(), name, "v"); err == nil {
				t.Fatalf("SetLocal accepted the name %q", name)
			}
			if srv.sent.Len() != before {
				t.Error("bytes went to the server for a name that was refused")
			}
		})
	}
}

func TestSetLocalAcceptsOrdinaryNames(t *testing.T) {
	for _, name := range []string{"app.tenant_id", "role", "statement_timeout", "a_1.b_2"} {
		t.Run(name, func(t *testing.T) {
			if err := validSettingName(name); err != nil {
				t.Errorf("validSettingName(%q) = %v", name, err)
			}
		})
	}
}

// bindParams parses back every Bind message the client sent and returns each
// one's parameter values, nil standing for NULL. The values travel in Bind,
// not in SQL text, so this is the only honest way to assert on one — a text
// search over the sent bytes matches the Parse SQL and proves nothing.
func bindParams(t *testing.T, srv *fakeServer) [][][]byte {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var out [][][]byte
	for {
		m, err := rd.Next()
		if err != nil {
			return out
		}
		if m.Type != pgwire.FrontendBind {
			continue
		}
		b := m.Body
		_, b, ok := cstring(b) // portal name
		if !ok {
			t.Fatal("Bind without a portal name")
		}
		_, b, ok = cstring(b) // statement name
		if !ok {
			t.Fatal("Bind without a statement name")
		}
		nfmt := int(binary.BigEndian.Uint16(b))
		b = b[2+2*nfmt:]
		nparams := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		params := make([][]byte, 0, nparams)
		for range nparams {
			n := int32(binary.BigEndian.Uint32(b)) //nolint:gosec // G115: -1 is the NULL marker
			b = b[4:]
			if n < 0 {
				params = append(params, nil)
				continue
			}
			params = append(params, append([]byte(nil), b[:n]...))
			b = b[n:]
		}
		out = append(out, params)
	}
}

func TestSetLocalTimeoutRoundsUpAndRefusesZero(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}

	// Sub-millisecond must not become zero: zero means *no limit*, which is
	// the opposite of what asking for 500 µs means.
	if err := tx.SetLocalTimeout(t.Context(), 500*time.Microsecond); err != nil {
		t.Fatalf("SetLocalTimeout returned %v", err)
	}
	// The earlier form of this assertion searched the sent bytes for "1" and
	// for a text pattern the client can never produce, both of which the
	// Parse SQL ("$1") satisfied on their own — a regression back to
	// truncation would have passed. The value travels as the second Bind
	// parameter, so that is what is read back and matched exactly.
	binds := bindParams(t, srv)
	if len(binds) != 1 {
		t.Fatalf("the client sent %d Bind messages, want 1", len(binds))
	}
	params := binds[0]
	if len(params) != 2 || string(params[0]) != "statement_timeout" {
		t.Fatalf("set_config was bound with %q, want statement_timeout and its value", params)
	}
	if got := string(params[1]); got != "1" {
		t.Errorf("500 µs travelled as statement_timeout = %q, want exactly \"1\": the duration must round up, because 0 means unbounded", got)
	}

	for _, d := range []time.Duration{0, -time.Second} {
		if err := tx.SetLocalTimeout(t.Context(), d); err == nil {
			t.Errorf("SetLocalTimeout(%v) was accepted", d)
		}
	}
}

// SetLocal is paid once per principal on the RLS path, so its wire shape is
// load-bearing: AllRows sends Parse/Bind/Describe/Execute/Sync in one flush
// and reads one answer. The shape a row limit produces — Execute(1), Flush,
// a second Execute to learn the portal is done, then a separate Sync — is
// three round trips for a row nobody reads.
func TestSetLocalIsOneRoundTrip(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetLocal(t.Context(), "app.tenant_id", "acme"); err != nil {
		t.Fatalf("SetLocal returned %v", err)
	}
	got := srv.clientMessages(t)
	want := []byte{pgwire.FrontendQuery, pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendSync}
	if !bytes.Equal(got, want) {
		t.Errorf("client sent %q, want %q — a Flush or a second Execute here is the three-round-trip shape", got, want)
	}
}

// The transaction-status byte on ReadyForQuery is the server's own word on
// whether a block is open, and inTx is derived from it — client bookkeeping
// alone was desynchronised by the SQL-level transaction control that Exec
// makes trivially reachable.
func TestExecDerivesTransactionStateFromReadyForQuery(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(beCommandComplete("COMMIT"), frame(pgwire.BackendReadyForQuery, "I")...),
		cmdCycle("BEGIN"),
	}}
	conn := NewConn(srv)
	ctx := t.Context()

	if err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	// A Begin here would nest, and the server would warn and ignore it —
	// the exact failure inTx exists to refuse.
	if _, err := conn.Begin(ctx, TxConfig{}); !errors.Is(err, ErrTxOpen) {
		t.Errorf("Begin after a SQL-level BEGIN returned %v, want ErrTxOpen", err)
	}
	if err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Begin(ctx, TxConfig{}); err != nil {
		t.Errorf("Begin after a SQL-level COMMIT returned %v — the 'I' status byte went unread", err)
	}
}

// 'E' is still an open block: aborted, but holding locks and needing a
// ROLLBACK. Treating it as idle would let Begin nest into it.
func TestAFailedTransactionBlockStillRefusesBegin(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(beErrorResponse("42703", "column does not exist"), frame(pgwire.BackendReadyForQuery, "E")...),
	}}
	conn := NewConn(srv)
	ctx := t.Context()
	if err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "SELECT nope"); err == nil {
		t.Fatal("the failing statement reported success")
	}
	if _, err := conn.Begin(ctx, TxConfig{}); !errors.Is(err, ErrTxOpen) {
		t.Errorf("Begin inside a failed block returned %v, want ErrTxOpen — the 'E' status byte went unread", err)
	}
}

// A COMMIT inside an aborted block is answered with CommandComplete "ROLLBACK"
// and no ErrorResponse: the server rolled back, said so only in the tag, and
// a Commit that returns nil here reports success for writes that are gone.
func TestCommitInAbortedBlockReportsRollback(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(beErrorResponse("42703", "column does not exist"), frame(pgwire.BackendReadyForQuery, "E")...),
		append(beCommandComplete("ROLLBACK"), frame(pgwire.BackendReadyForQuery, "I")...),
		cmdCycle("BEGIN"),
	}}
	conn := NewConn(srv)
	ctx := t.Context()

	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(ctx, "SELECT nope"); err == nil {
		t.Fatal("the failing statement reported success")
	}
	if err := tx.Commit(t.Context()); !errors.Is(err, ErrTxCommitRollback) {
		t.Fatalf("Commit in an aborted block returned %v, want ErrTxCommitRollback", err)
	}
	// The transaction is over either way: the deferred Rollback the docs
	// prescribe must send nothing, and the connection is idle again.
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback after the refused Commit returned %v, want nil", err)
	}
	if _, err := conn.Begin(ctx, TxConfig{}); err != nil {
		t.Errorf("Begin after the refused Commit returned %v — the connection is not back to idle", err)
	}
	if got, want := clientSQL(t, srv), []string{"BEGIN", "SELECT nope", "COMMIT", "BEGIN"}; !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q", got, want)
	}
}

// The tag alone is enough: a server that said 'T' all along and then
// completes COMMIT as "ROLLBACK" is still a COMMIT that committed nothing.
func TestCommitRefusesARollbackTagWithoutAFailedStatus(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(beCommandComplete("ROLLBACK"), frame(pgwire.BackendReadyForQuery, "I")...),
	}}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); !errors.Is(err, ErrTxCommitRollback) {
		t.Fatalf("Commit answered with a ROLLBACK tag returned %v, want ErrTxCommitRollback", err)
	}
	if _, err := conn.Begin(t.Context(), TxConfig{}); errors.Is(err, ErrTxOpen) {
		t.Error("the connection still believes a transaction is open")
	}
}

// TxOptions.Timeout must actually reach the server, and a transaction that
// could not be bounded must not be handed back as if it had been.
func TestBeginTimeoutIsAppliedAndFailsClosed(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Begin returned %v", err)
	}
	defer tx.Rollback() //nolint:errcheck // asserting on the wire, not the teardown
	if !bytes.Contains(srv.sent.Bytes(), []byte("statement_timeout")) {
		t.Error("TxOptions.Timeout never reached the server")
	}
	if !bytes.Contains(srv.sent.Bytes(), []byte("5000")) {
		t.Error("the timeout was not sent in milliseconds")
	}
}

func TestBeginRollsBackWhenTheTimeoutCannotBeSet(t *testing.T) {
	// One chunk through ReadyForQuery: the Sync left with the query (AllRows),
	// so the error and the recovery arrive as one answer.
	var refused []byte
	refused = append(refused, beSimple(pgwire.BackendParseComplete)...)
	refused = append(refused, beErrorResponse("42883", "function set_config does not exist")...)
	refused = append(refused, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{
		cmdCycle("BEGIN"),
		refused,
		cmdCycle("ROLLBACK"),
	}}
	conn := NewConn(srv)

	if _, err := conn.Begin(t.Context(), TxConfig{Timeout: time.Second}); err == nil {
		t.Fatal("Begin returned a transaction that is not bounded the way it was asked to be")
	}
	if conn.inTx {
		t.Error("the transaction was left open after Begin failed")
	}
	stmts := clientSQL(t, srv)
	if len(stmts) == 0 || stmts[len(stmts)-1] != "ROLLBACK" {
		t.Errorf("statements = %q, want a ROLLBACK at the end", stmts)
	}
}

// The lazy-Source trap: a stream built inside a transaction and consumed after
// it ends would run outside it, under nobody's tenant, and report the result as
// ordinary. It must be an error.
func TestQueryConsumedAfterCommitFails(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	src := tx.Query(t.Context(), "SELECT id", nil, QueryConfig{})
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	var batches int
	src.Stream()(func(sluice.Batch[Row]) bool { batches++; return true })
	if batches != 0 {
		t.Errorf("the stream produced %d batches outside its transaction", batches)
	}
	if err := src.Err(); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Err = %v, want ErrTxDone", err)
	}
	// And nothing was sent for it: only BEGIN and COMMIT.
	if got, want := clientSQL(t, srv), []string{"BEGIN", "COMMIT"}; !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q — the query ran outside its transaction", got, want)
	}
}

// A Source built after the transaction ended never runs either.
func TestQueryBuiltAfterCommitFails(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	src := tx.Query(t.Context(), "SELECT id", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { t.Error("a batch arrived"); return false })
	if err := src.Err(); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Err = %v, want ErrTxDone", err)
	}
}

func TestTxOperationsAfterEndAreRefused(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	for name, op := range map[string]func() error{
		"Commit":     func() error { return tx.Commit(ctx) },
		"Exec":       func() error { return tx.Exec(ctx, "SELECT 1") },
		"SetLocal":   func() error { return tx.SetLocal(ctx, "app.tenant_id", "x") },
		"Savepoint":  func() error { return tx.Savepoint(ctx, "sp") },
		"RollbackTo": func() error { return tx.RollbackTo(ctx, "sp") },
		"Release":    func() error { return tx.Release(ctx, "sp") },
	} {
		if err := op(); !errors.Is(err, ErrTxDone) {
			t.Errorf("%s on a finished transaction returned %v, want ErrTxDone", name, err)
		}
	}
}

func TestSavepointRoundTrip(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		cmdCycle("BEGIN"),
		cmdCycle("SAVEPOINT"),
		cmdCycle("ROLLBACK"),
		cmdCycle("RELEASE"),
		cmdCycle("COMMIT"),
	}}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := tx.Savepoint(ctx, "before_amend"); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(ctx, "before_amend"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Release(ctx, "before_amend"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"BEGIN",
		"SAVEPOINT before_amend",
		"ROLLBACK TO SAVEPOINT before_amend",
		"RELEASE SAVEPOINT before_amend",
		"COMMIT",
	}
	if got := clientSQL(t, srv); !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q", got, want)
	}
}

// A savepoint name is an identifier, so it is concatenated — which is exactly
// why it may not come from a request.
func TestSavepointRejectsHostileNames(t *testing.T) {
	for _, name := range []string{
		"", "sp; DROP TABLE orders", "sp name", "SP", "1sp", `sp"`, "sp'--",
		strings.Repeat("s", 64),
	} {
		t.Run(name, func(t *testing.T) {
			srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN")}}
			conn := NewConn(srv)
			tx, err := conn.Begin(t.Context(), TxConfig{})
			if err != nil {
				t.Fatal(err)
			}
			before := srv.sent.Len()
			if err := tx.Savepoint(t.Context(), name); err == nil {
				t.Fatalf("Savepoint accepted %q", name)
			}
			if srv.sent.Len() != before {
				t.Error("bytes went to the server for a name that was refused")
			}
		})
	}
}

func TestBeginRefusesACancelledContext(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := conn.Begin(ctx, TxConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin returned %v, want it to wrap context.Canceled", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("% x went to the server for a transaction the caller had abandoned", srv.sent.Bytes())
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The companion to the assertion above: with the right script, the call works.
// Kept separate so neither test can pass for the other's reason.
func TestSetLocalSucceeds(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetLocal(t.Context(), "app.tenant_id", "acme"); err != nil {
		t.Fatalf("SetLocal returned %v", err)
	}
}

// Savepoints exist because a savepoint costs a round trip like anything else,
// and one per element at sixty-four elements is twenty milliseconds. Sent
// together they must still be one message each on the wire, in order.
func TestSavepointsSendsThemAllBeforeReadingAny(t *testing.T) {
	responses := [][]byte{cmdCycle("BEGIN")}
	for range 4 {
		responses = append(responses, cmdCycle("SAVEPOINT"))
	}
	responses = append(responses, cmdCycle("COMMIT"))

	srv := &fakeServer{responses: responses}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Savepoints(t.Context(), "a", "b", "c", "d"); err != nil {
		t.Fatalf("Savepoints returned %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	want := []string{"BEGIN", "SAVEPOINT a", "SAVEPOINT b", "SAVEPOINT c", "SAVEPOINT d", "COMMIT"}
	if got := clientSQL(t, srv); !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q", got, want)
	}
}

// A savepoint established this way must be a real one: rolling back to it has
// to work, or the round trip was saved on something that does not exist.
func TestSavepointsAreUsable(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		cmdCycle("BEGIN"),
		cmdCycle("SAVEPOINT"), cmdCycle("SAVEPOINT"),
		cmdCycle("ROLLBACK"),
		cmdCycle("COMMIT"),
	}}
	conn := NewConn(srv)
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Savepoints(ctx, "first", "second"); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(ctx, "first"); err != nil {
		t.Fatalf("RollbackTo a pipelined savepoint returned %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Every statement must have been answered exactly once. Reading fewer
	// answers than statements leaves the connection one reply behind, and the
	// next statement reads the previous one's — which is a wrong answer rather
	// than an error, so the count is asserted rather than inferred.
	want := []string{
		"BEGIN", "SAVEPOINT first", "SAVEPOINT second",
		"ROLLBACK TO SAVEPOINT first", "COMMIT",
	}
	if got := clientSQL(t, srv); !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q", got, want)
	}
	if srv.pos != len(srv.responses) {
		t.Errorf("the server has %d of %d answers left unread; the connection is out of step",
			len(srv.responses)-srv.pos, len(srv.responses))
	}
}

// The names are concatenated into SQL, so they are validated — and nothing is
// sent if any one of them is refused, rather than half a list.
func TestSavepointsRejectHostileNamesBeforeSendingAnything(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN")}}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	before := srv.sent.Len()

	if err := tx.Savepoints(t.Context(), "good", "bad; DROP TABLE orders", "alsogood"); err == nil {
		t.Fatal("a hostile name was accepted")
	}
	if srv.sent.Len() != before {
		t.Error("part of the list was sent before the bad name was found")
	}
	// And nothing may be left queued in the writer: unflushed bytes are not
	// harmless, they leave with whatever is flushed next and the server reads
	// a SAVEPOINT nobody asked for in the middle of another statement.
	if n := conn.w.Buffered(); n != 0 {
		t.Errorf("%d bytes left queued after a refused name", n)
	}

	// The connection must still work, which is the thing those stray bytes
	// would break.
	srv.responses = append(srv.responses, cmdCycle("SAVEPOINT"), cmdCycle("COMMIT"))
	if err := tx.Savepoints(t.Context(), "fine"); err != nil {
		t.Fatalf("a good list after a refused one returned %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{"BEGIN", "SAVEPOINT fine", "COMMIT"}
	if got := clientSQL(t, srv); !equalStrings(got, want) {
		t.Errorf("statements = %q, want %q — a refused name left something behind", got, want)
	}
}

func TestSavepointsEmptyAndFinished(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
	conn := NewConn(srv)
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	before := srv.sent.Len()
	if err := tx.Savepoints(ctx); err != nil {
		t.Errorf("an empty list returned %v", err)
	}
	if srv.sent.Len() != before {
		t.Error("an empty list sent something")
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Savepoints(ctx, "late"); !errors.Is(err, ErrTxDone) {
		t.Errorf("err = %v, want ErrTxDone", err)
	}
}

// abortedBlock scripts BEGIN, a failing statement that leaves the block in
// the 'E' state, and then the answer to one more simple statement.
func abortedBlock(final []byte) *fakeServer {
	return &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(beErrorResponse("42703", "column does not exist"), frame(pgwire.BackendReadyForQuery, "E")...),
		final,
	}}
}

// A COMMIT through Exec in an aborted block is the same lie Tx.Commit used to
// tell: the server answers "ROLLBACK" with no error, so nil reported writes
// that never happened.
func TestExecCommitInAbortedBlockReportsRollback(t *testing.T) {
	rolledBack := append(beCommandComplete("ROLLBACK"), frame(pgwire.BackendReadyForQuery, "I")...)
	for _, commit := range []string{"COMMIT", "commit;", "  /* a /* nested */ note */ END", "-- done\nCommit and chain", "PREPARE TRANSACTION 'x'"} {
		t.Run(commit, func(t *testing.T) {
			conn := NewConn(abortedBlock(rolledBack))
			ctx := t.Context()
			if err := conn.Exec(ctx, "BEGIN"); err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec(ctx, "SELECT nope"); err == nil {
				t.Fatal("the failing statement reported success")
			}
			if err := conn.Exec(ctx, commit); !errors.Is(err, ErrTxCommitRollback) {
				t.Fatalf("Exec(%q) in an aborted block returned %v, want ErrTxCommitRollback", commit, err)
			}
			if conn.Err() != nil {
				t.Fatalf("the connection was broken: %v", conn.Err())
			}
		})
	}
}

// A ROLLBACK in an aborted block completes with the same tag and status as a
// refused COMMIT, and it did exactly what it was asked: it must stay nil.
func TestExecRollbackInAbortedBlockSucceeds(t *testing.T) {
	rolledBack := append(beCommandComplete("ROLLBACK"), frame(pgwire.BackendReadyForQuery, "I")...)
	for _, rollback := range []string{"ROLLBACK", "abort", "/* commit */ ROLLBACK"} {
		conn := NewConn(abortedBlock(rolledBack))
		ctx := t.Context()
		_ = conn.Exec(ctx, "BEGIN")
		_ = conn.Exec(ctx, "SELECT nope")
		if err := conn.Exec(ctx, rollback); err != nil {
			t.Errorf("Exec(%q) in an aborted block returned %v, want nil", rollback, err)
		}
	}
}

// A COMMIT that committed is answered "COMMIT", and a COMMIT followed by a
// ROLLBACK in one string must be judged on the first completion, not the last.
func TestExecCommitThatCommittedSucceeds(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		append(append(beCommandComplete("COMMIT"), beCommandComplete("ROLLBACK")...), frame(pgwire.BackendReadyForQuery, "I")...),
	}}
	conn := NewConn(srv)
	ctx := t.Context()
	if err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "COMMIT; ROLLBACK"); err != nil {
		t.Errorf("a COMMIT the server committed returned %v", err)
	}
}

// Tx.Exec is the other door to the same statement.
func TestTxExecCommitInAbortedBlockReportsRollback(t *testing.T) {
	conn := NewConn(abortedBlock(append(beCommandComplete("ROLLBACK"), frame(pgwire.BackendReadyForQuery, "I")...)))
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(ctx, "SELECT nope"); err == nil {
		t.Fatal("the failing statement reported success")
	}
	if err := tx.Exec(ctx, "COMMIT"); !errors.Is(err, ErrTxCommitRollback) {
		t.Fatalf("Tx.Exec(COMMIT) in an aborted block returned %v, want ErrTxCommitRollback", err)
	}
	// The server left the block: the Tx must not keep running statements,
	// which would now execute in autocommit outside any transaction.
	if err := tx.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Tx.Exec after a SQL-level COMMIT returned %v, want ErrTxDone", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("deferred Rollback after a SQL-level COMMIT returned %v, want nil", err)
	}
}

func TestAsksToCommit(t *testing.T) {
	for sql, want := range map[string]bool{
		"COMMIT":                    true,
		"end work":                  true,
		"\t\r\nCoMmIt":              true,
		"/* x */ /* y */ COMMIT":    true,
		"PREPARE TRANSACTION 'g'":   true,
		"prepare /* */ transaction": true,
		"PREPARE s AS SELECT 1":     false,
		"ROLLBACK":                  false,
		"ABORT":                     false,
		"COMMITTED":                 false,
		"ENDING":                    false,
		"SELECT 1; COMMIT":          false,
		"-- COMMIT":                 false,
		"/* unterminated COMMIT":    false,
		"":                          false,
	} {
		if got := asksToCommit(sql); got != want {
			t.Errorf("asksToCommit(%q) = %v, want %v", sql, got, want)
		}
	}
}

// tripwire wraps a scripted server and counts every Read or Write made after
// one of them failed: once the connection broke, the socket is off limits.
type tripwire struct {
	*fakeServer
	tripped bool
	after   int
}

func (w *tripwire) Read(p []byte) (int, error) {
	if w.tripped {
		w.after++
	}
	n, err := w.fakeServer.Read(p)
	if err != nil {
		w.tripped = true
	}
	return n, err
}

func (w *tripwire) Write(p []byte) (int, error) {
	if w.tripped {
		w.after++
	}
	return w.fakeServer.Write(p)
}

// Every entry point refuses a broken connection without touching the socket.
// Talking on one sends statements from a position nobody knows and reads
// replies meant for something else as this statement's answer.
func TestBrokenConnectionRefusesEveryEntryPoint(t *testing.T) {
	w := &tripwire{fakeServer: &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
	}}}
	conn := NewConn(w)
	ctx := t.Context()
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	copier, err := conn.CopyFrom(ctx, "COPY t FROM STDIN BINARY", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	// The script is exhausted: this query reads EOF and breaks the connection.
	if err := drain(t, tx.Query(ctx, "SELECT 1", nil, QueryConfig{})); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("setup: query returned %v, want ErrConnBroken", err)
	}
	if !w.tripped {
		t.Fatal("setup: the socket never failed")
	}

	calls := map[string]func() error{
		"Conn.Exec":              func() error { return conn.Exec(ctx, "SELECT 1") },
		"Conn.Begin":             func() error { _, err := conn.Begin(ctx, TxConfig{}); return err },
		"Conn.Query":             func() error { return drain(t, conn.Query(ctx, "SELECT 1", nil, QueryConfig{})) },
		"Conn.PrepareStatements": func() error { return conn.PrepareStatements(4) },
		"Conn.CopyFrom":          func() error { _, err := conn.CopyFrom(ctx, "COPY t FROM STDIN", CopyConfig{RowsPerTx: 1}); return err },
		"Tx.Exec":                func() error { return tx.Exec(ctx, "SELECT 1") },
		"Tx.Query":               func() error { return drain(t, tx.Query(ctx, "SELECT 1", nil, QueryConfig{})) },
		"Tx.SetLocal":            func() error { return tx.SetLocal(ctx, "app.tenant", "a") },
		"Tx.Savepoint":           func() error { return tx.Savepoint(ctx, "s") },
		"Tx.Savepoints":          func() error { return tx.Savepoints(ctx, "s", "t") },
		"Tx.RollbackTo":          func() error { return tx.RollbackTo(ctx, "s") },
		"Tx.Release":             func() error { return tx.Release(ctx, "s") },
		"Tx.Pipeline": func() error {
			return tx.Pipeline(ctx, []Part{{SQL: "SELECT 1"}}, QueryConfig{}, func(int, sluice.Batch[Row]) bool { return true })
		},
		"Copier.WriteTuples": func() error { return copier.WriteTuples([]byte{0, 0}, 1) },
		"Tx.Commit":          func() error { return tx.Commit(t.Context()) },
		"Tx.Rollback":        tx.Rollback,
	}
	for name, call := range calls {
		before := w.after
		if err := call(); !errors.Is(err, ErrConnBroken) {
			t.Errorf("%s on a broken connection returned %v, want ErrConnBroken", name, err)
		}
		if w.after != before {
			t.Errorf("%s touched the socket of a broken connection (%d calls)", name, w.after-before)
		}
	}
}

// A connection that breaks in the middle of Savepoints' reads has no further
// ReadyForQuery to wait for: the loop must stop rather than read again.
func TestSavepointsStopsReadingOnceBroken(t *testing.T) {
	w := &tripwire{fakeServer: &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
	}}}
	conn := NewConn(w)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Savepoints(t.Context(), "a", "b", "c"); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("Savepoints returned %v, want ErrConnBroken", err)
	}
	if w.after != 0 {
		t.Errorf("Savepoints touched the socket %d more times after it failed", w.after)
	}
}

// The round-up must not overflow at the top of Duration's range: adding a
// millisecond before dividing wrapped math.MaxInt64 to a negative count, and
// a negative statement_timeout is a server error the caller did not ask for.
func TestSetLocalTimeoutDoesNotOverflow(t *testing.T) {
	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN")}, setConfigCycle()...)}
	conn := NewConn(srv)
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetLocalTimeout(t.Context(), time.Duration(math.MaxInt64)); err != nil {
		t.Fatalf("SetLocalTimeout returned %v", err)
	}
	binds := bindParams(t, srv)
	if len(binds) != 1 || len(binds[0]) != 2 {
		t.Fatalf("binds = %q, want one set_config", binds)
	}
	want := strconv.FormatInt(math.MaxInt64/int64(time.Millisecond)+1, 10)
	if got := string(binds[0][1]); got != want {
		t.Errorf("MaxInt64 travelled as statement_timeout = %q, want %q", got, want)
	}
}
