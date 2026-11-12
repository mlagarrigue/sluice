package postgres

import (
	"errors"
	"testing"
)

// A Tx whose block the server ended without it — through Conn.Exec("COMMIT"),
// which the Begin godoc provides for, Conn.ExecParams, or a block-ending
// text passed to Tx.Query — refuses to serve: ErrTxDone, and nothing sent.
// Only Tx.Exec and Tx.ExecParams used to mark the Tx done; through the Conn
// the ReadyForQuery cleared inTx and left the Tx neither done nor of another
// generation, and Tx.SetLocal sent its set_config outside any transaction,
// where it held for that one statement, and returned nil. The control ends
// the block through Tx.Exec.
func TestATxWhoseBlockEndedElsewhereRefuses(t *testing.T) {
	ctx := t.Context()

	// Control: ended through Tx.Exec, the Tx refuses.
	{
		srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}}
		conn := NewConn(srv, Config{})
		tx, err := conn.Begin(ctx, TxConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Exec(ctx, "COMMIT"); err != nil {
			t.Fatal(err)
		}
		if err := tx.SetLocal(ctx, "app.tenant_id", "t1"); !errors.Is(err, ErrTxDone) {
			t.Fatalf("control: SetLocal after Tx.Exec(COMMIT) = %v, want ErrTxDone", err)
		}
	}

	srv := &fakeServer{responses: append([][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}, setConfigCycle()...)}
	conn := NewConn(srv, Config{})
	tx, err := conn.Begin(ctx, TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if conn.inTx {
		t.Fatal("setup: the connection still believes a block is open after the server answered COMMIT with 'I'")
	}
	before := srv.sent.Len()
	if err := tx.SetLocal(ctx, "app.tenant_id", "t1"); !errors.Is(err, ErrTxDone) {
		t.Errorf("SetLocal on a Tx whose block the server has ended = %v, want ErrTxDone", err)
	}
	if srv.sent.Len() != before {
		t.Error("set_config was sent outside any transaction: the setting holds for that one statement")
	}
}
