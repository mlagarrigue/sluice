package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// beFatal is the ErrorResponse a server sends before closing the connection
// itself: an administrator's termination, an idle timeout.
func beFatal(code, msg string) []byte {
	return frame(pgwire.BackendErrorResponse, string(errBody("S", "FATAL", "V", "FATAL", "C", code, "M", msg)))
}

// wantClosedBy checks that err says the server closed the connection and why:
// the server's error reachable by errors.As, the break by errors.Is.
func wantClosedBy(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *Error
	switch {
	case err == nil:
		t.Fatal("no error after the server closed the connection")
	case !errors.As(err, &pgErr) || pgErr.Code != code:
		t.Errorf("err = %v; want the server's %s reachable by errors.As", err, code)
	case !errors.Is(err, ErrConnBroken):
		t.Errorf("err = %v; want ErrConnBroken", err)
	case !strings.Contains(err.Error(), "closed by the server"):
		t.Errorf("err = %q; want it to say the server closed the connection", err)
	}
}

// The server ends the session — an idle timeout, an administrator — with a
// FATAL ErrorResponse and closes. Every call that was reading at that moment
// reports that reason, not the EOF that followed it.
func TestExecReportsWhyTheServerClosed(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{beFatal("57P01", "terminating connection due to administrator command")}}
	err := NewConn(srv, Config{}).Exec(t.Context(), "UPDATE orders SET status = 'late'")
	wantClosedBy(t, err, "57P01")
}

func TestCommitReportsWhyTheServerClosed(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		beFatal("25P03", "terminating connection due to idle-in-transaction timeout"),
	}}
	tx, err := NewConn(srv, Config{}).Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	wantClosedBy(t, tx.Commit(t.Context()), "25P03")
}

func TestPipelineReportsWhyTheServerClosed(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...),
		beFatal("57P05", "terminating connection due to idle-session timeout"),
	}}
	tx, err := NewConn(srv, Config{}).Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	err = tx.Pipeline(t.Context(), []Part{{SQL: "SELECT 1"}}, QueryConfig{},
		func(int, sluice.Batch[Row]) bool { return true })
	wantClosedBy(t, err, "57P05")
}

func TestCopyCommitReportsWhyTheServerClosed(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		beCopyInResponse(),
		beFatal("57P01", "terminating connection due to administrator command"),
	}}
	cp, err := NewConn(srv, Config{}).CopyFrom(t.Context(), "COPY orders (id, name) FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	id, name := int64(1), "a"
	if err := cp.WriteTuples(tuple(&id, &name), 1); err != nil {
		wantClosedBy(t, err, "57P01")
		return
	}
	wantClosedBy(t, cp.Close(), "57P01")
}
