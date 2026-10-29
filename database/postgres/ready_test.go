package postgres

import (
	"log/slog"
	"net"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// scriptedPipe is the server end of a pipe, read one client write at a time.
type scriptedPipe struct {
	t    *testing.T
	conn net.Conn
	buf  []byte
}

func newScriptedPipe(t *testing.T) (*scriptedPipe, *Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return &scriptedPipe{t: t, conn: server, buf: make([]byte, 64<<10)}, NewConn(client, Config{})
}

// read returns the types of the messages in the client's next write.
func (s *scriptedPipe) read() string {
	n, err := s.conn.Read(s.buf)
	if err != nil {
		s.t.Errorf("scripted server reading: %v", err)
	}
	return frontendTypes(s.buf[:n])
}

func (s *scriptedPipe) write(msgs ...[]byte) {
	for _, m := range msgs {
		if _, err := s.conn.Write(m); err != nil {
			s.t.Errorf("scripted server writing: %v", err)
		}
	}
}

// answer writes a one-row result of a batched read, to its CommandComplete.
func (s *scriptedPipe) answer() {
	s.write(beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
		beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(1)),
		beCommandComplete("SELECT 1"))
}

func readAll(t *testing.T, conn *Conn, sql string) {
	t.Helper()
	src := conn.Query(t.Context(), sql, nil, QueryConfig{BatchRows: 2})
	rows := 0
	src.Stream()(func(b sluice.Batch[Row]) bool { rows += len(b.Items); return true })
	if err := src.Err(); err != nil {
		t.Errorf("%s: %v", sql, err)
	}
	if rows != 1 {
		t.Errorf("%s: %d rows, want 1", sql, rows)
	}
}

// A batched read sends its Sync at its CommandComplete and returns without
// waiting for the ReadyForQuery: the next query leaves before it is read, and
// a Begin after that reads it and sees the connection idle. The server here
// writes nothing until the next query has arrived, so a client that waited
// for the ReadyForQuery before writing deadlocks the bubble.
func TestABatchedReadOwesItsReadyForQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newScriptedPipe(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			srv.read() // the first query
			srv.answer()
			if got := srv.read(); got != "S" {
				t.Errorf("after the first result the client sent %q, want its Sync", got)
			}
			if got := srv.read(); !strings.Contains(got, "B") {
				t.Errorf("the client's next write was %q, want the second query", got)
			}
			srv.write(beReadyForQuery()) // the first query's, owed
			srv.answer()
			srv.read() // Sync
			srv.write(beReadyForQuery())
			if got := srv.read(); got != "Q" {
				t.Errorf("Begin sent %q, want its BEGIN", got)
			}
			srv.write(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T"))
		}()
		readAll(t, conn, "SELECT 1")
		if !conn.readyOwed {
			t.Error("the first read read its ReadyForQuery, want it owed")
		}
		readAll(t, conn, "SELECT 2")
		tx, err := conn.Begin(t.Context(), TxConfig{})
		if err != nil {
			t.Fatalf("Begin after an owed ReadyForQuery: %v", err)
		}
		if !conn.inTx || conn.readyOwed || tx == nil {
			t.Errorf("after Begin: inTx %v, readyOwed %v, want a transaction and nothing owed", conn.inTx, conn.readyOwed)
		}
		<-done
	})
}

// In a transaction block the read waits for its ReadyForQuery, as an error
// after its CommandComplete would abort the block under the next statement;
// and so does a statement that may write, whose commit may fail after it.
func TestReadsThatMayNotOweTheirReadyForQuery(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		inTx      bool
	}{
		{"in a block", "SELECT 1", true},
		{"a statement that may write", "INSERT INTO t VALUES (1) RETURNING n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, conn := newScriptedPipe(t)
				conn.inTx = tc.inTx
				done := make(chan struct{})
				go func() {
					defer close(done)
					srv.read()
					srv.answer()
					srv.read() // Sync
					srv.write(frame(pgwire.BackendReadyForQuery, map[bool]string{true: "T", false: "I"}[tc.inTx]))
				}()
				readAll(t, conn, tc.sql)
				if conn.readyOwed {
					t.Error("the ReadyForQuery was left owed, want it read")
					_ = conn.settleReady()
				}
				<-done
			})
		})
	}
}

// An error between a read's CommandComplete and its ReadyForQuery — the
// read's own transaction failing after its last row — belongs to a read
// already reported complete. The next statement does not fail with it; it
// is logged.
func TestAnErrorAfterAnOwedReadIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newScriptedPipe(t)
		logs := &recordHandler{}
		conn.logger = slog.New(logs)
		go func() {
			srv.read()
			srv.answer()
			srv.read() // Sync
			srv.read() // the next query
			srv.write(beErrorResponse("23503", "deferred constraint violated"), beReadyForQuery())
			srv.answer()
			srv.read() // Sync
			srv.write(beReadyForQuery())
		}()
		readAll(t, conn, "SELECT 1")
		readAll(t, conn, "SELECT 2")
		_ = conn.settleReady()
		if got := logs.warning(t, "\x00")["error"].String(); !strings.Contains(got, "23503") {
			t.Errorf("logged error %q, want the server's 23503", got)
		}
	})
}
