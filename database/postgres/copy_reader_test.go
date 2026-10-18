package postgres

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// newCopyBackend runs a scripted backend over net.Pipe that reads the client
// one message at a time. A write on a pipe blocks until the other end reads,
// so a client that does not read while it sends deadlocks here exactly as it
// does against a server whose socket buffer is full — which a replayed buffer
// can never show. The script's error, nil included, arrives on the channel.
func newCopyBackend(t *testing.T, script func(rd *pgwire.Reader, w net.Conn) error) (*Conn, <-chan error) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	done := make(chan error, 1)
	go func() {
		done <- script(pgwire.NewReader(server), server)
		_ = server.Close()
	}()
	return NewConn(client), done
}

// expectQuery reads the next frontend message and checks it is the simple
// Query named.
func expectQuery(rd *pgwire.Reader, sql string) error {
	m, err := rd.Next()
	if err != nil {
		return err
	}
	if m.Type != pgwire.FrontendQuery || string(m.Body) != sql+"\x00" {
		return fmt.Errorf("backend got %q %q, want Query %q", m.Type, m.Body, sql)
	}
	return nil
}

// openCopy plays BEGIN and the COPY statement up to CopyInResponse.
func openCopy(rd *pgwire.Reader, w net.Conn) error {
	if err := expectQuery(rd, "BEGIN"); err != nil {
		return err
	}
	if _, err := w.Write(append(beCommandComplete("BEGIN"), frame(pgwire.BackendReadyForQuery, "T")...)); err != nil {
		return err
	}
	if err := expectQuery(rd, copyTestSQL); err != nil {
		return err
	}
	_, err := w.Write(beCopyInResponse())
	return err
}

const copyTestSQL = "COPY t FROM STDIN WITH (FORMAT BINARY)"

// within runs f and fails the test if it has not returned after d — the
// form a deadlock takes in a test.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	finished := make(chan struct{})
	go func() { defer close(finished); f() }()
	select {
	case <-finished:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v: client and server are each waiting for the other", what, d)
	}
}

// A trigger that raises a NOTICE per row makes the server write while the
// client writes. A client that reads only at CopyDone stops reading while the
// server blocks on a full socket and stops reading in turn: the load hung
// after about 100 000 rows on PostgreSQL 17. On a pipe, the second batch.
func TestCopyReadsTheServerWhileItSends(t *testing.T) {
	const batches = 50
	conn, backend := newCopyBackend(t, func(rd *pgwire.Reader, w net.Conn) error {
		if err := openCopy(rd, w); err != nil {
			return err
		}
		for {
			m, err := rd.Next()
			if err != nil {
				return err
			}
			if m.Type == pgwire.FrontendCopyDone {
				break
			}
			// A notice and a config reload per CopyData, written before the
			// next message is read — the order a backend runs a row trigger in.
			notice := frame(pgwire.BackendNoticeResponse, string(errBody("S", "NOTICE", "C", "00000", "M", "row")))
			if _, err := w.Write(append(notice, beParameterStatus("application_name", "loader")...)); err != nil {
				return err
			}
		}
		if _, err := w.Write(append(beCommandComplete("COPY 50"), frame(pgwire.BackendReadyForQuery, "T")...)); err != nil {
			return err
		}
		if err := expectQuery(rd, "COMMIT"); err != nil {
			return err
		}
		_, err := w.Write(append(beCommandComplete("COMMIT"), beReadyForQuery()...))
		return err
	})

	cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 1000, ChunkBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	within(t, 5*time.Second, "the copy", func() {
		for i := range batches {
			if err := cp.WriteTuples(tuple(new(int64(i)), nil), 1); err != nil {
				t.Errorf("batch %d: %v", i, err)
				return
			}
		}
		if err := cp.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if err := <-backend; err != nil {
		t.Fatal(err)
	}
	if got := conn.Parameter("application_name"); got != "loader" {
		t.Errorf("a ParameterStatus read during the copy was lost: application_name = %q", got)
	}
	if conn.Err() != nil || conn.inTx {
		t.Errorf("after the copy: Err %v, inTx %v; want a healthy idle connection", conn.Err(), conn.inTx)
	}
}

// The server reports a refused row as soon as it meets it, ends the COPY and
// drops every CopyData after that. The error comes back from a WriteTuples,
// not from the commit RowsPerTx rows later, and the transaction is rolled
// back on a connection that stays usable.
func TestCopyRefusedRowFailsAWriteBeforeTheCommit(t *testing.T) {
	conn, backend := newCopyBackend(t, func(rd *pgwire.Reader, w net.Conn) error {
		if err := openCopy(rd, w); err != nil {
			return err
		}
		if _, err := rd.Next(); err != nil {
			return err
		}
		refusal := beErrorResponse("23505", `duplicate key value violates unique constraint "t_pkey"`)
		if _, err := w.Write(append(refusal, frame(pgwire.BackendReadyForQuery, "E")...)); err != nil {
			return err
		}
		for {
			m, err := rd.Next()
			if err != nil {
				return err
			}
			switch m.Type {
			case pgwire.FrontendCopyData, pgwire.FrontendCopyDone, pgwire.FrontendCopyFail:
				continue // dropped, as §53.2.6 says
			}
			if string(m.Body) != "ROLLBACK\x00" {
				return fmt.Errorf("backend got %q %q after the refusal, want ROLLBACK", m.Type, m.Body)
			}
			_, err = w.Write(append(beCommandComplete("ROLLBACK"), beReadyForQuery()...))
			return err
		}
	})

	cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 1_000_000, ChunkBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var writeErr error
	within(t, 5*time.Second, "the copy", func() {
		deadline := time.Now().Add(2 * time.Second)
		for i := 0; time.Now().Before(deadline); i++ {
			if writeErr = cp.WriteTuples(tuple(new(int64(i)), nil), 1); writeErr != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	var pgErr *Error
	if !errors.As(writeErr, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("WriteTuples returned %v, want the server's 23505 before any commit", writeErr)
	}
	if err := <-backend; err != nil {
		t.Fatal(err)
	}
	if conn.Err() != nil || conn.inTx {
		t.Errorf("after the refusal: Err %v, inTx %v; want a usable connection with no transaction", conn.Err(), conn.inTx)
	}
	if err := cp.Close(); !errors.As(err, &pgErr) {
		t.Errorf("Close after the refusal returned %v, want the same error", err)
	}
}

// A statement sent in copy-in mode would not run: the server would end the
// COPY with an error, and its reply would land in the copy's reader. It is
// refused before a byte is written, and the copy stays finishable.
func TestExecRefusedWhileACopyIsOpen(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}
	err = conn.Exec(t.Context(), "SELECT 1")
	if !errors.Is(err, ErrConnBusy) || !strings.Contains(err.Error(), "COPY") {
		t.Fatalf("Exec during a copy returned %v, want ErrConnBusy naming the COPY", err)
	}
	if strings.Contains(srv.sent.String(), "SELECT 1") {
		t.Error("the refused statement reached the wire")
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close after the refused Exec: %v", err)
	}
	if conn.Err() != nil {
		t.Fatalf("the refusal broke the connection: %v", conn.Err())
	}
}

// A copier abandoned with its COPY open leaves its reader blocked in a read;
// closing the connection closes the socket under it, and nothing stays.
func TestAbandonedCopyLeavesNoGoroutineAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()
	conn, backend := newCopyBackend(t, func(rd *pgwire.Reader, w net.Conn) error {
		if err := openCopy(rd, w); err != nil {
			return err
		}
		for {
			if _, err := rd.Next(); errors.Is(err, io.EOF) {
				return nil // the client hung up
			} else if err != nil {
				return err
			}
		}
	})
	cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-backend; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines after Close, %d before the copy: the copy's reader outlived the connection", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}
