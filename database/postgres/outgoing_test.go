package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A query parameter past what a server accepts in one message is refused
// before a byte leaves, and the connection serves the next query. The wire
// layer refused it at Flush with the read side's sentinel, after the
// statement cache and the query's state had moved for a batch that never
// left; the connector then broke a connection whose stream was intact, and
// advised Config.MaxMessageBytes, which does not move this bound.
func TestAnOversizedBindLeavesTheConnectionUsable(t *testing.T) {
	saved := maxOutgoing
	maxOutgoing = 256 // stand-in for 1 GiB, so the test allocates nothing
	t.Cleanup(func() { maxOutgoing = saved })

	synctest.Test(t, func(t *testing.T) {
		srv, conn := newScriptedPipe(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			// The first write the server sees is the second query's: the
			// oversized one sent nothing.
			if got := srv.read(); !strings.HasPrefix(got, "P") {
				t.Errorf("the first write the server saw was %q, want the next query's Parse", got)
			}
			srv.answer()
			srv.read() // its Sync
			srv.write(beReadyForQuery())
		}()

		big := Param{Value: make([]byte, 300)}
		src := conn.Query(t.Context(), "SELECT $1::bytea", []Param{big}, QueryConfig{})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		err := src.Err()
		if !errors.Is(err, ErrOutgoingTooLarge) || errors.Is(err, ErrConnBroken) {
			t.Fatalf("the oversized query ended with %v, want ErrOutgoingTooLarge and not ErrConnBroken", err)
		}
		if strings.Contains(err.Error(), "MaxMessageBytes") {
			t.Errorf("the error advises Config.MaxMessageBytes, which does not move this bound: %v", err)
		}
		if err := conn.Err(); err != nil {
			t.Fatalf("the connection is broken after a refusal that sent nothing: %v", err)
		}
		readAll(t, conn, "SELECT 1")
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the ReadyForQuery owed: %v", err)
		}
		<-done
	})
}

// The same promise for a Tx.Pipeline part: its Bind is judged before
// anything is buffered, as Query's is. Refused at Flush instead, the sender
// closed the socket and the caller got a broken connection for a pipeline
// that sent nothing.
func TestAnOversizedPipelinePartLeavesTheConnectionUsable(t *testing.T) {
	saved := maxOutgoing
	maxOutgoing = 256
	t.Cleanup(func() { maxOutgoing = saved })

	synctest.Test(t, func(t *testing.T) {
		srv, conn := newScriptedPipe(t)
		conn.inTx = true
		tx := &Tx{conn: conn, gen: conn.txGen}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if got := srv.read(); !strings.HasPrefix(got, "P") {
				t.Errorf("the first write the server saw was %q, want the next query's Parse", got)
			}
			srv.answer()
			srv.read() // its Sync
			srv.write(beReadyForQuery())
		}()
		parts := []Part{{SQL: "SELECT 1"}, {SQL: "SELECT $1::bytea", Params: []Param{{Value: make([]byte, 300)}}}}
		err := tx.Pipeline(t.Context(), parts, QueryConfig{}, func(int, sluice.Batch[Row]) bool { return true })
		if !errors.Is(err, ErrOutgoingTooLarge) || errors.Is(err, ErrConnBroken) {
			t.Fatalf("the oversized pipeline ended with %v, want ErrOutgoingTooLarge and not ErrConnBroken", err)
		}
		if err := conn.Err(); err != nil {
			t.Fatalf("the connection is broken after a refusal that sent nothing: %v", err)
		}
		readAll(t, conn, "SELECT 1")
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the ReadyForQuery owed: %v", err)
		}
		<-done
	})
}

// A pipeline whose write fails ends the connection, and the write's error is
// the one reported: the sender closes the socket to release the reader, and
// the read that close cuts short is a consequence. It broke the connection
// first, with its own "closed pipe", and the write's error was dropped.
func TestAFailedPipelineWriteIsTheErrorReported(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(tx *Tx) error
	}{
		{"Tx.Pipeline", func(tx *Tx) error {
			return tx.Pipeline(t.Context(), []Part{{SQL: "SELECT 1"}}, QueryConfig{},
				func(int, sluice.Batch[Row]) bool { return true })
		}},
		{"Tx.Savepoints", func(tx *Tx) error { return tx.Savepoints(t.Context(), "a", "b") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rw := &writeRefused{closed: make(chan struct{})}
			conn := NewConn(rw, Config{})
			conn.inTx = true
			err := tc.run(&Tx{conn: conn, gen: conn.txGen})
			if !errors.Is(err, ErrConnBroken) || !errors.Is(err, errWriteRefused) {
				t.Fatalf("%s = %v, want a broken connection carrying the write's error (%v)", tc.name, err, errWriteRefused)
			}
		})
	}
}

var errWriteRefused = errors.New("the transport refused the write")

// writeRefused is a transport that refuses every write and releases a read
// only when it is closed.
type writeRefused struct {
	closed chan struct{}
	once   sync.Once
}

func (r *writeRefused) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *writeRefused) Write([]byte) (int, error) { return 0, errWriteRefused }

func (r *writeRefused) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

// A COPY batch larger than one message may carry leaves as several CopyData
// messages, each under the bound: the protocol lets a frontend divide the
// data stream arbitrarily (PostgreSQL 18 §54.7, CopyData). Sent as one, it
// was refused at Flush and the connection broken.
func TestACopyBatchOverTheBoundIsSplit(t *testing.T) {
	saved := maxOutgoing
	maxOutgoing = 256
	t.Cleanup(func() { maxOutgoing = saved })

	var wire bytes.Buffer
	conn := NewConn(struct {
		io.Reader
		io.Writer
		io.Closer
	}{strings.NewReader(""), &wire, io.NopCloser(nil)}, Config{})
	body := make([]byte, 700)
	for i := range body {
		body[i] = byte(i)
	}
	if err := (&Copier{conn: conn}).writeData(body); err != nil {
		t.Fatalf("writeData: %v", err)
	}
	var got []byte
	out := wire.Bytes()
	for len(out) > 0 {
		if out[0] != pgwire.FrontendCopyData || len(out) < 5 {
			t.Fatalf("a %q message on the wire, want CopyData", out[0])
		}
		n := int(binary.BigEndian.Uint32(out[1:5]))
		if n > maxOutgoing {
			t.Errorf("a CopyData of %d bytes, over the bound of %d", n, maxOutgoing)
		}
		got = append(got, out[5:1+n]...)
		out = out[1+n:]
	}
	if !bytes.Equal(got, body) {
		t.Errorf("the CopyData messages carry %d bytes, not the batch's %d in order", len(got), len(body))
	}
}

// A SQL text no statement message could carry under the bound is refused
// before anything is buffered, and the connection serves the next query. It
// was refused at Flush, and the connection broken.
func TestAnOversizedSQLTextLeavesTheConnectionUsable(t *testing.T) {
	saved := maxOutgoing
	maxOutgoing = 1024
	t.Cleanup(func() { maxOutgoing = saved })

	synctest.Test(t, func(t *testing.T) {
		_, conn := newScriptedPipe(t) // the server reads nothing: a write would block
		sql := "SELECT '" + strings.Repeat("x", 2000) + "'"
		err := conn.Exec(t.Context(), sql)
		if !errors.Is(err, ErrOutgoingTooLarge) || errors.Is(err, ErrConnBroken) {
			t.Fatalf("Exec of a %d-byte text = %v, want ErrOutgoingTooLarge and not ErrConnBroken", len(sql), err)
		}
		if err := conn.Err(); err != nil {
			t.Fatalf("the connection is broken after a refusal that sent nothing: %v", err)
		}
	})
}
