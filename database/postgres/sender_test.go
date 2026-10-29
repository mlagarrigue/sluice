package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Tx.Savepoints and Tx.Pipeline flush on a goroutine and read the answers
// meanwhile, and join the sender before returning. Until 2026-10-29 a read
// that broke the connection — an answer the reader refuses — left the join
// waiting on a sender that a server no longer reading held in its write:
// the connection was known broken, nothing closed it, and the call never
// returned, whatever ctx said (a ctx that cannot end, or a backend with no
// cancel key, arms no watch). joinSender now closes the socket first. Here
// a client_encoding nobody asked for breaks the connection after the first
// SAVEPOINT is read.
func TestSavepointsDoesNotWaitOnASenderTheServerLeft(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		// The server reads exactly the first SAVEPOINT, answers with a message
		// that breaks the connection, and reads nothing more.
		first := []byte("SAVEPOINT sp0\x00")
		go func() {
			buf := make([]byte, 1+pgwire.LengthSize+len(first))
			if _, err := io.ReadFull(server, buf); err != nil {
				return
			}
			_, _ = server.Write(frame(pgwire.BackendParameterStatus, "client_encoding\x00LATIN1\x00"))
		}()
		names := make([]string, 300)
		for i := range names {
			names[i] = fmt.Sprintf("sp%d", i)
		}
		tx := &Tx{conn: NewConn(client, Config{})}
		res := make(chan error, 1)
		go func() { res <- tx.Savepoints(t.Context(), names...) }()
		var err error
		select {
		case err = <-res:
		case <-time.After(10 * time.Second):
			// A guard, not a measurement: nothing here waits on time.
			_ = client.Close()
			_ = server.Close()
			<-res
			t.Fatal("Savepoints is still blocked ten seconds after the server broke the connection")
		}
		_ = client.Close()
		_ = server.Close()
		if !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Savepoints returned %v, want ErrConnBroken", err)
		}
	})
}

// The same with a header no Config.MaxMessage admits, under a context that
// can never end: no cancellation watch is armed, and only the close releases
// the sender.
func TestSavepointsReturnsOnceTheReadBreaksUnderAnyContext(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		buf := make([]byte, 4096)
		if _, err := server.Read(buf); err != nil { // BEGIN
			return
		}
		if _, err := server.Write(frame('C', "BEGIN\x00")); err != nil {
			return
		}
		if _, err := server.Write(frame('Z', "T")); err != nil {
			return
		}
		// A header whose length no Config.MaxMessage admits; nothing is read
		// from the client again.
		_, _ = server.Write([]byte{'Z', 0x7f, 0xff, 0xff, 0xff})
	}()
	conn := NewConn(client, Config{})
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- tx.Savepoints(context.Background(), "a", "b", "c") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Savepoints = %v, want ErrConnBroken", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Savepoints still waiting for its sender 5 s after the reader broke the connection")
	}
}

// Tx.Pipeline under a context that can never end: the answer to the start
// of the pipeline breaks the connection and the server reads nothing more;
// Pipeline returns ErrConnBroken instead of waiting on its sender.
func TestPipelineReturnsOnceTheReadBreaksUnderAnyContext(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		buf := make([]byte, 4096)
		if _, err := server.Read(buf); err != nil { // BEGIN
			return
		}
		if _, err := server.Write(frame('C', "BEGIN\x00")); err != nil {
			return
		}
		if _, err := server.Write(frame('Z', "T")); err != nil {
			return
		}
		// Read the start of the pipeline, then refuse to read more.
		if _, err := server.Read(buf); err != nil {
			return
		}
		_, _ = server.Write([]byte{'Z', 0x7f, 0xff, 0xff, 0xff})
	}()
	conn := NewConn(client, Config{})
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]Part, 200)
	for i := range parts {
		parts[i] = Part{Setting: "app.tenant", Value: strconv.Itoa(i), SQL: "SELECT 1"}
	}
	done := make(chan error, 1)
	go func() {
		done <- tx.Pipeline(context.Background(), parts, QueryConfig{}, func(int, sluice.Batch[Row]) bool { return true })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Pipeline = %v, want ErrConnBroken", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Pipeline still waiting for its sender 5 s after the reader broke the connection")
	}
}
