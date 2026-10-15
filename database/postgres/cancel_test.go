package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A caller that has already given up must cost the server nothing: not a
// wasted round trip, not a resynchronisation, not a byte.
func TestQueryRefusesACancelledContextWithoutSpeaking(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	src := conn.Query(ctx, "SELECT id", nil, QueryConfig{})
	var batches int
	src.Stream()(func(sluice.Batch[Row]) bool { batches++; return true })

	if batches != 0 {
		t.Errorf("the stream yielded %d batches from a cancelled context", batches)
	}
	if err := src.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want it to wrap context.Canceled", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("% x went to the server for a query the caller had already abandoned", srv.sent.Bytes())
	}
}

// The whole point of cancelling at a batch boundary: the query stops, and the
// connection survives it. A cancelled query that broke the connection would
// make a request deadline cost the caller their connection too.
func TestQueryCancelledBetweenBatchesKeepsTheConnection(t *testing.T) {
	// One row, then the portal suspends with more to come — which is where
	// the context is checked.
	var suspended []byte
	suspended = append(suspended, beSimple(pgwire.BackendParseComplete)...)
	suspended = append(suspended, beSimple(pgwire.BackendBindComplete)...)
	suspended = append(suspended, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	suspended = append(suspended, beDataRow(int8Val(1))...)
	suspended = append(suspended, beSimple(pgwire.BackendPortalSuspended)...)

	// A whole second query, to prove the connection is still usable.
	var second []byte
	second = append(second, beSimple(pgwire.BackendParseComplete)...)
	second = append(second, beSimple(pgwire.BackendBindComplete)...)
	second = append(second, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	second = append(second, beDataRow(int8Val(2))...)
	second = append(second, beCommandComplete("SELECT 1")...)

	// The answer to the two Executes sent ahead of the first batch's
	// delivery, which the resync drains.
	var ahead []byte
	ahead = append(ahead, beDataRow(int8Val(98))...)
	ahead = append(ahead, beSimple(pgwire.BackendPortalSuspended)...)
	ahead = append(ahead, beDataRow(int8Val(99))...)
	ahead = append(ahead, beSimple(pgwire.BackendPortalSuspended)...)

	srv := &fakeServer{responses: [][]byte{
		suspended,
		ahead,
		beReadyForQuery(), // answers the resync's Sync
		second,
		beReadyForQuery(),
	}}
	conn := NewConn(srv)

	ctx, cancel := context.WithCancel(t.Context())
	src := conn.Query(ctx, "SELECT id", nil, QueryConfig{BatchRows: 1})

	var seen []int64
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			id, _ := DecodeInt8(v)
			seen = append(seen, id)
		}
		cancel() // the caller gives up with the portal still suspended
		return true
	})

	if !slices.Equal(seen, []int64{1}) {
		t.Errorf("rows before the cancellation = %v, want [1]", seen)
	}
	if err := src.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want it to wrap context.Canceled", err)
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("the connection was broken by a cancellation: %v", err)
	}

	// The proof: another query runs on the same connection.
	again := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	var after []int64
	again.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			id, _ := DecodeInt8(v)
			after = append(after, id)
		}
		return true
	})
	if err := again.Err(); err != nil {
		t.Fatalf("the query after a cancellation returned %v", err)
	}
	if !slices.Equal(after, []int64{2}) {
		t.Errorf("rows after the cancellation = %v, want [2]", after)
	}

	// And the client must have resynchronised rather than left the portal
	// hanging: a Sync after the Executes sent ahead, and before the next query.
	// The next query binds the cached statement, and is fetched in batches:
	// a cancelled execution says nothing about the statement's size.
	sent := srv.clientMessages(t)
	wantSent := []byte{
		pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush,
		pgwire.FrontendExecute, pgwire.FrontendExecute, pgwire.FrontendFlush,
		pgwire.FrontendSync,
		pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush,
		pgwire.FrontendSync,
	}
	if !slices.Equal(sent, wantSent) {
		t.Errorf("client sent %q, want %q", sent, wantSent)
	}
}

// The context is checked once per batch, so a query whose every row fits in
// one batch is never interrupted halfway: cancelling inside the callback of
// the last batch cannot retroactively fail a result already complete.
func TestQueryCancelledInsideTheLastBatchStillSucceeds(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beDataRow(int8Val(7))...)
	script = append(script, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{script, beReadyForQuery()}}
	conn := NewConn(srv)

	ctx, cancel := context.WithCancel(t.Context())
	src := conn.Query(ctx, "SELECT id", nil, QueryConfig{BatchRows: 16})
	src.Stream()(func(sluice.Batch[Row]) bool { cancel(); return true })

	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v, want nil: the result was complete before the cancellation", err)
	}
}

func TestCopyFromRefusesACancelledContext(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := conn.CopyFrom(ctx, "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CopyFrom returned %v, want it to wrap context.Canceled", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("% x went to the server for a copy that never started", srv.sent.Bytes())
	}
}

// The one that would be got wrong for free: a cancelled WriteTuples must not
// clear the copier's transaction state, because Abort keys off it. Routed
// through fail, this test leaves a transaction open on a healthy connection —
// the vacuum-horizon failure Copier's documentation exists to prevent.
func TestCopyCancelledStillRollsBack(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		beCopyInResponse(),
		append(beErrorResponse("57014", "COPY from stdin failed"), beReadyForQuery()...),
		append(beCommandComplete("ROLLBACK"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)

	ctx, cancel := context.WithCancel(t.Context())
	cp, err := conn.CopyFrom(ctx, "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}

	cancel()
	if err := cp.WriteTuples(tuple(new(int64(2)), nil), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteTuples returned %v, want it to wrap context.Canceled", err)
	}
	if cp.Rows() != 1 {
		t.Errorf("Rows = %d: the cancelled tuples were counted as written", cp.Rows())
	}

	// Abort must still have something to abort.
	if err := cp.Abort("the request was cancelled"); err != nil {
		t.Fatalf("Abort after a cancellation returned %v", err)
	}
	sent := srv.clientMessages(t)
	if !bytes.Contains(sent, []byte{pgwire.FrontendCopyFail}) {
		t.Fatalf("client sent %q, with no CopyFail: the transaction was left open", sent)
	}
}

// Startup must keep the cancellation key, since CancelRequest is the only
// thing that can interrupt a statement the server is already executing.
func TestStartupKeepsTheCancelKey(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beAuth(0, ""), beReady()...),
	}}
	conn, err := startup(srv, Config{User: "u", Database: "d"})
	if err != nil {
		t.Fatalf("Startup returned %v", err)
	}
	key := conn.CancelKey()
	if !key.Known() {
		t.Fatal("the cancellation key was discarded")
	}
	if key.PID != 1234 || key.Secret != testSecret {
		t.Errorf("CancelKey = {%d, %x}, want {1234, %x}", key.PID, key.Secret, testSecret)
	}
}

// A Conn that never saw a startup exchange has no key, and must say so rather
// than offer a zero one that cancels backend 0.
func TestNewConnHasNoCancelKey(t *testing.T) {
	if conn := NewConn(&fakeServer{}); conn.CancelKey().Known() {
		t.Error("a Conn from NewConn reports a cancellation key it never received")
	}
}

func TestCancelRequestBytes(t *testing.T) {
	srv := &fakeServer{}
	if err := CancelRequest(srv, CancelKey{PID: 1234, Secret: testSecret}); err != nil {
		t.Fatalf("CancelRequest returned %v", err)
	}

	got := srv.sent.Bytes()
	want := make([]byte, 16)
	binary.BigEndian.PutUint32(want[0:], 16)
	binary.BigEndian.PutUint32(want[4:], 1234<<16|5678)
	binary.BigEndian.PutUint32(want[8:], 1234)
	binary.BigEndian.PutUint32(want[12:], 5678)
	if !bytes.Equal(got, want) {
		t.Errorf("cancellation packet:\n got % x\nwant % x", got, want)
	}
	if !srv.closed {
		t.Error("the cancellation connection was left open; it is one-shot")
	}
}

// Cancelling with a zero key would ask the server to interrupt backend 0,
// which is not this caller's to interrupt.
func TestCancelRequestRefusesAnUnknownKey(t *testing.T) {
	srv := &fakeServer{}
	if err := CancelRequest(srv, CancelKey{}); err == nil {
		t.Fatal("CancelRequest accepted an unknown key")
	}
	if srv.sent.Len() != 0 {
		t.Errorf("% x went to the server for a key that names no backend", srv.sent.Bytes())
	}
	if !srv.closed {
		t.Error("the connection was left open on the refusal path")
	}
}

// The secret is a credential: anyone who reads it can cancel that backend. It
// must not reach a log through the one method that gets called on everything.
func TestCancelKeyStringHidesTheSecret(t *testing.T) {
	key := CancelKey{PID: 1234, Secret: "s3cr3t-s3cr3t"}
	s := key.String()
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(verb, key); strings.Contains(got, "s3cr3t") {
			t.Errorf("Sprintf(%s) = %q, and the secret is in it", verb, got)
		}
	}
	if !strings.Contains(s, "1234") {
		t.Errorf("String() = %q, and the pid is not: the point is to be diagnosable", s)
	}
	if got := (CancelKey{}).String(); !strings.Contains(got, "unknown") {
		t.Errorf("String() on a zero key = %q", got)
	}
}

// Before 3.2 the secret is exactly four bytes; from 3.2 on it is 4 to 256.
func TestParseBackendKeyDataBounds(t *testing.T) {
	for _, tc := range []struct {
		minor uint32
		n     int
		ok    bool
	}{
		{0, 0, false},
		{0, 4, false},
		{0, 7, false},
		{0, 8, true},
		{0, 9, false},
		{0, 36, false},
		{2, 7, false},
		{2, 8, true},
		{2, 36, true},
		{2, 260, true},
		{2, 261, false},
	} {
		_, err := parseBackendKeyData(make([]byte, tc.n), tc.minor)
		if tc.ok && err != nil {
			t.Errorf("parseBackendKeyData(%d bytes, 3.%d) = %v, want nil", tc.n, tc.minor, err)
		}
		if !tc.ok && !errors.Is(err, ErrProtocol) {
			t.Errorf("parseBackendKeyData(%d bytes, 3.%d) = %v, want ErrProtocol", tc.n, tc.minor, err)
		}
	}
}

// Protocol 3.2 end to end on the client side: the startup packet asks for it,
// a 32-byte secret — what PostgreSQL 18 sends — is kept whole, and the
// CancelRequest hands it back byte for byte, its length in the length word.
func TestProtocol32LongCancelKeyRoundTrips(t *testing.T) {
	secret := strings.Repeat("\xa5\x5a", 16)
	srv := &fakeServer{responses: [][]byte{
		append(beAuth(0, ""), append(beBackendKeyDataSecret(secret), beReadyForQuery()...)...),
	}}
	conn, err := startup(srv, Config{User: "u", ProtocolMinor: 2})
	if err != nil {
		t.Fatalf("Startup returned %v", err)
	}
	startup, _, _ := clientTraffic(t, srv.sent.Bytes())
	if v := binary.BigEndian.Uint32(startup[:4]); v != 3<<16|2 {
		t.Errorf("protocol version = %#x, want %#x", v, 3<<16|2)
	}
	key := conn.CancelKey()
	if key.PID != 1234 || key.Secret != secret {
		t.Fatalf("CancelKey = {%d, %x}, want {1234, %x}", key.PID, key.Secret, secret)
	}

	cancel := &fakeServer{}
	if err := CancelRequest(cancel, key); err != nil {
		t.Fatalf("CancelRequest returned %v", err)
	}
	want := binary.BigEndian.AppendUint32(nil, 12+32)
	want = binary.BigEndian.AppendUint32(want, cancelRequestCode)
	want = binary.BigEndian.AppendUint32(want, 1234)
	want = append(want, secret...)
	if got := cancel.sent.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("cancellation packet:\n got % x\nwant % x", got, want)
	}
}

// A secret the protocol cannot carry is refused before a byte is written: the
// server matches it whole, so a mangled one would cancel nothing while the
// nil error said otherwise.
func TestCancelRequestRefusesASecretOutOfBounds(t *testing.T) {
	for _, n := range []int{1, 3, 257} {
		srv := &fakeServer{}
		if err := CancelRequest(srv, CancelKey{PID: 1, Secret: strings.Repeat("x", n)}); err == nil {
			t.Errorf("CancelRequest accepted a %d-byte secret", n)
		}
		if srv.sent.Len() != 0 {
			t.Errorf("%d-byte secret: % x went to the server", n, srv.sent.Bytes())
		}
	}
}

// pipeServer is a scripted server over net.Pipe for the tests that need the
// client to be *blocked in a read* when its context ends — which a replayed
// buffer cannot produce, since it never blocks. Reads are drained on a
// goroutine so the client's writes never stall; the script only sequences
// what the server says, and a write lands when the client reaches the read
// for it, which is the ordering the protocol itself imposes.
type pipeServer struct {
	conn net.Conn
	read chan struct{} // one tick per Read that returned bytes
}

func newPipeServer(t *testing.T) (*pipeServer, *Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	s := &pipeServer{conn: server, read: make(chan struct{}, 64)}
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := server.Read(buf)
			if n > 0 {
				s.read <- struct{}{}
			}
			if err != nil {
				return
			}
		}
	}()
	return s, NewConn(client)
}

func (s *pipeServer) write(t *testing.T, msgs ...[]byte) {
	t.Helper()
	for _, m := range msgs {
		if _, err := s.conn.Write(m); err != nil {
			t.Errorf("scripted server writing: %v", err)
		}
	}
}

// cancelPipe is a DialFunc whose second connection is a pipe: the packet
// written into it is handed to the test, and the other end is closed the way
// a server closes it.
func cancelPipe(dials *int, packets chan<- []byte) func(context.Context) (io.ReadWriteCloser, error) {
	return func(context.Context) (io.ReadWriteCloser, error) {
		*dials++
		c, s := net.Pipe()
		go func() {
			defer func() { _ = s.Close() }()
			pkt := make([]byte, 16)
			if _, err := io.ReadFull(s, pkt); err == nil {
				packets <- pkt
			}
		}()
		return c, nil
	}
}

// The promise of the recipe Connect keeps: a context that ends while the
// server is still executing the statement reaches the server, the statement
// ends with the context's error a round trip later, and the connection is
// still good for the next query. Replayed from the client's side by a
// server that answers nothing until the cancellation arrives — the real
// pg_sleep, in effect — so a client that only checked ctx at batch
// boundaries would sit in this test forever.
func TestQueryCancelsTheRunningStatementThroughTheRecipe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		key := CancelKey{PID: 4242, Secret: "\x00\x00\x5e\xca"}
		conn.cancelKey = key
		packets := make(chan []byte, 1)
		dials := 0
		conn.cancelDial = cancelPipe(&dials, packets)

		go func() {
			<-srv.read // the query
			pkt := <-packets
			if got := binary.BigEndian.Uint32(pkt[4:]); got != cancelRequestCode {
				t.Errorf("cancel packet code = %#x, want %#x", got, cancelRequestCode)
			}
			if pid, secret := binary.BigEndian.Uint32(pkt[8:]), string(pkt[12:]); pid != key.PID || secret != key.Secret {
				t.Errorf("cancel packet names pid %d secret %x, want %d %x", pid, secret, key.PID, key.Secret)
			}
			// The server aborts the statement; the ReadyForQuery follows the
			// client's Sync, as it does on every error.
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"pg_sleep"}, []uint32{OIDText}),
				beErrorResponse("57014", "canceling statement due to user request"))
			<-srv.read // Sync
			srv.write(t, beReadyForQuery())
			// A second query, answered in full.
			<-srv.read
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{OIDInt8}), beDataRow(int8Val(2)),
				beCommandComplete("SELECT 1"))
			<-srv.read // Sync
			srv.write(t, beReadyForQuery())
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		src := conn.Query(ctx, "SELECT pg_sleep(5)", nil, QueryConfig{})
		rows := 0
		src.Stream()(func(b sluice.Batch[Row]) bool { rows += len(b.Items); return true })
		err := src.Err()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err = %v, want it to wrap context.DeadlineExceeded", err)
		}
		if e, ok := errors.AsType[*Error](err); !ok || e.Code != "57014" {
			t.Errorf("Err = %v, want the server's 57014 reachable through errors.As", err)
		}
		if rows != 0 {
			t.Errorf("%d rows from a cancelled statement", rows)
		}
		if took := time.Since(start); took != 300*time.Millisecond {
			t.Errorf("the statement ended %v after it was sent, want the context's 300ms exactly (synctest)", took)
		}
		if dials != 1 {
			t.Errorf("the recipe was called %d times, want 1", dials)
		}

		src = conn.Query(t.Context(), "SELECT 2", nil, QueryConfig{})
		var got [][]byte
		src.Stream()(func(b sluice.Batch[Row]) bool {
			for _, r := range b.Items {
				v, _ := r.Value(0)
				got = append(got, slices.Clone(v))
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatalf("the query after the cancellation: %v", err)
		}
		if len(got) != 1 || !bytes.Equal(got[0], int8Val(2)) {
			t.Errorf("the query after the cancellation returned % x, want [2]", got)
		}
		if err := conn.Err(); err != nil {
			t.Errorf("the connection is broken: %v", err)
		}
		if dials != 1 {
			t.Errorf("the recipe was called %d times across both queries, want 1: the second context never ended", dials)
		}
	})
}

// Exec shares the mechanism: a context that ends while the server executes
// the statement reaches it, and the error says so in the context's terms.
func TestExecCancelsTheRunningStatementThroughTheRecipe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		dials := 0
		conn.cancelDial = cancelPipe(&dials, packets)

		go func() {
			<-srv.read
			<-packets
			srv.write(t, beErrorResponse("57014", "canceling statement due to user request"), beReadyForQuery())
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		err := conn.Exec(ctx, "CALL slow()")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Exec = %v, want it to wrap context.DeadlineExceeded", err)
		}
		if e, ok := errors.AsType[*Error](err); !ok || e.Code != "57014" {
			t.Errorf("Exec = %v, want the server's 57014 reachable through errors.As", err)
		}
		if dials != 1 {
			t.Errorf("the recipe was called %d times, want 1", dials)
		}
	})
}

// A cancellation that cannot be sent leaves the statement to run, as without
// the option — and when the query then ends on the context at a batch
// boundary, the error says why the deadline was not honoured earlier. A
// statement that finishes on its own keeps its result: a deadline the
// server beat is not an error.
func TestQueryReportsACancellationThatCouldNotBeSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		errDial := errors.New("no route to the server")
		dials := 0
		conn.cancelDial = func(context.Context) (io.ReadWriteCloser, error) {
			dials++
			return nil, errDial
		}

		go func() {
			<-srv.read
			// The server takes longer than the context allows, then suspends
			// after one row: the boundary where ctx is checked.
			time.Sleep(time.Second)
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{OIDInt8}), beDataRow(int8Val(1)),
				beSimple(pgwire.BackendPortalSuspended))
			<-srv.read // the Executes asked ahead, then the Sync
			<-srv.read
			srv.write(t, beReadyForQuery())
			// A statement that beats the deadline: one row and done.
			<-srv.read
			time.Sleep(time.Second)
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{OIDInt8}), beDataRow(int8Val(7)),
				beCommandComplete("SELECT 1"))
			<-srv.read
			srv.write(t, beReadyForQuery())
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		src := conn.Query(ctx, "SELECT id FROM slow", nil, QueryConfig{BatchRows: 1})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		err := src.Err()
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errDial) {
			t.Fatalf("Err = %v, want both context.DeadlineExceeded and the dial error", err)
		}
		if dials != 1 {
			t.Errorf("the recipe was called %d times, want 1", dials)
		}

		ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		src = conn.Query(ctx, "SELECT 7", nil, QueryConfig{})
		rows := 0
		src.Stream()(func(b sluice.Batch[Row]) bool { rows += len(b.Items); return true })
		if err := src.Err(); err != nil {
			t.Fatalf("a statement that finished on its own: %v", err)
		}
		if rows != 1 {
			t.Errorf("%d rows, want the 1 the server sent", rows)
		}
		if err := conn.Err(); err != nil {
			t.Errorf("the connection is broken: %v", err)
		}
	})
}

// Nothing is armed without a key, a dialer or a context that can end: no
// goroutine, no dial.
func TestCancelWatchIsNotArmedWithoutItsThreeInputs(t *testing.T) {
	conn := NewConn(&fakeServer{})
	dial := func(context.Context) (io.ReadWriteCloser, error) {
		t.Error("the recipe was called")
		return nil, errors.New("called")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if w := conn.watchCancel(ctx); w != nil {
		t.Error("armed without a dialer")
	}
	conn.cancelDial = dial
	if w := conn.watchCancel(ctx); w != nil {
		t.Error("armed without a key")
	}
	conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
	if w := conn.watchCancel(context.Background()); w != nil {
		t.Error("armed on a context that can never end")
	}
	w := conn.watchCancel(ctx)
	if w == nil {
		t.Fatal("not armed with all three inputs")
	}
	if err := w.end(); err != nil {
		t.Errorf("ending an unfired watch: %v", err)
	}
	cancel()
	// Stopped before it fired: the context ending now dials nothing.
	if err := settleCancel(ctx, nil, nil); err != nil {
		t.Errorf("settling nothing: %v", err)
	}
}
