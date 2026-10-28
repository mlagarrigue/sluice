package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
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
	// The clock stands still in a bubble, so the round trip and the first
	// batch time to zero and the client keeps the floor of two batches ahead.
	synctest.Test(t, func(t *testing.T) {
		// One row, then the portal suspends with more to come — which is where
		// the context is checked.
		var suspended []byte
		suspended = append(suspended, beSimple(pgwire.BackendParseComplete)...)
		suspended = append(suspended, beSimple(pgwire.BackendBindComplete)...)
		suspended = append(suspended, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
		suspended = append(suspended, beDataRow(int8Val(1))...)
		suspended = append(suspended, beSimple(pgwire.BackendPortalSuspended)...)

		// A whole second query, to prove the connection is still usable.
		var second []byte
		second = append(second, beSimple(pgwire.BackendParseComplete)...)
		second = append(second, beSimple(pgwire.BackendBindComplete)...)
		second = append(second, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
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
				id, _ := pgtype.DecodeInt8(v)
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
				id, _ := pgtype.DecodeInt8(v)
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
		// a cancelled execution says nothing about the statement's size. Its
		// portal suspended, though, so the two Executes ahead leave with it.
		sent := srv.clientMessages(t)
		wantSent := []byte{
			pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush,
			pgwire.FrontendExecute, pgwire.FrontendExecute, pgwire.FrontendFlush,
			pgwire.FrontendSync,
			pgwire.FrontendBind, pgwire.FrontendDescribe,
			pgwire.FrontendExecute, pgwire.FrontendExecute, pgwire.FrontendExecute, pgwire.FrontendFlush,
			pgwire.FrontendSync,
		}
		if !slices.Equal(sent, wantSent) {
			t.Errorf("client sent %q, want %q", sent, wantSent)
		}
	})
}

// The context is checked once per batch, so a query whose every row fits in
// one batch is never interrupted halfway: cancelling inside the callback of
// the last batch cannot retroactively fail a result already complete.
func TestQueryCancelledInsideTheLastBatchStillSucceeds(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
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

// The password is the same kind of credential, and a Config is the value a
// program most often prints whole when a connection fails.
func TestConfigStringHidesThePassword(t *testing.T) {
	cfg := Config{User: "app", Database: "orders", Password: "hunter2-pw"}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		got := fmt.Sprintf(verb, cfg)
		if strings.Contains(got, "hunter2") {
			t.Errorf("Sprintf(%s) = %q, and the password is in it", verb, got)
		}
		if !strings.Contains(got, "app") || !strings.Contains(got, "orders") {
			t.Errorf("Sprintf(%s) = %q, and the role or database is not: the point is to be diagnosable", verb, got)
		}
	}
}

// Before 3.2 the secret is exactly four bytes; from 3.2 on it is 4 to 256.
func TestParseBackendKeyDataBounds(t *testing.T) {
	for _, tc := range []struct {
		version ProtocolVersion
		n       int
		ok      bool
	}{
		{Protocol30, 0, false},
		{Protocol30, 4, false},
		{Protocol30, 7, false},
		{Protocol30, 8, true},
		{Protocol30, 9, false},
		{Protocol30, 36, false},
		{Protocol32, 7, false},
		{Protocol32, 8, true},
		{Protocol32, 36, true},
		{Protocol32, 260, true},
		{Protocol32, 261, false},
	} {
		_, err := parseBackendKeyData(make([]byte, tc.n), tc.version)
		if tc.ok && err != nil {
			t.Errorf("parseBackendKeyData(%d bytes, %v) = %v, want nil", tc.n, tc.version, err)
		}
		if !tc.ok && !errors.Is(err, ErrProtocol) {
			t.Errorf("parseBackendKeyData(%d bytes, %v) = %v, want ErrProtocol", tc.n, tc.version, err)
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
	conn, err := startup(srv, Config{User: "u", MaxProtocol: Protocol32})
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
	conn := NewConn(client)
	// Run before the close above: a read that ended the test with its
	// ReadyForQuery owed leaves the script writing it on a pipe, which waits
	// for a reader.
	t.Cleanup(func() { _ = conn.settleReady() })
	return s, conn
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
				beRowDescription([]string{"pg_sleep"}, []uint32{pgtype.OIDText}),
				beErrorResponse("57014", "canceling statement due to user request"))
			<-srv.read // Sync
			srv.write(t, beReadyForQuery())
			// A second query, answered in full.
			<-srv.read
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(2)),
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
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the owed ReadyForQuery: %v", err)
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

// recordHandler keeps every record it is handed, so a test can read the
// warnings a connection logged.
type recordHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordHandler) WithGroup(string) slog.Handler            { return h }

func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}

// warning returns the one warning logged, failing the test on any other
// count, and checks the secret is nowhere in it.
func (h *recordHandler) warning(t *testing.T, secret string) map[string]slog.Value {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.recs) != 1 || h.recs[0].Level != slog.LevelWarn {
		t.Fatalf("logged %d records, want one warning: %v", len(h.recs), h.recs)
	}
	attrs := map[string]slog.Value{}
	h.recs[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		if strings.Contains(a.Value.String(), secret) {
			t.Errorf("the warning's %s carries the cancellation secret", a.Key)
		}
		return true
	})
	return attrs
}

// cancelHolding is a DialFunc whose server reads the packet and holds the
// second connection open for hold before closing it; a negative hold never
// closes it, and the connection ends when the client gives up on it.
func cancelHolding(hold time.Duration, packets chan<- []byte) DialFunc {
	return func(context.Context) (io.ReadWriteCloser, error) {
		c, s := net.Pipe()
		go func() {
			defer func() { _ = s.Close() }()
			pkt := make([]byte, 16)
			if _, err := io.ReadFull(s, pkt); err != nil {
				return
			}
			packets <- pkt
			if hold < 0 {
				_, _ = io.Copy(io.Discard, s)
				return
			}
			time.Sleep(hold)
		}()
		return c, nil
	}
}

// The close of the second connection is the server's only word that it
// acted on the request, and nothing goes out on the main one before it: a
// request still in flight would otherwise cancel the next statement. The
// server aborts the statement at once and keeps the cancellation
// connection open two seconds more — the statement ends then, not before,
// and the connection is good.
func TestTheStatementWaitsForTheServerToConfirmTheCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		conn.cancelDial = cancelHolding(2*time.Second, packets)

		go func() {
			<-srv.read
			<-packets
			srv.write(t, beErrorResponse("57014", "canceling statement due to user request"), beReadyForQuery())
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := conn.Exec(ctx, "CALL slow()")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Exec = %v, want it to wrap context.DeadlineExceeded", err)
		}
		if took := time.Since(start); took != 2100*time.Millisecond {
			t.Errorf("Exec returned %v after it was sent, want 2.1s: the deadline, then the server's close", took)
		}
		if err := conn.Err(); err != nil {
			t.Errorf("the connection is broken after a confirmed cancellation: %v", err)
		}
	})
}

// A server that takes the packet and never closes the connection has
// confirmed nothing: at CancelTimeout the main connection is closed — the
// statement's read is released by it, the server never answering — and
// the call ends with the context's error and ErrConnBroken, one warning
// logged.
func TestAnUnconfirmedCancellationBreaksTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 7, Secret: testSecret}
		packets := make(chan []byte, 1)
		conn.cancelDial = cancelHolding(-1, packets)
		conn.cancelTimeout = 2 * time.Second
		logs := &recordHandler{}
		conn.logger = slog.New(logs)

		go func() {
			<-srv.read
			<-packets // and the statement runs on
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		src := conn.Query(ctx, "SELECT pg_sleep(60)", nil, QueryConfig{})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		err := src.Err()
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Err = %v, want both context.DeadlineExceeded and ErrConnBroken", err)
		}
		if !strings.Contains(err.Error(), "CancelTimeout") {
			t.Errorf("Err = %v, want it to name CancelTimeout", err)
		}
		if took := time.Since(start); took != 2100*time.Millisecond {
			t.Errorf("Query returned %v after it was sent, want the deadline plus the 2s CancelTimeout", took)
		}
		if !errors.Is(conn.Err(), ErrConnBroken) {
			t.Errorf("conn.Err() = %v, want ErrConnBroken", conn.Err())
		}
		attrs := logs.warning(t, testSecret)
		if done := attrs["statement_completed"]; done.Kind() != slog.KindBool || done.Bool() {
			t.Errorf("statement_completed = %v, want false", done)
		}
		if pid := attrs["pid"]; pid.Kind() != slog.KindUint64 || pid.Uint64() != 7 {
			t.Errorf("pid = %v, want 7", pid)
		}
	})
}

// A recipe that cannot reach the server breaks the connection at once,
// without waiting for CancelTimeout: nothing else would end a statement the
// server may run forever, and the error names both the context and why the
// cancellation could not be sent.
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
		logs := &recordHandler{}
		conn.logger = slog.New(logs)
		go func() { <-srv.read }()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		src := conn.Query(ctx, "SELECT id FROM slow", nil, QueryConfig{})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		err := src.Err()
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errDial) || !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Err = %v, want context.DeadlineExceeded, the dial error and ErrConnBroken", err)
		}
		if took := time.Since(start); took != 100*time.Millisecond {
			t.Errorf("Query returned %v after it was sent, want the 100ms deadline", took)
		}
		if dials != 1 {
			t.Errorf("the recipe was called %d times, want 1", dials)
		}
		logs.warning(t, testSecret)
	})
}

// A statement that completed while its cancellation failed keeps its
// result — an INSERT reported failed after it committed is an INSERT
// retried — and the connection is closed behind it, the warning being the
// only trace.
func TestACompletedStatementKeepsItsResultWhenTheCancellationFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		conn.cancelDial = func(ctx context.Context) (io.ReadWriteCloser, error) {
			<-ctx.Done() // a dial that hangs to the bound
			return nil, ctx.Err()
		}
		logs := &recordHandler{}
		conn.logger = slog.New(logs)

		go func() {
			<-srv.read
			time.Sleep(200 * time.Millisecond)
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(7)),
				beCommandComplete("SELECT 1"))
			<-srv.read
			srv.write(t, beReadyForQuery())
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		src := conn.Query(ctx, "SELECT 7", nil, QueryConfig{})
		rows := 0
		src.Stream()(func(b sluice.Batch[Row]) bool { rows += len(b.Items); return true })
		if err := src.Err(); err != nil {
			t.Fatalf("a statement that completed: %v", err)
		}
		if rows != 1 {
			t.Errorf("%d rows, want the 1 the server sent", rows)
		}
		if took := time.Since(start); took != 100*time.Millisecond+defaultCancelTimeout {
			t.Errorf("Query returned %v after it was sent, want the deadline plus the default CancelTimeout", took)
		}
		if !errors.Is(conn.Err(), ErrConnBroken) {
			t.Errorf("conn.Err() = %v, want ErrConnBroken", conn.Err())
		}
		if done := logs.warning(t, testSecret)["statement_completed"]; done.Kind() != slog.KindBool || !done.Bool() {
			t.Errorf("statement_completed = %v, want true", done)
		}
	})
}

// A recipe that hands back the main connection would write the packet into
// the statement's own byte stream, where the server reads it as a message.
// It is refused unwritten, and the connection closed as for any
// cancellation not confirmed.
func TestARecipeReturningTheMainConnectionIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		conn.cancelDial = func(context.Context) (io.ReadWriteCloser, error) { return conn.rw, nil }
		logs := &recordHandler{}
		conn.logger = slog.New(logs)
		go func() { <-srv.read }()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		err := conn.Exec(ctx, "CALL slow()")
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Exec = %v, want both context.DeadlineExceeded and ErrConnBroken", err)
		}
		if !strings.Contains(err.Error(), "connection being cancelled") {
			t.Errorf("Exec = %v, want it to say the recipe returned the connection being cancelled", err)
		}
		synctest.Wait()
		if n := len(srv.read); n != 0 {
			t.Errorf("the main connection carried %d writes after the statement, want none", n)
		}
		logs.warning(t, testSecret)
	})

	// A type == would panic on is compared as different, not crashed on.
	type uncomparable struct {
		io.ReadWriteCloser
		_ []byte
	}
	if sameTransport(uncomparable{}, uncomparable{}) {
		t.Error("two values of an uncomparable type compared as the same connection")
	}
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
	if err := conn.settleCancel(ctx, nil, nil); err != nil {
		t.Errorf("settling nothing: %v", err)
	}
}

// streamRows plays a server told to produce everything: after the query it
// writes a description and then rows, one at a time, until a cancellation
// packet arrives or limit rows are out. A cancellation aborts the statement
// with query_canceled; otherwise the result ends normally. Either way the
// ReadyForQuery of the Sync that left with the query follows. It reports
// how many rows went out and whether the cancellation arrived.
func streamRows(t *testing.T, srv *pipeServer, packets <-chan []byte, limit int) (sent int, cancelled bool) {
	t.Helper()
	<-srv.read // the query, Sync included
	srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
		beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}))
	for sent < limit {
		// Every other goroutine of the bubble goes as far as it can before
		// the next row: a cancellation already decided has reached packets,
		// so the count is the reader's doing, not the scheduler's. Without
		// it the client drained 743 rows on one run while the dial waited
		// for a turn.
		synctest.Wait()
		select {
		case <-packets:
			srv.write(t, beErrorResponse("57014", "canceling statement due to user request"), beReadyForQuery())
			return sent, true
		default:
		}
		sent++
		srv.write(t, beDataRow(int8Val(int64(sent))))
	}
	srv.write(t, beCommandComplete(fmt.Sprintf("SELECT %d", sent)), beReadyForQuery())
	return sent, false
}

// answerOne plays the next query, answered with the single row 2.
func answerOne(t *testing.T, srv *pipeServer) {
	t.Helper()
	<-srv.read
	srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
		beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(2)),
		beCommandComplete("SELECT 1"))
	<-srv.read // Sync
	srv.write(t, beReadyForQuery())
}

// firstBatchOnly runs sql with FetchAll and ten rows a batch, and stops after
// the first batch.
func firstBatchOnly(t *testing.T, conn *Conn, sql string) error {
	t.Helper()
	src := conn.Query(t.Context(), sql, nil, QueryConfig{Fetch: FetchAll, BatchRows: 10})
	batches := 0
	src.Stream()(func(sluice.Batch[Row]) bool { batches++; return false })
	if batches != 1 {
		t.Errorf("%d batches delivered, want 1", batches)
	}
	return src.Err()
}

// checkNextQuery runs a second query and checks it gets its own row on a
// connection still good.
func checkNextQuery(t *testing.T, conn *Conn) {
	t.Helper()
	src := conn.Query(t.Context(), "SELECT 2", nil, QueryConfig{})
	var got [][]byte
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			got = append(got, slices.Clone(v))
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the query after the early stop: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], int8Val(2)) {
		t.Errorf("the query after the early stop returned % x, want [2]", got)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("the connection is broken: %v", err)
	}
}

// A consumer that stops at the first batch of a read asked for whole does
// not pay for the rest: two batches past the stop, a cancellation goes out
// by the recipe, the server's query_canceled is not reported, and the next
// query finds the connection good. Draining instead would read all 100 000
// rows the server is prepared to send.
func TestAnEarlyStopOfAFetchAllCancelsTheRest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		dials := 0
		conn.cancelDial = cancelPipe(&dials, packets)

		type outcome struct {
			sent      int
			cancelled bool
		}
		out := make(chan outcome, 1)
		go func() {
			sent, cancelled := streamRows(t, srv, packets, 100_000)
			out <- outcome{sent, cancelled}
			answerOne(t, srv)
		}()

		err := firstBatchOnly(t, conn, "SELECT n FROM generate_series(1, 100000) n")
		o := <-out
		sent, cancelled := o.sent, o.cancelled
		if err != nil {
			t.Fatalf("Err = %v, want nil: the query_canceled is the package's own doing", err)
		}
		if !cancelled || dials != 1 {
			t.Fatalf("cancelled = %v after %d dials, %d rows sent: want the rest cancelled", cancelled, dials, sent)
		}
		// Ten delivered, twenty read past the stop before the cancellation,
		// and a few in flight while it travels.
		if sent < 30 || sent > 100 {
			t.Errorf("%d rows sent before the cancellation, want 30 to 100", sent)
		}
		checkNextQuery(t, conn)
		if dials != 1 {
			t.Errorf("the recipe was called %d times, want 1", dials)
		}
	})
}

// A rest shorter than the two batches a batched read would drain is read,
// not cancelled: a cancellation costs a connection and a fork on the server,
// which twenty rows do not.
func TestAShortRestIsReadRatherThanCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		dials := 0
		conn.cancelDial = cancelPipe(&dials, packets)

		go func() {
			streamRows(t, srv, packets, 30)
			answerOne(t, srv)
		}()
		if err := firstBatchOnly(t, conn, "SELECT n FROM t"); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if dials != 0 {
			t.Errorf("the recipe was called %d times for a 20-row rest, want 0", dials)
		}
		checkNextQuery(t, conn)
	})
}

// Where a cancellation would undo what reading to the end commits, or abort
// a transaction block the caller still holds, the rest is drained as before.
func TestAnEarlyStopDrainsWhatACancellationWouldRollBack(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		inTx      bool
	}{
		{"insert returning", "INSERT INTO t SELECT n FROM generate_series(1, 200) n RETURNING n", false},
		{"with", "WITH d AS (DELETE FROM t RETURNING n) SELECT n FROM d", false},
		{"call", "CALL p()", false},
		{"select in a transaction block", "SELECT n FROM t", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, conn := newPipeServer(t)
				conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
				conn.inTx = tc.inTx
				packets := make(chan []byte, 1)
				dials := 0
				conn.cancelDial = cancelPipe(&dials, packets)

				out := make(chan int, 1)
				go func() {
					sent, _ := streamRows(t, srv, packets, 200)
					out <- sent
					answerOne(t, srv)
				}()
				err := firstBatchOnly(t, conn, tc.sql)
				sent := <-out
				if err != nil {
					t.Fatalf("Err = %v", err)
				}
				if dials != 0 || sent != 200 {
					t.Errorf("%d dials, %d of 200 rows sent: want the rest drained, nothing cancelled", dials, sent)
				}
				conn.inTx = false
				checkNextQuery(t, conn)
			})
		})
	}
}

// The race the protocol warns of: the statement finishes on its own while
// the cancellation travels, and a request that lands after it would cancel
// whatever runs next. So the connection is handed back only once the server
// has closed the cancellation connection — its word that the backend took
// the signal, which an idle backend drops. Here it confirms two seconds
// after the result ended; the query returns then, not before, and the next
// one goes out after.
func TestAnEarlyStopWaitsForItsCancellationBeforeTheNextStatement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		conn.cancelDial = cancelHolding(2*time.Second, packets)

		start := time.Now()
		nextSent := make(chan time.Duration, 1)
		go func() {
			// The rest outruns the cancellation: every row is out and the
			// result ends before the packet is even looked for.
			<-srv.read
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}))
			for i := range 40 {
				srv.write(t, beDataRow(int8Val(int64(i))))
			}
			srv.write(t, beCommandComplete("SELECT 40"), beReadyForQuery())
			<-packets
			<-srv.read
			nextSent <- time.Since(start)
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(2)),
				beCommandComplete("SELECT 1"))
			<-srv.read
			srv.write(t, beReadyForQuery())
		}()

		if err := firstBatchOnly(t, conn, "SELECT n FROM t"); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if took := time.Since(start); took != 2*time.Second {
			t.Errorf("the early stop returned after %v, want 2s: the server's confirmation", took)
		}
		checkNextQuery(t, conn)
		if at := <-nextSent; at < 2*time.Second {
			t.Errorf("the next statement went out at %v, before the cancellation was confirmed at 2s", at)
		}
	})
}

// A cancellation the server never confirms leaves nothing to say where the
// request is: at CancelTimeout the connection is closed, which also ends a
// read the server stopped answering. The consumer had what it asked for, so
// the query reports nil; the connection is broken and a warning says why.
func TestAnUnconfirmedEarlyStopCancellationBreaksTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 9, Secret: testSecret}
		packets := make(chan []byte, 1)
		conn.cancelDial = cancelHolding(-1, packets)
		conn.cancelTimeout = 2 * time.Second
		logs := &recordHandler{}
		conn.logger = slog.New(logs)

		go func() {
			<-srv.read
			srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}))
			for i := 0; ; i++ {
				select {
				case <-packets:
					return // and the statement runs on, saying nothing
				default:
				}
				srv.write(t, beDataRow(int8Val(int64(i))))
			}
		}()

		start := time.Now()
		if err := firstBatchOnly(t, conn, "SELECT n FROM t"); err != nil {
			t.Fatalf("Err = %v, want nil: the consumer stopped, the statement only read", err)
		}
		if took := time.Since(start); took != 2*time.Second {
			t.Errorf("the early stop returned after %v, want the 2s CancelTimeout", took)
		}
		if !errors.Is(conn.Err(), ErrConnBroken) || !strings.Contains(conn.Err().Error(), "CancelTimeout") {
			t.Errorf("conn.Err() = %v, want ErrConnBroken naming CancelTimeout", conn.Err())
		}
		attrs := logs.warning(t, testSecret)
		if done := attrs["statement_completed"]; done.Kind() != slog.KindBool || !done.Bool() {
			t.Errorf("statement_completed = %v, want true", done)
		}
	})
}

func TestReadsOnly(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT 1": true,
		"  -- why\n/* a /* nested */ */select n from t": true,
		"VALUES (1), (2)": true,
		"table t":         true,
		"WITH d AS (DELETE FROM t RETURNING n) SELECT n FROM d": false,
		"(SELECT 1)":                           false,
		"INSERT INTO t VALUES (1) RETURNING n": false,
		"selectivity":                          false,
		"":                                     false,
	} {
		if got := readsOnly(sql); got != want {
			t.Errorf("readsOnly(%q) = %v, want %v", sql, got, want)
		}
	}
}

// A confirmed cancellation followed by silence on the main connection still
// ends the statement: CancelTimeout after the confirmation, the connection
// is closed under it, and the call returns the context's error with
// ErrConnBroken. Until 2026-10-28 the confirmation ended the watch's part,
// and a server that confirmed and then said nothing held the statement, the
// connection and its caller for as long as the socket lived
// (RP-20261027-secapp-03).
func TestAConfirmedCancellationThenSilenceStillEndsTheStatement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, conn := newPipeServer(t)
		conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
		packets := make(chan []byte, 1)
		conn.cancelDial = cancelHolding(0, packets) // confirms at once
		conn.cancelTimeout = 2 * time.Second
		go func() { <-srv.read; <-packets }() // and never answers the statement

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		res := make(chan error, 1)
		go func() { res <- conn.Exec(ctx, "CALL slow()") }()
		var err error
		select {
		case err = <-res:
		case <-time.After(time.Minute):
			_ = conn.rw.Close()
			<-res
			t.Fatal("Exec still blocked a minute after a confirmed cancellation, CancelTimeout 2 s")
		}
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Exec returned %v, want the context's error with ErrConnBroken", err)
		}
		if took := time.Since(start); took < 2*time.Second || took > 3*time.Second {
			t.Errorf("Exec returned %v after it began, want CancelTimeout after the confirmation", took)
		}
	})
}
