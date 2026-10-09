package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

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

	srv := &fakeServer{responses: [][]byte{
		suspended,
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
	// hanging: a Sync between the two Executes.
	sent := srv.clientMessages(t)
	wantSent := []byte{
		pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush,
		pgwire.FrontendSync,
		pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush,
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
	conn, err := Startup(srv, StartupConfig{User: "u", Database: "d"})
	if err != nil {
		t.Fatalf("Startup returned %v", err)
	}
	key := conn.CancelKey()
	if !key.Known() {
		t.Fatal("the cancellation key was discarded")
	}
	if key.PID != 1234 || key.Secret != 5678 {
		t.Errorf("CancelKey = {%d, %d}, want {1234, 5678}", key.PID, key.Secret)
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
	if err := CancelRequest(srv, CancelKey{PID: 1234, Secret: 5678}); err != nil {
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
	s := CancelKey{PID: 1234, Secret: 5678}.String()
	if strings.Contains(s, "5678") {
		t.Errorf("String() = %q, and the secret is in it", s)
	}
	if !strings.Contains(s, "1234") {
		t.Errorf("String() = %q, and the pid is not: the point is to be diagnosable", s)
	}
	if got := (CancelKey{}).String(); !strings.Contains(got, "unknown") {
		t.Errorf("String() on a zero key = %q", got)
	}
}

func TestParseBackendKeyDataRejectsShortBodies(t *testing.T) {
	for _, n := range []int{0, 4, 7, 9} {
		if _, err := parseBackendKeyData(make([]byte, n)); !errors.Is(err, ErrProtocol) {
			t.Errorf("parseBackendKeyData(%d bytes) = %v, want ErrProtocol", n, err)
		}
	}
}
