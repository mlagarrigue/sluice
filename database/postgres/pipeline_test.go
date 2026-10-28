package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A nil callback is the caller's mistake, not a reason to take the process
// down over it: Pipeline is called once per batch of parts, a cold path
// where nothing is saved by panicking instead of returning a loggable error
// — unlike Row.Value/Rows.Value's panics, which are deliberate and
// cost-justified on a hot path.
func TestPipelineRefusesANilCallback(t *testing.T) {
	tx := &Tx{}
	err := tx.Pipeline(t.Context(), []Part{{SQL: "SELECT 1"}}, QueryConfig{}, nil)
	if err == nil {
		t.Fatal("a nil callback was accepted")
	}
}

// The byte bound cuts a pipelined part's batches as it cuts a query's: six
// rows of a megabyte under BatchBytes of four megabytes reach the caller as
// 4 and 2, for the part they belong to.
func TestPipelineBatchBytesCutsABatchEarly(t *testing.T) {
	const rowBytes = 1 << 20
	value := bytes.Repeat([]byte("x"), rowBytes)
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"v"}, []uint32{pgtype.OIDText})...)
	for range 6 {
		script = append(script, beDataRow(value)...)
	}
	script = append(script, beCommandComplete("SELECT 6")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	var sizes []int
	err := tx.Pipeline(t.Context(), []Part{{SQL: "SELECT v"}}, QueryConfig{BatchBytes: 4 * rowBytes},
		func(i int, b sluice.Batch[Row]) bool {
			if i != 0 {
				t.Errorf("a batch for part %d, want 0", i)
			}
			sizes = append(sizes, b.Len())
			return true
		})
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if want := []int{4, 2}; !slices.Equal(sizes, want) {
		t.Errorf("batches of %v rows, want %v", sizes, want)
	}
}

// The pipeline has no resync behind it: everything it sends goes out before
// anything is read, and one Sync closes the whole sequence. So a part whose
// answer cannot be read has to recover the position itself before returning,
// or the next query on this connection reads someone else's messages.
//
// What is proved here is that it does — and that the connection is still
// usable afterwards. Behind a gateway that is the difference between one
// caller getting an error and every caller waiting on a reconnection.
func TestAPipelinePartThatCannotBeReadKeepsTheConnection(t *testing.T) {
	// Two parts. The first answers with a DataRow claiming more fields than
	// its description declared; the second is never delivered to the caller,
	// but its messages are still on the wire and have to be drained.
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, frame(pgwire.BackendDataRow, string(dataRow(int8Val(1), int8Val(2))))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(7))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beReadyForQuery()...)

	var next []byte
	next = append(next, beSimple(pgwire.BackendParseComplete)...)
	next = append(next, beSimple(pgwire.BackendBindComplete)...)
	next = append(next, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	next = append(next, beDataRow(int8Val(42))...)
	next = append(next, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{script, next, beReadyForQuery()}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	parts := []Part{{SQL: "SELECT id"}, {SQL: "SELECT id"}}
	err := tx.Pipeline(t.Context(), parts, QueryConfig{BatchRows: 4},
		func(int, sluice.Batch[Row]) bool { return true })
	if err == nil {
		t.Fatal("a malformed DataRow was accepted")
	}
	if conn.Err() != nil {
		t.Fatalf("the pipeline broke the connection over an unreadable row: %v", conn.Err())
	}

	// The position was recovered, so the next query reads its own answer
	// rather than the tail of the abandoned pipeline.
	var got []int64
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := pgtype.DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the query after an abandoned pipeline returned %v", err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("read %v after recovering, want [42] — the reader is still inside the pipeline's messages", got)
	}
}

// And the other side: if the drain cannot reach a ReadyForQuery, the position
// really is unknown and the connection must be marked broken rather than
// handed back looking healthy.
func TestAPipelineThatCannotRecoverBreaksTheConnection(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, frame(pgwire.BackendDataRow, string(dataRow(int8Val(1), int8Val(2))))...)
	// No ReadyForQuery follows, so the drain runs out of messages.

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	err := tx.Pipeline(t.Context(), []Part{{SQL: "SELECT id"}}, QueryConfig{BatchRows: 4},
		func(int, sluice.Batch[Row]) bool { return true })
	if err == nil {
		t.Fatal("a pipeline that could not be recovered reported success")
	}
	if conn.Err() == nil {
		t.Fatal("the connection was left usable although the drain never reached a ReadyForQuery")
	}
	// And the caller is told *that*, rather than the malformed row that
	// started it: whether to retry on this connection turns on ErrConnBroken,
	// and a caller shown only the decode failure would retry into a socket
	// that can no longer answer.
	if !errors.Is(err, ErrConnBroken) {
		t.Errorf("Pipeline returned %v; a failed recovery must report ErrConnBroken so the caller stops reusing the connection", err)
	}
}

// Everything a pipeline sends is buffered and leaves in one flush, so a part
// refused half way through the loop used to leave the earlier parts' Parse,
// Bind and Execute sitting in the writer with no flush and no way to drop
// them.
//
// Nothing said so at the time: Pipeline returned a clean validation error and
// the connection looked fine. The next query on it appended its own messages
// behind the stale ones and flushed the lot; the server ran them in order,
// and this client read the stale parts' answers as that query's own. A wrong
// result, from a call that had already reported an error.
func TestPipelineValidatesEveryPartBeforeWritingAny(t *testing.T) {
	srv := &fakeServer{responses: oneRow()}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	parts := []Part{
		{SQL: "SELECT a"}, // valid, and written first
		{Setting: "app.tenant; DROP TABLE orders", Value: "1", SQL: "SELECT b"}, // refused
	}
	err := tx.Pipeline(t.Context(), parts, QueryConfig{},
		func(int, sluice.Batch[Row]) bool { return true })
	if err == nil {
		t.Fatal("a part with an unusable setting name was accepted")
	}
	if srv.sent.Len() != 0 {
		t.Errorf("%d bytes reached the server from a pipeline that was refused", srv.sent.Len())
	}

	// The proof that matters is what the *next* query sends: anything the
	// refused pipeline left behind goes out in front of it.
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	ps := parsed(t, srv)
	if len(ps) != 1 {
		t.Fatalf("the next query sent %d Parse messages, want 1: %v", len(ps), ps)
	}
	if ps[0][1] != "SELECT id" {
		t.Errorf("the next query's first Parse was %q — a refused part rode out in front of it", ps[0][1])
	}
}

// A panic in the caller's callback leaves the connection mid-pipeline with
// rows still on the wire and nothing marking it broken, so the next query
// would read them as its own. Query has deferred that drain since S10; the
// pipeline had no equivalent.
func TestPipelineDrainsWhenTheCallbackPanics(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(1))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	// A second part whose rows nobody will ever read, because the panic
	// happens while the first one is being delivered.
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(2))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beReadyForQuery()...)

	var next []byte
	next = append(next, beSimple(pgwire.BackendParseComplete)...)
	next = append(next, beSimple(pgwire.BackendBindComplete)...)
	next = append(next, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	next = append(next, beDataRow(int8Val(42))...)
	next = append(next, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{script, next, beReadyForQuery()}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	boom := errors.New("the caller's own bug")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = tx.Pipeline(t.Context(), []Part{{SQL: "SELECT id"}, {SQL: "SELECT id"}},
			QueryConfig{BatchRows: 4},
			func(int, sluice.Batch[Row]) bool { panic(boom) })
	}()

	// The panic is the caller's and keeps going: recovering it here would turn
	// their bug into a return value.
	if err, _ := recovered.(error); !errors.Is(err, boom) {
		t.Fatalf("the pipeline swallowed or replaced the panic: %v", recovered)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection broke over a panicking callback: %v", conn.Err())
	}

	// The connection is back at a known position, so the next query reads its
	// own answer rather than the abandoned pipeline's leftovers.
	var got []int64
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			id, _ := pgtype.DecodeInt8(v)
			got = append(got, id)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the query after the panic failed: %v", err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("the next query read %v, want [42] — it was handed the abandoned pipeline's rows", got)
	}
}

// M-8: the reader counts CommandCompletes to know which part it is on, which
// makes the count an input the server controls. One extra result cycle used
// to drive fn(len(parts), …) — an index-out-of-range panic in the documented
// usage `serve(parts[i], b)`. It must be a protocol error instead.
func TestPipelineRefusesAnExtraResultCycle(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(1))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	// A second cycle nobody asked for.
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(99))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	parts := []Part{{SQL: "SELECT id"}}
	err := tx.Pipeline(t.Context(), parts, QueryConfig{BatchRows: 4},
		func(i int, b sluice.Batch[Row]) bool {
			_ = parts[i] // the documented usage: panics if i runs past the parts
			return true
		})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Pipeline returned %v, want ErrProtocol for a result cycle beyond the parts", err)
	}
	// The count can no longer be trusted, so neither can the attribution of
	// anything already delivered: the connection is done.
	if !errors.Is(err, ErrConnBroken) {
		t.Errorf("Pipeline returned %v, want it to also report the connection broken", err)
	}
}

// The subtler half of M-8: a CommandComplete splitting one part's result
// early used to shift every later result one part forward — rows delivered
// under the wrong principal's index, which on the RLS path is one tenant
// reading another's data. The shift surfaces as a cycle past the end, which
// must refuse rather than deliver.
func TestPipelineRefusesAnEarlySplitCommandComplete(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(1))...)
	script = append(script, beCommandComplete("SELECT 1")...) // splits part 0 early
	script = append(script, beDataRow(int8Val(99))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	// Part 1's real result, which now arrives one part late.
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(2))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	parts := []Part{{SQL: "SELECT a"}, {SQL: "SELECT b"}}
	var indices []int
	err := tx.Pipeline(t.Context(), parts, QueryConfig{BatchRows: 4},
		func(i int, b sluice.Batch[Row]) bool {
			indices = append(indices, i)
			return true
		})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Pipeline returned %v, want ErrProtocol once the cycle count runs past the parts", err)
	}
	for _, i := range indices {
		if i >= len(parts) {
			t.Errorf("fn was called with part index %d, past the %d parts sent", i, len(parts))
		}
	}
}

// A notification just before ReadyForQuery is something the docs tell clients
// to expect; it used to break the connection mid-pipeline.
func TestPipelineToleratesANotification(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	script = append(script, beDataRow(int8Val(7))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beNotification("jobs", "42")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	tx := &Tx{conn: conn}

	var rows int
	err := tx.Pipeline(t.Context(), []Part{{SQL: "SELECT id"}}, QueryConfig{BatchRows: 4},
		func(i int, b sluice.Batch[Row]) bool { rows += len(b.Items); return true })
	if err != nil {
		t.Fatalf("a notification failed the pipeline: %v", err)
	}
	if rows != 1 {
		t.Errorf("delivered %d rows, want 1", rows)
	}
	if conn.Err() != nil {
		t.Errorf("a notification broke the connection: %v", conn.Err())
	}
}

// A server does not wait for the end of a pipeline before answering: it
// executes each part as it arrives and writes the result at once. Over a
// transport with no buffer — net.Pipe, or a socket whose buffers are full —
// that answer cannot leave until someone reads it, and a client still busy
// sending the rest of the pipeline is not reading. The pipeline used to be
// that client: everything written, one synchronous Flush, then the first
// read. With the server blocked on its write and the client on its Flush,
// nobody moved and nothing failed.
//
// The fake server here answers each message the moment it has read it, which
// is the worst case of what a real one does.
func TestPipelineReadsWhileItStillWrites(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()
	// A deadline turns the hang this guards against into an error: the
	// unfixed code fails here in seconds instead of at the test timeout.
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}

	const parts = 64
	param := bytes.Repeat([]byte("p"), 16<<10)
	result := bytes.Repeat([]byte("r"), 4<<10)

	served := make(chan error, 1)
	go func() {
		served <- answerAsRead(server, parts, result)
	}()

	conn := NewConn(client)
	tx := &Tx{conn: conn}
	ps := make([]Part, parts)
	for i := range ps {
		ps[i] = Part{SQL: "SELECT $1", Params: []Param{{OID: pgtype.OIDText, Value: param}}}
	}
	var got []int
	err := tx.Pipeline(t.Context(), ps, QueryConfig{}, func(i int, b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			if v, _ := r.Value(0); !bytes.Equal(v, result) {
				t.Errorf("part %d: row is %d bytes, want %d", i, len(v), len(result))
			}
			got = append(got, i)
		}
		return true
	})
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("the fake server: %v", err)
	}
	if len(got) != parts {
		t.Fatalf("%d parts were answered, want %d", len(got), parts)
	}
	for i, p := range got {
		if p != i {
			t.Fatalf("part %d was delivered as part %d", i, p)
		}
	}
	if conn.Err() != nil {
		t.Fatalf("the connection is broken: %v", conn.Err())
	}
}

// answerAsRead is the fake server of [TestPipelineReadsWhileItStillWrites]:
// it reads frontend messages one at a time and writes each one's answer
// before reading the next. It stops at the Sync, having answered it.
func answerAsRead(server net.Conn, parts int, result []byte) error {
	rd := pgwire.NewReader(server)
	executes := 0
	for {
		m, err := rd.Next()
		if err != nil {
			return fmt.Errorf("reading the client: %w", err)
		}
		var answer []byte
		switch m.Type {
		case pgwire.FrontendParse:
			answer = beSimple(pgwire.BackendParseComplete)
		case pgwire.FrontendBind:
			answer = beSimple(pgwire.BackendBindComplete)
		case pgwire.FrontendDescribe:
			answer = beRowDescription([]string{"v"}, []uint32{pgtype.OIDText})
		case pgwire.FrontendExecute:
			executes++
			answer = append(beDataRow(result), beCommandComplete("SELECT 1")...)
		case pgwire.FrontendSync:
			if executes != parts {
				return fmt.Errorf("%d Executes before the Sync, want %d", executes, parts)
			}
			_, err := server.Write(beReadyForQuery())
			return err
		default:
			return fmt.Errorf("unexpected frontend message %q", m.Type)
		}
		if _, err := server.Write(answer); err != nil {
			return fmt.Errorf("answering %q: %w", m.Type, err)
		}
	}
}

// A write that fails mid-pipeline ends the connection, and ends it now: the
// reader waiting on the other side of the socket is released rather than
// left reading a server that will never answer a Sync it never received.
func TestPipelineSendFailureBreaksTheConnection(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// The server reads one message and hangs up.
	go func() {
		rd := pgwire.NewReader(server)
		_, _ = rd.Next()
		_ = server.Close()
	}()

	conn := NewConn(client)
	tx := &Tx{conn: conn}
	ps := make([]Part, 8)
	for i := range ps {
		ps[i] = Part{SQL: "SELECT 1"}
	}
	err := tx.Pipeline(t.Context(), ps, QueryConfig{}, func(int, sluice.Batch[Row]) bool { return true })
	if !errors.Is(err, ErrConnBroken) {
		t.Fatalf("Pipeline returned %v, want ErrConnBroken", err)
	}
	if !errors.Is(conn.Err(), ErrConnBroken) {
		t.Fatalf("the connection is not marked broken: %v", conn.Err())
	}
}

// Tx.Savepoints reads its answers while it still writes, as Pipeline does.
// It used to write every SAVEPOINT before reading any answer, leaning on the
// socket buffers for about twenty bytes of answer per name; on a transport
// with no buffer at all the server blocks writing the first answer, stops
// reading, and the client blocks writing the second SAVEPOINT. Three hundred
// names over net.Pipe, answered as they are read.
func TestSavepointsReadWhileTheyStillWrite(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	const n = 300
	served := make(chan error, 1)
	go func() {
		rd := pgwire.NewReader(server)
		for range n {
			m, err := rd.Next()
			if err != nil {
				served <- err
				return
			}
			if m.Type != pgwire.FrontendQuery {
				served <- fmt.Errorf("message %q, want a Query", m.Type)
				return
			}
			answer := append(frame(pgwire.BackendCommandComplete, "SAVEPOINT\x00"), frame(pgwire.BackendReadyForQuery, "T")...)
			if _, err := server.Write(answer); err != nil {
				served <- err
				return
			}
		}
		served <- nil
	}()

	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("sp%d", i)
	}
	tx := &Tx{conn: NewConn(client)}
	if err := tx.Savepoints(t.Context(), names...); err != nil {
		t.Fatalf("Savepoints over an unbuffered transport: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("the server: %v", err)
	}
}
