package postgres

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// fakeServer scripts a backend: it hands out prepared responses and records
// what the client sent, so a whole conversation is deterministic and needs no
// database. Every message the client writes is parsed back with this package's
// own reader, which is what makes "what did we send" an assertion rather than
// a hex dump.
type fakeServer struct {
	responses [][]byte // one chunk per point at which the client asked to be answered
	sent      bytes.Buffer
	pos       int
	readBuf   bytes.Buffer
	writeErr  error
	closed    bool

	// scanned and triggers track the one thing a script would otherwise get
	// wrong for free: see errNotAsked.
	scanned  int
	triggers int
}

// errNotAsked reports a script read from before the client asked to be
// answered.
//
// This exists because its absence cost a connector that could not run a single
// query. A real backend buffers everything an extended-query sequence produces
// and delivers it only on Sync or Flush; a script that answers whenever it is
// read from agrees with a client that never asks, and the disagreement only
// shows up against a server — where it is not a wrong answer but a hang until
// whichever deadline expires first.
//
// It is the handover's complaint about scripted servers made false in the one
// place it demonstrably bit: the script now disagrees.
var errNotAsked = errors.New("fakeServer: read before the client sent anything that makes a backend deliver (Sync, Flush, Query, CopyDone or CopyFail)")

func (s *fakeServer) Read(p []byte) (int, error) {
	if s.readBuf.Len() == 0 {
		if s.pos >= len(s.responses) {
			return 0, io.EOF
		}
		s.scanClient()
		if s.triggers == 0 {
			return 0, errNotAsked
		}
		// Decremented, not cleared: a client may send several deliverable
		// messages before reading any of them — which is what pipelining is —
		// and each one is owed its own answer. Clearing the count made the
		// second read of such a batch look like a read nobody had asked for.
		s.triggers--
		s.readBuf.Write(s.responses[s.pos])
		s.pos++
	}
	return s.readBuf.Read(p)
}

// scanClient walks everything the client has written since the last scan and
// counts the messages after which a real backend delivers what it holds.
func (s *fakeServer) scanClient() {
	buf := s.sent.Bytes()
	for {
		rest := buf[s.scanned:]
		if len(rest) < pgwire.LengthSize {
			return
		}
		// The startup packet is the one message with no type byte. It is
		// recognised by its first byte, which is the high byte of a small
		// length and therefore zero — a value no frontend message type has.
		if rest[0] == 0 {
			n := int(binary.BigEndian.Uint32(rest))
			if n < pgwire.LengthSize || len(rest) < n {
				return
			}
			s.scanned += n
			s.triggers++ // the server answers a startup packet
			continue
		}
		if len(rest) < 1+pgwire.LengthSize {
			return
		}
		n := int(binary.BigEndian.Uint32(rest[1:]))
		if n < pgwire.LengthSize || len(rest) < 1+n {
			return
		}
		switch rest[0] {
		case pgwire.FrontendPassword, pgwire.FrontendQuery, pgwire.FrontendSync, pgwire.FrontendFlush,
			pgwire.FrontendCopyDone, pgwire.FrontendCopyFail:
			s.triggers++
		}
		s.scanned += 1 + n
	}
}

func (s *fakeServer) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.sent.Write(p)
}

func (s *fakeServer) Close() error { s.closed = true; return nil }

// clientMessages parses back everything the client wrote, as type bytes.
func (s *fakeServer) clientMessages(t *testing.T) []byte {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(s.sent.Bytes()))
	var types []byte
	for {
		m, err := rd.Next()
		if err != nil {
			return types
		}
		types = append(types, m.Type)
	}
}

// Backend message builders, so the scripts below read like the protocol.
func beRowDescription(names []string, oids []uint32) []byte {
	return frame(pgwire.BackendRowDescription, string(rowDesc(names, oids)))
}

func beDataRow(values ...[]byte) []byte {
	return frame(pgwire.BackendDataRow, string(dataRow(values...)))
}

func beSimple(typ byte) []byte { return frame(typ, "") }

func beCommandComplete(tag string) []byte {
	return frame(pgwire.BackendCommandComplete, tag+"\x00")
}

func beReadyForQuery() []byte { return frame(pgwire.BackendReadyForQuery, "I") }

func beErrorResponse(code, msg string) []byte {
	return frame(pgwire.BackendErrorResponse, string(errBody("S", "ERROR", "C", code, "M", msg)))
}

func int8Val(v int64) []byte { return AppendInt8(nil, v) }

// A whole query, from the messages the client sends to the rows it reads back.
func TestConnQuery(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id", "total"}, []uint32{OIDInt8, OIDInt8})...)
	first = append(first, beDataRow(int8Val(1), int8Val(100))...)
	first = append(first, beDataRow(int8Val(2), nil)...)
	first = append(first, beCommandComplete("SELECT 2")...)

	srv := &fakeServer{responses: [][]byte{first, beReadyForQuery()}}
	conn := NewConn(srv)

	keys := AppendInt8Array(nil, []int64{1, 2})
	src := conn.Query(t.Context(), "SELECT id, total FROM orders WHERE id = ANY($1)",
		[][]byte{keys}, QueryConfig{BatchRows: 16})

	type got struct {
		id     int64
		total  int64
		isNull bool
	}
	var rows []got
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			idBytes, _ := r.Value(0)
			id, err := DecodeInt8(idBytes)
			if err != nil {
				t.Errorf("decoding id: %v", err)
			}
			totalBytes, isNull := r.Value(1)
			var total int64
			if !isNull {
				total, _ = DecodeInt8(totalBytes)
			}
			rows = append(rows, got{id, total, isNull})
		}
		return true
	})

	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	want := []got{{1, 100, false}, {2, 0, true}}
	if !slices.Equal(rows, want) {
		t.Errorf("rows = %v, want %v", rows, want)
	}

	// The client must have pipelined the sequence §8.5 describes, asked the
	// backend to deliver, then synced.
	//
	// The Flush is not decoration and removing it must fail here. The backend
	// holds everything an extended-query sequence produces until a Sync or a
	// Flush, and Sync is not usable at that point because it would close the
	// portal the following Executes read from. Without it this client waits
	// for an answer the server has already written down and is holding — a
	// hang against every real PostgreSQL, and invisible to a script that
	// answers whenever it is read from. fakeServer now refuses to.
	sent := srv.clientMessages(t)
	wantSent := []byte{
		pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute,
		pgwire.FrontendFlush, pgwire.FrontendSync,
	}
	if !slices.Equal(sent, wantSent) {
		t.Errorf("client sent %q, want %q", sent, wantSent)
	}
}

// The column description travels with the rows, so a caller can discover a
// shape it did not write the query for.
func TestConnQueryExposesFields(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beDataRow(int8Val(7))...)
	script = append(script, beCommandComplete("SELECT 1")...)

	conn := NewConn(&fakeServer{responses: [][]byte{script, beReadyForQuery()}})
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})

	var fields []Field
	src.Stream()(func(b sluice.Batch[Row]) bool {
		if b.Len() > 0 {
			fields = b.Items[0].Fields()
		}
		return true
	})
	if len(fields) != 1 || fields[0].Name != "id" || fields[0].TypeOID != OIDInt8 {
		t.Errorf("fields = %+v", fields)
	}
}

// The pull reaches the server: a suspended portal means the client asks for
// the next batch, and it asks once per batch rather than once for the result.
func TestConnQueryAsksForEachBatch(t *testing.T) {
	var head []byte
	head = append(head, beSimple(pgwire.BackendParseComplete)...)
	head = append(head, beSimple(pgwire.BackendBindComplete)...)
	head = append(head, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	head = append(head, beDataRow(int8Val(1))...)
	head = append(head, beDataRow(int8Val(2))...)
	head = append(head, beSimple(pgwire.BackendPortalSuspended)...)

	var second []byte
	second = append(second, beDataRow(int8Val(3))...)
	second = append(second, beCommandComplete("SELECT 3")...)

	srv := &fakeServer{responses: [][]byte{head, second, beReadyForQuery()}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 2})

	var sizes []int
	var ids []int64
	src.Stream()(func(b sluice.Batch[Row]) bool {
		sizes = append(sizes, b.Len())
		for _, r := range b.Items {
			v, _ := r.Value(0)
			id, _ := DecodeInt8(v)
			ids = append(ids, id)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if want := []int{2, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v — the portal was not resumed a batch at a time", sizes, want)
	}
	if want := []int64{1, 2, 3}; !slices.Equal(ids, want) {
		t.Errorf("ids = %v, want %v", ids, want)
	}

	// Two Executes: one in the opening pipeline, one to resume the portal.
	sent := srv.clientMessages(t)
	executes := bytes.Count(sent, []byte{pgwire.FrontendExecute})
	if executes != 2 {
		t.Errorf("client sent %d Executes, want 2 — one per batch", executes)
	}
}

// A consumer that stops mid-result must leave the connection usable: the
// protocol is resynchronised to ReadyForQuery before the stream returns.
func TestConnQueryEarlyStopResynchronises(t *testing.T) {
	var head []byte
	head = append(head, beSimple(pgwire.BackendParseComplete)...)
	head = append(head, beSimple(pgwire.BackendBindComplete)...)
	head = append(head, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	head = append(head, beDataRow(int8Val(1))...)
	head = append(head, beSimple(pgwire.BackendPortalSuspended)...)

	srv := &fakeServer{responses: [][]byte{head, beReadyForQuery()}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 1})

	n := 0
	src.Stream()(func(sluice.Batch[Row]) bool {
		n++
		return false // stop on the first batch
	})

	if n != 1 {
		t.Fatalf("saw %d batches, want 1", n)
	}
	if err := src.Err(); err != nil {
		t.Errorf("Err = %v, want nil — an early stop is not a failure", err)
	}
	if conn.Err() != nil {
		t.Errorf("the connection was left broken by an early stop: %v", conn.Err())
	}
	// The Sync that resynchronises must have been sent.
	sent := srv.clientMessages(t)
	if !bytes.Contains(sent, []byte{pgwire.FrontendSync}) {
		t.Errorf("client sent %q, with no Sync — the next query would read this one's rows", sent)
	}
}

// A server error is reported through the Source, carries its SQLSTATE, and
// leaves the connection usable: the server skips to the next Sync and carries
// on, so this is a query failure rather than a connection failure.
func TestConnQueryServerError(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beErrorResponse("42P01", `relation "orders" does not exist`)...)

	srv := &fakeServer{responses: [][]byte{script, beReadyForQuery()}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT * FROM orders", nil, QueryConfig{})

	n := 0
	src.Stream()(func(sluice.Batch[Row]) bool { n++; return true })

	if n != 0 {
		t.Errorf("yielded %d batches for a failing query", n)
	}
	var pgErr *Error
	if err := src.Err(); !errors.As(err, &pgErr) {
		t.Fatalf("Err = %v, want a *postgres.Error", err)
	}
	if pgErr.Code != "42P01" {
		t.Errorf("SQLSTATE = %q, want 42P01", pgErr.Code)
	}
	if conn.Err() != nil {
		t.Errorf("a query error broke the connection: %v — the server recovers at the next Sync", conn.Err())
	}
}

// A truncated response leaves the protocol position unknown, so the connection
// is finished: the alternative is the next query reading this one's leftovers
// and returning them as its own answer.
func TestConnQueryTruncatedResponseBreaksConn(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	// The stream stops here: no CommandComplete, no ReadyForQuery.

	conn := NewConn(&fakeServer{responses: [][]byte{script}})
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })

	if err := src.Err(); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("Err = %v, want ErrConnBroken", err)
	}
	if conn.Err() == nil {
		t.Error("the connection does not report itself broken")
	}

	// A second query must refuse rather than pretend.
	again := conn.Query(t.Context(), "SELECT 1", nil, QueryConfig{})
	again.Stream()(func(sluice.Batch[Row]) bool {
		t.Error("a broken connection yielded rows")
		return false
	})
	if err := again.Err(); !errors.Is(err, ErrConnBroken) {
		t.Errorf("second query Err = %v, want ErrConnBroken", err)
	}
}

// A DataRow arriving before any RowDescription means the stream is being read
// at the wrong offset, which is not something to interpret.
func TestConnQueryRowBeforeDescription(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beDataRow(int8Val(1))...)

	conn := NewConn(&fakeServer{responses: [][]byte{script}})
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })

	if err := src.Err(); !errors.Is(err, ErrProtocol) {
		t.Errorf("Err = %v, want ErrProtocol", err)
	}
}

// A failed write is a broken connection: after a partial write the position is
// unknown, and resending would corrupt rather than repair.
func TestConnQueryWriteFailure(t *testing.T) {
	srv := &fakeServer{writeErr: errors.New("connection reset")}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT 1", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })

	if err := src.Err(); !errors.Is(err, ErrConnBroken) {
		t.Errorf("Err = %v, want ErrConnBroken", err)
	}
}

// Notices arrive out of band and must not disturb a result.
func TestConnQueryIgnoresNotices(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, frame(pgwire.BackendNoticeResponse, string(errBody("S", "NOTICE", "M", "hello")))...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beDataRow(int8Val(9))...)
	script = append(script, beCommandComplete("SELECT 1")...)

	conn := NewConn(&fakeServer{responses: [][]byte{script, beReadyForQuery()}})
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})

	n := 0
	src.Stream()(func(b sluice.Batch[Row]) bool { n += b.Len(); return true })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if n != 1 {
		t.Errorf("read %d rows, want 1 — a notice disturbed the result", n)
	}
}

// The example the package exists for, checked end to end: a batch of keys
// leaves as one parameter.
func TestConnQueryBatchOfKeysIsOneParameter(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beCommandComplete("SELECT 0")...)

	srv := &fakeServer{responses: [][]byte{script, beReadyForQuery()}}
	conn := NewConn(srv)

	keys := make([]int64, 1000)
	for i := range keys {
		keys[i] = int64(i)
	}
	src := conn.Query(t.Context(), "SELECT id FROM orders WHERE id = ANY($1)",
		[][]byte{AppendInt8Array(nil, keys)}, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}

	// One Bind, and its parameter count is one however many keys it carries.
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	for {
		m, err := rd.Next()
		if err != nil {
			t.Fatal("no Bind was sent")
		}
		if m.Type != pgwire.FrontendBind {
			continue
		}
		// portal(1) stmt(1) formatCount(2) format(2) then the parameter count.
		n := binary.BigEndian.Uint16(m.Body[6:8])
		if n != 1 {
			t.Errorf("Bind carries %d parameters for 1000 keys, want 1", n)
		}
		return
	}
}

func TestConnCloseSendsTerminate(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	if !srv.closed {
		t.Error("the underlying connection was not closed")
	}
	if sent := srv.clientMessages(t); !slices.Equal(sent, []byte{pgwire.FrontendTerminate}) {
		t.Errorf("client sent %q on close, want a Terminate", sent)
	}
}

// A broken connection is closed without a Terminate: writing to a stream whose
// position is unknown adds bytes nobody can interpret.
func TestConnCloseSkipsTerminateWhenBroken(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT 1", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if conn.Err() == nil {
		t.Fatal("the connection should be broken after an empty response")
	}
	sentBefore := srv.sent.Len()
	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	if srv.sent.Len() != sentBefore {
		t.Error("Close wrote to a connection whose position is unknown")
	}
}

// AllRows sends the Sync with the query instead of a Flush, so the answer and
// the ReadyForQuery arrive together. One round trip instead of two.
func TestAllRowsSendsSyncInsteadOfFlush(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beDataRow(int8Val(1))...)
	script = append(script, beCommandComplete("SELECT 1")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8, AllRows: true})
	var seen int
	src.Stream()(func(b sluice.Batch[Row]) bool { seen += b.Len(); return true })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if seen != 1 {
		t.Errorf("read %d rows, want 1", seen)
	}

	sent := srv.clientMessages(t)
	want := []byte{pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendSync}
	if !slices.Equal(sent, want) {
		t.Errorf("client sent %q, want %q — a Flush here is the round trip AllRows exists to remove", sent, want)
	}
	if bytes.Contains(sent, []byte{pgwire.FrontendFlush}) {
		t.Error("a Flush went out under AllRows")
	}
}

// The Execute must carry no row limit: a limit makes the backend answer
// PortalSuspended, and the portal is closed by the Sync that follows.
func TestAllRowsAsksForEveryRow(t *testing.T) {
	var script []byte
	script = append(script, beSimple(pgwire.BackendParseComplete)...)
	script = append(script, beSimple(pgwire.BackendBindComplete)...)
	script = append(script, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	script = append(script, beCommandComplete("SELECT 0")...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4, AllRows: true})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}

	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	for {
		m, err := rd.Next()
		if err != nil {
			t.Fatal("no Execute was sent")
		}
		if m.Type != pgwire.FrontendExecute {
			continue
		}
		_, rest, ok := cstring(m.Body)
		if !ok {
			t.Fatal("malformed Execute")
		}
		if got := binary.BigEndian.Uint32(rest); got != 0 {
			t.Errorf("Execute asked for %d rows, want 0 (every row)", got)
		}
		return
	}
}

// A second Sync would draw a second ReadyForQuery that nothing reads, and the
// next query on the connection would take it for its own answer.
func TestAllRowsDoesNotSyncTwice(t *testing.T) {
	one := func(v int64) []byte {
		var s []byte
		s = append(s, beSimple(pgwire.BackendParseComplete)...)
		s = append(s, beSimple(pgwire.BackendBindComplete)...)
		s = append(s, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
		s = append(s, beDataRow(int8Val(v))...)
		s = append(s, beCommandComplete("SELECT 1")...)
		return append(s, beReadyForQuery()...)
	}
	srv := &fakeServer{responses: [][]byte{one(1), one(2)}}
	conn := NewConn(srv)

	for _, want := range []int64{1, 2} {
		var got []int64
		src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8, AllRows: true})
		src.Stream()(func(b sluice.Batch[Row]) bool {
			for _, r := range b.Items {
				v, _ := r.Value(0)
				n, _ := DecodeInt8(v)
				got = append(got, n)
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("read %v, want [%d] — the connection desynchronised", got, want)
		}
	}

	// Exactly one Sync per query, and no Flush at all.
	sent := srv.clientMessages(t)
	var syncs, flushes int
	for _, typ := range sent {
		switch typ {
		case pgwire.FrontendSync:
			syncs++
		case pgwire.FrontendFlush:
			flushes++
		}
	}
	if syncs != 2 {
		t.Errorf("%d Syncs for two queries, want 2", syncs)
	}
	if flushes != 0 {
		t.Errorf("%d Flushes under AllRows, want 0", flushes)
	}
}

// Stopping early under AllRows must still leave the connection usable — the
// rest of the result is drained to the ReadyForQuery already on its way.
func TestAllRowsEarlyStopKeepsTheConnection(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	for i := range 5 {
		first = append(first, beDataRow(int8Val(int64(i)))...)
	}
	first = append(first, beCommandComplete("SELECT 5")...)
	first = append(first, beReadyForQuery()...)

	var second []byte
	second = append(second, beSimple(pgwire.BackendParseComplete)...)
	second = append(second, beSimple(pgwire.BackendBindComplete)...)
	second = append(second, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	second = append(second, beDataRow(int8Val(99))...)
	second = append(second, beCommandComplete("SELECT 1")...)
	second = append(second, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{first, second}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 2, AllRows: true})
	var batches int
	src.Stream()(func(sluice.Batch[Row]) bool { batches++; return false })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if batches != 1 {
		t.Errorf("consumer saw %d batches after stopping at the first", batches)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection broke on an early stop: %v", conn.Err())
	}

	// And the next query reads its own answer rather than the leftovers.
	var got []int64
	again := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8, AllRows: true})
	again.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := again.Err(); err != nil {
		t.Fatalf("the query after an early stop returned %v", err)
	}
	if len(got) != 1 || got[0] != 99 {
		t.Errorf("read %v after an early stop, want [99]", got)
	}
}

// The default path must be untouched: a Flush, a row limit, and a Sync of its
// own from the resync.
func TestWithoutAllRowsTheFlushRemains(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	first = append(first, beDataRow(int8Val(1))...)
	first = append(first, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{first, beReadyForQuery()}}
	conn := NewConn(srv)
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	sent := srv.clientMessages(t)
	want := []byte{pgwire.FrontendParse, pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush, pgwire.FrontendSync}
	if !slices.Equal(sent, want) {
		t.Errorf("client sent %q, want %q", sent, want)
	}
}

// A socket read is a system call whatever its size, and the protocol's
// messages are small and numerous — so a connection over one must buffer.
// Profiling a wide scan put 79% of its time in those calls.
func TestConnBuffersASocketAndNothingElse(t *testing.T) {
	t.Run("a net.Conn is buffered", func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close(); _ = server.Close() }()

		conn := NewConn(client)
		if _, ok := conn.r.Underlying().(*bufio.Reader); !ok {
			t.Errorf("reads from a net.Conn go through %T, want a *bufio.Reader — "+
				"every protocol message would cost two syscalls", conn.r.Underlying())
		}
	})

	t.Run("an in-memory transport is not", func(t *testing.T) {
		conn := NewConn(&fakeServer{})
		if _, ok := conn.r.Underlying().(*bufio.Reader); ok {
			t.Error("an in-memory transport was buffered; that is a pure copy, " +
				"and it measured +5% on the replay benchmark")
		}
	})
}

// The buffer must not change what is read, only how often the kernel is asked.
// A message split across two fills, or one larger than the buffer, has to come
// back whole.
func TestBufferedReadsSpanFills(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	// One message comfortably larger than the read buffer, so its body cannot
	// arrive in a single fill.
	big := make([]byte, ReadBufferSize+4096)
	for i := range big {
		big[i] = byte(i)
	}
	go func() {
		defer func() { _ = server.Close() }()
		_, _ = server.Write(frame(pgwire.BackendDataRow, string(big)))
		_, _ = server.Write(frame(pgwire.BackendCommandComplete, "SELECT 1\x00"))
	}()

	conn := NewConn(client)
	m, err := conn.r.Next()
	if err != nil {
		t.Fatalf("reading a message larger than the buffer: %v", err)
	}
	if m.Type != pgwire.BackendDataRow {
		t.Fatalf("type = %q", m.Type)
	}
	if len(m.Body) != len(big) {
		t.Fatalf("body is %d bytes, want %d", len(m.Body), len(big))
	}
	if !bytes.Equal(m.Body, big) {
		t.Error("the body came back altered across buffer fills")
	}

	// And the message after it, which begins inside a fill the previous one
	// consumed part of.
	m, err = conn.r.Next()
	if err != nil {
		t.Fatalf("reading the message after a large one: %v", err)
	}
	if m.Type != pgwire.BackendCommandComplete {
		t.Errorf("type = %q, want CommandComplete", m.Type)
	}
}

// A row this package cannot read is a failed query, not a lost connection.
// The reader framed the message whole, so the position is still known and the
// resync recovers it — throwing away a working socket over one unreadable row
// costs a reconnection and, behind a gateway, every caller waiting on it.
func TestAnUnreadableRowDoesNotBreakTheConnection(t *testing.T) {
	// A DataRow claiming three fields where the description declared one.
	var bad []byte
	bad = append(bad, beSimple(pgwire.BackendParseComplete)...)
	bad = append(bad, beSimple(pgwire.BackendBindComplete)...)
	bad = append(bad, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	bad = append(bad, frame(pgwire.BackendDataRow, string(dataRow(int8Val(1), int8Val(2), int8Val(3))))...)

	var good []byte
	good = append(good, beSimple(pgwire.BackendParseComplete)...)
	good = append(good, beSimple(pgwire.BackendBindComplete)...)
	good = append(good, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	good = append(good, beDataRow(int8Val(42))...)
	good = append(good, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{
		bad, beReadyForQuery(), // the resync recovers the position
		good, beReadyForQuery(),
	}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err == nil {
		t.Fatal("a malformed DataRow was accepted")
	}
	if !errors.Is(src.Err(), ErrProtocol) {
		t.Errorf("err = %v, want ErrProtocol", src.Err())
	}

	// The point of the test: the connection survived.
	if err := conn.Err(); err != nil {
		t.Fatalf("the connection was broken by an unreadable row: %v", err)
	}

	var got []int64
	again := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	again.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := again.Err(); err != nil {
		t.Fatalf("the query after an unreadable row returned %v", err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("read %v after recovering, want [42]", got)
	}
}

// The same for a description that cannot be parsed: the message was framed, so
// the conversation is intact even though the query is not.
func TestAnUnreadableDescriptionDoesNotBreakTheConnection(t *testing.T) {
	var bad []byte
	bad = append(bad, beSimple(pgwire.BackendParseComplete)...)
	bad = append(bad, beSimple(pgwire.BackendBindComplete)...)
	// A RowDescription whose field count exceeds what its body carries.
	bad = append(bad, frame(pgwire.BackendRowDescription, "\x00\x7f")...)

	srv := &fakeServer{responses: [][]byte{bad, beReadyForQuery(), beReadyForQuery()}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if src.Err() == nil {
		t.Fatal("a malformed RowDescription was accepted")
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("the connection was broken by an unreadable description: %v", err)
	}
}

// And the distinction that makes the two above safe: a *framing* failure still
// breaks the connection, because after one the position is genuinely unknown.
func TestAFramingFailureStillBreaksTheConnection(t *testing.T) {
	// A message whose declared length runs past what the server sends.
	truncated := []byte{pgwire.BackendDataRow, 0x7f, 0xff, 0xff, 0xff, 0x01, 0x02}
	srv := &fakeServer{responses: [][]byte{truncated}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if src.Err() == nil {
		t.Fatal("a truncated message was accepted")
	}
	if conn.Err() == nil {
		t.Fatal("a framing failure left the connection usable; after one the position is unknown")
	}

	// And it was the framing that broke it, not the resync afterwards. A
	// connection marked broken skips the deferred resync, so no Sync goes out
	// — which is the observable difference between "the read broke it" and
	// "the recovery attempt did". Without this the test passes even when the
	// framing failure is allowed through, because the resync then fails too.
	if sent := srv.clientMessages(t); bytes.Contains(sent, []byte{pgwire.FrontendSync}) {
		t.Errorf("client sent %q: a Sync means the connection was still considered usable "+
			"after a framing failure, and the resync broke it instead", sent)
	}
}

// The batch contract says a Row is valid for the duration of the call that
// received it. Before the read path reused its buffers, breaking that rule met
// a freshly allocated accumulator and panicked on the index; now it would meet
// the *next* query's rows in the same storage, which under row-level security
// is another tenant's data. The violation has to stay a crash.
//
// The read below happens while the second query's rows are in the buffer —
// the one moment the stale view would return plausible values rather than
// nothing, and so the only moment worth asserting on.
func TestRowRefusesToBeReadAfterItsQuery(t *testing.T) {
	for _, read := range []struct {
		what string
		call func(Row)
	}{
		{"Value", func(r Row) { r.Value(0) }},
		{"Fields", func(r Row) { r.Fields() }},
	} {
		t.Run(read.what, func(t *testing.T) {
			srv := &fakeServer{responses: append(oneRow(), oneRow()...)}
			conn := NewConn(srv)

			var retained Row
			conn.Query(t.Context(), "SELECT id", nil, QueryConfig{}).Stream()(
				func(b sluice.Batch[Row]) bool {
					retained = b.Items[0] // the mistake this test exists to catch
					return true
				})

			var recovered any
			func() {
				defer func() { recovered = recover() }()
				conn.Query(t.Context(), "SELECT id", nil, QueryConfig{}).Stream()(
					func(sluice.Batch[Row]) bool {
						read.call(retained) // reads the second query's storage
						return true
					})
			}()

			switch r := recovered.(type) {
			case nil:
				t.Errorf("%s read a row belonging to a finished query and returned quietly", read.what)
			case error:
				if !errors.Is(r, ErrRowRetained) {
					t.Errorf("panicked with %v, want ErrRowRetained", r)
				}
			default:
				t.Errorf("panicked with %v (%T), want ErrRowRetained", r, r)
			}
		})
	}
}

// A second query started from inside a consumer's callback would rewrite the
// buffers the batch being iterated points into. The protocol never allowed it;
// what is new is that it is refused rather than left to corrupt.
func TestConnRefusesANestedQuery(t *testing.T) {
	srv := &fakeServer{responses: oneRow()}
	conn := NewConn(srv)

	var inner error
	conn.Query(t.Context(), "SELECT id", nil, QueryConfig{}).Stream()(
		func(sluice.Batch[Row]) bool {
			inner = drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{}))
			return true
		})

	if !errors.Is(inner, ErrConnBusy) {
		t.Fatalf("the nested query returned %v, want ErrConnBusy", inner)
	}
	// Refused before anything was written: a nested query that had already put
	// a Parse on the wire would leave the outer conversation unreadable.
	for _, p := range parsed(t, srv) {
		if p[1] != "SELECT id" {
			t.Errorf("the refused query still sent %q", p[1])
		}
	}
	if got := len(parsed(t, srv)); got != 1 {
		t.Errorf("%d Parse messages, want 1: the nested query reached the wire", got)
	}
	// The connection is usable afterwards: a refusal is not a breakage.
	if conn.Err() != nil {
		t.Errorf("the connection broke over a refused nested query: %v", conn.Err())
	}
}

// beNotification builds a NotificationResponse: PID, channel, payload. The
// content is irrelevant — nothing parses it — but the frame must be shaped
// like the real thing so tolerance is not tested against an empty body.
func beNotification(channel, payload string) []byte {
	body := binary.BigEndian.AppendUint32(nil, 4242)
	body = append(body, channel...)
	body = append(body, 0)
	body = append(body, payload...)
	body = append(body, 0)
	return frame(pgwire.BackendNotificationResponse, string(body))
}

// The busy guard has to cover every wire-touching entry point, not just
// Query and Pipeline: a nested Exec from inside a consumer's callback used to
// go straight to the wire, and the outer query's reader took the nested
// statement's answer for its own — swapped result streams with no error
// anywhere.
func TestWireTouchingCallsInsideAConsumerAreRefused(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	first = append(first, beDataRow(int8Val(7))...)
	first = append(first, beCommandComplete("SELECT 1")...)
	first = append(first, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{first}}
	conn := NewConn(srv)
	ctx := t.Context()

	cp, err := conn.CopyFrom(ctx, "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}

	nested := map[string]error{}
	var got []int64
	src := conn.Query(ctx, "SELECT id", nil, QueryConfig{BatchRows: 4, AllRows: true})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		nested["Exec"] = conn.Exec(ctx, "SELECT 1")
		_, nested["Begin"] = conn.Begin(ctx, TxConfig{})
		nested["PrepareStatements"] = conn.PrepareStatements(4)
		nested["WriteTuples"] = cp.WriteTuples(tuple(new(int64(1)), nil), 1)
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the outer query failed: %v", err)
	}
	for name, err := range nested {
		if !errors.Is(err, ErrConnBusy) {
			t.Errorf("nested %s returned %v, want ErrConnBusy", name, err)
		}
	}
	// The outer result is intact: nothing else spoke on the wire.
	if len(got) != 1 || got[0] != 7 {
		t.Errorf("the outer query read %v, want [7]", got)
	}
	if conn.Err() != nil {
		t.Errorf("the connection broke over refused nested calls: %v", conn.Err())
	}
	// And the refusals sent nothing: the one conversation on the wire is the
	// outer query's.
	if got := clientSQL(t, srv); len(got) != 1 || got[0] != "SELECT id" {
		t.Errorf("statements on the wire = %q, want only the outer query", got)
	}
}

// A consumer that stops early leaves the drain to read what was already on
// its way — including the error that aborted the statement after the rows the
// consumer took. Discarding it reported success for a statement the server
// rolled back, and the next statement failed 25P02 with nothing client-side
// to explain why.
func TestAnErrorMetDuringTheDrainIsSurfaced(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	first = append(first, beDataRow(int8Val(1))...)
	first = append(first, beSimple(pgwire.BackendPortalSuspended)...)
	var second []byte
	second = append(second, beErrorResponse("57014", "canceling statement due to statement timeout")...)
	second = append(second, frame(pgwire.BackendReadyForQuery, "E")...)

	srv := &fakeServer{responses: [][]byte{first, second}}
	conn := NewConn(srv)

	src := conn.Query(t.Context(), "SELECT id FROM big", nil, QueryConfig{BatchRows: 1})
	src.Stream()(func(sluice.Batch[Row]) bool { return false }) // an early stop
	err := src.Err()
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("Err = %v, want the 57014 the drain read", err)
	}
	if conn.Err() != nil {
		t.Errorf("a statement error broke the connection: %v", conn.Err())
	}
	// And the 'E' status byte was believed: the block is aborted, not gone.
	if !conn.inTx {
		t.Error("ReadyForQuery said 'E' and inTx is false — the status byte went unread")
	}
}

// A notification can land just before ReadyForQuery on any connection that
// ever ran LISTEN, which Exec makes one statement away. It used to break the
// connection in the query read loop.
func TestQueryToleratesANotification(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	first = append(first, beDataRow(int8Val(7))...)
	first = append(first, beNotification("jobs", "42")...)
	first = append(first, beCommandComplete("SELECT 1")...)
	first = append(first, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{first}}
	conn := NewConn(srv)

	var got []int64
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4, AllRows: true})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("a notification mid-result failed the query: %v", err)
	}
	if len(got) != 1 || got[0] != 7 {
		t.Errorf("read %v, want [7]", got)
	}
	if conn.Err() != nil {
		t.Errorf("a notification broke the connection: %v", conn.Err())
	}
}

// COPY ... TO STDOUT through Query has no API to land in. The statement must
// fail — not the connection, and not a silent success with the data gone.
func TestQueryCopyToStdoutFailsTheStatementNotTheConnection(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, frame(pgwire.BackendCopyOutResponse, "\x01\x00\x01\x00\x01")...)
	first = append(first, frame(pgwire.BackendCopyData, "PGCOPY")...)
	first = append(first, frame(pgwire.BackendCopyDone, "")...)
	first = append(first, beCommandComplete("COPY 1")...)

	var next []byte
	next = append(next, beSimple(pgwire.BackendParseComplete)...)
	next = append(next, beSimple(pgwire.BackendBindComplete)...)
	next = append(next, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	next = append(next, beDataRow(int8Val(42))...)
	next = append(next, beCommandComplete("SELECT 1")...)

	srv := &fakeServer{responses: [][]byte{first, beReadyForQuery(), next, beReadyForQuery()}}
	conn := NewConn(srv)

	err := drain(t, conn.Query(t.Context(), "COPY t TO STDOUT", nil, QueryConfig{BatchRows: 4}))
	if err == nil {
		t.Fatal("COPY TO STDOUT through Query reported success with its data discarded")
	}
	if !strings.Contains(err.Error(), "TO STDOUT") {
		t.Errorf("error = %v, want it to name the copy-out statement", err)
	}
	if conn.Err() != nil {
		t.Fatalf("copy-out broke the connection: %v", conn.Err())
	}
	// The copy-out stream was drained and the next query reads its own rows.
	var got []int64
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 4})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the query after a refused copy-out failed: %v", err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("read %v after the copy-out, want [42]", got)
	}
}

// COPY ... FROM STDIN through Query must answer the server's CopyInResponse
// with CopyFail before resynchronising: the backend ignores Sync while it
// waits for CopyData, so a drain that skips the message hangs the connection
// until its deadline.
func TestQueryCopyFromStdinFailsCleanlyWithCopyFail(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, frame(pgwire.BackendCopyInResponse, "\x01\x00\x01\x00\x01")...)

	srv := &fakeServer{responses: [][]byte{
		first,
		beErrorResponse("57014", "COPY from stdin failed"), // answers the CopyFail
		beReadyForQuery(), // answers the Sync
	}}
	conn := NewConn(srv)

	err := drain(t, conn.Query(t.Context(), "COPY t FROM STDIN", nil, QueryConfig{BatchRows: 4}))
	if err == nil || !strings.Contains(err.Error(), "CopyFrom") {
		t.Fatalf("error = %v, want a refusal naming Conn.CopyFrom", err)
	}
	if conn.Err() != nil {
		t.Fatalf("the refusal broke the connection: %v", conn.Err())
	}
	// CopyFail actually left: it is what unblocks a real backend.
	types := srv.clientMessages(t)
	if !bytes.ContainsRune(types, rune(pgwire.FrontendCopyFail)) {
		t.Errorf("client messages %q carry no CopyFail — a real backend would wait for CopyData forever", types)
	}
}

// A NUL in statement text would end the C string the protocol frames it as,
// and the server would read the rest as the message's other fields. Every
// entry point that sends caller SQL refuses it before a byte is written, and
// the connection stays usable.
func TestSQLWithNULIsRefusedBeforeSending(t *testing.T) {
	const bad = "SELECT 1\x00; DROP TABLE t"
	srv := &fakeServer{responses: [][]byte{cmdCycle("BEGIN")}}
	conn := NewConn(srv)
	isNUL := func(what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "NUL") {
			t.Errorf("%s: error = %v, want the NUL refused", what, err)
		}
	}

	src := conn.Query(t.Context(), bad, nil, QueryConfig{})
	for range src.Stream() {
	}
	isNUL("Query", src.Err())
	isNUL("Exec", conn.Exec(t.Context(), bad))
	_, err := conn.CopyFrom(t.Context(), bad, CopyConfig{RowsPerTx: 1})
	isNUL("CopyFrom", err)
	if srv.sent.Len() != 0 {
		t.Fatalf("%d bytes reached the wire for refused statements", srv.sent.Len())
	}

	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatalf("the connection was not left usable: %v", err)
	}
	before := srv.sent.Len()
	isNUL("Tx.Exec", tx.Exec(t.Context(), bad))
	isNUL("Pipeline", tx.Pipeline(t.Context(), []Part{{SQL: bad}}, QueryConfig{}, func(int, sluice.Batch[Row]) bool { return true }))
	if srv.sent.Len() != before {
		t.Errorf("%d bytes reached the wire inside the transaction", srv.sent.Len()-before)
	}
	if conn.Err() != nil {
		t.Errorf("connection broken by a refused statement: %v", conn.Err())
	}
}
