package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

func beCopyInResponse() []byte {
	// format byte (1 = binary), column count, then a format per column.
	body := []byte{1, 0, 2, 0, 1, 0, 1}
	return frame(pgwire.BackendCopyInResponse, string(body))
}

// txCycle scripts one transaction's worth of server responses: BEGIN, the COPY
// entering copy mode, the CopyDone acknowledgement, and COMMIT.
func txCycle() [][]byte {
	return [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		beCopyInResponse(),
		append(beCommandComplete("COPY 0"), beReadyForQuery()...),
		append(beCommandComplete("COMMIT"), beReadyForQuery()...),
	}
}

// tuple encodes one two-column row: an int8 and a text, either possibly NULL.
func tuple(id *int64, name *string) []byte {
	b := AppendTupleHeader(nil, 2)
	if id == nil {
		b = AppendFieldNull(b)
	} else {
		b = AppendField(b, AppendInt8(nil, *id))
	}
	if name == nil {
		b = AppendFieldNull(b)
	} else {
		b = AppendField(b, []byte(*name))
	}
	return b
}

// copyPayload extracts the concatenated CopyData bodies the client sent, which
// is the byte stream the server would reassemble.
func copyPayload(t *testing.T, srv *fakeServer) []byte {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var payload []byte
	for {
		m, err := rd.Next()
		if err != nil {
			return payload
		}
		if m.Type == pgwire.FrontendCopyData {
			payload = append(payload, m.Body...)
		}
	}
}

// A copy from end to end: the signature, the tuples, the trailer, and the
// transaction around them.
func TestCopyFrom(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)

	cp, err := conn.CopyFrom(t.Context(), "COPY orders (id, name) FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatalf("CopyFrom returned %v", err)
	}

	var buf []byte
	buf = append(buf, tuple(new(int64(1)), new("first"))...)
	buf = append(buf, tuple(new(int64(2)), nil)...)
	if err := cp.WriteTuples(buf, 2); err != nil {
		t.Fatalf("WriteTuples returned %v", err)
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	if cp.Rows() != 2 {
		t.Errorf("Rows = %d, want 2", cp.Rows())
	}

	payload := copyPayload(t, srv)
	if !bytes.HasPrefix(payload, copySignature) {
		t.Fatalf("payload does not start with the COPY signature: % x", payload[:min(11, len(payload))])
	}
	// signature(11) + flags(4) + header extension(4)
	body := payload[19:]
	want := append(tuple(new(int64(1)), new("first")), tuple(new(int64(2)), nil)...)
	want = binary.BigEndian.AppendUint16(want, ^uint16(0)) // the trailer
	if !bytes.Equal(body, want) {
		t.Errorf("tuples on the wire:\n got % x\nwant % x", body, want)
	}

	// The conversation must be BEGIN, COPY, data, CopyDone, COMMIT.
	sent := srv.clientMessages(t)
	wantTypes := []byte{
		pgwire.FrontendQuery,    // BEGIN
		pgwire.FrontendQuery,    // COPY ... FROM STDIN
		pgwire.FrontendCopyData, // the tuples
		pgwire.FrontendCopyDone,
		pgwire.FrontendQuery, // COMMIT
	}
	if !bytes.Equal(sent, wantTypes) {
		t.Errorf("client sent %q, want %q", sent, wantTypes)
	}
}

// The transaction granularity is the point of the type: reaching RowsPerTx
// closes a transaction and the next row opens another, which means a second
// COPY statement — a copy cannot be committed part-way.
func TestCopyCutsIntoTransactions(t *testing.T) {
	var script [][]byte
	script = append(script, txCycle()...)
	script = append(script, txCycle()...)
	srv := &fakeServer{responses: script}
	conn := NewConn(srv)

	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if err := cp.WriteTuples(tuple(new(int64(i)), nil), 1); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}

	sent := srv.clientMessages(t)
	// Two cycles: BEGIN, COPY, COMMIT each — six simple queries in all.
	if got := bytes.Count(sent, []byte{pgwire.FrontendQuery}); got != 6 {
		t.Errorf("client sent %d simple queries, want 6 — two BEGIN/COPY/COMMIT cycles", got)
	}
	if got := bytes.Count(sent, []byte{pgwire.FrontendCopyDone}); got != 2 {
		t.Errorf("client sent %d CopyDone, want 2", got)
	}
	if cp.Rows() != 4 {
		t.Errorf("Rows = %d, want 4", cp.Rows())
	}
}

// RowsPerTx is a floor: a batch is never split, so one WriteTuples carrying
// more rows than the bound lands in a single transaction, committed at the
// end of that call. The doc on CopyConfig.RowsPerTx promises exactly this.
func TestCopyRowsPerTxIsAFloorNotACap(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 2})
	if err != nil {
		t.Fatal(err)
	}
	var buf []byte
	for i := range 5 {
		buf = append(buf, tuple(new(int64(i)), nil)...)
	}
	if err := cp.WriteTuples(buf, 5); err != nil {
		t.Fatal(err)
	}
	sent := srv.clientMessages(t)
	if got := bytes.Count(sent, []byte{pgwire.FrontendCopyDone}); got != 1 {
		t.Errorf("client sent %d CopyDone, want 1: the five-row batch is one transaction", got)
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close after the committing batch returned %v", err)
	}
}

// A transaction size nobody chose is refused rather than defaulted: the whole
// reason the parameter exists is that the obvious default is a defect at
// scale.
func TestCopyRequiresRowsPerTx(t *testing.T) {
	conn := NewConn(&fakeServer{})
	for _, n := range []int{0, -1} {
		if _, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN", CopyConfig{RowsPerTx: n}); err == nil {
			t.Errorf("RowsPerTx %d was accepted", n)
		}
	}
}

// Rows accumulate into one CopyData until MaxMessage, because message
// boundaries need not fall on row boundaries — that is what lets a batch leave
// as one message rather than one per row.
func TestCopyBatchesRowsIntoMessages(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1000, MaxMessage: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		if err := cp.WriteTuples(tuple(new(int64(i)), nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}

	sent := srv.clientMessages(t)
	if got := bytes.Count(sent, []byte{pgwire.FrontendCopyData}); got != 1 {
		t.Errorf("50 rows left in %d CopyData messages, want 1", got)
	}
}

// A server refusing the COPY — a missing table is the usual reason — is a
// statement failure, not a broken connection: the error carries its SQLSTATE
// and the connection survives.
func TestCopyServerRefuses(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		append(beErrorResponse("42P01", `relation "nope" does not exist`), beReadyForQuery()...),
		append(beCommandComplete("ROLLBACK"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY nope FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}

	err = cp.WriteTuples(tuple(new(int64(1)), nil), 1)
	if !errors.Is(err, ErrCopyRefused) {
		t.Fatalf("WriteTuples returned %v, want ErrCopyRefused", err)
	}
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
		t.Errorf("the SQLSTATE did not survive: %v", err)
	}
	if conn.Err() != nil {
		t.Errorf("a refused COPY broke the connection: %v", conn.Err())
	}
	// A copier that failed stays failed rather than half-working.
	if err := cp.WriteTuples(tuple(new(int64(2)), nil), 1); !errors.Is(err, ErrCopyRefused) {
		t.Errorf("writing after a failure returned %v", err)
	}
}

// Abort tells the server why, then rolls back — so the reason appears in the
// server's log rather than only in the client's.
func TestCopyAbort(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		beCopyInResponse(),
		append(beErrorResponse("57014", "COPY from stdin failed"), beReadyForQuery()...),
		append(beCommandComplete("ROLLBACK"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}
	if err := cp.Abort("the pipeline stopped\x00 at row 7"); err != nil {
		t.Fatalf("Abort returned %v", err)
	}

	sent := srv.clientMessages(t)
	if !bytes.Contains(sent, []byte{pgwire.FrontendCopyFail}) {
		t.Errorf("client sent %q, with no CopyFail — the server would never learn why", sent)
	}
	// The reason must actually travel, not just the message type.
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	for {
		m, err := rd.Next()
		if err != nil {
			t.Fatal("no CopyFail body was found")
		}
		if m.Type == pgwire.FrontendCopyFail {
			// A NUL in the reason would end the C string there and drop
			// the rest from the server's log; it arrives spelled out.
			if want := "the pipeline stopped\\x00 at row 7\x00"; string(m.Body) != want {
				t.Errorf("CopyFail carries %q, want %q", m.Body, want)
			}
			return
		}
	}
}

// Close with nothing written must not open a transaction: a copier that was
// never used leaves no trace on the server.
func TestCopyCloseWithoutRows(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	if err := cp.Close(); err != nil {
		t.Errorf("second Close returned %v — Close is idempotent", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("an unused copier wrote %d bytes", srv.sent.Len())
	}
}

// The encoders produce what the format specifies, stated byte for byte rather
// than checked against themselves.
func TestGoldenVectorCopyTuple(t *testing.T) {
	got := tuple(new(int64(1)), new("ab"))
	want := []byte{
		0, 2, // two fields
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, // int8 1
		0, 0, 0, 2, 'a', 'b', // text "ab"
	}
	if !bytes.Equal(got, want) {
		t.Errorf("tuple = % x, want % x", got, want)
	}

	null := AppendFieldNull(nil)
	if want := []byte{0xFF, 0xFF, 0xFF, 0xFF}; !bytes.Equal(null, want) {
		t.Errorf("NULL field = % x, want % x", null, want)
	}
}

// The signature is what makes the server reject a stream mangled by a
// line-ending conversion, so its bytes are not negotiable.
func TestGoldenVectorCopySignature(t *testing.T) {
	want := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n', 0}
	if !bytes.Equal(copySignature, want) {
		t.Errorf("signature = % x, want % x", copySignature, want)
	}
}

// A server that hangs up in the middle of a COPY — a restart, a failover, an
// idle-transaction timeout on the other side.
//
// The distinction that matters is between a stream that ended cleanly and one
// that stopped mid-message: the first is a server that finished, the second is
// a load whose outcome nobody knows. Collapsing them would let a bulk load
// that wrote half its rows report the same thing as one that wrote all of
// them.
func TestCopyServerHangsUpMidLoad(t *testing.T) {
	t.Run("before the copy starts", func(t *testing.T) {
		// BEGIN is answered and then nothing: the server never says whether it
		// entered copy mode.
		//
		// The failure surfaces at the first WriteTuples rather than at
		// CopyFrom, because CopyFrom is deliberately lazy — it validates the
		// configuration and touches the connection only when there are rows
		// to send. A copier built and never written to costs no transaction,
		// which is the right shape for a caller that discovers it has nothing
		// to load.
		srv := &fakeServer{responses: [][]byte{
			append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		}}
		conn := NewConn(srv)
		cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
		if err != nil {
			t.Fatalf("CopyFrom = %v, want it to defer to the first write", err)
		}
		err = cp.WriteTuples(tuple(new(int64(1)), nil), 1)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("WriteTuples = %v, want io.ErrUnexpectedEOF", err)
		}
		if !errors.Is(err, ErrConnBroken) {
			t.Errorf("error = %v, want the connection reported as unusable", err)
		}
	})

	t.Run("while the rows are being committed", func(t *testing.T) {
		srv := &fakeServer{responses: [][]byte{
			append(beCommandComplete("BEGIN"), beReadyForQuery()...),
			beCopyInResponse(),
			// The client sends CopyDone and waits for ReadyForQuery. Nothing
			// comes.
		}}
		conn := NewConn(srv)
		cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
			t.Fatal(err)
		}
		if err := cp.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Close = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

// A COPY carrying an array column, with NULLs both around the array and
// inside it. Those are two different absences and the format says each one
// differently: a NULL *field* is a length of -1 where the array would be, a
// NULL *element* is a length of -1 inside it.
func TestCopyWithNullBearingArray(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)

	cp, err := conn.CopyFrom(t.Context(), "COPY t (id, tags) FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}

	tags := []Null[int64]{Some(int64(10)), None[int64](), Some(int64(30))}

	var buf []byte
	// Row one: an array with a NULL inside it, written straight into the tuple.
	buf = AppendTupleHeader(buf, 2)
	buf = AppendField(buf, AppendInt8(nil, 1))
	buf, at := BeginField(buf)
	buf = AppendNullableInt8Array(buf, tags)
	buf = EndField(buf, at)
	// Row two: the whole array column is NULL.
	buf = AppendTupleHeader(buf, 2)
	buf = AppendField(buf, AppendInt8(nil, 2))
	buf = AppendFieldNull(buf)

	if err := cp.WriteTuples(buf, 2); err != nil {
		t.Fatalf("WriteTuples returned %v", err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}

	payload := copyPayload(t, srv)
	body := payload[19:] // signature(11) + flags(4) + header extension(4)

	// Row one, field two must be the array, byte for byte, with its length in
	// front of it. A wrong back-patch here would corrupt every field after.
	wantArray := AppendNullableInt8Array(nil, tags)
	wantRow1 := AppendTupleHeader(nil, 2)
	wantRow1 = AppendField(wantRow1, AppendInt8(nil, 1))
	wantRow1 = AppendField(wantRow1, wantArray)
	if !bytes.HasPrefix(body, wantRow1) {
		t.Fatalf("row one on the wire:\n got % x\nwant % x", body[:min(len(wantRow1), len(body))], wantRow1)
	}

	// Row two's array field must be a NULL field, not an empty array — the
	// two are different values and the difference is four bytes.
	rest := body[len(wantRow1):]
	wantRow2 := AppendTupleHeader(nil, 2)
	wantRow2 = AppendField(wantRow2, AppendInt8(nil, 2))
	wantRow2 = AppendFieldNull(wantRow2)
	if !bytes.HasPrefix(rest, wantRow2) {
		t.Fatalf("row two on the wire:\n got % x\nwant % x", rest[:min(len(wantRow2), len(rest))], wantRow2)
	}
}

// BeginField/EndField must produce exactly what AppendField produces, or the
// same value reaches the server two ways and only one was tested against it.
func TestBeginFieldMatchesAppendField(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {1}, {1, 2, 3, 4, 5}} {
		direct := AppendField(nil, payload)

		inPlace, at := BeginField(nil)
		inPlace = append(inPlace, payload...)
		inPlace = EndField(inPlace, at)

		if !bytes.Equal(direct, inPlace) {
			t.Errorf("for % x:\n AppendField % x\n BeginField  % x", payload, direct, inPlace)
		}
	}
}

// The offset has to survive the slice growing underneath it, which is the one
// way an in-place length patch goes wrong.
func TestBeginFieldSurvivesReallocation(t *testing.T) {
	buf := make([]byte, 0, 8) // deliberately too small for what follows
	buf = append(buf, 0xff)   // something before the field, so at is not zero

	buf, at := BeginField(buf)
	big := make([]byte, 1000)
	for i := range big {
		big[i] = byte(i)
	}
	buf = append(buf, big...)
	buf = EndField(buf, at)

	if buf[0] != 0xff {
		t.Error("the bytes before the field were disturbed")
	}
	if got := binary.BigEndian.Uint32(buf[1:5]); got != 1000 {
		t.Errorf("the patched length is %d, want 1000", got)
	}
	if !bytes.Equal(buf[5:], big) {
		t.Error("the payload was disturbed")
	}
}

// A batch large enough to send on its own skips the copier's buffer, and a
// small one still accumulates. Both must put exactly the same bytes on the
// wire — the fast path is an optimisation, not a second format.
func TestCopyDirectAndBufferedPathsAgree(t *testing.T) {
	rows := func() ([]byte, int) {
		var buf []byte
		for i := range 200 {
			buf = append(buf, tuple(new(int64(i)), new("label"))...)
		}
		return buf, 200
	}

	// One call whose payload exceeds MaxMessage: the direct path.
	direct := &fakeServer{responses: txCycle()}
	dconn := NewConn(direct)
	dcp, err := dconn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1000, MaxMessage: 64})
	if err != nil {
		t.Fatal(err)
	}
	all, n := rows()
	if err := dcp.WriteTuples(all, n); err != nil {
		t.Fatal(err)
	}
	if err := dcp.Close(); err != nil {
		t.Fatal(err)
	}

	// The same rows one at a time under a bound they never reach: the
	// buffered path.
	buffered := &fakeServer{responses: txCycle()}
	bconn := NewConn(buffered)
	bcp, err := bconn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1000, MaxMessage: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		if err := bcp.WriteTuples(tuple(new(int64(i)), new("label")), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := bcp.Close(); err != nil {
		t.Fatal(err)
	}

	if d, b := copyPayload(t, direct), copyPayload(t, buffered); !bytes.Equal(d, b) {
		t.Errorf("the two paths put different bytes on the wire:\n direct   %d bytes\n buffered %d bytes", len(d), len(b))
	}
	if dcp.Rows() != bcp.Rows() {
		t.Errorf("row counts differ: %d and %d", dcp.Rows(), bcp.Rows())
	}
}

// The direct path hands the caller's slice straight to the writer. The caller
// is promised it may reuse that slice as soon as WriteTuples returns, so the
// bytes must already be gone — not retained for a later flush.
func TestCopyDirectPathDoesNotRetainTheCallersBuffer(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1000, MaxMessage: 16})
	if err != nil {
		t.Fatal(err)
	}

	// One write first, to flush the COPY signature beginTx leaves in the
	// buffer. Without it cp.buf is never empty on the first call and the
	// direct path is not taken at all — which is how the first version of this
	// test passed against a mutation that broke the path entirely.
	if err := cp.WriteTuples(tuple(new(int64(1)), new("warm")), 1); err != nil {
		t.Fatal(err)
	}

	caller := append([]byte(nil), tuple(new(int64(7)), new("original"))...)
	want := append([]byte(nil), caller...)
	if err := cp.WriteTuples(caller, 1); err != nil {
		t.Fatal(err)
	}

	// The caller reuses its buffer immediately, as the contract allows.
	for i := range caller {
		caller[i] = 0xff
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}

	payload := copyPayload(t, srv)
	if !bytes.Contains(payload, want) {
		t.Error("the tuple on the wire was overwritten by the caller reusing its buffer; " +
			"the direct path retained it instead of writing it")
	}
	if bytes.Contains(payload, bytes.Repeat([]byte{0xff}, len(want))) {
		t.Error("the caller's overwritten bytes reached the server")
	}
}

// A large batch arriving when something is already buffered must not jump the
// queue: the rows would reach the server out of order.
func TestCopyDirectPathDoesNotOvertakeBufferedRows(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1000, MaxMessage: 128})
	if err != nil {
		t.Fatal(err)
	}

	first := tuple(new(int64(1)), new("first"))
	var second []byte
	for i := range 40 {
		second = append(second, tuple(new(int64(100+i)), new("second"))...)
	}

	if err := cp.WriteTuples(first, 1); err != nil { // small: buffered
		t.Fatal(err)
	}
	if err := cp.WriteTuples(second, 40); err != nil { // large: would go direct
		t.Fatal(err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}

	payload := copyPayload(t, srv)
	firstAt := bytes.Index(payload, first)
	secondAt := bytes.Index(payload, second[:len(first)])
	if firstAt < 0 || secondAt < 0 {
		t.Fatalf("rows missing from the stream: first at %d, second at %d", firstAt, secondAt)
	}
	if firstAt > secondAt {
		t.Error("the large batch overtook the buffered row; COPY rows reached the server out of order")
	}
}

// The same when the refusal's own payload cannot be read. An ErrorResponse is
// framed like any other message, so a body this package cannot parse says
// nothing about where the conversation is — the copy is refused, the reason is
// unreportable, and the connection is recovered rather than discarded.
//
// The mistake this guards against is treating "I could not read this" as "I no
// longer know where I am". They look alike at the call site and they cost
// differently: one fails a statement, the other costs a reconnection and,
// behind a gateway, every caller waiting on it.
func TestCopyRefusedWithAnUnreadableReasonKeepsTheConnection(t *testing.T) {
	// An ErrorResponse whose field list is not terminated.
	malformed := frame(pgwire.BackendErrorResponse, "Cthis field never ends")

	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		append(malformed, beReadyForQuery()...),
		append(beCommandComplete("ROLLBACK"), beReadyForQuery()...),
		append(beCommandComplete("SELECT 1"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}

	err = cp.WriteTuples(tuple(new(int64(1)), nil), 1)
	if !errors.Is(err, ErrCopyRefused) {
		t.Fatalf("WriteTuples returned %v, want ErrCopyRefused", err)
	}
	if conn.Err() != nil {
		t.Fatalf("an unreadable refusal broke the connection: %v", conn.Err())
	}
	// Recovered to a known position: the connection takes another statement.
	if err := conn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Errorf("the connection did not survive: %v", err)
	}
}

// A COPY the server rejects at CopyDone — a constraint, a trigger, its last
// look at the data — must leave the connection usable.
//
// The failure arrives after the transaction is open, on a socket whose
// position is perfectly known. Returning without rolling back left Conn.inTx
// true with nothing to clear it: Abort saw the copier's own flag already down
// and did nothing, and every later Begin or CopyFrom on that healthy
// connection refused with ErrTxOpen for the life of the process.
func TestCopyFailedCommitRollsBackAndLeavesTheConnectionUsable(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		beCopyInResponse(),
		append(beErrorResponse("23505", `duplicate key value violates unique constraint "orders_pkey"`), beReadyForQuery()...),
		append(beCommandComplete("ROLLBACK"), beReadyForQuery()...),
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)
	cp, err := conn.CopyFrom(t.Context(), "COPY orders FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), new("a")), 1); err != nil {
		t.Fatal(err)
	}

	err = cp.Close()
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("Close returned %v, want the server's 23505", err)
	}
	if conn.Err() != nil {
		t.Fatalf("a rejected COPY broke the connection: %v", conn.Err())
	}

	// The transaction the copy opened must be gone from the connection's own
	// view, or nothing can ever open another one.
	if _, err := conn.Begin(t.Context(), TxConfig{}); err != nil {
		t.Fatalf("Begin after a failed COPY commit returned %v, want a usable connection", err)
	}

	// The rollback has to reach the server too: a client-side flag cleared
	// over a transaction the backend still holds is the same wedge one layer
	// down, and it pins the vacuum horizon while it lasts.
	if !strings.Contains(srv.sent.String(), "ROLLBACK") {
		t.Error("no ROLLBACK was sent; the server's transaction was left open")
	}
}

// A SIGHUP config reload or a notification landing between the COPY statement
// and its CopyInResponse used to break the connection — killing a running
// bulk load over messages the protocol allows at any time.
func TestCopyToleratesAsyncMessagesBeforeCopyInResponse(t *testing.T) {
	var entering []byte
	entering = append(entering, beParameterStatus("application_name", "reloaded")...)
	entering = append(entering, beNotification("jobs", "42")...)
	entering = append(entering, beCopyInResponse()...)

	srv := &fakeServer{responses: [][]byte{
		append(beCommandComplete("BEGIN"), beReadyForQuery()...),
		entering,
		append(beCommandComplete("COPY 1"), beReadyForQuery()...),
		append(beCommandComplete("COMMIT"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)

	cp, err := conn.CopyFrom(t.Context(), "COPY t (id, name) FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatalf("WriteTuples failed over async messages before CopyInResponse: %v", err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection broke: %v", conn.Err())
	}
	// The ParameterStatus was recorded, not merely skipped: this is where a
	// SET's announcement lands when it races a COPY.
	if got := conn.Parameter("application_name"); got != "reloaded" {
		t.Errorf("Parameter(application_name) = %q, want %q", got, "reloaded")
	}
}

// Exec("COPY ... FROM STDIN") used to deadlock the connection: drainToReady
// skipped the CopyInResponse and the backend sat waiting for CopyData,
// ignoring the Sync, until the socket deadline. The statement must fail and
// the connection must come back.
func TestExecRefusesCopyFromStdinAndKeepsTheConnection(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		beCopyInResponse(), // answers the Query
		append(beErrorResponse("57014", "COPY from stdin failed"), beReadyForQuery()...), // answers the CopyFail
		append(beCommandComplete("SELECT 1"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)

	err := conn.Exec(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)")
	if err == nil || !strings.Contains(err.Error(), "CopyFrom") {
		t.Fatalf("error = %v, want a refusal naming Conn.CopyFrom", err)
	}
	if conn.Err() != nil {
		t.Fatalf("the refusal broke the connection: %v", conn.Err())
	}
	// CopyFail left the client: it is the one message that ends copy-in mode,
	// and without it a real backend waits forever.
	types := srv.clientMessages(t)
	if !bytes.ContainsRune(types, rune(pgwire.FrontendCopyFail)) {
		t.Fatalf("client messages %q carry no CopyFail", types)
	}
	// And the connection answers the next statement.
	if err := conn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Errorf("the statement after a refused COPY returned %v", err)
	}
}

// Exec("COPY ... TO STDOUT") used to discard the entire output and report
// success. The data is still discarded — there is no API to put it in — but
// the statement fails and says so.
func TestExecFailsCopyToStdoutInsteadOfDiscardingIt(t *testing.T) {
	var first []byte
	first = append(first, frame(pgwire.BackendCopyOutResponse, "\x01\x00\x01\x00\x01")...)
	first = append(first, frame(pgwire.BackendCopyData, "PGCOPY")...)
	first = append(first, frame(pgwire.BackendCopyDone, "")...)
	first = append(first, beCommandComplete("COPY 1")...)
	first = append(first, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{
		first,
		append(beCommandComplete("SELECT 1"), beReadyForQuery()...),
	}}
	conn := NewConn(srv)

	err := conn.Exec(t.Context(), "COPY t TO STDOUT")
	if err == nil {
		t.Fatal("COPY TO STDOUT through Exec reported success with its data thrown away")
	}
	if !strings.Contains(err.Error(), "TO STDOUT") {
		t.Errorf("error = %v, want it to name the copy-out statement", err)
	}
	if conn.Err() != nil {
		t.Fatalf("copy-out broke the connection: %v", conn.Err())
	}
	if err := conn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Errorf("the statement after a refused copy-out returned %v", err)
	}
}
