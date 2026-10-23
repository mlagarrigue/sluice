package postgres

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// updateReturningBatched is an UPDATE ... RETURNING of three rows read in
// batches of two: the portal suspends after two, the two Executes asked
// ahead bring the third with the statement's tag and then the empty end the
// resync discards, and the Sync brings ReadyForQuery. The tag counts the one
// row of its own Execute, as a real server's does.
func updateReturningBatched() [][]byte {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	first = append(first, beDataRow(int8Val(1))...)
	first = append(first, beDataRow(int8Val(2))...)
	first = append(first, beSimple(pgwire.BackendPortalSuspended)...)
	var rest []byte
	rest = append(rest, beDataRow(int8Val(3))...)
	rest = append(rest, beCommandComplete("UPDATE 1")...)
	rest = append(rest, beCommandComplete("UPDATE 0")...)
	return [][]byte{first, rest, beReadyForQuery()}
}

// A streamed UPDATE ... RETURNING reports its tag once consumed, and the
// count it carries is the rows delivered — not the last batch's count the
// server wrote, nor the empty completion of the Execute asked ahead, which
// the resync reads after it.
func TestQueryTagOfAStreamedUpdate(t *testing.T) {
	conn := NewConn(&fakeServer{responses: updateReturningBatched()})
	tag := CommandTag("stale")
	src := conn.Query(t.Context(), "UPDATE t SET v = v + 1 RETURNING id", nil,
		QueryConfig{BatchRows: 2, Tag: &tag})

	var rows int64
	for b := range src.Stream() {
		rows += int64(len(b.Items))
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if tag != "UPDATE 3" {
		t.Errorf("tag = %q, want UPDATE 3", tag)
	}
	if n, ok := tag.RowsAffected(); n != rows || !ok {
		t.Errorf("RowsAffected() = %d, %v; want the %d rows delivered", n, ok, rows)
	}
}

// The rows of the suspended Executes join the count, whatever the tag's
// shape; a result read in one Execute, or a tag without a count, is the
// server's as it is.
func TestResultTag(t *testing.T) {
	for _, tc := range []struct {
		body      string
		suspended int
		want      CommandTag
	}{
		{"UPDATE 1", 4, "UPDATE 5"},
		{"UPDATE 1", 0, "UPDATE 1"},
		{"INSERT 0 2", 6, "INSERT 0 8"},
		{"SELECT 0", 1024, "SELECT 1024"},
		{"MERGE 3", 2, "MERGE 5"},
		{"CREATE TABLE", 3, "CREATE TABLE"},
		{"UPDATE x", 3, "UPDATE x"},
	} {
		if got := resultTag([]byte(tc.body), tc.suspended); got != tc.want {
			t.Errorf("resultTag(%q, %d) = %q, want %q", tc.body, tc.suspended, got, tc.want)
		}
	}
}

// A consumer that stops before the last batch never sees the result end:
// the tag the drain reads is not reported, and a tag left by an earlier run
// is cleared.
func TestQueryTagEmptyAfterAnEarlyStop(t *testing.T) {
	conn := NewConn(&fakeServer{responses: updateReturningBatched()})
	tag := CommandTag("stale")
	src := conn.Query(t.Context(), "UPDATE t SET v = v + 1 RETURNING id", nil,
		QueryConfig{BatchRows: 2, Tag: &tag})

	src.Stream()(func(sluice.Batch[Row]) bool { return false })
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if tag != "" {
		t.Errorf("tag = %q after an early stop, want empty", tag)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection was broken: %v", conn.Err())
	}
}

// A failed query reports no tag, even one the server sent before failing:
// a deferred constraint checked at the implicit commit fails after the
// statement's CommandComplete, and its writes are rolled back.
func TestQueryTagEmptyAfterAnError(t *testing.T) {
	var chunk []byte
	chunk = append(chunk, beSimple(pgwire.BackendParseComplete)...)
	chunk = append(chunk, beSimple(pgwire.BackendBindComplete)...)
	chunk = append(chunk, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
	chunk = append(chunk, beDataRow(int8Val(1))...)
	chunk = append(chunk, beCommandComplete("UPDATE 1")...)
	chunk = append(chunk, beErrorResponse("23503", "deferred foreign key")...)
	chunk = append(chunk, beReadyForQuery()...)
	conn := NewConn(&fakeServer{responses: [][]byte{chunk}})
	tag := CommandTag("stale")

	err := drain(t, conn.Query(t.Context(), "UPDATE t SET v = v + 1 RETURNING id", nil,
		QueryConfig{Fetch: FetchAll, Tag: &tag}))
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("err = %v, want the server's 23503", err)
	}
	if tag != "" {
		t.Errorf("tag = %q next to an error, want empty", tag)
	}
}

// A transaction's query consumed after the commit fails with ErrTxDone, and
// reports no tag either.
func TestQueryTagEmptyAfterTheTransactionEnded(t *testing.T) {
	conn := NewConn(&fakeServer{responses: [][]byte{cmdCycle("BEGIN"), cmdCycle("COMMIT")}})
	tx, err := conn.Begin(t.Context(), TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	tag := CommandTag("stale")
	src := tx.Query(t.Context(), "UPDATE t SET v = 1 RETURNING id", nil, QueryConfig{Tag: &tag})
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, src); !errors.Is(err, ErrTxDone) {
		t.Fatalf("err = %v, want ErrTxDone", err)
	}
	if tag != "" {
		t.Errorf("tag = %q after the transaction ended, want empty", tag)
	}

	tag = "stale"
	if err := drain(t, tx.Query(t.Context(), "UPDATE t SET v = 1 RETURNING id", nil, QueryConfig{Tag: &tag})); !errors.Is(err, ErrTxDone) {
		t.Fatalf("err = %v, want ErrTxDone", err)
	}
	if tag != "" {
		t.Errorf("tag = %q from a query built after the end, want empty", tag)
	}
}

// The tag is opt-in: asking for it costs exactly the one copy of its text,
// and a query that does not ask pays nothing — the difference between the
// two is the whole price.
func TestQueryTagCostsNothingUnasked(t *testing.T) {
	whole := func() []byte {
		var chunk []byte
		chunk = append(chunk, beSimple(pgwire.BackendParseComplete)...)
		chunk = append(chunk, beSimple(pgwire.BackendBindComplete)...)
		chunk = append(chunk, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
		chunk = append(chunk, beDataRow(int8Val(1))...)
		chunk = append(chunk, beCommandComplete("UPDATE 1")...)
		return append(chunk, beReadyForQuery()...)
	}()
	var tag CommandTag
	allocs := func(cfg QueryConfig) float64 {
		return testing.AllocsPerRun(50, func() {
			conn := NewConn(&fakeServer{responses: [][]byte{whole}})
			src := conn.Query(t.Context(), "UPDATE t SET v = 1 RETURNING id", nil, cfg)
			src.Stream()(func(sluice.Batch[Row]) bool { return true })
			if err := src.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
	unasked := allocs(QueryConfig{Fetch: FetchAll})
	asked := allocs(QueryConfig{Fetch: FetchAll, Tag: &tag})
	if tag != "UPDATE 1" {
		t.Fatalf("tag = %q, want UPDATE 1", tag)
	}
	if asked-unasked != 1 {
		t.Errorf("allocations: %v without the tag, %v with it; want exactly one more, the tag's copy", unasked, asked)
	}
}
