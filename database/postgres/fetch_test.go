package postgres

import (
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// oneRowWhole is the server's answer to a statement it already holds, asked
// for whole: everything, ReadyForQuery included, in the one chunk the Sync
// draws.
func oneRowWhole() [][]byte {
	var chunk []byte
	chunk = append(chunk, beSimple(pgwire.BackendBindComplete)...)
	chunk = append(chunk, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	chunk = append(chunk, beDataRow(int8Val(1))...)
	chunk = append(chunk, beCommandComplete("SELECT 1")...)
	chunk = append(chunk, beReadyForQuery()...)
	return [][]byte{chunk}
}

// oneRowCached is the batched answer to a statement the server already holds:
// no ParseComplete, the rows on the Flush, ReadyForQuery on the resync's Sync.
func oneRowCached() [][]byte {
	var chunk []byte
	chunk = append(chunk, beSimple(pgwire.BackendBindComplete)...)
	chunk = append(chunk, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	chunk = append(chunk, beDataRow(int8Val(1))...)
	chunk = append(chunk, beCommandComplete("SELECT 1")...)
	return [][]byte{chunk, beReadyForQuery()}
}

var (
	batchedRead = []byte{pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendFlush, pgwire.FrontendSync}
	wholeRead   = []byte{pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute, pgwire.FrontendSync}
)

// sentSince returns the message types the client wrote after the first n.
func sentSince(t *testing.T, srv *fakeServer, n int) []byte {
	t.Helper()
	return srv.clientMessages(t)[n:]
}

// A point read costs one round trip from its second execution on, with no
// option set: the cache is on from NewConn, it remembers that the statement's
// last result fit in one batch, and the query goes out as Bind, Describe,
// Execute and Sync in a single write. The first execution knows nothing and
// is fetched in batches, Flush then Sync — the second round trip.
func TestFetchAutoReadsAKnownSmallStatementInOneWrite(t *testing.T) {
	srv := &fakeServer{responses: slices.Concat(oneRow(), oneRowWhole())}
	conn := NewConn(srv)

	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8})); err != nil {
		t.Fatal(err)
	}
	first := srv.clientMessages(t)
	if want := append([]byte{pgwire.FrontendParse}, batchedRead...); !slices.Equal(first, want) {
		t.Errorf("first execution sent %q, want %q: nothing is known of the statement yet", first, want)
	}

	writes := srv.writes
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8})); err != nil {
		t.Fatal(err)
	}
	if got := sentSince(t, srv, len(first)); !slices.Equal(got, wholeRead) {
		t.Errorf("second execution sent %q, want %q: a statement known to fit is asked for whole", got, wholeRead)
	}
	if got := srv.writes - writes; got != 1 {
		t.Errorf("second execution took %d writes, want 1", got)
	}
	if prepared, hits, misses := conn.StatementCacheStats(); prepared != 1 || hits != 1 || misses != 1 {
		t.Errorf("stats = %d/%d/%d, want 1/1/1: the cache is on without PrepareStatements", prepared, hits, misses)
	}
}

// A result larger than BatchRows makes the next execution batched again, and
// a smaller one after it makes the one after whole: the cache remembers the
// last execution, not the first.
func TestFetchAutoFollowsTheLastResult(t *testing.T) {
	// The clock stands still in a bubble, so the client keeps the floor of
	// two batches ahead.
	synctest.Test(t, func(t *testing.T) {
		// Three rows in batches of two: the portal suspends after two, the two
		// Executes asked ahead bring the third and an empty end, the Sync the
		// ReadyForQuery.
		var first []byte
		first = append(first, beSimple(pgwire.BackendParseComplete)...)
		first = append(first, beSimple(pgwire.BackendBindComplete)...)
		first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
		first = append(first, beDataRow(int8Val(1))...)
		first = append(first, beDataRow(int8Val(2))...)
		first = append(first, beSimple(pgwire.BackendPortalSuspended)...)
		var rest []byte
		rest = append(rest, beDataRow(int8Val(3))...)
		rest = append(rest, beCommandComplete("SELECT 3")...)
		rest = append(rest, beCommandComplete("SELECT 0")...)

		srv := &fakeServer{responses: slices.Concat(
			[][]byte{first, rest, beReadyForQuery()},
			oneRowCached(),
			oneRowWhole(),
		)}
		conn := NewConn(srv)
		cfg := QueryConfig{BatchRows: 2}

		if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
			t.Fatal(err)
		}
		n := len(srv.clientMessages(t))
		if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
			t.Fatal(err)
		}
		// Batched again, and the two Executes ahead leave with the query: the
		// portal suspended last time, so it is one the backend fetches from.
		aheadRead := []byte{
			pgwire.FrontendBind, pgwire.FrontendDescribe, pgwire.FrontendExecute,
			pgwire.FrontendExecute, pgwire.FrontendExecute, pgwire.FrontendFlush, pgwire.FrontendSync,
		}
		if got := sentSince(t, srv, n); !slices.Equal(got, aheadRead) {
			t.Errorf("after three rows in batches of two, sent %q, want %q", got, aheadRead)
		}
		n = len(srv.clientMessages(t))
		if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
			t.Fatal(err)
		}
		if got := sentSince(t, srv, n); !slices.Equal(got, wholeRead) {
			t.Errorf("after one row, sent %q, want %q", got, wholeRead)
		}
	})
}

// An execution that fails says nothing about the statement's size: the next
// one is batched, even though the one before the failure fit.
func TestFetchAutoForgetsTheCountOfAFailedExecution(t *testing.T) {
	var failed []byte
	failed = append(failed, beSimple(pgwire.BackendBindComplete)...)
	failed = append(failed, beErrorResponse("22012", "division by zero")...)
	failed = append(failed, beReadyForQuery()...)

	srv := &fakeServer{responses: slices.Concat(oneRow(), [][]byte{failed}, oneRowCached())}
	conn := NewConn(srv)
	cfg := QueryConfig{BatchRows: 8}

	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err == nil {
		t.Fatal("the scripted error was not reported")
	}
	n := len(srv.clientMessages(t))
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
		t.Fatal(err)
	}
	if got := sentSince(t, srv, n); !slices.Equal(got, batchedRead) {
		t.Errorf("after a failed execution, sent %q, want %q", got, batchedRead)
	}
}

// FetchBatches is never asked for whole, whatever the cache knows.
func TestFetchBatchesIsNeverWhole(t *testing.T) {
	srv := &fakeServer{responses: slices.Concat(oneRow(), oneRowCached())}
	conn := NewConn(srv)
	cfg := QueryConfig{BatchRows: 8, Fetch: FetchBatches}

	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
		t.Fatal(err)
	}
	n := len(srv.clientMessages(t))
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, cfg)); err != nil {
		t.Fatal(err)
	}
	if got := sentSince(t, srv, n); !slices.Equal(got, batchedRead) {
		t.Errorf("FetchBatches sent %q, want %q", got, batchedRead)
	}
}

// A value outside the three is refused before anything is sent, rather than
// read as one of them.
func TestFetchOutOfRangeIsRefused(t *testing.T) {
	srv := &fakeServer{}
	conn := NewConn(srv)
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{Fetch: FetchBatches + 1})); err == nil {
		t.Error("an unknown Fetch was accepted")
	}
	if srv.sent.Len() != 0 {
		t.Errorf("%d bytes sent for a refused query", srv.sent.Len())
	}
}

// A batch bound out of range is refused before anything is sent, by an error
// naming the field, rather than replaced by a default nobody chose: zero is
// the default, a negative value or a byte bound past 1 GiB is a typo.
func TestOutOfRangeBatchBoundsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		field string
		cfg   QueryConfig
	}{
		{"BatchRows", QueryConfig{BatchRows: -1}},
		{"BatchBytes", QueryConfig{BatchBytes: -1}},
		{"BatchBytes", QueryConfig{BatchBytes: maxBatchBytes + 1}},
	} {
		srv := &fakeServer{}
		err := drain(t, NewConn(srv).Query(t.Context(), "SELECT id", nil, tc.cfg))
		if err == nil || !strings.Contains(err.Error(), "QueryConfig."+tc.field) {
			t.Errorf("Query with %+v returned %v, want an error naming %s", tc.cfg, err, tc.field)
		}
		if srv.sent.Len() != 0 {
			t.Errorf("Query with %+v sent %d bytes", tc.cfg, srv.sent.Len())
		}

		srv = &fakeServer{}
		tx := &Tx{conn: NewConn(srv)}
		err = tx.Pipeline(t.Context(), []Part{{SQL: "SELECT 1"}}, tc.cfg,
			func(int, sluice.Batch[Row]) bool { return true })
		if err == nil || !strings.Contains(err.Error(), "QueryConfig."+tc.field) {
			t.Errorf("Pipeline with %+v returned %v, want an error naming %s", tc.cfg, err, tc.field)
		}
		if srv.sent.Len() != 0 {
			t.Errorf("Pipeline with %+v sent %d bytes", tc.cfg, srv.sent.Len())
		}
	}
}
