package postgres

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// parsed returns the (name, sql) of every Parse the client sent, which is what
// makes "this query was not re-parsed" an assertion rather than a hope.
func parsed(t *testing.T, srv *fakeServer) [][2]string {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var out [][2]string
	for {
		m, err := rd.Next()
		if err != nil {
			return out
		}
		if m.Type != pgwire.FrontendParse {
			continue
		}
		name, rest, ok := cstring(m.Body)
		if !ok {
			continue
		}
		sql, _, _ := cstring(rest)
		out = append(out, [2]string{name, sql})
	}
}

// bound returns the statement name of every Bind.
func bound(t *testing.T, srv *fakeServer) []string {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var out []string
	for {
		m, err := rd.Next()
		if err != nil {
			return out
		}
		if m.Type != pgwire.FrontendBind {
			continue
		}
		_, rest, ok := cstring(m.Body) // portal
		if !ok {
			continue
		}
		stmt, _, _ := cstring(rest)
		out = append(out, stmt)
	}
}

// closed returns the name of every prepared statement the client asked the
// server to release.
func closed(t *testing.T, srv *fakeServer) []string {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	var out []string
	for {
		m, err := rd.Next()
		if err != nil {
			return out
		}
		if m.Type != pgwire.FrontendClose || len(m.Body) == 0 || m.Body[0] != 'S' {
			continue
		}
		name, _, _ := cstring(m.Body[1:])
		out = append(out, name)
	}
}

// oneRow is a complete extended-query answer for a single-column result.
func oneRow() [][]byte {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, beRowDescription([]string{"id"}, []uint32{OIDInt8})...)
	first = append(first, beDataRow(int8Val(1))...)
	first = append(first, beCommandComplete("SELECT 1")...)
	return [][]byte{first, beReadyForQuery()}
}

// closeCycle is what the server answers to a Close plus Sync.
func closeCycle() []byte {
	return append(beSimple(pgwire.BackendCloseComplete), beReadyForQuery()...)
}

func drain(t *testing.T, src sluice.Source[Row]) error {
	t.Helper()
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	return src.Err()
}

// Off by default, and off must mean exactly what it meant before the cache
// existed: the unnamed statement, re-parsed every time.
func TestStatementCacheOffByDefault(t *testing.T) {
	srv := &fakeServer{responses: append(oneRow(), oneRow()...)}
	conn := NewConn(srv)

	for range 2 {
		if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range parsed(t, srv) {
		if p[0] != "" {
			t.Errorf("a statement was named %q with the cache off", p[0])
		}
	}
	if got := len(parsed(t, srv)); got != 2 {
		t.Errorf("%d Parse messages, want 2: with no cache every query re-parses", got)
	}
	if prepared, hits, misses := conn.StatementCacheStats(); prepared != 0 || hits != 0 || misses != 0 {
		t.Errorf("stats = %d/%d/%d with the cache off", prepared, hits, misses)
	}
}

// The feature itself: the second run of the same query binds what the server
// already parsed.
func TestStatementCacheReusesAPreparedStatement(t *testing.T) {
	srv := &fakeServer{responses: append(oneRow(), oneRow()...)}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})); err != nil {
			t.Fatal(err)
		}
	}

	ps := parsed(t, srv)
	if len(ps) != 1 {
		t.Fatalf("%d Parse messages, want 1: the second query re-parsed a statement the server already had", len(ps))
	}
	if ps[0][0] == "" {
		t.Fatal("the statement was prepared unnamed, so the server replaces it on the next Parse and nothing is cached")
	}
	name := ps[0][0]

	bs := bound(t, srv)
	if len(bs) != 2 || bs[0] != name || bs[1] != name {
		t.Errorf("Binds went to %q, want both to %q", bs, name)
	}

	prepared, hits, misses := conn.StatementCacheStats()
	if prepared != 1 || hits != 1 || misses != 1 {
		t.Errorf("stats = prepared %d, hits %d, misses %d; want 1/1/1", prepared, hits, misses)
	}
}

// Different SQL is a different statement, and so is the same SQL with
// different declared parameter types — reusing one for the other is how a
// query silently starts comparing an int4 against a bigint column.
func TestStatementCacheKeyIncludesTheParameterTypes(t *testing.T) {
	srv := &fakeServer{responses: append(oneRow(), oneRow()...)}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}

	const sql = "SELECT id WHERE k = $1"
	if err := drain(t, conn.Query(t.Context(), sql, [][]byte{nil},
		QueryConfig{ParamOIDs: []uint32{OIDInt8}})); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), sql, [][]byte{nil},
		QueryConfig{ParamOIDs: []uint32{OIDInt4}})); err != nil {
		t.Fatal(err)
	}

	ps := parsed(t, srv)
	if len(ps) != 2 {
		t.Fatalf("%d Parse messages, want 2: the same text with different types was reused", len(ps))
	}
	if ps[0][0] == ps[1][0] {
		t.Errorf("both prepared under %q", ps[0][0])
	}
}

// The bound has to be real on the server, not only in this process: an evicted
// statement is Closed, or the backend keeps a plan nobody will bind again.
func TestStatementCacheEvictsLeastRecentlyUsedAndClosesIt(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		oneRow()[0], oneRow()[1], // A
		oneRow()[0], oneRow()[1], // B
		oneRow()[0], oneRow()[1], // A again — a hit, which makes B the oldest
		// C, with B's eviction pipelined in front of its Parse: the
		// CloseComplete arrives in the same answer, not in a round trip of
		// its own.
		append(beSimple(pgwire.BackendCloseComplete), oneRow()[0]...), oneRow()[1],
	}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(2); err != nil {
		t.Fatal(err)
	}

	for _, sql := range []string{"SELECT a", "SELECT b", "SELECT a", "SELECT c"} {
		if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	ps := parsed(t, srv)
	if len(ps) != 3 {
		t.Fatalf("%d Parse messages, want 3 (a, b, c)", len(ps))
	}
	// The one closed must be B's name, not A's: A was used more recently.
	var bName string
	for _, p := range ps {
		if p[1] == "SELECT b" {
			bName = p[0]
		}
	}
	cs := closed(t, srv)
	if len(cs) != 1 {
		t.Fatalf("%d statements closed, want 1: the cache is bounded here and unbounded on the server", len(cs))
	}
	if cs[0] != bName {
		t.Errorf("closed %q, want %q — the least recently used was not the one evicted", cs[0], bName)
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 2 {
		t.Errorf("%d statements prepared, want the bound of 2", prepared)
	}
	// The Close travels with C's Parse: nothing that makes the server answer
	// (Sync, Flush) separates them.
	if got := frontendTypes(srv.sent.Bytes()); !strings.Contains(got, "CP") {
		t.Errorf("client messages %q: the eviction's Close is not pipelined with the next Parse", got)
	}
}

// frontendTypes lists the type byte of every message the client sent.
func frontendTypes(raw []byte) string {
	rd := pgwire.NewReader(bytes.NewReader(raw))
	var b strings.Builder
	for {
		m, err := rd.Next()
		if err != nil {
			return b.String()
		}
		b.WriteByte(m.Type)
	}
}

// A pipelined Close the server never confirmed is not forgotten: the batch
// failed before it, so the backend may still hold the statement, and the
// name goes out again with the next miss. Dropping it would let a run of
// failures grow the server's statement count past the bound.
func TestStatementCacheRetriesAnUnconfirmedClose(t *testing.T) {
	failed := beErrorResponse("57014", "canceling statement due to user request")
	srv := &fakeServer{responses: [][]byte{
		oneRow()[0], oneRow()[1], // A
		failed, beReadyForQuery(), // B: evicts A, but the batch fails before CloseComplete
		append(append(beSimple(pgwire.BackendCloseComplete), beSimple(pgwire.BackendCloseComplete)...), oneRow()[0]...), oneRow()[1], // C
	}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(1); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), "SELECT a", nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), "SELECT b", nil, QueryConfig{})); err == nil {
		t.Fatal("the failed query reported no error")
	}
	if err := drain(t, conn.Query(t.Context(), "SELECT c", nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	ps := parsed(t, srv)
	cs := closed(t, srv)
	// A's Close went out with B, unconfirmed, and again with C; B (never
	// prepared, so dropped by settleParse) is not closed, and C's eviction
	// had nothing to evict since B's entry was gone.
	if len(cs) != 2 || cs[0] != ps[0][0] || cs[1] != ps[0][0] {
		t.Errorf("closed %q, want A's name %q twice", cs, ps[0][0])
	}
	if len(conn.stmts.closing) != 0 {
		t.Errorf("closing = %q after the server confirmed everything", conn.stmts.closing)
	}
}

// A schema change under a live connection invalidates a plan. The entry is
// dropped so the caller's retry re-prepares, and the error says what happened
// rather than being retried into silence.
func TestStatementCacheDropsAStalePlan(t *testing.T) {
	var stale []byte
	stale = append(stale, beSimple(pgwire.BackendParseComplete)...)
	stale = append(stale, beErrorResponse("0A000", "cached plan must not change result type")...)

	srv := &fakeServer{responses: [][]byte{
		oneRow()[0], oneRow()[1], // prepared and run once
		stale, beReadyForQuery(), // then the schema moves
		oneRow()[0], oneRow()[1], // and the retry re-prepares
	}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}

	const sql = "SELECT id"
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}

	err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{}))
	if !errors.Is(err, ErrStalePlan) {
		t.Fatalf("error = %v, want it to wrap ErrStalePlan", err)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection was broken by a stale plan: %v", conn.Err())
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 0 {
		t.Errorf("%d statements still cached after the server rejected one", prepared)
	}

	// The retry must re-prepare rather than bind a name the server discarded.
	// Two Parses, not three: the query that hit the stale plan was a cache
	// *hit*, so it sent a Bind and no Parse at all — which is precisely why
	// the entry had to be dropped for the retry to work.
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatalf("the retry after a stale plan returned %v", err)
	}
	ps := parsed(t, srv)
	if len(ps) != 2 {
		t.Fatalf("%d Parse messages, want 2 (the original and the re-prepare)", len(ps))
	}
	if ps[0][0] == ps[1][0] {
		t.Errorf("the retry re-used the discarded name %q instead of preparing a fresh one", ps[0][0])
	}
	// And the last Bind must go to the new name, not the one the server threw
	// away — binding a discarded statement is an error on every later query.
	bs := bound(t, srv)
	if bs[len(bs)-1] != ps[1][0] {
		t.Errorf("the retry bound %q, want the freshly prepared %q", bs[len(bs)-1], ps[1][0])
	}
}

// Turning the cache off must release what it holds on the server, not just
// forget about it here.
func TestPrepareStatementsZeroClosesEverything(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		oneRow()[0], oneRow()[1],
		closeCycle(),
	}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(4); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), "SELECT id", nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	if err := conn.PrepareStatements(0); err != nil {
		t.Fatal(err)
	}

	if got := len(closed(t, srv)); got != 1 {
		t.Errorf("%d statements closed when the cache was turned off, want 1", got)
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 0 {
		t.Errorf("%d statements still reported after the cache was turned off", prepared)
	}
}

// Shrinking the bound must evict down to it on the server too.
func TestPrepareStatementsShrinkEvicts(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		oneRow()[0], oneRow()[1],
		oneRow()[0], oneRow()[1],
		closeCycle(),
	}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(4); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"SELECT a", "SELECT b"} {
		if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.PrepareStatements(1); err != nil {
		t.Fatal(err)
	}
	if got := len(closed(t, srv)); got != 1 {
		t.Errorf("%d statements closed when shrinking 4 → 1, want 1", got)
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 1 {
		t.Errorf("%d statements prepared after shrinking to 1", prepared)
	}
}

// Statement names are generated here and never taken from a caller: they are
// concatenated into SQL-adjacent protocol fields, and a name from a request
// would be the one place this package builds an identifier it did not choose.
func TestStatementNamesAreGenerated(t *testing.T) {
	srv := &fakeServer{responses: append(oneRow(), oneRow()...)}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"SELECT a", "SELECT b"} {
		if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for _, p := range parsed(t, srv) {
		if !strings.HasPrefix(p[0], "sluice_") {
			t.Errorf("statement named %q, which did not come from this package", p[0])
		}
		if seen[p[0]] {
			t.Errorf("two statements prepared under the same name %q", p[0])
		}
		seen[p[0]] = true
	}
}

// The LRU bookkeeping, exercised directly: the list is hand-rolled to keep the
// hot path allocation-free, and a hand-rolled list is exactly the thing that
// works for three entries and corrupts on the fourth.
func TestStmtCacheLRUOrder(t *testing.T) {
	c := newStmtCache(3)
	keys := []stmtKey{{sql: "a"}, {sql: "b"}, {sql: "c"}}
	for _, k := range keys {
		c.add(k)
	}
	// Touch a, so the oldest becomes b.
	if _, ok := c.lookup(keys[0]); !ok {
		t.Fatal("a was not found")
	}
	_, name, ok := c.evictee()
	if !ok {
		t.Fatal("the cache is full but named no evictee")
	}
	if want := c.entries[c.byKey[keys[1]]].name; name != want {
		t.Errorf("evictee = %q, want b's %q", name, want)
	}

	// Remove from the middle and from the ends, then check the list is still
	// walkable in both directions and agrees with the map.
	c.remove(keys[1])
	c.remove(keys[0])
	c.add(stmtKey{sql: "d"})
	c.add(stmtKey{sql: "e"})

	var forward int
	for i := c.head; i >= 0; i = c.entries[i].next {
		forward++
		if forward > len(c.entries)+1 {
			t.Fatal("the list loops walking forward")
		}
	}
	var backward int
	for i := c.tail; i >= 0; i = c.entries[i].prev {
		backward++
		if backward > len(c.entries)+1 {
			t.Fatal("the list loops walking backward")
		}
	}
	if forward != len(c.byKey) || backward != len(c.byKey) {
		t.Errorf("list holds %d forward and %d backward, map holds %d", forward, backward, len(c.byKey))
	}
}

// A Parse the server refuses prepares nothing, so the cache must not go on
// believing it did.
//
// The entry is recorded before the Parse is sent — it has to be, since the
// name is what the Parse carries — and nothing used to remove it when the
// server answered with an error instead of ParseComplete. Every later
// execution of that SQL then bound a statement the backend had never created
// and failed with 26000, forever. One query run inside an aborted transaction
// was enough to poison it.
func TestStatementCacheForgetsAStatementTheServerRefused(t *testing.T) {
	refused := append(beErrorResponse("42601", "syntax error at or near \"SELCT\""), beReadyForQuery()...)
	srv := &fakeServer{responses: [][]byte{refused, beReadyForQuery()}}
	srv.responses = append(srv.responses, oneRow()...)
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}

	const sql = "SELCT id"
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err == nil {
		t.Fatal("the refused query reported no error")
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 0 {
		t.Errorf("%d statements cached after the server refused the only Parse", prepared)
	}

	// The retry is what a caller does next, and it must re-prepare rather than
	// bind the name the server never created.
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	if got := len(parsed(t, srv)); got != 2 {
		t.Fatalf("%d Parse messages, want 2: the retry bound a statement that was never prepared", got)
	}
}

// The other direction: the entry was real, and the server lost it.
//
// A pooler running DISCARD ALL, a swapped backend, anything that resets the
// session leaves this cache naming statements the connection no longer holds.
// The server says so with 26000, and without dropping the entry every later
// execution of that SQL re-binds the dead name and fails the same way.
func TestStatementCacheForgetsAStatementTheServerNoLongerHolds(t *testing.T) {
	gone := append(beErrorResponse("26000", "prepared statement \"sluice_0\" does not exist"), beReadyForQuery()...)
	srv := &fakeServer{responses: oneRow()}
	srv.responses = append(srv.responses, gone, beReadyForQuery())
	srv.responses = append(srv.responses, oneRow()...)
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}

	const sql = "SELECT id"
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err == nil {
		t.Fatal("the query against the vanished statement reported no error")
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 0 {
		t.Errorf("%d statements cached after the server said it holds none", prepared)
	}

	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	ps := parsed(t, srv)
	if len(ps) != 2 {
		t.Fatalf("%d Parse messages, want 2: the retry re-bound the statement the server had lost", len(ps))
	}
	if ps[0][0] == ps[1][0] {
		t.Errorf("both prepared under %q, so the dead name was reused", ps[0][0])
	}
}

// Closing every statement stops at the first broken connection: the rest die
// with the backend session, and writing their Close messages is talking on a
// socket whose position nobody knows.
func TestCloseAllStatementsStopsOnceBroken(t *testing.T) {
	w := &tripwire{fakeServer: &fakeServer{responses: append(oneRow(), oneRow()...)}}
	conn := NewConn(w)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"SELECT a", "SELECT b"} {
		if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
			t.Fatal(err)
		}
	}
	// The script is exhausted: the first Close reads EOF and breaks.
	if err := conn.PrepareStatements(0); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("PrepareStatements(0) returned %v, want ErrConnBroken", err)
	}
	if w.after != 0 {
		t.Errorf("closing statements touched the socket %d more times after it failed", w.after)
	}
}

// 0A000 is every "feature not supported", not only a stale plan. Only the
// stale-plan case may drop the cache entry and wrap ErrStalePlan; recognised
// by its routine (not localised) or by its English message.
func TestStalePlanIsOnlyTheRevalidateCase(t *testing.T) {
	for _, tc := range []struct {
		name string
		e    *Error
		want bool
	}{
		{"english message", &Error{Code: "0A000", Message: "cached plan must not change result type"}, true},
		{"localised message, routine", &Error{Code: "0A000", Message: "le plan en cache ne doit pas modifier le type en résultat", Routine: "RevalidateCachedQuery"}, true},
		{"other 0A000", &Error{Code: "0A000", Message: "cannot use subquery in check constraint", Routine: "transformSubLink"}, false},
		{"routine under another code", &Error{Code: "XX000", Routine: "RevalidateCachedQuery"}, false},
		{"not a server error", nil, false},
	} {
		var err error
		if tc.e != nil {
			err = tc.e
		}
		if got := isStalePlan(err); got != tc.want {
			t.Errorf("%s: isStalePlan = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// An unrelated 0A000 through a cached statement is an ordinary error: not
// labelled a schema change, and the healthy cache entry is kept.
func TestStatementCacheKeepsEntryOnOtherFeatureNotSupported(t *testing.T) {
	var refused []byte
	refused = append(refused, beSimple(pgwire.BackendBindComplete)...)
	refused = append(refused, frame(pgwire.BackendErrorResponse, string(errBody("S", "ERROR", "C", "0A000", "M", "FOR UPDATE is not allowed with aggregate functions", "R", "CheckSelectLocking")))...)

	srv := &fakeServer{responses: [][]byte{oneRow()[0], oneRow()[1], refused, beReadyForQuery()}}
	conn := NewConn(srv)
	if err := conn.PrepareStatements(8); err != nil {
		t.Fatal(err)
	}
	const sql = "SELECT id"
	if err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{})); err != nil {
		t.Fatal(err)
	}
	err := drain(t, conn.Query(t.Context(), sql, nil, QueryConfig{}))
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "0A000" {
		t.Fatalf("error = %v, want the server's 0A000", err)
	}
	if errors.Is(err, ErrStalePlan) {
		t.Errorf("an unrelated 0A000 was reported as a stale plan: %v", err)
	}
	if prepared, _, _ := conn.StatementCacheStats(); prepared != 1 {
		t.Errorf("%d statements cached, want the healthy entry kept", prepared)
	}
}
