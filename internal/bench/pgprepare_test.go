package bench

import (
	"strconv"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// The statement cache, measured — and the first thing to measure is what this
// harness *cannot* say.
//
// # What a replayed server cannot show
//
// The reason a statement cache exists is that the server stops parsing and
// planning the same SQL for every batch. A canned conversation replayed out of
// memory does none of that work, so the saving the cache exists for is
// invisible here by construction. Anyone reading a win out of these numbers is
// reading something that is not in them; the win is in the integration suite,
// against a server that actually parses.
//
// # What it can show, and why it is the honest half
//
// Two things, both entirely client-side:
//
//   - The **cost**. A hit is a map lookup on a key built from the SQL text and
//     the parameter OIDs, plus the LRU bookkeeping. A miss is that plus a name
//     and an entry. That cost is paid on every query whether or not the cache
//     ever pays off, so it is what a workload of distinct SQL is left with.
//   - The **bytes not sent**. A hit sends a Bind against a name instead of a
//     Parse carrying the whole statement. That is a real saving on a real
//     socket, it is measurable here, and it grows with the length of the SQL —
//     which is why the long-statement case is measured separately.
//
// BenchmarkPGPrepareMissUnbounded is the case where the cache loses, and it is
// here because a feature measured only where it wins has not been measured.

// countingServer replays a script and counts what the client wrote, so "the
// bytes not sent" is an assertion rather than an argument.
type countingServer struct {
	script  []byte
	pos     int
	written int
}

func (s *countingServer) Read(p []byte) (int, error) {
	if s.pos >= len(s.script) {
		s.pos = 0
	}
	n := copy(p, s.script[s.pos:])
	s.pos += n
	return n, nil
}

func (s *countingServer) Write(p []byte) (int, error) { s.written += len(p); return len(p), nil }
func (s *countingServer) Close() error                { return nil }

// A statement long enough to be worth not re-sending — a join with a handful
// of predicates, which is what a real query looks like and what the short
// "SELECT id, total FROM orders" of the other benchmarks is not.
const longSQL = `SELECT o.order_id, o.line_index, o.sale_qty, o.price
FROM order_lines o
JOIN unnest($1::bigint[], $2::int[]) AS k(order_id, line_index)
  ON o.order_id = k.order_id AND o.line_index = k.line_index
WHERE o.sale_qty > 0 AND o.price IS NOT NULL
ORDER BY o.order_id, o.line_index`

func runQuery(b *testing.B, conn *postgres.Conn, sql string) {
	b.Helper()
	src := conn.Query(b.Context(), sql, nil, postgres.QueryConfig{BatchRows: 8})
	var seen int
	src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
		seen += len(batch.Items)
		return true
	})
	if err := src.Err(); err != nil {
		b.Fatalf("the replayed conversation desynchronised: %v", err)
	}
	sink = int64(seen)
}

// BenchmarkPGPrepareOff: the same query repeatedly with no cache — every one
// carries its full text and the server re-parses it.
func BenchmarkPGPrepareOff(b *testing.B) {
	srv := &countingServer{script: pgScript(8)}
	conn := postgres.NewConn(srv)
	for b.Loop() {
		runQuery(b, conn, longSQL)
	}
	b.StopTimer()
	reportBytesPerQuery(b, srv.written)
}

// BenchmarkPGPrepareHit: the same query repeatedly with the cache on. The CPU
// difference against Off is small and the byte difference is not — which is
// the shape of the feature on the client side.
func BenchmarkPGPrepareHit(b *testing.B) {
	srv := &countingServer{script: pgScript(8)}
	conn := postgres.NewConn(srv)
	if err := conn.PrepareStatements(16); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		runQuery(b, conn, longSQL)
	}
	b.StopTimer()
	reportBytesPerQuery(b, srv.written)

	if _, hits, misses := conn.StatementCacheStats(); hits == 0 || misses > 1 {
		b.Fatalf("this is meant to be the hit case: %d hits, %d misses", hits, misses)
	}
}

// BenchmarkPGPrepareMissUnbounded: **the case where the cache loses**, in its
// worst-behaved form. Every query is distinct, so nothing is ever reused, and
// the bound is set high enough that nothing is ever evicted either.
//
// Read the name: what this measures is a cache *without an effective bound* on
// a workload of generated SQL. The map grows for the whole run, so the figure
// includes its growth — which is the point, because that is exactly what an
// over-generous bound does to a connection that lives for hours. The cost of a
// miss against a *properly* bounded cache is isolated separately, without the
// protocol in the way, by BenchmarkStmtCacheChurn in the postgres package.
//
// Eviction traffic is absent here — a Close needs a reply this replay script
// does not carry — so a live connection pays a round trip per eviction on top.
// The direction is what matters and this harness understates it.
func BenchmarkPGPrepareMissUnbounded(b *testing.B) {
	srv := &countingServer{script: pgScript(8)}
	conn := postgres.NewConn(srv)
	// Deliberately unbounded in practice: nothing is ever evicted, so the map
	// grows for the whole run and its growth is part of the figure.
	if err := conn.PrepareStatements(1 << 20); err != nil {
		b.Fatal(err)
	}
	var n int
	for b.Loop() {
		n++
		runQuery(b, conn, longSQL+" -- "+strconv.Itoa(n))
	}
	b.StopTimer()
	reportBytesPerQuery(b, srv.written)

	if _, hits, _ := conn.StatementCacheStats(); hits != 0 {
		b.Fatalf("this is meant to be the miss case and it hit %d times", hits)
	}
}

// reportBytesPerQuery publishes the client-side traffic, which is the part of
// the cache's benefit this harness can actually see.
func reportBytesPerQuery(b *testing.B, written int) {
	b.Helper()
	if b.N > 0 {
		b.ReportMetric(float64(written)/float64(b.N), "B/query")
	}
}
