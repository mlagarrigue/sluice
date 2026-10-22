package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/web"
)

// The comparison this project exists for, and the one it had never run.
//
// Every figure published before this one measured what batching **costs**:
// ×600 serially with no database, two round trips on a point read, four times
// pgx on a wide scan. The thesis — sixty-four callers waiting on one round trip
// beat sixty-four round trips — had not been measured once. This is that
// measurement.
//
// # Equal connections, and then honest about it
//
// The comparison runs at equal connection counts, because this connector has
// no pool and a comparison that ignores that measures pooling. So the first
// reading gives everyone **one** connection.
//
// That reading favours batching by construction: with one connection, sixty-four
// concurrent requests without a gateway serialise into sixty-four round trips,
// while the gateway turns them into one. It is a real configuration — it is
// what you get when connections are scarce, which is the case a gateway is for
// — but presenting it alone would be winning against a handicapped opponent,
// because nobody deploys pgx with `MaxConns(1)`.
//
// So the second reading gives pgx a **pool of sixteen**, which is what a real
// deployment does, and the third gives it sixty-four. Those are the numbers
// that say what batching is actually worth: not "faster", but "the same
// throughput on one connection instead of sixteen".

const dbQuery = `SELECT id, total FROM bench_orders WHERE id = ANY($1) ORDER BY id`

const singleQuery = `SELECT id, total FROM bench_orders WHERE id = $1`

// order is what a handler answers with.
type order struct {
	id    int64
	total int64
}

// --- sluice: one connection, one gateway, one query per batch -----------------

func sluiceDBHandler(tb testing.TB) http.Handler {
	tb.Helper()
	h, _ := sluiceDBHandlerCounted(tb)
	return h
}

// sluiceDBHandlerCounted also returns how many queries the pipeline has issued,
// which is the claim itself: sixty-four requests must reach the database as one.
func sluiceDBHandlerCounted(tb testing.TB) (http.Handler, *atomic.Int64) {
	tb.Helper()
	conn := sluiceConn(tb)
	buf := make([]byte, 0, 64*12+64)
	return dbGatewayHandler(tb, func(ids []int64, found map[int64]int64) error {
		// The whole thesis, in one statement: every id in the batch leaves
		// as one array parameter and comes back in one result.
		buf = postgres.AppendInt8Array(buf[:0], ids)
		src := conn.Query(context.Background(), dbQuery, []postgres.Param{{OID: postgres.OIDInt8Array, Value: buf}},
			postgres.QueryConfig{
				BatchRows: 128, Fetch: postgres.FetchAll,
			})
		src.Stream()(func(rows sluice.Batch[postgres.Row]) bool {
			for _, r := range rows.Items {
				idBytes, _ := r.Value(0)
				totalBytes, _ := r.Value(1)
				id, _ := postgres.DecodeInt8(idBytes)
				total, _ := postgres.DecodeInt8(totalBytes)
				found[id] = total
			}
			return true
		})
		return src.Err()
	})
}

// pgxGatewayHandler is axis B of the internal study, in process: the same
// gateway, the same `= ANY($1)` on one connection, with pgx between the
// gateway and the socket instead of database/postgres. Against
// sluiceDBHandlerCounted it isolates the driver — the comparison above, pgx
// one query per request, mixes the driver with the grouping.
func pgxGatewayHandler(tb testing.TB) (http.Handler, *atomic.Int64) {
	tb.Helper()
	conn := pgxConn(tb)
	return dbGatewayHandler(tb, func(ids []int64, found map[int64]int64) error {
		rows, err := conn.Query(context.Background(), dbQuery, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, total int64
			if err := rows.Scan(&id, &total); err != nil {
				return err
			}
			found[id] = total
		}
		return rows.Err()
	})
}

// dbGatewayHandler is the gateway and the handler both drivers share; fetch
// fills found with the rows of one batch, in one round trip. The counter is
// the claim itself: sixty-four requests must reach the database as one.
func dbGatewayHandler(tb testing.TB, fetch func(ids []int64, found map[int64]int64) error) (http.Handler, *atomic.Int64) {
	tb.Helper()
	var queries atomic.Int64
	gw := gateway.New(
		gateway.Config{Size: 64, Within: 2 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int64, order]]) {
			ids := make([]int64, 0, 64)
			found := make(map[int64]int64, 64)

			calls(func(batch sluice.Batch[*gateway.Call[int64, order]]) bool {
				ids = ids[:0]
				clear(found)
				for _, c := range batch.Items {
					ids = append(ids, c.In)
				}
				queries.Add(1)
				if err := fetch(ids, found); err != nil {
					for _, c := range batch.Items {
						c.Fail(err)
					}
					return true
				}
				for _, c := range batch.Items {
					c.Reply(order{id: c.In, total: found[c.In]})
				}
				return true
			})
		})
	tb.Cleanup(func() { _ = gw.Close() })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, d, ok := web.PathInt64(r, "id")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		got, err := gw.Do(r.Context(), id)
		if err != nil {
			_ = web.WriteJSON(w, http.StatusServiceUnavailable, web.InternalReport())
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, map[string]any{"id": got.id, "total": got.total})
	})
	return mux, &queries
}

// --- pgx: one query per request, over a pool of a chosen size -----------------

func pgxDBHandler(tb testing.TB, poolSize int32) http.Handler {
	tb.Helper()
	cfg, err := pgxpool.ParseConfig(dsn(tb))
	if err != nil {
		tb.Fatal(err)
	}
	cfg.MaxConns = poolSize
	cfg.MinConns = poolSize
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		var got order
		if err := pool.QueryRow(r.Context(), singleQuery, id).Scan(&got.id, &got.total); err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = writeOrder(w, got)
	})
	return mux
}

func writeOrder(w http.ResponseWriter, o order) error {
	_, err := fmt.Fprintf(w, `{"id":%d,"total":%d}`+"\n", o.id, o.total)
	return err
}

// --- database/sql, one connection, for the like-for-like row -------------------

func sqlDBHandler(tb testing.TB, driver string, conns int) http.Handler {
	tb.Helper()
	db, err := sql.Open(driver, dsn(tb))
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.Ping(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		var got order
		if err := db.QueryRowContext(r.Context(), singleQuery, id).Scan(&got.id, &got.total); err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = writeOrder(w, got)
	})
	return mux
}

// --- the measurement -----------------------------------------------------------

// serveConcurrently fires n requests at once and waits for all of them, which
// is the shape a gateway is built for and the shape a per-request driver is
// judged on.
func serveConcurrently(b *testing.B, h http.Handler, n int) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				body := &replayBody{}
				w := &discardWriter{}
				r := *request(9)
				r.Method = http.MethodGet
				r.URL.Path = "/orders/" + strconv.Itoa(i+1)
				r.Body = body
				h.ServeHTTP(w, &r)
				if w.status != http.StatusOK {
					b.Errorf("status %d", w.status)
				}
			})
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportMetric(float64(n), "req/op")
}

const concurrency = 64

// One connection each: the configuration a fair comparison demands, and the one where
// batching does what it was built to do.
func BenchmarkWebDBSluiceOneConn(b *testing.B) {
	serveConcurrently(b, sluiceDBHandler(b), concurrency)
}

func BenchmarkWebDBPgxOneConn(b *testing.B) {
	serveConcurrently(b, pgxDBHandler(b, 1), concurrency)
}

// Axis B of the internal study: pgx under the same gateway, so that the only
// difference from BenchmarkWebDBSluiceOneConn is the driver. Paired against
// it with internal/bench/scripts/versus.sh, never read as a single run.
func BenchmarkWebDBPgxGateway(b *testing.B) {
	h, _ := pgxGatewayHandler(b)
	serveConcurrently(b, h, concurrency)
}

func BenchmarkWebDBLibPQOneConn(b *testing.B) {
	serveConcurrently(b, sqlDBHandler(b, "postgres", 1), concurrency)
}

// And what a real deployment does instead: give the driver a pool. This is the
// row that says what batching is actually worth — not "faster", but "the same
// work on one connection instead of sixteen".
func BenchmarkWebDBPgxPool16(b *testing.B) {
	serveConcurrently(b, pgxDBHandler(b, 16), concurrency)
}

func BenchmarkWebDBPgxPool64(b *testing.B) {
	serveConcurrently(b, pgxDBHandler(b, 64), concurrency)
}

// Both sides must answer the same thing, or the figures beside each other mean
// nothing.
func TestWebDBContendersAgree(t *testing.T) {
	if _, _, _, _, ok := pgTarget(); !ok {
		t.Skip("SLUICE_PG is not set")
	}

	pgxGW, _ := pgxGatewayHandler(t)
	handlers := map[string]http.Handler{
		"sluice":      sluiceDBHandler(t),
		"pgx":         pgxDBHandler(t, 4),
		"pgx+gateway": pgxGW,
		"lib/pq":      sqlDBHandler(t, "postgres", 4),
	}

	for _, id := range []int64{1, 42, 1000} {
		var want string
		for _, name := range []string{"sluice", "pgx", "pgx+gateway", "lib/pq"} {
			w := httptest.NewRecorder()
			r := *request(9)
			r.Method = http.MethodGet
			r.URL.Path = "/orders/" + strconv.FormatInt(id, 10)
			r.Body = &replayBody{}
			handlers[name].ServeHTTP(w, &r)

			if w.Code != http.StatusOK {
				t.Fatalf("%s: status %d for id %d", name, w.Code, id)
			}
			// The whole body, byte for byte: all three handlers encode the
			// same struct with encoding/json, so anything short of identity
			// means one of them answered something else.
			got := w.Body.String()
			if want == "" {
				want = got
				continue
			}
			if got != want {
				t.Errorf("id %d: %s answered %q, first contender answered %q", id, name, got, want)
			}
		}
	}
}

// The claim in one assertion: sixty-four concurrent requests must reach the
// database as **one** query, not sixty-four. Without it the benchmark could be
// fast for some other reason and nobody would know.
//
// Counted where the queries are issued rather than in pg_stat_database, whose
// counters are collected asynchronously and read as zero for a run this short —
// a check that reported "0 queries for 64 requests" and passed, which is worse
// than no check at all.
func TestGatewayIssuesOneQueryPerBatch(t *testing.T) {
	if _, _, _, _, ok := pgTarget(); !ok {
		t.Skip("SLUICE_PG is not set")
	}
	h, queries := sluiceDBHandlerCounted(t)

	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Go(func() {
			w := &discardWriter{}
			r := *request(9)
			r.Method = http.MethodGet
			r.URL.Path = "/orders/" + strconv.Itoa(i+1)
			r.Body = &replayBody{}
			h.ServeHTTP(w, &r)
			if w.status != http.StatusOK {
				t.Errorf("status %d", w.status)
			}
		})
	}
	wg.Wait()

	got := queries.Load()
	if got == 0 {
		t.Fatal("no query was issued at all, so this proves nothing")
	}
	// Sixty-four callers arriving together fill a batch of sixty-four. Timing
	// can split them across two, and a third would still be a win; more than
	// that means the batching is not happening.
	if got > 3 {
		t.Errorf("%d requests reached the database as %d queries; batching did not happen", concurrency, got)
	}
	t.Logf("%d concurrent requests reached the database as %d queries", concurrency, got)
}
