package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// The connector comparison, against a live server.
//
// # One connection each
//
// Every contender gets exactly one. This connector has no pool by standing
// decision, so a comparison against a pooled driver would be measuring pooling
// — the one thing this package deliberately does not do. `pgx.Connect`,
// `sql.DB` with `SetMaxOpenConns(1)`, and one `postgres.Conn`.
//
// # The same SQL, the same scans
//
// Same statement text on every side, same values decoded into the same Go
// types. Where a driver offers a faster path that changes the work — a
// row-by-row scan against a whole-result read — both are measured rather than
// one being chosen to flatter someone.
//
// # What is not measured
//
// A loopback round trip, which is the smallest one that exists. On a real
// network every figure here moves toward the round trip and the differences
// between drivers shrink accordingly. Read the *shape*: where a driver spends
// allocations, and whether a batched read costs one round trip or a thousand.

func pgTarget() (addr, user, db, password string, ok bool) {
	addr = os.Getenv("SLUICE_PG")
	if addr == "" {
		return "", "", "", "", false
	}
	user, db = os.Getenv("SLUICE_PG_USER"), os.Getenv("SLUICE_PG_DB")
	if user == "" {
		user = "postgres"
	}
	if db == "" {
		db = user
	}
	return addr, user, db, os.Getenv("SLUICE_PG_PASSWORD"), true
}

func dsn(tb testing.TB) string {
	tb.Helper()
	addr, user, db, pass, ok := pgTarget()
	if !ok {
		tb.Skip("SLUICE_PG is not set")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)
}

func sluiceConn(tb testing.TB) *postgres.Conn {
	tb.Helper()
	addr, user, db, pass, ok := pgTarget()
	if !ok {
		tb.Skip("SLUICE_PG is not set")
	}
	dial := func(ctx context.Context) (io.ReadWriteCloser, error) { return dialCount(ctx, "tcp", addr) }
	conn, err := postgres.Connect(context.Background(), dial, postgres.Config{User: user, Database: db, Password: pass})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close() })
	return conn
}

func pgxConn(tb testing.TB) *pgx.Conn {
	tb.Helper()
	pcfg, err := pgx.ParseConfig(dsn(tb))
	if err != nil {
		tb.Fatal(err)
	}
	pcfg.DialFunc = dialCount
	conn, err := pgx.ConnectConfig(context.Background(), pcfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func sqlDB(tb testing.TB, driver string) *sql.DB {
	tb.Helper()
	db, err := sql.Open(driver, dsn(tb))
	if err != nil {
		tb.Fatal(err)
	}
	// One connection, so this measures the driver rather than the pool.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	return db
}

// --- point read: one row by primary key ---------------------------------------

const pointSQL = `SELECT id, total FROM bench_orders WHERE id = $1`

// No option set: the default statement cache remembers that the read fits in
// one batch, and FetchAuto asks for it whole from the second execution on.
func BenchmarkPGPointSluice(b *testing.B) {
	conn := sluiceConn(b)
	key := make([]byte, 0, 8)
	var acc int64
	b.ReportAllocs()
	report := countReport(b)
	for b.Loop() {
		key = postgres.AppendInt8(key[:0], 42)
		src := conn.Query(b.Context(), pointSQL, [][]byte{key},
			postgres.QueryConfig{BatchRows: 2, ParamOIDs: []uint32{postgres.OIDInt8}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			for _, r := range batch.Items {
				v, _ := r.Value(1)
				t, _ := postgres.DecodeInt8(v)
				acc += t
			}
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	report()
	sink = int(acc)
}

// The same read with the statement cache on, which is what pgx does by
// default. Without this the comparison is one round trip against two — pgx
// binding a statement the server already parsed, sluice re-parsing every time —
// and that is a difference in work, not in speed.
func BenchmarkPGPointSluiceCached(b *testing.B) {
	conn := sluiceConn(b)
	if err := conn.PrepareStatements(16); err != nil {
		b.Fatal(err)
	}
	key := make([]byte, 0, 8)
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		key = postgres.AppendInt8(key[:0], 42)
		src := conn.Query(b.Context(), pointSQL, [][]byte{key},
			postgres.QueryConfig{BatchRows: 2, ParamOIDs: []uint32{postgres.OIDInt8}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			for _, r := range batch.Items {
				v, _ := r.Value(1)
				t, _ := postgres.DecodeInt8(v)
				acc += t
			}
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = int(acc)
}

// The same read with FetchAll: one round trip from the first execution on,
// which was the entire gap against pgx before FetchAuto. What it gives up is a
// cheap early stop, and a point read is never abandoned half way.
func BenchmarkPGPointSluiceFetchAll(b *testing.B) {
	conn := sluiceConn(b)
	if err := conn.PrepareStatements(16); err != nil {
		b.Fatal(err)
	}
	key := make([]byte, 0, 8)
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		key = postgres.AppendInt8(key[:0], 42)
		src := conn.Query(b.Context(), pointSQL, [][]byte{key},
			postgres.QueryConfig{BatchRows: 2, Fetch: postgres.FetchAll, ParamOIDs: []uint32{postgres.OIDInt8}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			for _, r := range batch.Items {
				v, _ := r.Value(1)
				t, _ := postgres.DecodeInt8(v)
				acc += t
			}
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = int(acc)
}

func BenchmarkPGAnySluiceFetchAll(b *testing.B) {
	conn := sluiceConn(b)
	if err := conn.PrepareStatements(16); err != nil {
		b.Fatal(err)
	}
	keys := thousandKeys()
	buf := make([]byte, 0, 1024*12+64)
	var seen int
	b.ReportAllocs()
	for b.Loop() {
		buf = postgres.AppendInt8Array(buf[:0], keys)
		src := conn.Query(b.Context(), anySQL, [][]byte{buf},
			postgres.QueryConfig{BatchRows: 1024, Fetch: postgres.FetchAll, ParamOIDs: []uint32{postgres.OIDInt8Array}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			seen += len(batch.Items)
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = seen
}

func BenchmarkPGPointPgx(b *testing.B) {
	conn := pgxConn(b)
	ctx := b.Context()
	var acc int64
	b.ReportAllocs()
	report := countReport(b)
	for b.Loop() {
		var id, total int64
		if err := conn.QueryRow(ctx, pointSQL, 42).Scan(&id, &total); err != nil {
			b.Fatal(err)
		}
		acc += total
	}
	report()
	sink = int(acc)
}

func BenchmarkPGPointLibPQ(b *testing.B)  { benchPointSQL(b, "postgres") }
func BenchmarkPGPointPgxSQL(b *testing.B) { benchPointSQL(b, "pgx") }

func benchPointSQL(b *testing.B, driver string) {
	b.Helper()
	db := sqlDB(b, driver)
	// lib/pq numbers its parameters the same way, so the text is identical.
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		var id, total int64
		if err := db.QueryRow(pointSQL, 42).Scan(&id, &total); err != nil {
			b.Fatal(err)
		}
		acc += total
	}
	sink = int(acc)
}

// --- the claim: a thousand keys as one parameter -------------------------------

const anySQL = `SELECT id, total FROM bench_orders WHERE id = ANY($1) ORDER BY id`

func thousandKeys() []int64 {
	keys := make([]int64, 1000)
	for i := range keys {
		keys[i] = int64(i + 1)
	}
	return keys
}

func BenchmarkPGAnySluice(b *testing.B) {
	conn := sluiceConn(b)
	keys := thousandKeys()
	buf := make([]byte, 0, 1024*12+64)
	var seen int
	b.ReportAllocs()
	for b.Loop() {
		buf = postgres.AppendInt8Array(buf[:0], keys)
		src := conn.Query(b.Context(), anySQL, [][]byte{buf},
			postgres.QueryConfig{BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8Array}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			seen += len(batch.Items)
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = seen
}

func BenchmarkPGAnySluiceCached(b *testing.B) {
	conn := sluiceConn(b)
	if err := conn.PrepareStatements(16); err != nil {
		b.Fatal(err)
	}
	keys := thousandKeys()
	buf := make([]byte, 0, 1024*12+64)
	var seen int
	b.ReportAllocs()
	for b.Loop() {
		buf = postgres.AppendInt8Array(buf[:0], keys)
		src := conn.Query(b.Context(), anySQL, [][]byte{buf},
			postgres.QueryConfig{BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8Array}})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			seen += len(batch.Items)
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = seen
}

func BenchmarkPGAnyPgx(b *testing.B) {
	conn := pgxConn(b)
	keys := thousandKeys()
	ctx := b.Context()
	var seen int
	b.ReportAllocs()
	for b.Loop() {
		rows, err := conn.Query(ctx, anySQL, keys)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var id, total int64
			if err := rows.Scan(&id, &total); err != nil {
				b.Fatal(err)
			}
			seen++
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
	}
	sink = seen
}

func BenchmarkPGAnyPgxSQL(b *testing.B) { benchAnySQL(b, "pgx") }

func benchAnySQL(b *testing.B, driver string) {
	b.Helper()
	db := sqlDB(b, driver)
	keys := thousandKeys()
	var seen int
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Query(anySQL, keys)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var id, total int64
			if err := rows.Scan(&id, &total); err != nil {
				b.Fatal(err)
			}
			seen++
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		_ = rows.Close()
	}
	sink = seen
}

// --- wide-row scan: ten columns, twenty thousand rows --------------------------

const wideSQL = `SELECT id, c1, c2, c3, c4, c5, t1, t2, t3, t4, t5 FROM bench_wide`

// The wide read is also timed per query, from the call to the first batch in
// hand (TTFB) and to the end of the result (TTLB), and reported as a
// distribution. ns/op says how long a whole read takes; the two delays say
// where the time goes. If the server streamed the result and the client
// decoded it as it came, the last batch would trail the first by the decode
// time alone; a round trip per batch adds its wait twenty times over.
func BenchmarkPGWideSluice(b *testing.B) {
	benchWideSluice(b, postgres.QueryConfig{BatchRows: 1024})
}

// The same read with FetchAll: one Execute for the whole result, so the server
// streams every row while the client decodes the batches as they come. The
// batches the consumer sees are the same size. Against BenchmarkPGWideSluice
// this isolates the cost of asking for each batch after the previous one has
// been delivered.
func BenchmarkPGWideSluiceFetchAll(b *testing.B) {
	benchWideSluice(b, postgres.QueryConfig{BatchRows: 1024, Fetch: postgres.FetchAll})
}

func benchWideSluice(b *testing.B, cfg postgres.QueryConfig) {
	b.Helper()
	conn := sluiceConn(b)
	var acc int64
	var first, last []time.Duration
	b.ReportAllocs()
	report := countReport(b)
	for b.Loop() {
		start := time.Now()
		seen := false
		src := conn.Query(b.Context(), wideSQL, nil, cfg)
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			if !seen {
				first = append(first, time.Since(start))
				seen = true
			}
			spinBatch()
			for _, r := range batch.Items {
				for col := range 6 {
					v, isNull := r.Value(col)
					if isNull {
						continue
					}
					n, err := postgres.DecodeInt8(v)
					if err != nil {
						b.Errorf("column %d: %v", col, err)
						return false
					}
					acc += n
				}
				for col := 6; col < 11; col++ {
					v, _ := r.Value(col)
					acc += int64(len(v))
				}
			}
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
		last = append(last, time.Since(start))
	}
	sink = int(acc)
	report()
	reportPercentiles(b, "ttfb", first)
	reportPercentiles(b, "ttlb", last)
}

// pgx hands over a row at a time, so its first batch is its first row; it
// asks for the whole result in one Execute, as FetchAll does.
func BenchmarkPGWidePgx(b *testing.B) {
	conn := pgxConn(b)
	ctx := b.Context()
	var acc int64
	var first, last []time.Duration
	b.ReportAllocs()
	report := countReport(b)
	for b.Loop() {
		start := time.Now()
		rows, err := conn.Query(ctx, wideSQL)
		if err != nil {
			b.Fatal(err)
		}
		seen := false
		for rows.Next() {
			if !seen {
				first = append(first, time.Since(start))
				seen = true
			}
			var id, c1, c2, c3, c4, c5 int64
			var t1, t2, t3, t4, t5 string
			if err := rows.Scan(&id, &c1, &c2, &c3, &c4, &c5, &t1, &t2, &t3, &t4, &t5); err != nil {
				b.Fatal(err)
			}
			acc += id + c1 + c2 + c3 + c4 + c5 +
				int64(len(t1)+len(t2)+len(t3)+len(t4)+len(t5))
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		last = append(last, time.Since(start))
	}
	sink = int(acc)
	report()
	reportPercentiles(b, "ttfb", first)
	reportPercentiles(b, "ttlb", last)
}

func BenchmarkPGWidePgxSQL(b *testing.B) { benchWideSQL(b, "pgx") }
func BenchmarkPGWideLibPQ(b *testing.B)  { benchWideSQL(b, "postgres") }

func benchWideSQL(b *testing.B, driver string) {
	b.Helper()
	db := sqlDB(b, driver)
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Query(wideSQL)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var id, c1, c2, c3, c4, c5 int64
			var t1, t2, t3, t4, t5 string
			if err := rows.Scan(&id, &c1, &c2, &c3, &c4, &c5, &t1, &t2, &t3, &t4, &t5); err != nil {
				b.Fatal(err)
			}
			acc += id + c1 + c2 + c3 + c4 + c5 +
				int64(len(t1)+len(t2)+len(t3)+len(t4)+len(t5))
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		_ = rows.Close()
	}
	sink = int(acc)
}

// --- bulk COPY -----------------------------------------------------------------

// copyRows is one COPY. The table is emptied after every one, outside the
// timer: a second of COPY is a few hundred megabytes, which fills the
// devcontainer's 512 MB tmpfs if it is kept until the end of the run.
const copyRows = 50_000

func BenchmarkPGCopySluice(b *testing.B) {
	conn := sluiceConn(b)
	scratch := make([]byte, 0, 32)
	b.ReportAllocs()
	for b.Loop() {
		cp, err := conn.CopyFrom(b.Context(),
			"COPY bench_copy (id, label) FROM STDIN WITH (FORMAT BINARY)",
			postgres.CopyConfig{RowsPerTx: copyRows})
		if err != nil {
			b.Fatal(err)
		}
		buf := make([]byte, 0, copyRows*24)
		for i := range copyRows {
			buf = postgres.AppendTupleHeader(buf, 2)
			scratch = postgres.AppendInt8(scratch[:0], int64(i))
			buf = postgres.AppendField(buf, scratch)
			buf = postgres.AppendField(buf, []byte("label"))
		}
		if err := cp.WriteTuples(buf, copyRows); err != nil {
			b.Fatal(err)
		}
		if err := cp.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		truncate(b, conn)
		b.StartTimer()
	}
}

func BenchmarkPGCopyPgx(b *testing.B) {
	conn := pgxConn(b)
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		rows := make([][]any, copyRows)
		for i := range rows {
			rows[i] = []any{int64(i), "label"}
		}
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{"bench_copy"},
			[]string{"id", "label"}, pgx.CopyFromRows(rows)); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if _, err := conn.Exec(ctx, "TRUNCATE bench_copy"); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

func truncate(tb testing.TB, conn *postgres.Conn) {
	tb.Helper()
	if err := conn.Exec(context.Background(), "TRUNCATE bench_copy"); err != nil {
		tb.Fatal(err)
	}
}

// Every contender must read the same rows, or the figures beside each other
// mean nothing. This runs before the benchmarks are believed.
func TestPGContendersAgree(t *testing.T) {
	if _, _, _, _, ok := pgTarget(); !ok {
		t.Skip("SLUICE_PG is not set")
	}

	keys := thousandKeys()
	want := map[int64]int64{}

	conn := pgxConn(t)
	rows, err := conn.Query(t.Context(), anySQL, keys)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, total int64
		if err := rows.Scan(&id, &total); err != nil {
			t.Fatal(err)
		}
		want[id] = total
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 1000 {
		t.Fatalf("pgx read %d rows, want 1000 — is the fixture loaded?", len(want))
	}

	sc := sluiceConn(t)
	got := map[int64]int64{}
	src := sc.Query(t.Context(), anySQL, [][]byte{postgres.AppendInt8Array(nil, keys)},
		postgres.QueryConfig{BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8Array}})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, r := range b.Items {
			idBytes, _ := r.Value(0)
			totalBytes, _ := r.Value(1)
			id, _ := postgres.DecodeInt8(idBytes)
			total, _ := postgres.DecodeInt8(totalBytes)
			got[id] = total
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != len(want) {
		t.Fatalf("sluice read %d rows, pgx read %d", len(got), len(want))
	}
	for id, total := range want {
		if got[id] != total {
			t.Errorf("id %d: sluice %d, pgx %d", id, got[id], total)
		}
	}
}

// reportPercentiles reports p50, p90, p99 and p99.9 of ds in nanoseconds,
// under the names the interop benchmarks use. A percentile is the nearest
// rank: below 1,000 samples, p99.9 is the slowest query.
func reportPercentiles(b *testing.B, name string, ds []time.Duration) {
	b.Helper()
	slices.Sort(ds)
	for _, q := range []struct {
		label string
		per   int // per thousand
	}{{"p50", 500}, {"p90", 900}, {"p99", 990}, {"p99.9", 999}} {
		i := (len(ds)*q.per + 999) / 1000 // ceil(len × q), a rank from 1
		b.ReportMetric(float64(ds[max(i, 1)-1]), name+"-"+q.label+"-ns")
	}
}
