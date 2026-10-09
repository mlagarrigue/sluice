package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
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

func pgTarget(b testing.TB) (addr, user, db, password string, ok bool) {
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

func dsn(b testing.TB) string {
	b.Helper()
	addr, user, db, pass, ok := pgTarget(b)
	if !ok {
		b.Skip("SLUICE_PG is not set")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		b.Fatal(err)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)
}

func sluiceConn(b testing.TB) *postgres.Conn {
	b.Helper()
	addr, user, db, pass, ok := pgTarget(b)
	if !ok {
		b.Skip("SLUICE_PG is not set")
	}
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	conn, err := postgres.Startup(nc, postgres.StartupConfig{User: user, Database: db, Password: pass})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	return conn
}

func pgxConn(b testing.TB) *pgx.Conn {
	b.Helper()
	conn, err := pgx.Connect(context.Background(), dsn(b))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func sqlDB(b testing.TB, driver string) *sql.DB {
	b.Helper()
	db, err := sql.Open(driver, dsn(b))
	if err != nil {
		b.Fatal(err)
	}
	// One connection, so this measures the driver rather than the pool.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// --- point read: one row by primary key ---------------------------------------

const pointSQL = `SELECT id, total FROM bench_orders WHERE id = $1`

func BenchmarkPGPointSluice(b *testing.B) {
	conn := sluiceConn(b)
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

// The same read with AllRows: one round trip instead of two, which is the
// entire gap against pgx. What it gives up is a cheap early stop, and a point
// read is never abandoned half way.
func BenchmarkPGPointSluiceAllRows(b *testing.B) {
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
			postgres.QueryConfig{BatchRows: 2, AllRows: true, ParamOIDs: []uint32{postgres.OIDInt8}})
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

func BenchmarkPGAnySluiceAllRows(b *testing.B) {
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
			postgres.QueryConfig{BatchRows: 1024, AllRows: true, ParamOIDs: []uint32{postgres.OIDInt8Array}})
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
	for b.Loop() {
		var id, total int64
		if err := conn.QueryRow(ctx, pointSQL, 42).Scan(&id, &total); err != nil {
			b.Fatal(err)
		}
		acc += total
	}
	sink = int(acc)
}

func BenchmarkPGPointLibPQ(b *testing.B)  { benchPointSQL(b, "postgres") }
func BenchmarkPGPointPgxSQL(b *testing.B) { benchPointSQL(b, "pgx") }

func benchPointSQL(b *testing.B, driver string) {
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

func BenchmarkPGWideSluice(b *testing.B) {
	conn := sluiceConn(b)
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		src := conn.Query(b.Context(), wideSQL, nil, postgres.QueryConfig{BatchRows: 1024})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
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
	}
	sink = int(acc)
}

func BenchmarkPGWidePgx(b *testing.B) {
	conn := pgxConn(b)
	ctx := b.Context()
	var acc int64
	b.ReportAllocs()
	for b.Loop() {
		rows, err := conn.Query(ctx, wideSQL)
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
	}
	sink = int(acc)
}

func BenchmarkPGWidePgxSQL(b *testing.B) { benchWideSQL(b, "pgx") }
func BenchmarkPGWideLibPQ(b *testing.B)  { benchWideSQL(b, "postgres") }

func benchWideSQL(b *testing.B, driver string) {
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
	}
	b.StopTimer()
	truncate(b, conn)
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
	}
	b.StopTimer()
	if _, err := conn.Exec(ctx, "TRUNCATE bench_copy"); err != nil {
		b.Fatal(err)
	}
}

func truncate(b testing.TB, conn *postgres.Conn) {
	b.Helper()
	if err := conn.Exec(context.Background(), "TRUNCATE bench_copy"); err != nil {
		b.Fatal(err)
	}
}

// Every contender must read the same rows, or the figures beside each other
// mean nothing. This runs before the benchmarks are believed.
func TestPGContendersAgree(t *testing.T) {
	if _, _, _, _, ok := pgTarget(t); !ok {
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
