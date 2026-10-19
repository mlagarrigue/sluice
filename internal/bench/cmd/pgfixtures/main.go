// Command pgfixtures loads the tables the PostgreSQL benchmarks read —
// bench_orders, bench_wide and bench_copy — into the server SLUICE_PG names.
//
//	go run ./internal/bench/cmd/pgfixtures
//
// It reads the same variables as the benchmarks (SLUICE_PG, SLUICE_PG_USER,
// SLUICE_PG_DB, SLUICE_PG_PASSWORD) and talks to the server through this
// repository's connector, so loading needs neither psql nor initdb: the
// devcontainer has neither, and the CI job runs the same program. The tables
// are dropped and recreated on every run.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// fixtures is the one copy of the benchmark tables. Separate from the
// integration suite's own setup, because loading 120 000 rows is not something
// a unit-test run should pay for.
//
// bench_copy is UNLOGGED: the COPY benchmarks compare clients, and the write
// of every row to the WAL is server work common to both. Logged, a few seconds
// of COPY fill the devcontainer's 512 MB tmpfs with WAL before a checkpoint
// recycles it, and the server stops.
const fixtures = `
DROP TABLE IF EXISTS bench_orders, bench_wide, bench_copy;
CREATE TABLE bench_orders (id bigint PRIMARY KEY, total bigint NOT NULL,
  label text NOT NULL, created timestamptz NOT NULL DEFAULT now());
INSERT INTO bench_orders (id, total, label)
  SELECT g, g * 7, 'order-' || g FROM generate_series(1, 100000) g;
CREATE TABLE bench_wide (id bigint PRIMARY KEY,
  c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint,
  t1 text, t2 text, t3 text, t4 text, t5 text);
INSERT INTO bench_wide SELECT g, g, g+1, g+2, g+3, g+4,
  'a'||g, 'b'||g, 'c'||g, 'd'||g, 'e'||g FROM generate_series(1, 20000) g;
CREATE UNLOGGED TABLE bench_copy (id bigint, label text);
ANALYZE bench_orders; ANALYZE bench_wide;
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pgfixtures:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		return errors.New("SLUICE_PG is not set (host:port of the server)")
	}
	// The same defaults as the benchmarks' pgTarget, so the tables land where
	// the benchmarks look for them.
	user, db := os.Getenv("SLUICE_PG_USER"), os.Getenv("SLUICE_PG_DB")
	if user == "" {
		user = "postgres"
	}
	if db == "" {
		db = user
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), postgres.Config{
		User:     user,
		Database: db,
		Password: os.Getenv("SLUICE_PG_PASSWORD"),
	})
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Exec(ctx, fixtures); err != nil {
		return fmt.Errorf("loading the tables: %w", err)
	}
	fmt.Printf("pgfixtures: bench_orders, bench_wide, bench_copy loaded into %s/%s\n", addr, db)
	return nil
}
