package csvcopy_test

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/csvcopy"
)

// dial connects to the server SLUICE_PG names and gives the test a table of
// its own, in a schema dropped afterwards, so two runs against one server do
// not see each other's rows.
func dial(t *testing.T) *postgres.Conn {
	t.Helper()
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		t.Skip("SLUICE_PG is not set: no server to load into")
	}
	user := os.Getenv("SLUICE_PG_USER")
	if user == "" {
		user = "postgres"
	}
	db := os.Getenv("SLUICE_PG_DB")
	if db == "" {
		db = user
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), postgres.Config{
		User: user, Database: db, Password: os.Getenv("SLUICE_PG_PASSWORD"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	schema := "sluice_t_" + strings.ToLower(rand.Text())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		if err := conn.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
			t.Logf("schema %s left behind: %v", schema, err)
		}
	})
	for _, s := range []string{
		`CREATE SCHEMA ` + schema,
		`SET search_path TO ` + schema,
		`CREATE TABLE orders (id bigint PRIMARY KEY, customer text NOT NULL, total_cents bigint NOT NULL)`,
	} {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return conn
}

func count(t *testing.T, conn *postgres.Conn) (rows, cents int64) {
	t.Helper()
	src := conn.Query(t.Context(), "SELECT count(*)::text, coalesce(sum(total_cents), 0)::text FROM orders", nil, postgres.QueryConfig{})
	for b := range src.Stream() {
		for _, r := range b.Items {
			a, _ := r.Value(0)
			c, _ := r.Value(1)
			rows, _ = strconv.ParseInt(string(a), 10, 64)
			cents, _ = strconv.ParseInt(string(c), 10, 64)
		}
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	return rows, cents
}

func file(n int, bad string) string {
	var b strings.Builder
	b.WriteString("id,customer,total\n")
	for i := 1; i <= n; i++ {
		if i == n/2 && bad != "" {
			b.WriteString(bad)
			continue
		}
		fmt.Fprintf(&b, "%d,customer %d,%d\n", i, i%97, i*10)
	}
	return b.String()
}

func TestLoadCopiesTheFile(t *testing.T) {
	conn := dial(t)
	const n = 5000 // several batches of 1024
	got, err := csvcopy.Load(t.Context(), conn, strings.NewReader(file(n, "")))
	if err != nil {
		t.Fatal(err)
	}
	rows, cents := count(t, conn)
	if got != n || rows != n || cents != 10*n*(n+1)/2 {
		t.Fatalf("Load reported %d rows; the table holds %d rows, %d cents; want %d rows, %d cents", got, rows, cents, n, 10*n*(n+1)/2)
	}
}

// A record that cannot be read halfway through the file aborts the copy:
// the batches already sent are rolled back with it, and the error is the
// reader's, with its line.
func TestLoadLeavesNothingOnABadRecord(t *testing.T) {
	conn := dial(t)
	for name, tc := range map[string]struct {
		bad  string
		want func(error) bool
	}{
		"unreadable": {"1,\"open quote,3\n", func(err error) bool {
			var pe *csv.ParseError
			return errors.As(err, &pe) && pe.Line > 2000
		}},
		"not a number": {"7,someone,twelve\n", func(err error) bool {
			return errors.Is(err, strconv.ErrSyntax) && strings.Contains(err.Error(), "total")
		}},
	} {
		_, err := csvcopy.Load(t.Context(), conn, strings.NewReader(file(5000, tc.bad)))
		if err == nil || !tc.want(err) {
			t.Fatalf("%s: err %v", name, err)
		}
		if rows, _ := count(t, conn); rows != 0 {
			t.Fatalf("%s: %d rows left in the table after the abort", name, rows)
		}
	}
}
