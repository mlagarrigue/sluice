package pgfake_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/internal/pgfake"
)

// A simple Query may carry several statements: each runs and answers in
// turn, and the string gets one ReadyForQuery — a semicolon inside a string
// literal splits nothing.
func TestQueryRunsEveryStatement(t *testing.T) {
	b := &pgfake.Backend{Exec: rows(1)}
	conn := b.Conn(t)
	if err := conn.Exec(context.Background(), "UPDATE a SET x = 1; UPDATE b SET y = ';'; DELETE FROM c"); err != nil {
		t.Fatalf("Exec of three statements: %v", err)
	}
	got := b.Statements()
	want := []string{"UPDATE a SET x = 1", "UPDATE b SET y = ';'", "DELETE FROM c"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("statements run = %q, want %q", got, want)
	}
}

// Through Connect the fake answers the startup packet like a trusting
// server: the session parameters are there to read, and a query runs.
func TestDialAnswersTheStartup(t *testing.T) {
	b := &pgfake.Backend{Exec: rows(2)}
	conn, err := postgres.Connect(context.Background(), b.Dial(t), postgres.Config{User: "orders"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if v := conn.Parameter("server_version"); v == "" {
		t.Error("no server_version after the startup")
	}
	src := conn.Query(context.Background(), "SELECT n", nil, postgres.QueryConfig{})
	n := 0
	src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
		n += len(batch.Items)
		return true
	})
	if err := src.Err(); err != nil || n != 2 {
		t.Errorf("%d rows, err %v; want 2 and none", n, err)
	}
}
