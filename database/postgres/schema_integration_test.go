package postgres_test

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// testSchema creates a schema of this run's own and drops it, with what it
// holds, when the test ends: two `go test` processes against one server no
// longer share a table by name and drop it under each other. The drop runs
// on a context of its own — t.Context is cancelled before cleanups run, and
// the fixtures' drops used to fail on it and leave their tables behind.
// Registered after the connection's own close, it runs before it.
func testSchema(t *testing.T, conn *postgres.Conn) string {
	t.Helper()
	name := "sluice_t_" + strings.ToLower(rand.Text())
	if err := conn.Exec(t.Context(), `CREATE SCHEMA `+name); err != nil {
		t.Fatalf("creating schema %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		if err := conn.Exec(ctx, `DROP SCHEMA `+name+` CASCADE`); err != nil {
			t.Logf("schema %s left behind: %v", name, err)
		}
	})
	return name
}
