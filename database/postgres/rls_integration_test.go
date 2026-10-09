package postgres_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// Row-level security against a real server, which is the only place the claim
// can be made at all.
//
// The architecture's answer to multi-tenancy is that the security boundary
// stays on the server: the pipeline sets a transaction-local setting, a policy
// reads it, and the server decides which rows exist. Written as prose that is
// a sentence anyone can type. What makes it a fact is a query with **no
// tenant predicate in it** returning one tenant's rows and not another's — so
// that is what these tests run.
//
// Three things they are careful about, each of which would otherwise produce a
// test that passes while proving nothing:
//
//   - A superuser bypasses row-level security entirely. The suite skips rather
//     than reporting success it did not earn.
//   - A table's owner bypasses its own policies unless the table is declared
//     FORCE ROW LEVEL SECURITY. The fixture declares it, because the test user
//     is usually the owner of what it just created.
//   - `SET LOCAL` reverting at the end of the transaction is not a detail: a
//     setting that outlived its transaction would be a tenant's identity
//     leaking into the next piece of work on the same connection. It is
//     asserted separately.

// rlsFixture creates a two-tenant table protected by a policy that reads
// app.tenant_id, and returns its name. It skips the test when the connection
// cannot demonstrate anything — a superuser is not subject to the policies it
// writes.
func rlsFixture(t *testing.T, conn *postgres.Conn) string {
	t.Helper()
	ctx := t.Context()

	if superuser(t, conn) {
		t.Skip("SLUICE_PG_USER is a superuser, which bypasses row-level security: this suite would pass without proving anything")
	}

	const table = "sluice_rls_orders"
	stmts := []string{
		`DROP TABLE IF EXISTS ` + table,
		`CREATE TABLE ` + table + ` (id bigint primary key, tenant text not null, total bigint not null)`,
		`INSERT INTO ` + table + ` VALUES (1,'acme',100), (2,'acme',200), (3,'globex',300)`,
		`ALTER TABLE ` + table + ` ENABLE ROW LEVEL SECURITY`,
		// Without FORCE, the owner of the table — which is whoever just
		// created it, i.e. this test — is exempt from its own policy, and
		// every assertion below would pass by seeing everything.
		`ALTER TABLE ` + table + ` FORCE ROW LEVEL SECURITY`,
		`CREATE POLICY tenant_isolation ON ` + table +
			` USING (tenant = current_setting('app.tenant_id', true))`,
	}
	for _, s := range stmts {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("fixture %q: %v", s, err)
		}
	}
	t.Cleanup(func() { _ = conn.Exec(ctx, `DROP TABLE IF EXISTS `+table) })
	return table
}

func superuser(t *testing.T, conn *postgres.Conn) bool {
	t.Helper()
	var is bool
	src := conn.Query(t.Context(), `SELECT current_setting('is_superuser')`, nil,
		postgres.QueryConfig{BatchRows: 1})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, r := range b.Items {
			v, isNull := r.Value(0)
			is = !isNull && strings.EqualFold(string(v), "on")
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("asking whether the user is a superuser: %v", err)
	}
	return is
}

func readIDs(t *testing.T, src sluice.Source[postgres.Row]) []int64 {
	t.Helper()
	var ids []int64
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, r := range b.Items {
			v, isNull := r.Value(0)
			if isNull {
				t.Error("a NULL id came back from a primary key column")
				continue
			}
			id, err := postgres.DecodeInt8(v)
			if err != nil {
				t.Errorf("decoding id: %v", err)
				continue
			}
			ids = append(ids, id)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	return ids
}

// The claim of §4, run rather than written: with the setting in place and no
// tenant predicate in the SQL, the server returns one tenant's rows.
func TestIntegrationRLSIsEnforcedByTheServer(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	for _, tc := range []struct {
		tenant string
		want   []int64
	}{
		{"acme", []int64{1, 2}},
		{"globex", []int64{3}},
		{"nobody", nil},
	} {
		t.Run(tc.tenant, func(t *testing.T) {
			tx, err := conn.Begin(ctx, postgres.TxConfig{Timeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback() //nolint:errcheck // the read is the assertion

			if err := tx.SetLocal(ctx, "app.tenant_id", tc.tenant); err != nil {
				t.Fatal(err)
			}
			// No WHERE tenant = ... anywhere in this statement. That is the
			// whole point: the filter is not the application's.
			got := readIDs(t, tx.Query(ctx,
				`SELECT id FROM `+table+` ORDER BY id`, nil,
				postgres.QueryConfig{BatchRows: 16}))

			if len(got) != len(tc.want) {
				t.Fatalf("ids = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ids = %v, want %v", got, tc.want)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// With no setting at all the policy matches nothing, which is the failure mode
// §0.3 names: the pipeline reports "not found" for rows that exist. It must be
// empty rather than everything — a policy that fails open is not a boundary.
func TestIntegrationRLSWithNoPrincipalShowsNothing(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // the read is the assertion

	got := readIDs(t, tx.Query(ctx, `SELECT id FROM `+table, nil, postgres.QueryConfig{BatchRows: 16}))
	if len(got) != 0 {
		t.Fatalf("with no principal set, the server returned %v — the policy fails open", got)
	}
}

// The invariant the whole batching design rests on: SET LOCAL is scoped to its
// transaction. A setting that survived into the next one would be one tenant's
// identity applied to another tenant's work, on the same connection, with no
// error anywhere.
func TestIntegrationSetLocalDoesNotOutliveItsTransaction(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetLocal(ctx, "app.tenant_id", "acme"); err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, tx.Query(ctx, `SELECT id FROM `+table+` ORDER BY id`, nil,
		postgres.QueryConfig{BatchRows: 16})); len(got) != 2 {
		t.Fatalf("inside the transaction the tenant saw %v, want two rows", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// A second transaction on the same connection, with no setting of its own.
	next, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Rollback() //nolint:errcheck // the read is the assertion

	if got := readIDs(t, next.Query(ctx, `SELECT id FROM `+table, nil,
		postgres.QueryConfig{BatchRows: 16})); len(got) != 0 {
		t.Fatalf("the previous transaction's tenant leaked into the next one: %v", got)
	}
}

// The injection this package's SetLocal exists to make impossible, tried
// against a server that would actually execute it.
func TestIntegrationSetLocalIsNotInjectable(t *testing.T) {
	conn := dial(t)
	table := rlsFixture(t, conn)
	ctx := t.Context()

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // the read is the assertion

	// If the value were concatenated into SQL, this would set the tenant to
	// acme and then escalate. Through set_config it is one opaque string that
	// matches no tenant at all.
	const hostile = "acme'; SET LOCAL ROLE postgres; SELECT '"
	if err := tx.SetLocal(ctx, "app.tenant_id", hostile); err != nil {
		t.Fatalf("SetLocal returned %v — a hostile value must be stored, not refused, since refusing it would only move the problem", err)
	}

	if got := readIDs(t, tx.Query(ctx, `SELECT id FROM `+table, nil,
		postgres.QueryConfig{BatchRows: 16})); len(got) != 0 {
		t.Fatalf("the crafted tenant matched %v rows", got)
	}

	// And the role must be unchanged: the injected SET LOCAL ROLE never ran.
	var role string
	src := tx.Query(ctx, `SELECT current_user`, nil, postgres.QueryConfig{BatchRows: 1})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			role = string(v)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if role == "postgres" {
		t.Fatal("the injected SET LOCAL ROLE ran: the value reached the server as SQL")
	}
}

// statement_timeout is the bound a context cannot be. Against a real server it
// is the difference between a slow query and a stalled batch.
func TestIntegrationStatementTimeoutStopsTheServer(t *testing.T) {
	conn := dial(t)
	ctx := t.Context()

	tx, err := conn.Begin(ctx, postgres.TxConfig{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // the error is the assertion

	start := time.Now()
	src := tx.Query(ctx, `SELECT pg_sleep(5)`, nil, postgres.QueryConfig{BatchRows: 1})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	err = src.Err()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a five-second sleep completed under a hundred-millisecond timeout")
	}
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("error = %v, want SQLSTATE 57014 (query_canceled)", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("the timeout took %v to fire; it was set to 100ms", elapsed)
	}
}
