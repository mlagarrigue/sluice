package postgres_test

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// ScanText reads a type the database created when its binary form is its
// label, as an enum's is, and refuses one whose binary form is not a string:
// a composite or an array of an enum came back as the wire's bytes taken for
// a string, with no error. Every type here lives in pg_temp and goes with
// the session.
func TestScanTextRefusesACompositeColumn(t *testing.T) {
	addr, cfg := pgTarget(t)
	conn := dialWith(t, addr, cfg)
	for _, ddl := range []string{
		"CREATE TYPE pg_temp.scantext_pt AS (a int4, b int4)",
		"CREATE TYPE pg_temp.scantext_mood AS ENUM ('calm', 'busy')",
	} {
		if err := conn.Exec(t.Context(), ddl); err != nil {
			t.Fatal(err)
		}
	}
	scan := func(sql string) ([]string, error) {
		t.Helper()
		var got []string
		var scanErr error
		src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{BatchRows: 4})
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			got, scanErr = postgres.ScanText(got, b.Items[0].Block(), 0)
			return scanErr == nil
		})
		if err := src.Err(); err != nil {
			t.Fatal(err)
		}
		return got, scanErr
	}
	if got, err := scan("SELECT 'busy'::pg_temp.scantext_mood"); err != nil || len(got) != 1 || got[0] != "busy" {
		t.Errorf("an enum: %q, %v; want its label", got, err)
	}
	for _, sql := range []string{
		"SELECT ROW(1,2)::pg_temp.scantext_pt",
		"SELECT ARRAY['calm']::pg_temp.scantext_mood[]",
	} {
		if got, err := scan(sql); !errors.Is(err, pgtype.ErrCodec) {
			t.Errorf("%s: %q, %v; want pgtype.ErrCodec", sql, got, err)
		}
	}
}
