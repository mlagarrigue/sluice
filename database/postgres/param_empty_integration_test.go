package postgres_test

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// The server, not the wire bytes, decides: a parameter built by the documented
// idiom from an empty value is asked `$1 IS NULL`, and answers false — it was
// NULL, as pgtype.AppendText(nil, "") returned nil. Control: []byte{} built by
// hand is not NULL, so a red result is the Append functions, not the server.
func TestIntegrationAnEmptyValueIsNotNull(t *testing.T) {
	conn := dial(t)

	isNull := func(t *testing.T, sql string, p postgres.Param) bool {
		t.Helper()
		var got, seen bool
		src := conn.Query(t.Context(), sql, []postgres.Param{p}, postgres.QueryConfig{BatchRows: 1})
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, row := range b.Items {
				v, null := row.Value(0)
				if null {
					t.Fatal("IS NULL itself returned NULL")
				}
				d, err := pgtype.DecodeBool(v)
				if err != nil {
					t.Fatal(err)
				}
				got, seen = d, true
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatal(err)
		}
		if !seen {
			t.Fatal("no row")
		}
		return got
	}

	// Controls: a hand-built empty slice and a one-byte value are not NULL.
	if isNull(t, "SELECT $1::text IS NULL", postgres.Param{OID: pgtype.OIDText, Value: []byte{}}) {
		t.Fatal("control: []byte{} reached the server as NULL")
	}
	if isNull(t, "SELECT $1::text IS NULL", postgres.Param{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, "x")}) {
		t.Fatal("control: AppendText(nil, \"x\") reached the server as NULL")
	}

	if isNull(t, "SELECT $1::text IS NULL", postgres.Param{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, "")}) {
		t.Error("pgtype.AppendText(nil, \"\") reached the server as NULL, not as the empty string")
	}
	if isNull(t, "SELECT $1::bytea IS NULL", postgres.Param{OID: pgtype.OIDBytea, Value: pgtype.AppendBytea(nil, []byte{})}) {
		t.Error("pgtype.AppendBytea(nil, []byte{}) reached the server as NULL, not as an empty bytea")
	}
	// The filter consequence: an existing empty row is not matched.
	if isNull(t, "SELECT NOT EXISTS (SELECT 1 FROM (VALUES ('')) v(c) WHERE c = $1)", postgres.Param{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, "")}) {
		t.Error("WHERE c = $1 with AppendText(nil, \"\") matched nothing although the row '' exists")
	}
}
