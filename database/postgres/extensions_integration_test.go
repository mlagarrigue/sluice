package postgres_test

import (
	"errors"
	"os"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// lookup resolves a type the suite needs, failing the test when the database
// lacks it: init.sql creates hstore, citext and ltree in every database the
// suite runs against.
func lookup(t *testing.T, conn *postgres.Conn, name string) (oid, arrayOID uint32) {
	t.Helper()
	oid, arrayOID, err := conn.LookupType(t.Context(), name)
	if err != nil {
		t.Fatalf("LookupType(%q): %v — run .devcontainer/postgres/init.sql against the database", name, err)
	}
	return oid, arrayOID
}

func TestIntegrationLookupType(t *testing.T) {
	conn := dial(t)
	for _, tc := range []struct {
		name          string
		oid, arrayOID uint32
	}{
		{"int4", pgtype.OIDInt4, pgtype.OIDInt4Array},
		{"pg_catalog.text", pgtype.OIDText, pgtype.OIDTextArray},
		{"double precision", pgtype.OIDFloat8, pgtype.OIDFloat8Array},
		{"int4[]", pgtype.OIDInt4Array, 0}, // an array type has no array of its own
	} {
		oid, arrayOID, err := conn.LookupType(t.Context(), tc.name)
		if err != nil || oid != tc.oid || arrayOID != tc.arrayOID {
			t.Errorf("LookupType(%q) = %d, %d, %v; want %d, %d", tc.name, oid, arrayOID, err, tc.oid, tc.arrayOID)
		}
	}
	// An extension's type has an OID past the built-in range, which is the
	// whole reason it is looked up.
	if oid, arrayOID := lookup(t, conn, "hstore"); oid < 16384 || arrayOID < 16384 {
		t.Errorf("hstore = %d, %d; want OIDs a database assigned", oid, arrayOID)
	}
	if _, _, err := conn.LookupType(t.Context(), "sluice_no_such_type"); !errors.Is(err, postgres.ErrUnknownType) {
		t.Errorf("an unknown type: err = %v, want ErrUnknownType", err)
	}
	// The connection is usable after a lookup that found nothing.
	if oid, _ := lookup(t, conn, "bool"); oid != pgtype.OIDBool {
		t.Errorf("bool = %d after an unknown type", oid)
	}
}

func TestIntegrationExtensionTypesMatchTheServer(t *testing.T) {
	conn := dial(t)

	citext := pgtype.CitextCodec(lookup(t, conn, "citext"))
	codecMatchesServer(t, conn, `'Hello World'::citext`, citext, "Hello World")

	hstore := pgtype.HstoreCodec(lookup(t, conn, "hstore"))
	codecMatchesServer(t, conn, `'a=>1, bb=>NULL, c=>"", "é k"=>"v w"'::hstore`, hstore, pgtype.Hstore{
		"a":   {Value: "1", Valid: true},
		"bb":  {},
		"c":   {Value: "", Valid: true},
		"é k": {Value: "v w", Valid: true},
	})
	codecMatchesServer(t, conn, `''::hstore`, hstore, pgtype.Hstore{})

	ltree := pgtype.LtreeCodec(lookup(t, conn, "ltree"))
	codecMatchesServer(t, conn, `'top.science.astronomy'::ltree`, ltree, "top.science.astronomy")
	// lquery and ltxtquery have no equality operator; their text is what the
	// server parsed, so it says whether the bound value is the one meant.
	codecMatchesServerBy(t, conn, `'*.science.*'::lquery`, `SELECT $1::text = '*.science.*'`,
		pgtype.LqueryCodec(lookup(t, conn, "lquery")), "*.science.*")
	codecMatchesServerBy(t, conn, `'science & !physics'::ltxtquery`, `SELECT $1::text = 'science & !physics'`,
		pgtype.LtxtqueryCodec(lookup(t, conn, "ltxtquery")), "science & !physics")

	// An array of an extension's type names the type's looked-up OID as its
	// element, which array_recv checks against the parameter's.
	codecMatchesServer(t, conn, `ARRAY['a.b', NULL]::ltree[]`,
		pgtype.Codec[pgtype.Array[string]]{
			OID: ltree.ArrayOID,
			Append: func(dst []byte, a pgtype.Array[string]) []byte {
				return pgtype.AppendArray(dst, a, ltree)
			},
			Decode: func(b []byte) (pgtype.Array[string], error) {
				return pgtype.DecodeArray(pgtype.Array[string]{}, b, ltree)
			},
		},
		pgtype.Array[string]{Elems: []pgtype.Null[string]{{Value: "a.b", Valid: true}, {}}})
}

// pgvector is not in the official images; one CI entry runs on pgvector's and
// sets SLUICE_PG_VECTOR, which turns its absence into a failure there.
func TestIntegrationVectorMatchesTheServer(t *testing.T) {
	conn := dial(t)
	oid, arrayOID, err := conn.LookupType(t.Context(), "vector")
	if errors.Is(err, postgres.ErrUnknownType) {
		if os.Getenv("SLUICE_PG_VECTOR") != "" {
			t.Fatal("SLUICE_PG_VECTOR is set and the database has no vector type")
		}
		t.Skip("the vector extension is not installed in this database")
	}
	if err != nil {
		t.Fatal(err)
	}
	vector := pgtype.VectorCodec(oid, arrayOID)
	codecMatchesServer(t, conn, `'[1.5, -2, 0, 3.25]'::vector`, vector, []float32{1.5, -2, 0, 3.25})
	codecMatchesServer(t, conn, `'[0.1]'::vector(1)`, vector, []float32{0.1})
}
