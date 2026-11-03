package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// ErrUnknownType reports a type name the database does not resolve: no such
// type on the search path, or an extension not created in this database.
var ErrUnknownType = errors.New("postgres: the database has no type of that name")

// LookupType returns the OID of the type the database knows by name, and the
// OID of its array — zero when it has none. The name is resolved as a cast to
// it would be: through the search path, or qualified by its schema
// (`public.hstore`).
//
// It is how a type an extension created is bound and read: its OID is chosen
// by each database at CREATE EXTENSION, so it is looked up once per database
// and handed to the type's Codec.
//
//	oid, arrayOID, err := conn.LookupType(ctx, "hstore")
//	hstore := pgtype.HstoreCodec(oid, arrayOID)
//
// The OIDs stay valid until the type is dropped; an extension dropped and
// created again has new ones.
func (c *Conn) LookupType(ctx context.Context, name string) (oid, arrayOID uint32, err error) {
	src := c.Query(ctx, "SELECT t.oid, t.typarray FROM pg_catalog.pg_type t WHERE t.oid = pg_catalog.to_regtype($1)",
		[]Param{{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, name)}}, QueryConfig{BatchRows: 1})
	found := false
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, row := range b.Items {
			o, _ := row.Value(0)
			a, _ := row.Value(1)
			if oid, err = pgtype.DecodeOID(o); err == nil {
				arrayOID, err = pgtype.DecodeOID(a)
			}
			found = true
		}
		return true
	})
	switch {
	case src.Err() != nil:
		return 0, 0, src.Err()
	case err != nil:
		return 0, 0, err
	case !found:
		return 0, 0, fmt.Errorf("%w: %q", ErrUnknownType, name)
	}
	return oid, arrayOID, nil
}
