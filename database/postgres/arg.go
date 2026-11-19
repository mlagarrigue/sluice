package postgres

import "github.com/mlagarrigue/sluice/database/postgres/pgtype"

// ParamOf is one parameter in codec c's binary encoding, with the type's OID:
//
//	params := []postgres.Param{
//	    postgres.ParamOf(pgtype.Int8Codec(), from),
//	    postgres.ParamOf(pgtype.Int4Codec(), limit),
//	}
//
// It is the [Param] literal of every type at once — `{OID: pgtype.OIDInt8,
// Value: pgtype.AppendInt8(nil, from)}` — so the OID and the encoding come
// from one place and cannot disagree. A NULL has no value to encode: it is a
// Param with a nil Value.
func ParamOf[T any](c pgtype.Codec[T], v T) Param {
	return Param{OID: c.OID, Value: c.Append(nil, v)}
}

// ArrayParamOf is a one-dimensional array of vs as one parameter, with the
// array type's OID — what `= ANY($1)` takes to match a batch of keys in one
// statement:
//
//	params := []postgres.Param{postgres.ArrayParamOf(pgtype.Int8Codec(), ids)}
//
// An empty vs is the empty array, which matches nothing, rather than NULL. A
// codec whose type has no array type (ArrayOID zero) panics: there is no
// parameter to build, and an OID left to the server would hide that. An
// array of more dimensions, or with NULL elements, is
// [pgtype.AppendArray]'s, in a Param of its own.
func ArrayParamOf[T any](c pgtype.Codec[T], vs []T) Param {
	if c.ArrayOID == 0 {
		panic("postgres: ArrayParamOf of a type that has no array type")
	}
	return Param{OID: c.ArrayOID, Value: pgtype.AppendArray(nil, pgtype.Array[T]{Elems: vs}, c)}
}
