package postgres_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// ParamOf is the Param literal: the codec's OID beside the codec's bytes.
func TestArg(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  postgres.Param
		want postgres.Param
	}{
		{"int8", postgres.ParamOf(pgtype.Int8Codec(), int64(-7)), postgres.Param{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, -7)}},
		{"text", postgres.ParamOf(pgtype.TextCodec(), "é"), postgres.Param{OID: pgtype.OIDText, Value: []byte("é")}},
		{"bool", postgres.ParamOf(pgtype.BoolCodec(), true), postgres.Param{OID: pgtype.OIDBool, Value: pgtype.AppendBool(nil, true)}},
	} {
		if tc.got.OID != tc.want.OID || !bytes.Equal(tc.got.Value, tc.want.Value) {
			t.Errorf("%s: %d %x, want %d %x", tc.name, tc.got.OID, tc.got.Value, tc.want.OID, tc.want.Value)
		}
	}
}

// ArrayParamOf is a one-dimensional array the codec reads back as it was, an
// empty one included, under the array type's OID.
func TestArrayArg(t *testing.T) {
	for _, ids := range [][]int64{{3, 1, 2}, {}, nil} {
		p := postgres.ArrayParamOf(pgtype.Int8Codec(), ids)
		if p.OID != pgtype.OIDInt8Array || p.Value == nil {
			t.Fatalf("%v: OID %d, Value %x; want int8[] and an array, never NULL", ids, p.OID, p.Value)
		}
		back, err := pgtype.DecodeArray(pgtype.Array[int64]{}, p.Value, pgtype.Int8Codec())
		if err != nil {
			t.Fatalf("%v: %v", ids, err)
		}
		if !slices.Equal(back.Elems, ids) || len(back.Elems) != len(ids) {
			t.Errorf("%v came back as %v", ids, back.Elems)
		}
	}
}

func TestArrayArgOfATypeWithoutArray(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a codec without an array type was accepted")
		}
	}()
	c := pgtype.Int8Codec()
	c.ArrayOID = 0
	postgres.ArrayParamOf(c, []int64{1})
}

// The parameters of a query that reads a page of keys above a bound.
func ExampleParamOf() {
	page := []postgres.Param{
		postgres.ParamOf(pgtype.Int8Codec(), int64(1000)), // id > $1
		postgres.ParamOf(pgtype.Int4Codec(), int32(50)),   // LIMIT $2
	}
	fmt.Println(page[0].OID == pgtype.OIDInt8, page[1].OID == pgtype.OIDInt4)
	// Output: true true
}

// The one parameter of a query that reads a batch of keys at once.
func ExampleArrayParamOf() {
	batch := []postgres.Param{postgres.ArrayParamOf(pgtype.Int8Codec(), []int64{7, 8, 9})} // id = ANY($1)
	fmt.Println(batch[0].OID == pgtype.OIDInt8Array)
	// Output: true
}
