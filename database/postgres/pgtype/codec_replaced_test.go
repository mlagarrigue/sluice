package pgtype_test

import (
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// A Codec that changes how a built-in type reads or writes — to validate,
// convert or intern — is a NewCodec over the built-in Codec's methods, and
// an array calls its functions for every element. A Codec's functions
// cannot be reassigned: a copy of Int4Codec() whose Decode was replaced
// kept int4's own element loop, and the caller's function was never called.
func TestACodecOverABuiltInOneIsCalledPerElement(t *testing.T) {
	int4 := pgtype.Int4Codec()
	oids := pgtype.TypeOIDs{OID: int4.OID, ArrayOID: int4.ArrayOID}
	wire := pgtype.AppendArray(nil, pgtype.Array[int32]{Elems: []int32{1, 2, 3}}, int4)

	decoded := 0
	negDecode := pgtype.NewCodec(oids, int4.Append, func(b []byte) (int32, error) {
		decoded++
		v, err := int4.Decode(b)
		return -v, err
	})
	got, err := pgtype.DecodeArray(pgtype.Array[int32]{}, wire, negDecode)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != 3 || !slices.Equal(got.Elems, []int32{-1, -2, -3}) {
		t.Errorf("the Codec's Decode ran %d times for three elements; decoded %v, want [-1 -2 -3]", decoded, got.Elems)
	}

	appended := 0
	negAppend := pgtype.NewCodec(oids, func(dst []byte, v int32) []byte {
		appended++
		return int4.Append(dst, -v)
	}, int4.Decode)
	for _, arr := range []pgtype.Array[int32]{
		{Elems: []int32{1, 2, 3}},
		{Elems: []int32{1, 0, 3}, Nulls: []bool{false, true, false}},
	} {
		appended = 0
		back, err := pgtype.DecodeArray(pgtype.Array[int32]{}, pgtype.AppendArray(nil, arr, negAppend), int4)
		if err != nil {
			t.Fatal(err)
		}
		want := 3 - countTrue(arr.Nulls)
		if appended != want || back.Elems[0] != -1 || back.Elems[2] != -3 {
			t.Errorf("nulls %v: the Codec's Append ran %d times, want %d; wrote %v", arr.Nulls, appended, want, back.Elems)
		}
	}

	// The control: the built-in Codec reads its own values.
	same, err := pgtype.DecodeArray(pgtype.Array[int32]{}, wire, int4)
	if err != nil || !slices.Equal(same.Elems, []int32{1, 2, 3}) {
		t.Errorf("the built-in codec: %v, %v", same.Elems, err)
	}
}

func countTrue(bs []bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}
