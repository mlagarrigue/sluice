package pgtype_test

import (
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// An int8 in binary is eight bytes, big-endian, with no length: the
// container — a row, an array — writes the length around it.
func ExampleAppendInt8() {
	fmt.Printf("%x\n", pgtype.AppendInt8(nil, 7))
	// Output: 0000000000000007
}

// DecodeInt8 reads exactly the eight bytes a value was sent as, and refuses
// any other length.
func ExampleDecodeInt8() {
	v, err := pgtype.DecodeInt8(pgtype.AppendInt8(nil, -42))
	fmt.Println(v, err)
	_, err = pgtype.DecodeInt8([]byte{1, 2, 3})
	fmt.Println(err != nil)
	// Output:
	// -42 <nil>
	// true
}

// text in binary is the string's bytes, UTF-8, unchanged.
func ExampleAppendText() {
	fmt.Printf("%x\n", pgtype.AppendText(nil, "hé"))
	// Output: 68c3a9
}

// An int8[] parameter, whole: header, dimensions, then each element with its
// length. DecodeArray reads it back with the type's codec.
func ExampleAppendInt8Array() {
	b := pgtype.AppendInt8Array(nil, []int64{1, 2, 3})
	arr, err := pgtype.DecodeArray(pgtype.Array[int64]{}, b, pgtype.Int8Codec())
	fmt.Println(len(b), len(arr.Elems), arr.Elems[2].Value, err)
	// Output: 56 3 3 <nil>
}

// Any array of any type the package encodes, through its codec: a NULL
// element comes back invalid rather than as a zero.
func ExampleDecodeArray() {
	in := pgtype.Array[int32]{Elems: []pgtype.Null[int32]{{Value: 4, Valid: true}, {}, {Value: 6, Valid: true}}}
	b := pgtype.AppendArray(nil, in, pgtype.Int4Codec())
	out, err := pgtype.DecodeArray(pgtype.Array[int32]{}, b, pgtype.Int4Codec())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, e := range out.Elems {
		fmt.Println(e.Value, e.Valid)
	}
	// Output:
	// 4 true
	// 0 false
	// 6 true
}

// The codec of int4: its OIDs, and the pair of functions the containers
// call for each element.
func ExampleInt4Codec() {
	c := pgtype.Int4Codec()
	v, err := c.Decode(c.Append(nil, 1234))
	fmt.Println(c.OID, c.ArrayOID, v, err)
	// Output: 23 1007 1234 <nil>
}

// The codec of date, here inside a daterange: the bounds go out and come
// back through it.
func ExampleDateCodec() {
	jan1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	r := pgtype.Range[time.Time]{
		Lower: pgtype.RangeBound[time.Time]{Value: jan1, Inclusive: true},
		Upper: pgtype.RangeBound[time.Time]{Value: feb1},
	}
	got, err := pgtype.DecodeRange(pgtype.AppendRange(nil, r, pgtype.DateCodec()), pgtype.DateCodec())
	fmt.Println(got.Lower.Value.Format(time.DateOnly), got.Upper.Value.Format(time.DateOnly), err)
	// Output: 2026-01-01 2026-02-01 <nil>
}

// A range with an absent bound, and the empty range, read back as sent.
func ExampleDecodeRange() {
	open := pgtype.Range[int32]{
		Lower: pgtype.RangeBound[int32]{Value: 10, Inclusive: true},
		Upper: pgtype.RangeBound[int32]{Unbounded: true},
	}
	r, err := pgtype.DecodeRange(pgtype.AppendRange(nil, open, pgtype.Int4Codec()), pgtype.Int4Codec())
	fmt.Println(r.Lower.Value, r.Lower.Inclusive, r.Upper.Unbounded, err)
	r, err = pgtype.DecodeRange(pgtype.AppendRange(nil, pgtype.Range[int32]{Empty: true}, pgtype.Int4Codec()), pgtype.Int4Codec())
	fmt.Println(r.Empty, err)
	// Output:
	// 10 true true <nil>
	// true <nil>
}

// hstore is an extension type, so its OIDs are the database's own: the
// caller reads them from pg_type once and builds the codec with them.
func ExampleHstoreCodec() {
	const hstoreOID, hstoreArrayOID = 16385, 16390 // SELECT oid, typarray FROM pg_type WHERE typname = 'hstore'
	c := pgtype.HstoreCodec(hstoreOID, hstoreArrayOID)
	h, err := c.Decode(c.Append(nil, pgtype.Hstore{"color": {Value: "blue", Valid: true}, "size": {}}))
	fmt.Println(h["color"].Value, h["size"].Valid, err)
	// Output: blue false <nil>
}
