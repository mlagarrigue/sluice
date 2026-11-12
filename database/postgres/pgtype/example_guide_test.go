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
func ExampleAppendArray() {
	b := pgtype.AppendArray(nil, pgtype.Array[int64]{Elems: []int64{1, 2, 3}}, pgtype.Int8Codec())
	arr, err := pgtype.DecodeArray(pgtype.Array[int64]{}, b, pgtype.Int8Codec())
	fmt.Println(len(b), len(arr.Elems), arr.Elems[2], err)
	// Output: 56 3 3 <nil>
}

// A batch of keys as one int8[] parameter, the form `= ANY($1)` takes: the
// codec of bigint is what AppendArray writes each element with.
func ExampleInt8Codec() {
	keys := []int64{7, 11, 13}
	b := pgtype.AppendArray(nil, pgtype.Array[int64]{Elems: keys}, pgtype.Int8Codec())
	fmt.Println(pgtype.Int8Codec().ArrayOID == pgtype.OIDInt8Array, len(b))
	// Output: true 56
}

// Any array of any type the package encodes, through its codec: the values
// in a plain slice, and a NULL marked in Nulls rather than read as a zero.
func ExampleDecodeArray() {
	in := pgtype.Array[int32]{Elems: []int32{4, 0, 6}, Nulls: []bool{false, true, false}}
	b := pgtype.AppendArray(nil, in, pgtype.Int4Codec())
	out, err := pgtype.DecodeArray(pgtype.Array[int32]{}, b, pgtype.Int4Codec())
	if err != nil {
		fmt.Println(err)
		return
	}
	for i, v := range out.Elems {
		fmt.Println(v, out.Nulls[i])
	}
	// Output:
	// 4 false
	// 0 true
	// 6 false
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
