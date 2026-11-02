package postgres_test

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// The generic containers settled against the server three ways: the bytes
// this package writes are the ones the type's send function writes (the
// oracle of TestIntegrationEveryCodecMatchesTheServersEncoding), what the
// server sends decodes to the value the literal names, and a bound parameter
// is the value the server's own equality says it is.
func TestIntegrationContainersMatchTheServer(t *testing.T) {
	conn := dial(t)
	if err := exec(t, conn, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}

	// check runs the three checks for one value: expr is the SQL that builds
	// it, oid the parameter's type, mine this package's encoding, and decode
	// says whether this package reads the server's bytes as the value meant.
	check := func(t *testing.T, expr, cast string, oid uint32, mine []byte, decode func([]byte) bool) {
		t.Helper()
		theirs := serverEncoding(t, conn, expr)
		if !bytes.Equal(mine, theirs) {
			t.Errorf("this package encodes %x\n          the server %x", mine, theirs)
		}
		if !decode(theirs) {
			t.Errorf("the server's bytes for %s decode to something else", expr)
		}
		if !serverAgrees(t, conn, "SELECT $1::"+cast+" = "+expr, oid, mine) {
			t.Errorf("bound as a parameter, the value is not %s", expr)
		}
	}

	t.Run("int4[] of 2x3 with a NULL", func(t *testing.T) {
		a := pgtype.Array[int32]{
			Dims:  []pgtype.ArrayDim{{Len: 2, Lower: 1}, {Len: 3, Lower: 1}},
			Elems: []pgtype.Null[int32]{{Value: 1, Valid: true}, {Value: 2, Valid: true}, {Value: 3, Valid: true}, {Value: 4, Valid: true}, {}, {Value: 6, Valid: true}},
		}
		check(t, `'{{1,2,3},{4,NULL,6}}'::int4[]`, "int4[]", pgtype.OIDInt4Array,
			pgtype.AppendArray(nil, a, pgtype.Int4Codec()), func(b []byte) bool {
				got, err := pgtype.DecodeArray(pgtype.Array[int32]{}, b, pgtype.Int4Codec())
				return err == nil && slices.Equal(got.Dims, a.Dims) && slices.Equal(got.Elems, a.Elems)
			})
	})

	t.Run("text[] numbered from 0 and from -1", func(t *testing.T) {
		a := pgtype.Array[string]{
			Dims:  []pgtype.ArrayDim{{Len: 2, Lower: 0}, {Len: 1, Lower: -1}},
			Elems: []pgtype.Null[string]{{Value: "a", Valid: true}, {Value: "", Valid: true}},
		}
		check(t, `'[0:1][-1:-1]={{a},{""}}'::text[]`, "text[]", pgtype.OIDTextArray,
			pgtype.AppendArray(nil, a, pgtype.TextCodec()), func(b []byte) bool {
				got, err := pgtype.DecodeArray(pgtype.Array[string]{}, b, pgtype.TextCodec())
				return err == nil && slices.Equal(got.Dims, a.Dims) && slices.Equal(got.Elems, a.Elems)
			})
	})

	t.Run("int4range", func(t *testing.T) {
		// Written [1,5]; the server's canonical form, which is what it sends.
		r := pgtype.Range[int32]{
			Lower: pgtype.RangeBound[int32]{Value: 1, Inclusive: true},
			Upper: pgtype.RangeBound[int32]{Value: 6},
		}
		check(t, `int4range(1, 5, '[]')`, "int4range", pgtype.OIDInt4Range,
			pgtype.AppendRange(nil, r, pgtype.Int4Codec()), func(b []byte) bool {
				got, err := pgtype.DecodeRange(b, pgtype.Int4Codec())
				return err == nil && got == r
			})
	})

	t.Run("tstzrange unbounded above", func(t *testing.T) {
		r := pgtype.Range[time.Time]{
			Lower: pgtype.RangeBound[time.Time]{Value: time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)},
			Upper: pgtype.RangeBound[time.Time]{Unbounded: true},
		}
		check(t, `tstzrange('2024-03-01 12:00+00', NULL, '()')`, "tstzrange", pgtype.OIDTSTZRange,
			pgtype.AppendRange(nil, r, pgtype.TimestampTZCodec()), func(b []byte) bool {
				got, err := pgtype.DecodeRange(b, pgtype.TimestampTZCodec())
				return err == nil && got.Lower.Value.Equal(r.Lower.Value) && !got.Lower.Inclusive &&
					!got.Lower.Unbounded && got.Upper.Unbounded
			})
	})

	t.Run("empty numrange", func(t *testing.T) {
		r := pgtype.Range[pgtype.Numeric]{Empty: true}
		check(t, `'empty'::numrange`, "numrange", pgtype.OIDNumRange,
			pgtype.AppendRange(nil, r, pgtype.NumericCodec()), func(b []byte) bool {
				got, err := pgtype.DecodeRange(b, pgtype.NumericCodec())
				return err == nil && got.Empty
			})
	})

	t.Run("int8multirange", func(t *testing.T) {
		m := pgtype.Multirange[int64]{
			{Lower: pgtype.RangeBound[int64]{Value: 1, Inclusive: true}, Upper: pgtype.RangeBound[int64]{Value: 3}},
			{Lower: pgtype.RangeBound[int64]{Value: 5, Inclusive: true}, Upper: pgtype.RangeBound[int64]{Unbounded: true}},
		}
		check(t, `'{[1,3), [5,)}'::int8multirange`, "int8multirange", pgtype.OIDInt8Multirange,
			pgtype.AppendMultirange(nil, m, pgtype.Int8Codec()), func(b []byte) bool {
				got, err := pgtype.DecodeMultirange(nil, b, pgtype.Int8Codec())
				return err == nil && slices.Equal(got, m)
			})
	})

	// Every built-in range and multirange through its named Codec, bound by
	// the OID the Codec carries, and an array of ranges, whose element OID is
	// the range's.
	day := func(d int) time.Time { return time.Date(2024, 3, d, 0, 0, 0, 0, time.UTC) }
	num := func(s string) pgtype.Numeric { return mustParseNumeric(s) }
	closedOpen := func(lo, hi time.Time) pgtype.Range[time.Time] {
		return pgtype.Range[time.Time]{
			Lower: pgtype.RangeBound[time.Time]{Value: lo, Inclusive: true},
			Upper: pgtype.RangeBound[time.Time]{Value: hi},
		}
	}
	codecMatchesServer(t, conn, `int8range(-3, 9)`, pgtype.Int8RangeCodec(), pgtype.Range[int64]{
		Lower: pgtype.RangeBound[int64]{Value: -3, Inclusive: true}, Upper: pgtype.RangeBound[int64]{Value: 9},
	})
	codecMatchesServer(t, conn, `numrange(0.5, 2, '(]')`, pgtype.NumRangeCodec(), pgtype.Range[pgtype.Numeric]{
		Lower: pgtype.RangeBound[pgtype.Numeric]{Value: num("0.5")},
		Upper: pgtype.RangeBound[pgtype.Numeric]{Value: num("2"), Inclusive: true},
	})
	codecMatchesServer(t, conn, `tsrange('2024-03-01', '2024-03-02')`, pgtype.TSRangeCodec(), closedOpen(day(1), day(2)))
	codecMatchesServer(t, conn, `daterange('2024-03-01', '2024-03-03', '[]')`, pgtype.DateRangeCodec(), closedOpen(day(1), day(4)))
	codecMatchesServer(t, conn, `'{[1,3), [7,9)}'::int4multirange`, pgtype.Int4MultirangeCodec(), pgtype.Multirange[int32]{
		{Lower: pgtype.RangeBound[int32]{Value: 1, Inclusive: true}, Upper: pgtype.RangeBound[int32]{Value: 3}},
		{Lower: pgtype.RangeBound[int32]{Value: 7, Inclusive: true}, Upper: pgtype.RangeBound[int32]{Value: 9}},
	})
	codecMatchesServer(t, conn, `'{}'::nummultirange`, pgtype.NumMultirangeCodec(), pgtype.Multirange[pgtype.Numeric]{})
	codecMatchesServer(t, conn, `'{[2024-03-01,)}'::tsmultirange`, pgtype.TSMultirangeCodec(), pgtype.Multirange[time.Time]{{
		Lower: pgtype.RangeBound[time.Time]{Value: day(1), Inclusive: true}, Upper: pgtype.RangeBound[time.Time]{Unbounded: true},
	}})
	codecMatchesServer(t, conn, `'{[2024-03-01 00:00+00,2024-03-02 00:00+00)}'::tstzmultirange`, pgtype.TSTZMultirangeCodec(),
		pgtype.Multirange[time.Time]{closedOpen(day(1), day(2))})
	codecMatchesServer(t, conn, `'{[2024-03-01,2024-03-05), [2024-03-03,2024-03-07)}'::datemultirange`, pgtype.DateMultirangeCodec(),
		pgtype.Multirange[time.Time]{closedOpen(day(1), day(7))}) // merged by the server
	codecMatchesServer(t, conn, `ARRAY['[1,6)', NULL, 'empty']::int4range[]`,
		pgtype.Codec[pgtype.Array[pgtype.Range[int32]]]{
			OID: pgtype.OIDInt4RangeArray,
			Append: func(dst []byte, a pgtype.Array[pgtype.Range[int32]]) []byte {
				return pgtype.AppendArray(dst, a, pgtype.Int4RangeCodec())
			},
			Decode: func(b []byte) (pgtype.Array[pgtype.Range[int32]], error) {
				return pgtype.DecodeArray(pgtype.Array[pgtype.Range[int32]]{}, b, pgtype.Int4RangeCodec())
			},
		},
		pgtype.Array[pgtype.Range[int32]]{Elems: []pgtype.Null[pgtype.Range[int32]]{
			{Value: pgtype.Range[int32]{
				Lower: pgtype.RangeBound[int32]{Value: 1, Inclusive: true}, Upper: pgtype.RangeBound[int32]{Value: 6},
			}, Valid: true},
			{},
			{Value: pgtype.Range[int32]{Empty: true}, Valid: true},
		}})

	t.Run("record", func(t *testing.T) {
		// record_recv refuses the anonymous record, so the bound parameter is
		// of a named composite type, inferred from the cast in the query.
		schema := testSchema(t, conn)
		if err := exec(t, conn, "CREATE TYPE "+schema+".item AS (id bigint, label text, note text, ok boolean)"); err != nil {
			t.Fatal(err)
		}
		r := pgtype.Record{
			{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 42)},
			{OID: pgtype.OIDText},
			{OID: pgtype.OIDText, Value: []byte{}},
			{OID: pgtype.OIDBool, Value: pgtype.AppendBool(nil, true)},
		}
		check(t, `ROW(42::bigint, NULL::text, ''::text, true)::`+schema+`.item`, schema+".item", 0,
			pgtype.AppendRecord(nil, r), func(b []byte) bool {
				got, err := pgtype.DecodeRecord(nil, b)
				if err != nil || len(got) != 4 {
					return false
				}
				id, err := pgtype.DecodeRecordField(got[0], pgtype.Int8Codec())
				label, err2 := pgtype.DecodeRecordField(got[1], pgtype.TextCodec())
				note, err3 := pgtype.DecodeRecordField(got[2], pgtype.TextCodec())
				return err == nil && err2 == nil && err3 == nil &&
					id == pgtype.Null[int64]{Value: 42, Valid: true} && !label.Valid && note.Valid && note.Value == ""
			})
	})
}

// The anonymous record a query builds with ROW(…) is sent as record_send
// writes any other, and decodes here although it cannot be bound.
func TestIntegrationAnonymousRecordDecodes(t *testing.T) {
	conn := dial(t)
	theirs := serverEncoding(t, conn, `ROW(1::int4, ARRAY[2,3]::int8[])`)
	rec, err := pgtype.DecodeRecord(nil, theirs)
	if err != nil || len(rec) != 2 {
		t.Fatalf("DecodeRecord = %v, %v", rec, err)
	}
	if v, err := pgtype.DecodeRecordField(rec[0], pgtype.Int4Codec()); err != nil || v.Value != 1 {
		t.Errorf("field 0 = %v, %v", v, err)
	}
	if rec[1].OID != pgtype.OIDInt8Array {
		t.Fatalf("field 1 has OID %d, want int8[]", rec[1].OID)
	}
	a, err := pgtype.DecodeArray(pgtype.Array[int64]{}, rec[1].Value, pgtype.Int8Codec())
	if err != nil || !slices.Equal(a.Elems, []pgtype.Null[int64]{{Value: 2, Valid: true}, {Value: 3, Valid: true}}) {
		t.Errorf("field 1 = %+v, %v", a, err)
	}
	if _, err := pgtype.DecodeRecordField(rec[1], pgtype.Int8Codec()); err == nil {
		t.Error("an int8[] field read as int8 was not refused")
	}
}

// codecMatchesServer settles one value of a Codec against the server: this
// package's bytes are the send function's, the server's bytes decode to a
// value that encodes back to them, and the value bound under c.OID is the
// one expr names by the server's own equality.
func codecMatchesServer[T any](t *testing.T, conn *postgres.Conn, expr string, c pgtype.Codec[T], v T) {
	t.Helper()
	t.Run(expr, func(t *testing.T) {
		mine := c.Append(nil, v)
		theirs := serverEncoding(t, conn, expr)
		if !bytes.Equal(mine, theirs) {
			t.Errorf("this package encodes %x\n          the server %x", mine, theirs)
		}
		got, err := c.Decode(theirs)
		if err != nil {
			t.Fatalf("decoding the server's bytes: %v", err)
		}
		if again := c.Append(nil, got); !bytes.Equal(again, theirs) {
			t.Errorf("the server's bytes decode to %+v, which encodes to %x", got, again)
		}
		if !serverAgrees(t, conn, "SELECT $1 = "+expr, c.OID, mine) {
			t.Errorf("bound under OID %d, the value is not %s", c.OID, expr)
		}
	})
}
