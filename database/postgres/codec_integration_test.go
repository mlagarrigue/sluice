package postgres_test

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// The codecs added for `time`, `timetz`, `macaddr`, `macaddr8`, `bit` and
// NULL-bearing arrays, settled against a server rather than against this
// package's reading of the protocol.
//
// Their unit tests round-trip each value through this package's own encoder
// and decoder, which is exactly the check that cannot fail when both halves
// share a misreading — the differential-oracle gap the repository already
// names. So each type is also asked `SELECT $1 = <literal>` here: the
// parameter goes out in binary, the literal is parsed by the server, and the
// type's **own comparison operator** decides whether they are the same value.
// A sign error, a byte order, a length in the wrong unit — none of them
// survive that, and none of them are visible to a round trip.

// serverAgrees sends value as a binary parameter of the given OID and asks the
// server whether it equals the literal. It reports what the server said.
func serverAgrees(t *testing.T, conn *postgres.Conn, sql string, oid uint32, value []byte) bool {
	t.Helper()
	src := conn.Query(t.Context(), sql, [][]byte{value},
		postgres.QueryConfig{BatchRows: 1, ParamOIDs: []uint32{oid}})

	var agreed bool
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, isNull := row.Value(0)
			if isNull {
				t.Error("the comparison returned NULL, which means one side was NULL")
				continue
			}
			got, err := postgres.DecodeBool(v)
			if err != nil {
				t.Errorf("decoding the comparison: %v", err)
				continue
			}
			agreed = got
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return agreed
}

func TestIntegrationTimeVectors(t *testing.T) {
	conn := dial(t)

	for _, tc := range []struct {
		literal string
		value   postgres.TimeOfDay
	}{
		{"'00:00:00'", postgres.TimeOfDay{}},
		{"'12:00:00'", postgres.TimeOfDay{Micros: 12 * 3_600_000_000}},
		{"'01:02:03.000456'", postgres.TimeOfDay{Micros: 3_723_000_456}},
		{"'23:59:59.999999'", postgres.TimeOfDay{Micros: 86_399_999_999}},
		{"'24:00:00'", postgres.TimeOfDay{Micros: 86_400_000_000}},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			if !serverAgrees(t, conn, "SELECT $1::time = "+tc.literal+"::time",
				postgres.OIDTime, postgres.AppendTime(nil, tc.value)) {
				t.Errorf("the server says our encoding of %s is a different value", tc.literal)
			}
		})
	}
}

// The one where the sign convention is settled by something other than this
// package's own belief about it.
func TestIntegrationTimeTZVectors(t *testing.T) {
	conn := dial(t)

	for _, tc := range []struct {
		literal string
		value   postgres.TimeTZ
	}{
		{"'09:00:00+02'", postgres.TimeTZ{Micros: 9 * 3_600_000_000, OffsetSeconds: -7200}},
		{"'09:00:00-05'", postgres.TimeTZ{Micros: 9 * 3_600_000_000, OffsetSeconds: 18000}},
		{"'00:00:00+00'", postgres.TimeTZ{}},
		{"'13:45:30.5+05:30'", postgres.TimeTZ{Micros: 13*3_600_000_000 + 45*60_000_000 + 30_500_000, OffsetSeconds: -19800}},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			if !serverAgrees(t, conn, "SELECT $1::timetz = "+tc.literal+"::timetz",
				postgres.OIDTimeTZ, postgres.AppendTimeTZ(nil, tc.value)) {
				t.Errorf("the server says our encoding of %s is a different value — most likely the offset sign", tc.literal)
			}
		})
	}
}

func TestIntegrationMacaddrVectors(t *testing.T) {
	conn := dial(t)

	if !serverAgrees(t, conn, "SELECT $1::macaddr = '08:00:2b:01:02:03'::macaddr",
		postgres.OIDMacaddr, postgres.AppendMacaddr(nil, [6]byte{0x08, 0x00, 0x2b, 0x01, 0x02, 0x03})) {
		t.Error("the server disagrees about macaddr")
	}
	if !serverAgrees(t, conn, "SELECT $1::macaddr8 = '08:00:2b:ff:fe:01:02:03'::macaddr8",
		postgres.OIDMacaddr8, postgres.AppendMacaddr8(nil, [8]byte{0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03})) {
		t.Error("the server disagrees about macaddr8")
	}
}

// The bit-count-in-bits question, settled: B'1' and B'10000000' are the same
// byte on the wire and different values, so a comparison against each literal
// is the only thing that can tell the two encodings apart.
func TestIntegrationBitVectors(t *testing.T) {
	conn := dial(t)

	for _, tc := range []struct {
		literal string
		value   postgres.Bits
	}{
		{"B'1'", postgres.Bits{Len: 1, Bytes: []byte{0x80}}},
		{"B'10000000'", postgres.Bits{Len: 8, Bytes: []byte{0x80}}},
		{"B'1011000101'", postgres.Bits{Len: 10, Bytes: []byte{0xb1, 0x40}}},
		{"B'0'", postgres.Bits{Len: 1, Bytes: []byte{0x00}}},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			if !serverAgrees(t, conn, "SELECT $1::varbit = "+tc.literal+"::varbit",
				postgres.OIDVarbit, postgres.AppendBits(nil, tc.value)) {
				t.Errorf("the server says our encoding of %s is a different value", tc.literal)
			}
		})
	}
}

// NULL inside an array, against the server that decides what NULL means. The
// header's flags word and the -1 element length have to agree, and only a
// server can say whether they do.
func TestIntegrationNullBearingArrays(t *testing.T) {
	conn := dial(t)

	t.Run("int8[] with a NULL", func(t *testing.T) {
		v := postgres.AppendNullableInt8Array(nil, []postgres.Null[int64]{
			postgres.Some(int64(1)), postgres.None[int64](), postgres.Some(int64(3)),
		})
		if !serverAgrees(t, conn, "SELECT $1::bigint[] = ARRAY[1, NULL, 3]::bigint[] IS NOT FALSE",
			postgres.OIDInt8Array, v) {
			t.Error("the server disagrees about an int8[] carrying a NULL")
		}
		// IS NOT FALSE above because NULL = NULL is NULL, not true. The
		// stronger check is that the server counts the NULLs where we put them.
		if !serverAgrees(t, conn,
			"SELECT array_position($1::bigint[], NULL) = 2 AND cardinality($1::bigint[]) = 3",
			postgres.OIDInt8Array, v) {
			t.Error("the server does not see the NULL at position 2 of a three-element array")
		}
	})

	t.Run("text[] keeps empty distinct from NULL", func(t *testing.T) {
		v := postgres.AppendNullableTextArray(nil, []postgres.Null[string]{
			postgres.Some(""), postgres.None[string](),
		})
		if !serverAgrees(t, conn,
			"SELECT ($1::text[])[1] = '' AND ($1::text[])[2] IS NULL",
			postgres.OIDTextArray, v) {
			t.Error("the server collapsed the empty string and the NULL, or we did")
		}
	})

	t.Run("all NULL", func(t *testing.T) {
		v := postgres.AppendNullableInt8Array(nil, []postgres.Null[int64]{
			postgres.None[int64](), postgres.None[int64](),
		})
		if !serverAgrees(t, conn,
			"SELECT cardinality($1::bigint[]) = 2 AND $1::bigint[] IS NOT NULL",
			postgres.OIDInt8Array, v) {
			t.Error("an array of two NULLs did not arrive as an array of two NULLs")
		}
	})

	// And the round trip the other way: what the server sends for an array
	// with a NULL must decode here.
	t.Run("decoding what the server sends", func(t *testing.T) {
		src := conn.Query(t.Context(), "SELECT ARRAY[1, NULL, 3]::bigint[]", nil,
			postgres.QueryConfig{BatchRows: 1})
		var got []postgres.Null[int64]
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, row := range b.Items {
				v, _ := row.Value(0)
				out, err := postgres.DecodeNullableInt8Array(nil, v)
				if err != nil {
					t.Errorf("decoding the server's array: %v", err)
					continue
				}
				got = out
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("decoded %d elements, want 3", len(got))
		}
		if !got[0].Valid || got[0].Value != 1 || got[1].Valid || !got[2].Valid || got[2].Value != 3 {
			t.Errorf("decoded %v, want [1 NULL 3]", got)
		}
	})
}
