package pgtype

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"
)

// words writes big-endian 32-bit words, the unit every container header is
// counted in.
func words(ws ...int64) []byte {
	var b []byte
	for _, w := range ws {
		b = binary.BigEndian.AppendUint32(b, uint32(w)) //nolint:gosec // G115: test vectors, -1 included on purpose
	}
	return b
}

func valid[T any](vs ...T) []Null[T] {
	out := make([]Null[T], len(vs))
	for i, v := range vs {
		out[i] = Null[T]{Value: v, Valid: true}
	}
	return out
}

// '{{1,2,3},{4,NULL,6}}'::int4[] as array_send writes it: two dimensions, the
// NULL flag, each dimension's length and lower bound, then the six elements
// in row-major order.
var twoByThree = slices.Concat(
	words(2, 1, int64(OIDInt4), 2, 1, 3, 1),
	words(4, 1), words(4, 2), words(4, 3),
	words(4, 4), words(-1), words(4, 6),
)

func TestArrayTwoDimensionsMatchesArraySend(t *testing.T) {
	a := Array[int32]{
		Dims:  []ArrayDim{{Len: 2, Lower: 1}, {Len: 3, Lower: 1}},
		Elems: []Null[int32]{{1, true}, {2, true}, {3, true}, {4, true}, {}, {6, true}},
	}
	if got := AppendArray(nil, a, Int4Codec()); !bytes.Equal(got, twoByThree) {
		t.Errorf("AppendArray = % x\n        want % x", got, twoByThree)
	}
	got, err := DecodeArray(Array[int32]{}, twoByThree, Int4Codec())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Dims, a.Dims) || !slices.Equal(got.Elems, a.Elems) {
		t.Errorf("DecodeArray = %+v, want %+v", got, a)
	}
}

// The generic encoder writes what the named ones write for the shape they
// share — one dimension from 1 — so a caller moving from one to the other
// sends the same bytes.
func TestArrayOneDimensionMatchesTheNamedEncoders(t *testing.T) {
	plain := AppendArray(nil, Array[int32]{Elems: valid[int32](1, -2, 3)}, Int4Codec())
	if want := AppendInt4Array(nil, []int32{1, -2, 3}); !bytes.Equal(plain, want) {
		t.Errorf("one dimension, no NULL:\n generic % x\n named   % x", plain, want)
	}
	withNull := AppendArray(nil, Array[int64]{Elems: []Null[int64]{{7, true}, {}}}, Int8Codec())
	if want := AppendNullableInt8Array(nil, []Null[int64]{{7, true}, {}}); !bytes.Equal(withNull, want) {
		t.Errorf("one dimension, a NULL:\n generic % x\n named   % x", withNull, want)
	}
	text := AppendArray(nil, Array[string]{Elems: valid("a", "", "ccc")}, TextCodec())
	if want := AppendTextArray(nil, []string{"a", "", "ccc"}); !bytes.Equal(text, want) {
		t.Errorf("text:\n generic % x\n named   % x", text, want)
	}
	empty := AppendArray(nil, Array[int64]{}, Int8Codec())
	if want := AppendInt8Array(nil, nil); !bytes.Equal(empty, want) {
		t.Errorf("empty:\n generic % x\n named   % x", empty, want)
	}
}

func TestArrayRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   Array[string]
		want Array[string] // what comes back, when it is not in
	}{
		{name: "empty", in: Array[string]{}},
		{
			name: "one dimension, Dims left nil",
			in:   Array[string]{Elems: valid("x", "y")},
			want: Array[string]{Dims: []ArrayDim{{2, 1}}, Elems: valid("x", "y")},
		},
		{name: "numbered from zero", in: Array[string]{Dims: []ArrayDim{{2, 0}}, Elems: valid("x", "y")}},
		{name: "numbered from below zero", in: Array[string]{Dims: []ArrayDim{{1, -5}}, Elems: valid("x")}},
		{name: "three dimensions", in: Array[string]{
			Dims:  []ArrayDim{{1, 1}, {2, 1}, {2, 3}},
			Elems: []Null[string]{{"a", true}, {}, {"", true}, {"d", true}},
		}},
		{name: "all NULL", in: Array[string]{Dims: []ArrayDim{{2, 1}}, Elems: make([]Null[string], 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.want
			if want.Elems == nil && want.Dims == nil {
				want = tc.in
			}
			got, err := DecodeArray(Array[string]{}, AppendArray(nil, tc.in, TextCodec()), TextCodec())
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Dims, want.Dims) || !slices.Equal(got.Elems, want.Elems) {
				t.Errorf("got %+v, want %+v", got, want)
			}
		})
	}
}

// The storage handed in is reused, and what was in it is gone: a decode that
// appended to the caller's elements would hand back last batch's rows.
func TestDecodeArrayReusesItsDestination(t *testing.T) {
	dst := Array[int32]{Dims: make([]ArrayDim, 0, 4), Elems: make([]Null[int32], 3, 16)}
	got, err := DecodeArray(dst, twoByThree, Int4Codec())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Elems) != 6 || &got.Elems[0] != &dst.Elems[:1][0] {
		t.Errorf("elements = %d, reused = %v", len(got.Elems), &got.Elems[0] == &dst.Elems[:1][0])
	}
}

func TestDecodeArrayRefusesWhatArraySendNeverWrites(t *testing.T) {
	header := func(ndim, flags int64, elem uint32, dims ...int64) []byte {
		return slices.Concat(words(ndim, flags, int64(elem)), words(dims...))
	}
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"short header", words(1, 0), "header is 8 bytes"},
		{"wrong element type", header(1, 0, OIDInt8, 1, 1), "element type is OID 20, want 23"},
		{"empty of the wrong element type", header(0, 0, OIDInt8), "element type is OID 20"},
		{"flags", header(1, 2, OIDInt4, 1, 1), "flags are 0x2"},
		{"negative dimensions", header(-1, 0, OIDInt4), "-1 dimensions"},
		{"seven dimensions", header(7, 0, OIDInt4), "7 dimensions"},
		{"dimensions cut short", header(2, 0, OIDInt4, 1, 1), "2 dimensions has a 20-byte header"},
		{"a dimension of no elements", header(1, 0, OIDInt4, 0, 1), "dimension 0 has 0 elements"},
		{"a negative dimension", header(1, 0, OIDInt4, -3, 1), "dimension 0 has -3 elements"},
		{"last index past int32", header(1, 0, OIDInt4, 2, math.MaxInt32), "from 2147483647"},
		{
			"more elements than bytes", slices.Concat(header(2, 0, OIDInt4, 65536, 1, 32768, 1), words(4, 1)),
			"more elements than its 8 bytes",
		},
		{"element cut short", slices.Concat(header(1, 0, OIDInt4, 1, 1), words(4)), "declares 4 bytes, 0 remain"},
		{"element of the wrong width", slices.Concat(header(1, 0, OIDInt4, 1, 1), words(2), []byte{0, 1}), "int4 is 2 bytes"},
		{"bytes after the last element", slices.Concat(header(1, 0, OIDInt4, 1, 1), words(4, 1), []byte{0}), "after the last"},
		{"bytes after an empty array", slices.Concat(header(0, 0, OIDInt4), []byte{0}), "after the last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := Array[int32]{Elems: make([]Null[int32], 0, 4)}
			got, err := DecodeArray(dst, tc.b, Int4Codec())
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrCodec saying %q", err, tc.want)
			}
			if len(got.Dims) != 0 || len(got.Elems) != 0 {
				t.Errorf("a refused array came back holding %+v", got)
			}
		})
	}
}

// '[1,6)'::int4range, '(,5]'::int4range and 'empty'::int4range as range_send
// writes them.
func TestRangeMatchesRangeSend(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    Range[int32]
		want []byte
	}{
		{
			"[1,6)",
			Range[int32]{Lower: RangeBound[int32]{Value: 1, Inclusive: true}, Upper: RangeBound[int32]{Value: 6}},
			slices.Concat([]byte{rangeLBInc}, words(4, 1, 4, 6)),
		},
		{
			"(,5]",
			Range[int32]{Lower: RangeBound[int32]{Unbounded: true}, Upper: RangeBound[int32]{Value: 5, Inclusive: true}},
			slices.Concat([]byte{rangeLBInf | rangeUBInc}, words(4, 5)),
		},
		{
			// An absent bound is never inclusive, whatever the caller said.
			"[,) written inclusive",
			Range[int32]{Lower: RangeBound[int32]{Unbounded: true, Inclusive: true}, Upper: RangeBound[int32]{Unbounded: true, Inclusive: true}},
			[]byte{rangeLBInf | rangeUBInf},
		},
		{"empty", Range[int32]{Empty: true, Lower: RangeBound[int32]{Value: 3}}, []byte{rangeEmpty}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := AppendRange(nil, tc.r, Int4Codec())
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("AppendRange = % x, want % x", got, tc.want)
			}
			back, err := DecodeRange(got, Int4Codec())
			if err != nil {
				t.Fatal(err)
			}
			if again := AppendRange(nil, back, Int4Codec()); !bytes.Equal(again, got) {
				t.Errorf("decoded %+v, which encodes to % x", back, again)
			}
		})
	}
}

func TestRangeRoundTripsOverAVariableWidthBound(t *testing.T) {
	jan, feb := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	in := Range[time.Time]{Lower: RangeBound[time.Time]{Value: jan, Inclusive: true}, Upper: RangeBound[time.Time]{Value: feb}}
	got, err := DecodeRange(AppendRange(nil, in, DateCodec()), DateCodec())
	if err != nil || got != in {
		t.Errorf("date range = %+v, %v; want %+v", got, err, in)
	}
	num := Range[Numeric]{Lower: RangeBound[Numeric]{Value: mustNumeric(t, "-1.5")}, Upper: RangeBound[Numeric]{Unbounded: true}}
	gotNum, err := DecodeRange(AppendRange(nil, num, NumericCodec()), NumericCodec())
	if err != nil || gotNum.Lower.Value.String() != "-1.5" || !gotNum.Upper.Unbounded || gotNum.Lower.Inclusive {
		t.Errorf("numeric range = %+v, %v", gotNum, err)
	}
}

func mustNumeric(t *testing.T, s string) Numeric {
	t.Helper()
	n, err := ParseNumeric(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDecodeRangeRefusesWhatRangeSendNeverWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"nothing", nil, "want at least its flags"},
		{"a reserved flag", []byte{0x20}, "flags are 0x20"},
		{"index-page flag", []byte{0x80}, "flags are 0x80"},
		{"empty with a bound", []byte{rangeEmpty | rangeLBInc}, "empty range with flags 0x3"},
		{"absent lower bound inclusive", []byte{rangeLBInf | rangeLBInc | rangeUBInf}, "absent bound inclusive"},
		{"absent upper bound inclusive", []byte{rangeLBInf | rangeUBInf | rangeUBInc}, "absent bound inclusive"},
		{"bytes after empty", []byte{rangeEmpty, 0}, "after an empty range"},
		{"lower bound missing", []byte{0}, "lower bound has no length"},
		{"upper bound missing", slices.Concat([]byte{rangeUBInc}, words(4, 1)), "upper bound has no length"},
		{"a NULL bound", slices.Concat([]byte{rangeLBInf}, words(-1)), "declares -1 bytes"},
		{"bound cut short", slices.Concat([]byte{rangeLBInf}, words(8, 1)), "declares 8 bytes, 4 remain"},
		{"bound of the wrong width", slices.Concat([]byte{rangeLBInf}, words(2), []byte{0, 1}), "int4 is 2 bytes"},
		{"bytes after the bounds", slices.Concat([]byte{rangeLBInf}, words(4, 1), []byte{0}), "after the range's bounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRange(tc.b, Int4Codec())
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrCodec saying %q", err, tc.want)
			}
		})
	}
}

func TestMultirangeRoundTrips(t *testing.T) {
	in := Multirange[int64]{
		{Lower: RangeBound[int64]{Value: 1, Inclusive: true}, Upper: RangeBound[int64]{Value: 3}},
		{Lower: RangeBound[int64]{Value: 5, Inclusive: true}, Upper: RangeBound[int64]{Unbounded: true}},
	}
	b := AppendMultirange(nil, in, Int8Codec())
	// The count, then each range as range_send writes it, length first.
	want := slices.Concat(words(2),
		words(25), []byte{rangeLBInc}, words(8), AppendInt8(nil, 1), words(8), AppendInt8(nil, 3),
		words(13), []byte{rangeLBInc | rangeUBInf}, words(8), AppendInt8(nil, 5))
	if !bytes.Equal(b, want) {
		t.Fatalf("AppendMultirange = % x\n             want % x", b, want)
	}
	got, err := DecodeMultirange(make(Multirange[int64], 5), b, Int8Codec())
	if err != nil || !slices.Equal(got, in) {
		t.Errorf("DecodeMultirange = %+v, %v", got, err)
	}
	empty, err := DecodeMultirange(nil, AppendMultirange(nil, nil, Int8Codec()), Int8Codec())
	if err != nil || len(empty) != 0 {
		t.Errorf("empty multirange = %+v, %v", empty, err)
	}
}

func TestDecodeMultirangeRefusesMalformedInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"short", []byte{0, 0}, "want at least 4"},
		{"negative count", words(-1), "declares -1 ranges"},
		{"more ranges than bytes", slices.Concat(words(1<<30), []byte{1, 2, 3, 4, 5}), "declares 1073741824 ranges in 5 bytes"},
		{"range cut short", slices.Concat(words(1, 9), []byte{0}), "range 0 declares 9 bytes, 1 remain"},
		{
			"second range with no length", slices.Concat(words(2, 9), []byte{rangeLBInc | rangeUBInf}, words(4, 1), []byte{0, 0, 0}),
			"range 1 has no length",
		},
		{"a malformed range", slices.Concat(words(1, 1), []byte{0x40}), "multirange range 0: "},
		{"bytes after the last range", slices.Concat(words(1, 1), []byte{rangeEmpty, 0}), "after the last range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeMultirange(make(Multirange[int32], 0, 2), tc.b, Int4Codec())
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) || len(got) != 0 {
				t.Fatalf("got %v, err = %v, want ErrCodec saying %q", got, err, tc.want)
			}
		})
	}
}

func TestRecordRoundTrips(t *testing.T) {
	in := Record{
		{OID: OIDInt8, Value: AppendInt8(nil, 42)},
		{OID: OIDText},                  // NULL
		{OID: OIDText, Value: []byte{}}, // the empty string, which is not NULL
		{OID: OIDBool, Value: AppendBool(nil, true)},
	}
	b := AppendRecord(nil, in)
	want := slices.Concat(words(4),
		words(int64(OIDInt8), 8), AppendInt8(nil, 42),
		words(int64(OIDText), -1),
		words(int64(OIDText), 0),
		words(int64(OIDBool), 1), []byte{1})
	if !bytes.Equal(b, want) {
		t.Fatalf("AppendRecord = % x\n         want % x", b, want)
	}
	got, err := DecodeRecord(nil, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[1].Value != nil || got[2].Value == nil || len(got[2].Value) != 0 {
		t.Fatalf("NULL and empty were not kept apart: %+v", got)
	}

	id, err := DecodeRecordField(got[0], Int8Codec())
	if err != nil || id != (Null[int64]{42, true}) {
		t.Errorf("field 0 = %v, %v", id, err)
	}
	null, err := DecodeRecordField(got[1], TextCodec())
	if err != nil || null.Valid {
		t.Errorf("field 1 = %v, %v; want NULL", null, err)
	}
	if _, err := DecodeRecordField(got[0], Int4Codec()); !errors.Is(err, ErrCodec) {
		t.Errorf("a bigint field read as integer: err = %v", err)
	}
	if _, err := DecodeRecordField(RecordField{OID: OIDInt4, Value: []byte{1}}, Int4Codec()); !errors.Is(err, ErrCodec) {
		t.Errorf("a malformed field: err = %v", err)
	}

	// The wire buffer is overwritten by the next batch; the fields must not
	// change with it.
	clear(b)
	if v, _ := DecodeInt8(got[0].Value); v != 42 {
		t.Errorf("field 0 changed with the buffer it was read from: %d", v)
	}
}

func TestDecodeRecordRefusesMalformedInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"short", []byte{0}, "want at least 4"},
		{"negative count", words(-2), "declares -2 fields"},
		{"more fields than bytes", words(2, 23, 4), "declares 2 fields in 8 bytes"},
		{"field length below -1", words(1, 23, -2), "field 0 declares -2 bytes"},
		{"field cut short", words(1, 23, 8), "field 0 declares 8 bytes, 0 remain"},
		{"field with no length", slices.Concat(words(2, 23, 4, 1), words(23)), "field 1 has no type and length"},
		{"bytes after the last field", slices.Concat(words(1, 23, -1), []byte{0}), "after the last record field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeRecord(make(Record, 0, 2), tc.b)
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) || len(got) != 0 {
				t.Fatalf("got %v, err = %v, want ErrCodec saying %q", got, err, tc.want)
			}
		})
	}
}

// The containers built over every Codec: each one's element OID and array
// OID are distinct, so no two types can be mistaken for each other in an
// array header, and its pair of functions round-trips a value.
func TestEveryCodecIsDistinctAndRoundTrips(t *testing.T) {
	seen := map[uint32]string{}
	note := func(name string, oids ...uint32) {
		for _, oid := range oids {
			if oid == 0 {
				t.Errorf("%s has a zero OID", name)
			}
			if other, ok := seen[oid]; ok {
				t.Errorf("%s and %s share OID %d", name, other, oid)
			}
			seen[oid] = name
		}
	}
	check := func(name string, c func(*testing.T) (uint32, uint32)) {
		t.Run(name, func(t *testing.T) { note(name, func() []uint32 { a, b := c(t); return []uint32{a, b} }()...) })
	}
	check("bool", roundTrip(BoolCodec(), true))
	check("bytea", roundTrip(ByteaCodec(), []byte{1, 2}))
	check("char", roundTrip(CharCodec(), byte('x')))
	check("name", roundTrip(NameCodec(), "n"))
	check("int8", roundTrip(Int8Codec(), int64(-8)))
	check("int2", roundTrip(Int2Codec(), int16(-2)))
	check("int2vector", roundTrip(Int2VectorCodec(), []int16{1, 2}))
	check("int4", roundTrip(Int4Codec(), int32(-4)))
	check("regproc", roundTrip(RegprocCodec(), uint32(1)))
	check("text", roundTrip(TextCodec(), "t"))
	check("oid", roundTrip(OIDCodec(), uint32(2)))
	check("tid", roundTrip(TIDCodec(), TID{Block: 1, Offset: 2}))
	check("xid", roundTrip(XIDCodec(), uint32(3)))
	check("cid", roundTrip(CIDCodec(), uint32(4)))
	check("oidvector", roundTrip(OIDVectorCodec(), []uint32{5}))
	check("json", roundTrip(JSONCodec(), []byte(`{}`)))
	check("xml", roundTrip(XMLCodec(), "<a/>"))
	check("point", roundTrip(PointCodec(), Point{X: 1, Y: 2}))
	check("lseg", roundTrip(LSegCodec(), LSeg{P: [2]Point{{X: 1}, {Y: 1}}}))
	check("path", roundTrip(PathCodec(), Path{Closed: true, Points: []Point{{X: 1}}}))
	check("box", roundTrip(BoxCodec(), Box{High: Point{X: 1, Y: 1}}))
	check("polygon", roundTrip(PolygonCodec(), []Point{{X: 1}, {Y: 1}}))
	check("line", roundTrip(LineCodec(), Line{A: 1, B: 2, C: 3}))
	check("cidr", roundTrip(CIDRCodec(), netip.MustParsePrefix("10.0.0.0/8")))
	check("float4", roundTrip(Float4Codec(), float32(1.5)))
	check("float8", roundTrip(Float8Codec(), -2.5))
	check("circle", roundTrip(CircleCodec(), Circle{Radius: 1}))
	check("macaddr8", roundTrip(Macaddr8Codec(), [8]byte{1}))
	check("money", roundTrip(MoneyCodec(), int64(1234)))
	check("macaddr", roundTrip(MacaddrCodec(), [6]byte{1}))
	check("inet", roundTrip(InetCodec(), netip.MustParsePrefix("192.168.1.1/24")))
	check("bpchar", roundTrip(BPCharCodec(), "ab "))
	check("varchar", roundTrip(VarcharCodec(), "v"))
	check("date", roundTrip(DateCodec(), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)))
	check("time", roundTrip(TimeCodec(), TimeOfDay{Micros: 1}))
	check("timestamp", roundTrip(TimestampCodec(), time.Date(2024, 3, 1, 1, 2, 3, 0, time.UTC)))
	check("timestamptz", roundTrip(TimestampTZCodec(), time.Date(2024, 3, 1, 1, 2, 3, 0, time.UTC)))
	check("interval", roundTrip(IntervalCodec(), Interval{Months: 1, Days: 2, Micros: 3}))
	check("timetz", roundTrip(TimeTZCodec(), TimeTZ{Micros: 1, OffsetSeconds: 3600}))
	check("bit", roundTrip(BitCodec(), Bits{Len: 3, Bytes: []byte{0xa0}}))
	check("varbit", roundTrip(VarbitCodec(), Bits{Len: 1, Bytes: []byte{0x80}}))
	check("numeric", roundTrip(NumericCodec(), mustNumeric(t, "12.5")))
	check("refcursor", roundTrip(RefcursorCodec(), "c"))
	check("regprocedure", roundTrip(RegprocedureCodec(), uint32(6)))
	check("regoper", roundTrip(RegoperCodec(), uint32(7)))
	check("regoperator", roundTrip(RegoperatorCodec(), uint32(8)))
	check("regclass", roundTrip(RegclassCodec(), uint32(9)))
	check("regtype", roundTrip(RegtypeCodec(), uint32(10)))
	check("uuid", roundTrip(UUIDCodec(), [16]byte{15: 1}))
	check("txid_snapshot", roundTrip(TxidSnapshotCodec(), Snapshot{Xmin: 3, Xmax: 5, Xip: []uint64{4}}))
	check("pg_lsn", roundTrip(PGLSNCodec(), uint64(11)))
	check("tsvector", roundTrip(TSVectorCodec(), []Lexeme{{Word: "w"}}))
	check("tsquery", roundTrip(TSQueryCodec(), []TSQueryItem{{Word: "q"}}))
	check("regconfig", roundTrip(RegconfigCodec(), uint32(12)))
	check("regdictionary", roundTrip(RegdictionaryCodec(), uint32(13)))
	check("jsonb", roundTrip(JSONBCodec(), []byte(`[1]`)))
	check("jsonpath", roundTrip(JSONPathCodec(), "$.a"))
	check("regnamespace", roundTrip(RegnamespaceCodec(), uint32(14)))
	check("regrole", roundTrip(RegroleCodec(), uint32(15)))
	check("regcollation", roundTrip(RegcollationCodec(), uint32(16)))
	check("pg_snapshot", roundTrip(PGSnapshotCodec(), Snapshot{Xmin: 3, Xmax: 3}))
	check("xid8", roundTrip(XID8Codec(), uint64(17)))
	check("oid8", roundTrip(OID8Codec(), uint64(18)))
	check("regdatabase", roundTrip(RegdatabaseCodec(), uint32(19)))

	day := func(d int) time.Time { return time.Date(2024, 3, d, 0, 0, 0, 0, time.UTC) }
	closedOpen := func(lo, hi int64) Range[int64] {
		return Range[int64]{Lower: RangeBound[int64]{Value: lo, Inclusive: true}, Upper: RangeBound[int64]{Value: hi}}
	}
	check("int4range", roundTrip(Int4RangeCodec(), Range[int32]{Lower: RangeBound[int32]{Value: 1, Inclusive: true}, Upper: RangeBound[int32]{Unbounded: true}}))
	check("int8range", roundTrip(Int8RangeCodec(), closedOpen(-3, 9)))
	check("numrange", roundTrip(NumRangeCodec(), Range[Numeric]{Lower: RangeBound[Numeric]{Value: mustNumeric(t, "0.5")}, Upper: RangeBound[Numeric]{Value: mustNumeric(t, "2"), Inclusive: true}}))
	check("tsrange", roundTrip(TSRangeCodec(), Range[time.Time]{Lower: RangeBound[time.Time]{Value: day(1), Inclusive: true}, Upper: RangeBound[time.Time]{Value: day(2)}}))
	check("tstzrange", roundTrip(TSTZRangeCodec(), Range[time.Time]{Empty: true}))
	check("daterange", roundTrip(DateRangeCodec(), Range[time.Time]{Lower: RangeBound[time.Time]{Unbounded: true}, Upper: RangeBound[time.Time]{Value: day(3)}}))
	check("int4multirange", roundTrip(Int4MultirangeCodec(), Multirange[int32]{{Lower: RangeBound[int32]{Value: 1, Inclusive: true}, Upper: RangeBound[int32]{Value: 2}}}))
	check("int8multirange", roundTrip(Int8MultirangeCodec(), Multirange[int64]{closedOpen(1, 2), closedOpen(5, 7)}))
	check("nummultirange", roundTrip(NumMultirangeCodec(), Multirange[Numeric]{}))
	check("tsmultirange", roundTrip(TSMultirangeCodec(), Multirange[time.Time]{{Lower: RangeBound[time.Time]{Value: day(1), Inclusive: true}, Upper: RangeBound[time.Time]{Unbounded: true}}}))
	check("tstzmultirange", roundTrip(TSTZMultirangeCodec(), Multirange[time.Time]{{Lower: RangeBound[time.Time]{Unbounded: true}, Upper: RangeBound[time.Time]{Value: day(4)}}}))
	check("datemultirange", roundTrip(DateMultirangeCodec(), Multirange[time.Time]{{Lower: RangeBound[time.Time]{Value: day(1), Inclusive: true}, Upper: RangeBound[time.Time]{Value: day(5)}}}))
	for _, oid := range []uint32{OIDInt4Range, OIDInt8Range, OIDNumRange, OIDTSRange, OIDTSTZRange, OIDDateRange} {
		if _, ok := seen[oid]; !ok {
			t.Errorf("range OID %d has no codec", oid)
		}
	}
}

// roundTrip checks one Codec through an array of two elements and a NULL, and
// returns its OIDs.
func roundTrip[T any](c Codec[T], v T) func(*testing.T) (uint32, uint32) {
	return func(t *testing.T) (uint32, uint32) {
		t.Helper()
		in := Array[T]{Elems: []Null[T]{{v, true}, {}}}
		got, err := DecodeArray(Array[T]{}, AppendArray(nil, in, c), c)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Elems[1].Valid && got.Elems[0].Valid &&
			bytes.Equal(c.Append(nil, got.Elems[0].Value), c.Append(nil, v)) {
			return c.OID, c.ArrayOID
		}
		t.Errorf("%v came back as %+v", v, got.Elems)
		return c.OID, c.ArrayOID
	}
}

// A range type a database created is the same container over another
// codec, and its array is an Array over the result.
func TestRangeCodecOfACreatedType(t *testing.T) {
	const oid, arrayOID = 16500, 16499
	floatrange := RangeCodec(Float8Codec(), oid, arrayOID)
	in := Array[Range[float64]]{Elems: []Null[Range[float64]]{
		{Value: Range[float64]{Lower: RangeBound[float64]{Value: -0.5, Inclusive: true}, Upper: RangeBound[float64]{Unbounded: true}}, Valid: true},
		{},
	}}
	b := AppendArray(nil, in, floatrange)
	if got := binary.BigEndian.Uint32(b[8:12]); got != oid {
		t.Errorf("the array names element type %d, want the range's %d", got, oid)
	}
	got, err := DecodeArray(Array[Range[float64]]{}, b, floatrange)
	if err != nil || !slices.Equal(got.Elems, in.Elems) {
		t.Errorf("floatrange[] = %+v, %v", got, err)
	}
	if _, err := DecodeArray(Array[Range[int32]]{}, b, Int4RangeCodec()); !errors.Is(err, ErrCodec) {
		t.Errorf("a floatrange[] read as int4range[]: err = %v", err)
	}
}

// The codecs whose plain decoder borrows the wire buffer copy instead: a value
// kept from a container must not change under its owner.
func TestCodecDecodersDoNotBorrow(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    Codec[[]byte]
	}{
		{"bytea", ByteaCodec()},
		{"json", JSONCodec()},
		{"jsonb", JSONBCodec()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.c.Append(nil, []byte(`"x"`))
			v, err := tc.c.Decode(b)
			if err != nil {
				t.Fatal(err)
			}
			clear(b)
			if string(v) != `"x"` {
				t.Errorf("the value changed with the buffer it was read from: %q", v)
			}
		})
	}
	if _, err := JSONBCodec().Decode([]byte{2, '1'}); !errors.Is(err, ErrCodec) {
		t.Errorf("jsonb of version 2: err = %v", err)
	}
}
