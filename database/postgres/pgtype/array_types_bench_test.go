package pgtype

import (
	"strconv"
	"testing"
	"time"
)

// The array benchmarks of the common types, a thousand elements and no
// NULL, through DecodeArray and AppendArray. Before AppendArray and
// DecodeArray became the one way to write and read an array, they were held
// to the named per-type functions they replaced (AppendInt4Array and the
// others) by a copy of this file calling those, built on the same tree: a
// paired A/B under the same names, docs/benchmarks.md, "Arrays". A change to
// the element loops (array_fast.go) is held to these the same way, the tree
// before it against the tree after. Numeric values are parsed once, outside
// the loop.

var benchBase = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

func benchNumeric(s string) Numeric {
	n, err := ParseNumeric(s)
	if err != nil {
		panic(err)
	}
	return n
}

func benchBoolValues() []bool {
	v := make([]bool, 1000)
	for i := range v {
		v[i] = i%2 == 0 //nolint:gosec // G115: small test values
	}
	return v
}

func benchBoolArray() Array[bool] {
	values := benchBoolValues()
	return Array[bool]{Elems: values}
}

func BenchmarkArrayDecodeBool1000(b *testing.B) {
	c := BoolCodec()
	raw := AppendArray(nil, benchBoolArray(), c)
	var out Array[bool]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendBool1000(b *testing.B) {
	c, a := BoolCodec(), benchBoolArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchInt2Values() []int16 {
	v := make([]int16, 1000)
	for i := range v {
		v[i] = int16(i) //nolint:gosec // G115: small test values
	}
	return v
}

func benchInt2Array() Array[int16] {
	values := benchInt2Values()
	return Array[int16]{Elems: values}
}

func BenchmarkArrayDecodeInt21000(b *testing.B) {
	c := Int2Codec()
	raw := AppendArray(nil, benchInt2Array(), c)
	var out Array[int16]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendInt21000(b *testing.B) {
	c, a := Int2Codec(), benchInt2Array()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchInt4Values() []int32 {
	v := make([]int32, 1000)
	for i := range v {
		v[i] = int32(i*7919 - 500_000) //nolint:gosec // G115: small test values
	}
	return v
}

func benchInt4Array() Array[int32] {
	values := benchInt4Values()
	return Array[int32]{Elems: values}
}

func BenchmarkArrayDecodeInt41000(b *testing.B) {
	c := Int4Codec()
	raw := AppendArray(nil, benchInt4Array(), c)
	var out Array[int32]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendInt41000(b *testing.B) {
	c, a := Int4Codec(), benchInt4Array()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchInt8Values() []int64 {
	v := make([]int64, 1000)
	for i := range v {
		v[i] = int64(i) * 1_000_003 //nolint:gosec // G115: small test values
	}
	return v
}

func benchInt8Array() Array[int64] {
	values := benchInt8Values()
	return Array[int64]{Elems: values}
}

func BenchmarkArrayDecodeInt81000(b *testing.B) {
	c := Int8Codec()
	raw := AppendArray(nil, benchInt8Array(), c)
	var out Array[int64]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendInt81000(b *testing.B) {
	c, a := Int8Codec(), benchInt8Array()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchFloat4Values() []float32 {
	v := make([]float32, 1000)
	for i := range v {
		v[i] = float32(i) / 3 //nolint:gosec // G115: small test values
	}
	return v
}

func benchFloat4Array() Array[float32] {
	values := benchFloat4Values()
	return Array[float32]{Elems: values}
}

func BenchmarkArrayDecodeFloat41000(b *testing.B) {
	c := Float4Codec()
	raw := AppendArray(nil, benchFloat4Array(), c)
	var out Array[float32]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendFloat41000(b *testing.B) {
	c, a := Float4Codec(), benchFloat4Array()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchFloat8Values() []float64 {
	v := make([]float64, 1000)
	for i := range v {
		v[i] = float64(i) / 7 //nolint:gosec // G115: small test values
	}
	return v
}

func benchFloat8Array() Array[float64] {
	values := benchFloat8Values()
	return Array[float64]{Elems: values}
}

func BenchmarkArrayDecodeFloat81000(b *testing.B) {
	c := Float8Codec()
	raw := AppendArray(nil, benchFloat8Array(), c)
	var out Array[float64]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendFloat81000(b *testing.B) {
	c, a := Float8Codec(), benchFloat8Array()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchUUIDValues() [][16]byte {
	v := make([][16]byte, 1000)
	for i := range v {
		v[i] = [16]byte{byte(i), byte(i >> 8), 3} //nolint:gosec // G115: small test values
	}
	return v
}

func benchUUIDArray() Array[[16]byte] {
	values := benchUUIDValues()
	return Array[[16]byte]{Elems: values}
}

func BenchmarkArrayDecodeUUID1000(b *testing.B) {
	c := UUIDCodec()
	raw := AppendArray(nil, benchUUIDArray(), c)
	var out Array[[16]byte]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendUUID1000(b *testing.B) {
	c, a := UUIDCodec(), benchUUIDArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchTimestampTZValues() []time.Time {
	v := make([]time.Time, 1000)
	for i := range v {
		v[i] = benchBase.Add(time.Duration(i) * time.Second) //nolint:gosec // G115: small test values
	}
	return v
}

func benchTimestampTZArray() Array[time.Time] {
	values := benchTimestampTZValues()
	return Array[time.Time]{Elems: values}
}

func BenchmarkArrayDecodeTimestampTZ1000(b *testing.B) {
	c := TimestampTZCodec()
	raw := AppendArray(nil, benchTimestampTZArray(), c)
	var out Array[time.Time]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendTimestampTZ1000(b *testing.B) {
	c, a := TimestampTZCodec(), benchTimestampTZArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchTimestampValues() []time.Time {
	v := make([]time.Time, 1000)
	for i := range v {
		v[i] = benchBase.Add(time.Duration(i) * time.Minute) //nolint:gosec // G115: small test values
	}
	return v
}

func benchTimestampArray() Array[time.Time] {
	values := benchTimestampValues()
	return Array[time.Time]{Elems: values}
}

func BenchmarkArrayDecodeTimestamp1000(b *testing.B) {
	c := TimestampCodec()
	raw := AppendArray(nil, benchTimestampArray(), c)
	var out Array[time.Time]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendTimestamp1000(b *testing.B) {
	c, a := TimestampCodec(), benchTimestampArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchDateValues() []time.Time {
	v := make([]time.Time, 1000)
	for i := range v {
		v[i] = benchBase.AddDate(0, 0, i) //nolint:gosec // G115: small test values
	}
	return v
}

func benchDateArray() Array[time.Time] {
	values := benchDateValues()
	return Array[time.Time]{Elems: values}
}

func BenchmarkArrayDecodeDate1000(b *testing.B) {
	c := DateCodec()
	raw := AppendArray(nil, benchDateArray(), c)
	var out Array[time.Time]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendDate1000(b *testing.B) {
	c, a := DateCodec(), benchDateArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchTextValues() []string {
	v := make([]string, 1000)
	for i := range v {
		v[i] = "value-" + strconv.Itoa(i) //nolint:gosec // G115: small test values
	}
	return v
}

func benchTextArray() Array[string] {
	values := benchTextValues()
	return Array[string]{Elems: values}
}

func BenchmarkArrayDecodeText1000(b *testing.B) {
	c := TextCodec()
	raw := AppendArray(nil, benchTextArray(), c)
	var out Array[string]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendText1000(b *testing.B) {
	c, a := TextCodec(), benchTextArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchByteaValues() [][]byte {
	v := make([][]byte, 1000)
	for i := range v {
		v[i] = []byte(strconv.Itoa(i)) //nolint:gosec // G115: small test values
	}
	return v
}

func benchByteaArray() Array[[]byte] {
	values := benchByteaValues()
	return Array[[]byte]{Elems: values}
}

func BenchmarkArrayDecodeBytea1000(b *testing.B) {
	c := ByteaCodec()
	raw := AppendArray(nil, benchByteaArray(), c)
	var out Array[[]byte]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendBytea1000(b *testing.B) {
	c, a := ByteaCodec(), benchByteaArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func benchNumericValues() []Numeric {
	v := make([]Numeric, 1000)
	for i := range v {
		v[i] = benchNumeric(strconv.Itoa(i) + ".25") //nolint:gosec // G115: small test values
	}
	return v
}

func benchNumericArray() Array[Numeric] {
	values := benchNumericValues()
	return Array[Numeric]{Elems: values}
}

func BenchmarkArrayDecodeNumeric1000(b *testing.B) {
	c := NumericCodec()
	raw := AppendArray(nil, benchNumericArray(), c)
	var out Array[Numeric]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArrayAppendNumeric1000(b *testing.B) {
	c, a := NumericCodec(), benchNumericArray()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}
