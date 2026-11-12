package pgtype

import "testing"

// The array benchmarks with NULLs: the same thousand int4, int8 and text
// values as the benchmarks without (array_types_bench_test.go), one element
// in ten NULL. An array with a NULL leaves the element loops of
// array_fast.go for the general path, Codec.Append and Decode per element,
// and the revision condition of that choice is a benchmark with NULLs that
// suffers from it (docs/benchmarks.md, "Arrays, one way to write them").

// withNulls marks one element in ten NULL.
func withNulls[T any](a Array[T]) Array[T] {
	a.Nulls = make([]bool, len(a.Elems))
	for i := range a.Nulls {
		a.Nulls[i] = i%10 == 3
	}
	return a
}

func benchDecodeNulls[T any](b *testing.B, a Array[T], c Codec[T]) {
	b.Helper()
	raw := AppendArray(nil, withNulls(a), c)
	var out Array[T]
	var err error
	b.ReportAllocs()
	for b.Loop() {
		out, err = DecodeArray(out, raw, c)
	}
	if err != nil {
		b.Fatal(err)
	}
	if len(out.Nulls) != len(out.Elems) || !out.Nulls[3] || out.Nulls[4] {
		b.Fatal("the NULLs did not survive the round trip")
	}
}

func benchAppendNulls[T any](b *testing.B, a Array[T], c Codec[T]) {
	b.Helper()
	a = withNulls(a)
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendArray(buf[:0], a, c)
	}
}

func BenchmarkArrayDecodeInt41000Nulls(b *testing.B) {
	benchDecodeNulls(b, benchInt4Array(), Int4Codec())
}

func BenchmarkArrayAppendInt41000Nulls(b *testing.B) {
	benchAppendNulls(b, benchInt4Array(), Int4Codec())
}

func BenchmarkArrayDecodeInt81000Nulls(b *testing.B) {
	benchDecodeNulls(b, benchInt8Array(), Int8Codec())
}

func BenchmarkArrayAppendInt81000Nulls(b *testing.B) {
	benchAppendNulls(b, benchInt8Array(), Int8Codec())
}

func BenchmarkArrayDecodeText1000Nulls(b *testing.B) {
	benchDecodeNulls(b, benchTextArray(), TextCodec())
}

func BenchmarkArrayAppendText1000Nulls(b *testing.B) {
	benchAppendNulls(b, benchTextArray(), TextCodec())
}
