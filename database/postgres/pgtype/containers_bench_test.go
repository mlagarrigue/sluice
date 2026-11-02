package pgtype

import "testing"

// benchArrayDecoder selects the decoder BenchmarkDecodeInt4Array1000
// measures. The paired A/B script compares two binaries under one benchmark
// name, so the three ways of reading an int4[] are one benchmark built three
// times rather than three benchmarks:
//
//	go test -c -o bench.before ./database/postgres/pgtype
//	go test -c -o bench.after -ldflags \
//	    -X=github.com/mlagarrigue/sluice/database/postgres/pgtype.benchArrayDecoder=generic \
//	    ./database/postgres/pgtype
//
// "named" is DecodeInt4Array, written by hand; "skeleton" is the unexported
// generic loop the other one-dimensional arrays share, which the named one
// would become were it a wrapper; "generic" is DecodeArray over Int4Codec,
// which also returns the dimensions and a Null per element.
var benchArrayDecoder = "named"

func BenchmarkDecodeInt4Array1000(b *testing.B) {
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i*7919 - 500_000) //nolint:gosec // G115: a small test value
	}
	raw := AppendInt4Array(nil, values)
	plain := make([]int32, 0, len(values))
	arr := Array[int32]{Dims: make([]ArrayDim, 0, 1), Elems: make([]Null[int32], 0, len(values))}
	var err error
	b.ReportAllocs()
	switch benchArrayDecoder {
	case "named":
		for b.Loop() {
			plain, err = DecodeInt4Array(plain, raw)
		}
	case "skeleton":
		for b.Loop() {
			plain, err = decodeArrayOf(plain, raw, OIDInt4, DecodeInt4)
		}
	case "generic":
		for b.Loop() {
			arr, err = DecodeArray(arr, raw, Int4Codec())
		}
	default:
		b.Fatalf("benchArrayDecoder %q is none of named, skeleton, generic", benchArrayDecoder)
	}
	if err != nil {
		b.Fatal(err)
	}
}
