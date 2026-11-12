package pgtype

import "testing"

// BenchmarkDecodePointArray1000 is a point[] of a thousand elements through
// DecodeArray: the fixed-size geometric decoders allocate nothing per
// element.
func BenchmarkDecodePointArray1000(b *testing.B) {
	elems := make([]Point, 1000)
	for i := range elems {
		elems[i] = Point{X: float64(i), Y: -float64(i) / 3}
	}
	raw := AppendArray(nil, Array[Point]{Elems: elems}, PointCodec())
	arr := Array[Point]{Dims: make([]ArrayDim, 0, 1), Elems: make([]Point, 0, len(elems))}
	var err error
	b.ReportAllocs()
	for b.Loop() {
		arr, err = DecodeArray(arr, raw, PointCodec())
	}
	if err != nil {
		b.Fatal(err)
	}
	if len(arr.Elems) != len(elems) || arr.Elems[999] != elems[999] {
		b.Fatalf("decoded %d points, the last %v", len(arr.Elems), arr.Elems[len(arr.Elems)-1])
	}
}
