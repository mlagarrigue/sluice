package pgtype

import (
	"errors"
	"reflect"
	"slices"
	"testing"
)

// The array tests below once ran against one exported function per type and
// shape (AppendInt8Array, DecodeNullableTextArray…). Those are gone: an
// array is written and read by AppendArray and DecodeArray alone. These
// helpers put the same vectors through the two, in the shapes the tests were
// written in — a []T with no NULL, or a []Null[T].

// errNotASlice is what decodeSlice returns for an array that a []T cannot
// hold. It is not ErrCodec on purpose: a test that wants the decoder's own
// refusal fails on it instead of passing on the helper's.
var errNotASlice = errors.New("test helper: the array has a NULL or is not one dimension numbered from 1")

// appendSlice writes values as a one-dimensional array with no NULL.
func appendSlice[T any](c Codec[T], dst []byte, values []T) []byte {
	return AppendArray(dst, Array[T]{Elems: values}, c)
}

// decodeSlice reads a one-dimensional array with no NULL into dst's storage.
func decodeSlice[T any](c Codec[T], dst []T, b []byte) ([]T, error) {
	a, err := DecodeArray(Array[T]{Elems: dst}, b, c)
	if err != nil {
		return a.Elems, err
	}
	if len(a.Nulls) > 0 || len(a.Dims) > 1 || (len(a.Dims) == 1 && a.Dims[0].Lower != 1) {
		return a.Elems[:0], errNotASlice
	}
	return a.Elems, nil
}

// appendNulls writes values as a one-dimensional array, an invalid Null as a
// NULL element.
func appendNulls[T any](c Codec[T], dst []byte, values []Null[T]) []byte {
	a := Array[T]{Elems: make([]T, len(values)), Nulls: make([]bool, len(values))}
	for i, v := range values {
		a.Elems[i], a.Nulls[i] = v.Value, !v.Valid
	}
	return AppendArray(dst, a, c)
}

// decodeNulls reads an array into dst's storage, a NULL element as an
// invalid Null.
func decodeNulls[T any](c Codec[T], dst []Null[T], b []byte) ([]Null[T], error) {
	a, err := DecodeArray(Array[T]{}, b, c)
	dst = dst[:0]
	if err != nil {
		return dst, err
	}
	for i, v := range a.Elems {
		dst = append(dst, Null[T]{Value: v, Valid: i >= len(a.Nulls) || !a.Nulls[i]})
	}
	return dst, nil
}

// sliceDecoder and nullsDecoder are the two decoders as values, for the
// tests that take a decoder as an argument.
func sliceDecoder[T any](c Codec[T]) func([]T, []byte) ([]T, error) {
	return func(dst []T, b []byte) ([]T, error) { return decodeSlice(c, dst, b) }
}

func nullsDecoder[T any](c Codec[T]) func([]Null[T], []byte) ([]Null[T], error) {
	return func(dst []Null[T], b []byte) ([]Null[T], error) { return decodeNulls(c, dst, b) }
}

// wantNulls checks that DecodeArray reads raw with its NULL elements marked
// in Nulls, as want says, each with the zero value in Elems. The named
// decoders of a []T refused such an array, having nowhere to put a NULL;
// an Array has.
func wantNulls[T any](t *testing.T, name string, c Codec[T], raw []byte, want []bool) {
	t.Helper()
	a, err := DecodeArray(Array[T]{}, raw, c)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if !slices.Equal(a.Nulls, want) || len(a.Elems) != len(want) {
		t.Fatalf("%s: %d elements, Nulls %v, want %v", name, len(a.Elems), a.Nulls, want)
	}
	for i, null := range want {
		if null && !reflect.ValueOf(a.Elems[i]).IsZero() {
			t.Errorf("%s: NULL element %d holds %v, want the zero value", name, i, a.Elems[i])
		}
	}
}

// fuzzArrayRoundTrip is the body of the array fuzz targets: whatever the
// bytes, DecodeArray returns or refuses with ErrCodec, and an array it
// accepts — NULLs, several dimensions, a lower bound other than 1 included,
// which decodeSlice would refuse — decodes again to the same elements,
// NULLs and dimensions once AppendArray has written it. Idempotence rather
// than byte equality: the header carries a flags word saying whether NULLs
// may be present, which a re-encoding is allowed to normalise.
func fuzzArrayRoundTrip[T comparable](t *testing.T, c Codec[T], b []byte) {
	t.Helper()
	a, err := DecodeArray(Array[T]{}, b, c)
	if err != nil {
		if !errors.Is(err, ErrCodec) {
			t.Fatalf("unclassified error %v for %x", err, b)
		}
		return
	}
	if len(a.Elems) == 0 {
		return // the empty array is zero dimensions, not a header to reproduce
	}
	again, err := DecodeArray(Array[T]{}, AppendArray(nil, a, c), c)
	if err != nil {
		t.Fatalf("DecodeArray(%x) wrote an array that then failed to decode: %v", b, err)
	}
	null := func(a Array[T], i int) bool { return i < len(a.Nulls) && a.Nulls[i] }
	same := len(again.Elems) == len(a.Elems) && slices.Equal(again.Dims, a.Dims)
	for i := 0; same && i < len(a.Elems); i++ {
		same = null(again, i) == null(a, i) && (null(a, i) || again.Elems[i] == a.Elems[i])
	}
	if !same {
		t.Fatalf("DecodeArray(%x) is not idempotent: %+v then %+v", b, a, again)
	}
}
