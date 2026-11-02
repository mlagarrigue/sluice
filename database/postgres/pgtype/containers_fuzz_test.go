package pgtype

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

// The containers read lengths and counts out of the value itself, which is
// where a hostile or misread stream does its damage: a count sized against
// nothing, a length that runs past the end. The contract is the one of
// codec_fuzz_test.go — no panic, every failure an ErrCodec, and an accepted
// value that re-encodes to the bytes it came from.

func fuzzAccepts(t *testing.T, err error, b []byte) bool {
	t.Helper()
	if err != nil && !errors.Is(err, ErrCodec) {
		t.Fatalf("unclassified error %v for %x", err, b)
	}
	return err == nil
}

func FuzzDecodeArray(f *testing.F) {
	f.Add(twoByThree)
	f.Add(AppendArray(nil, Array[int32]{}, Int4Codec()))
	f.Add(AppendArray(nil, Array[int32]{Dims: []ArrayDim{{2, -3}}, Elems: valid[int32](1, 2)}, Int4Codec()))
	f.Add(slices.Concat(words(2, 0, int64(OIDInt4), 65536, 1, 32768, 1), words(4, 1)))

	f.Fuzz(func(t *testing.T, b []byte) {
		a, err := DecodeArray(Array[int32]{}, b, Int4Codec())
		if !fuzzAccepts(t, err, b) {
			return
		}
		// The flags word says whether a NULL is present; the decoder does not
		// hold a 1 to there being one, so it is the one word not compared.
		got := AppendArray(nil, a, Int4Codec())
		if len(got) != len(b) || !bytes.Equal(got[:4], b[:4]) || !bytes.Equal(got[8:], b[8:]) {
			t.Fatalf("DecodeArray(%x) -> %+v -> %x: the value did not survive", b, a, got)
		}
	})
}

func FuzzDecodeRange(f *testing.F) {
	f.Add(slices.Concat([]byte{rangeLBInc}, words(4, 1, 4, 6)))
	f.Add([]byte{rangeEmpty})
	f.Add([]byte{rangeLBInf | rangeUBInf})
	f.Add(slices.Concat([]byte{rangeLBInf | rangeUBInc}, words(4, 5)))

	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := DecodeRange(b, Int4Codec())
		if !fuzzAccepts(t, err, b) {
			return
		}
		if got := AppendRange(nil, r, Int4Codec()); !bytes.Equal(got, b) {
			t.Fatalf("DecodeRange(%x) -> %+v -> %x: the value did not survive", b, r, got)
		}
	})
}

func FuzzDecodeMultirange(f *testing.F) {
	f.Add(AppendMultirange(nil, Multirange[int32]{
		{Lower: RangeBound[int32]{Value: 1, Inclusive: true}, Upper: RangeBound[int32]{Value: 3}},
		{Empty: true},
	}, Int4Codec()))
	f.Add(words(0))

	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeMultirange(nil, b, Int4Codec())
		if !fuzzAccepts(t, err, b) {
			return
		}
		if got := AppendMultirange(nil, m, Int4Codec()); !bytes.Equal(got, b) {
			t.Fatalf("DecodeMultirange(%x) -> %+v -> %x: the value did not survive", b, m, got)
		}
	})
}

func FuzzDecodeRecord(f *testing.F) {
	f.Add(AppendRecord(nil, Record{
		{OID: OIDInt8, Value: AppendInt8(nil, 42)},
		{OID: OIDText},
		{OID: OIDText, Value: []byte{}},
	}))
	f.Add(words(0))

	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := DecodeRecord(nil, b)
		if !fuzzAccepts(t, err, b) {
			return
		}
		if got := AppendRecord(nil, r); !bytes.Equal(got, b) {
			t.Fatalf("DecodeRecord(%x) -> %+v -> %x: the value did not survive", b, r, got)
		}
	})
}
