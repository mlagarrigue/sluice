package pgtype

import (
	"bytes"
	"errors"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
)

// hstore_send's order: shorter keys first, then by byte. The map's order is
// Go's to choose, and the bytes must not follow it.
func TestHstoreMatchesHstoreSend(t *testing.T) {
	h := Hstore{"bb": {}, "a": {Value: "1", Valid: true}, "c": {Value: "", Valid: true}}
	want := slices.Concat(words(3),
		words(1), []byte("a"), words(1), []byte("1"),
		words(1), []byte("c"), words(0),
		words(2), []byte("bb"), words(-1))
	for range 8 { // map iteration order varies from one range to the next
		if got := AppendHstore(nil, h); !bytes.Equal(got, want) {
			t.Fatalf("AppendHstore = % x\n        want % x", got, want)
		}
	}
	got, err := DecodeHstore(want)
	if err != nil || !maps.Equal(got, h) {
		t.Errorf("DecodeHstore = %v, %v", got, err)
	}
	empty, err := DecodeHstore(AppendHstore(nil, nil))
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty hstore = %#v, %v", empty, err)
	}
}

func TestDecodeHstoreRefusesWhatHstoreSendNeverWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"short", []byte{0}, "want at least 4"},
		{"negative count", words(-1), "declares -1 pairs"},
		{"more pairs than bytes", words(2, 0, 0), "declares 2 pairs in 8 bytes"},
		{"NULL key", words(1, -1, 0), "key 0 is NULL"},
		{"key cut short", words(1, 9, 0), "hstore key: "},
		{"value cut short", slices.Concat(words(1, 1), []byte("a"), words(5), []byte("x")), "hstore value: "},
		{
			"key twice", slices.Concat(words(2, 1), []byte("a"), words(-1, 1), []byte("a"), words(-1)),
			`key "a" appears twice`,
		},
		{"bytes after the last pair", slices.Concat(words(1, 1), []byte("a"), words(-1), []byte{0}), "after the last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := DecodeHstore(tc.b)
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) || h != nil {
				t.Fatalf("got %v, err = %v, want ErrCodec saying %q", h, err, tc.want)
			}
		})
	}
}

func TestLtree(t *testing.T) {
	b := AppendLtree(nil, "top.science")
	if want := append([]byte{1}, "top.science"...); !bytes.Equal(b, want) {
		t.Fatalf("AppendLtree = % x, want % x", b, want)
	}
	if got, err := DecodeLtree(b); err != nil || got != "top.science" {
		t.Errorf("DecodeLtree = %q, %v", got, err)
	}
	for _, bad := range [][]byte{nil, {2, 'a'}} {
		if _, err := DecodeLtree(bad); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeLtree(% x): err = %v", bad, err)
		}
	}
}

func TestVector(t *testing.T) {
	in := []float32{1.5, -2, 0, float32(math.Copysign(0, -1))}
	b := AppendVector(nil, in)
	want := slices.Concat([]byte{0, 4, 0, 0},
		AppendFloat4(nil, 1.5), AppendFloat4(nil, -2), AppendFloat4(nil, 0), AppendFloat4(nil, in[3]))
	if !bytes.Equal(b, want) {
		t.Fatalf("AppendVector = % x\n        want % x", b, want)
	}
	got, err := DecodeVector(make([]float32, 0, 1), b)
	if err != nil || !bytes.Equal(AppendVector(nil, got), b) {
		t.Errorf("DecodeVector = %v, %v", got, err)
	}
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"short", []byte{0, 1}, "want at least 4"},
		{"no dimension", []byte{0, 0, 0, 0}, "0 dimensions"},
		{"negative dimension", []byte{0xff, 0xff, 0, 0}, "-1 dimensions"},
		{"past the maximum", []byte{0x3e, 0x81, 0, 0}, "16001 dimensions"},
		{"unused word set", slices.Concat([]byte{0, 1, 0, 1}, AppendFloat4(nil, 1)), "unused word is 1"},
		{"cut short", []byte{0, 2, 0, 0, 0, 0, 0, 0}, "is 8 bytes, want 12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeVector(nil, tc.b)
			if !errors.Is(err, ErrCodec) || !strings.Contains(err.Error(), tc.want) || len(got) != 0 {
				t.Fatalf("got %v, err = %v, want ErrCodec saying %q", got, err, tc.want)
			}
		})
	}
}

// An extension's codec carries the OIDs it was given, which is all that
// tells its array header from another's.
func TestExtensionCodecsCarryTheirOIDs(t *testing.T) {
	const oid, arr = 16400, 16401
	for name, got := range map[string][2]uint32{
		"citext":    {CitextCodec(oid, arr).OID, CitextCodec(oid, arr).ArrayOID},
		"hstore":    {HstoreCodec(oid, arr).OID, HstoreCodec(oid, arr).ArrayOID},
		"ltree":     {LtreeCodec(oid, arr).OID, LtreeCodec(oid, arr).ArrayOID},
		"lquery":    {LqueryCodec(oid, arr).OID, LqueryCodec(oid, arr).ArrayOID},
		"ltxtquery": {LtxtqueryCodec(oid, arr).OID, LtxtqueryCodec(oid, arr).ArrayOID},
		"vector":    {VectorCodec(oid, arr).OID, VectorCodec(oid, arr).ArrayOID},
	} {
		if got != [2]uint32{oid, arr} {
			t.Errorf("%s codec has OIDs %v", name, got)
		}
	}
	check := func(name string, ok bool) {
		if !ok {
			t.Errorf("%s codec does not round-trip", name)
		}
	}
	c := CitextCodec(oid, arr)
	v, err := c.Decode(c.Append(nil, "Ab"))
	check("citext", err == nil && v == "Ab")
	l := LqueryCodec(oid, arr)
	q, err := l.Decode(l.Append(nil, "*.a"))
	check("lquery", err == nil && q == "*.a")
	x := LtxtqueryCodec(oid, arr)
	q, err = x.Decode(x.Append(nil, "a & b"))
	check("ltxtquery", err == nil && q == "a & b")
	vc := VectorCodec(oid, arr)
	vec, err := vc.Decode(vc.Append(nil, []float32{1}))
	check("vector", err == nil && slices.Equal(vec, []float32{1}))
}

func FuzzDecodeHstore(f *testing.F) {
	f.Add(AppendHstore(nil, Hstore{"a": {Value: "1", Valid: true}, "bb": {}}))
	f.Add(words(0))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := DecodeHstore(b)
		if !fuzzAccepts(t, err, b) {
			return
		}
		// Accepted pairs in another order re-encode in the server's; the
		// count and the pairs survive, not the order.
		again, err := DecodeHstore(AppendHstore(nil, h))
		if err != nil || !maps.Equal(again, h) {
			t.Fatalf("DecodeHstore(%x) -> %v -> %v, %v", b, h, again, err)
		}
	})
}

func FuzzDecodeVector(f *testing.F) {
	f.Add(AppendVector(nil, []float32{1, -2}))
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeVector(nil, b)
		if !fuzzAccepts(t, err, b) {
			return
		}
		if got := AppendVector(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("DecodeVector(%x) -> %v -> %x", b, v, got)
		}
	})
}
