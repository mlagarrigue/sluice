package pgtype

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
)

// # tsvector and tsquery

func TestTextSearchAgainstHandWrittenVectors(t *testing.T) {
	vec := AppendTSVector(nil, []Lexeme{{Word: "a", Positions: []TSPosition{1, 0xc002}}, {Word: "bc"}})
	if got, want := hex.EncodeToString(vec), "00000002"+"6100"+"0002"+"0001"+"c002"+"626300"+"0000"; got != want {
		t.Errorf("tsvector = %s, want %s", got, want)
	}
	q := AppendTSQuery(nil, []TSQueryItem{{Operator: TSPhrase, Distance: 2}, {Word: "a", Weight: 8, Prefix: true}, {Operator: TSNot}, {Word: "b"}})
	if got, want := hex.EncodeToString(q), "00000004"+"02040002"+"01080161"+"00"+"0201"+"01000062"+"00"; got != want {
		t.Errorf("tsquery = %s, want %s", got, want)
	}
	if p := TSPosition(0xc002); p.Pos() != 2 || p.Weight() != 'A' {
		t.Errorf("position %#x = %d%c, want 2A", uint16(p), p.Pos(), p.Weight())
	}
}

func TestTextSearchRoundTrips(t *testing.T) {
	lex := []Lexeme{{Word: "cat", Positions: []TSPosition{3, 0x4007}}, {Word: "fat"}}
	got, err := DecodeTSVector(nil, AppendTSVector(nil, lex))
	if err != nil || len(got) != 2 || got[0].Word != "cat" || !slices.Equal(got[0].Positions, lex[0].Positions) || got[1].Word != "fat" || got[1].Positions != nil {
		t.Errorf("tsvector = %v, %v", got, err)
	}
	items := []TSQueryItem{{Operator: TSAnd}, {Word: "fat", Weight: 3}, {Operator: TSOr}, {Word: "cat", Prefix: true}, {Word: "rat"}}
	back, err := DecodeTSQuery(nil, AppendTSQuery(nil, items))
	if err != nil || !slices.Equal(back, items) {
		t.Errorf("tsquery = %v, %v", back, err)
	}
}

// Counts past their bytes, a word without its NUL, a node type or operator
// the server never writes, a prefix flag other than 0 or 1: ErrCodec.
func TestTextSearchRefusesMalformedValues(t *testing.T) {
	for name, b := range map[string][]byte{
		"tsvector count past its bytes": {0x7f, 0xff, 0xff, 0xff, 'a', 0, 0, 0},
		"tsvector word without NUL":     {0, 0, 0, 1, 'a', 'b', 'c'},
		"tsvector positions past bytes": {0, 0, 0, 1, 'a', 0, 0, 9},
	} {
		if _, err := DecodeTSVector(nil, b); !errors.Is(err, ErrCodec) {
			t.Errorf("%s: %v, want ErrCodec", name, err)
		}
	}
	for name, b := range map[string][]byte{
		"tsquery count past its bytes": {0x7f, 0xff, 0xff, 0xff, 2, 2},
		"tsquery node type 3":          {0, 0, 0, 1, 3, 0},
		"tsquery operator 5":           {0, 0, 0, 1, 2, 5},
		"tsquery prefix 2":             {0, 0, 0, 1, 1, 0, 2, 'a', 0},
		"tsquery operand without NUL":  {0, 0, 0, 1, 1, 0, 0, 'a'},
		"tsquery phrase cut short":     {0, 0, 0, 1, 2, 4, 0},
	} {
		if _, err := DecodeTSQuery(nil, b); !errors.Is(err, ErrCodec) {
			t.Errorf("%s: %v, want ErrCodec", name, err)
		}
	}
}

func FuzzRoundTripTSQuery(f *testing.F) {
	f.Add(AppendTSQuery(nil, []TSQueryItem{{Operator: TSPhrase, Distance: 1}, {Word: "a"}, {Word: "b", Prefix: true}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		items, err := DecodeTSQuery(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendTSQuery(nil, items); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, items, got)
		}
	})
}

func FuzzRoundTripTSVector(f *testing.F) {
	f.Add(AppendTSVector(nil, []Lexeme{{Word: "a", Positions: []TSPosition{1}}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		lex, err := DecodeTSVector(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendTSVector(nil, lex); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, lex, got)
		}
	})
}
