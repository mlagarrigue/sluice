package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// validJSONSeeds are the edges of the grammar and of the scanner's
// leniencies, each on the side json.Valid puts it.
var validJSONSeeds = []string{
	``, ` `, `0`, `-0`, `01`, `-`, `1.`, `.5`, `1.5e`, `1e+`, `1E-07`, `-12.5e+3`,
	`true`, `tru`, `truex`, `false`, `null`, `nul`, `"`, `""`, `"é"`, `"\u00G0"`,
	`"\x"`, `"\/\b\f\n\r\t\"\\"`, "\"\x1f\"", "\"\x7f\xff\xfe\"", `[]`, `[ ]`, `[,]`,
	`[1,]`, `[1 2]`, `{}`, `{ }`, `{"a"}`, `{"a":}`, `{"a":1,}`, `{"a":1 "b":2}`,
	`{1:2}`, `{"a":[{"b":null}],"c":{}}`, " \t\r\n[1]\n ", `[1]]`, `[1]x`, `{"a":1}}`,
	`{"from":7,"limit":2,"items":[{"id":8,"total":56,"label":"order-8"},{"id":9,"total":63,"label":"order-9"}]}`,
	strings.Repeat("[", maxNesting) + strings.Repeat("]", maxNesting),
	strings.Repeat("[", maxNesting+1) + strings.Repeat("]", maxNesting+1),
	strings.Repeat(`{"a":`, maxNesting) + "1" + strings.Repeat("}", maxNesting),
	strings.Repeat(`{"a":`, maxNesting) + "{}" + strings.Repeat("}", maxNesting),
}

// TestValidJSONAgreesOnTheEdges: on every seed, the answer json.Valid gives.
func TestValidJSONAgreesOnTheEdges(t *testing.T) {
	for _, s := range validJSONSeeds {
		if got, want := validJSON([]byte(s)), json.Valid([]byte(s)); got != want {
			t.Errorf("%.60q: validJSON %v, json.Valid %v", s, got, want)
		}
	}
}

// FuzzValidJSON holds validJSON to json.Valid on any input: the cheap
// control of a list (§4) must refuse exactly what encoding/json refuses.
func FuzzValidJSON(f *testing.F) {
	for _, s := range validJSONSeeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if got, want := validJSON(b), json.Valid(b); got != want {
			t.Fatalf("%q: validJSON %v, json.Valid %v", b, got, want)
		}
	})
}

// BenchmarkValidJSON: a list page as the trivial server serves it, through
// both validators.
func BenchmarkValidJSON(b *testing.B) {
	var page bytes.Buffer
	page.WriteString(`{"from":1,"limit":1000,"items":[`)
	for i := range 1000 {
		if i > 0 {
			page.WriteByte(',')
		}
		page.WriteString(`{"id":123456,"total":864192,"label":"order-123456"}`)
	}
	page.WriteString("]}")
	doc := page.Bytes()
	b.Run("validJSON", func(b *testing.B) {
		b.SetBytes(int64(len(doc)))
		for b.Loop() {
			if !validJSON(doc) {
				b.Fatal("refused")
			}
		}
	})
	b.Run("json.Valid", func(b *testing.B) {
		b.SetBytes(int64(len(doc)))
		for b.Loop() {
			if !json.Valid(doc) {
				b.Fatal("refused")
			}
		}
	})
}
