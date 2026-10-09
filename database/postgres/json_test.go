package postgres

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// Where these vectors come from, and why that matters.
//
// The two golden vectors this repository got wrong were written from memory —
// an IEEE 754 mantissa and a microsecond count across twenty-four years, both
// times the encoder being right and the vector being a second guess. These are
// not derived by eye and not derived by this package. They are what PostgreSQL
// 15 itself produced, through its own send functions, which are the exact code
// the server uses to put a value on the wire:
//
//	SELECT encode(jsonb_send('{"a":1}'::jsonb), 'hex');
//	SELECT encode(interval_send('1 mon'::interval), 'hex');
//	SELECT encode(inet_send('192.168.1.1/24'::inet), 'hex');
//
// That is an oracle this package cannot agree with by accident, which is the
// property a round trip does not have: an encoder and a decoder that share a
// misreading agree with each other forever.

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad test vector %q: %v", s, err)
	}
	return b
}

// # jsonb

func TestJSONBAgainstServerVectors(t *testing.T) {
	tests := []struct {
		json string
		hex  string
	}{
		{`{"a": 1}`, "017b2261223a20317d"},
		{`null`, "016e756c6c"},
		{`[]`, "015b5d"},
		{`"x"`, "01227822"},
		{`123`, "01313233"},
	}
	for _, tc := range tests {
		t.Run(tc.json, func(t *testing.T) {
			want := mustHex(t, tc.hex)
			if got := AppendJSONB(nil, []byte(tc.json)); !bytes.Equal(got, want) {
				t.Errorf("AppendJSONB = %x, want %x", got, want)
			}
			back, err := DecodeJSONB(want)
			if err != nil {
				t.Fatalf("DecodeJSONB: %v", err)
			}
			if string(back) != tc.json {
				t.Errorf("DecodeJSONB = %q, want %q", back, tc.json)
			}
		})
	}
}

// The one byte between the two types, shown rather than described: the server
// produced both of these for the same input.
func TestJSONAndJSONBDifferByTheVersionByte(t *testing.T) {
	const input = `{"a":1}`
	jsonVector := mustHex(t, "7b2261223a317d")      // json_send
	jsonbVector := mustHex(t, "017b2261223a20317d") // jsonb_send, normalised

	if got := AppendJSON(nil, []byte(input)); !bytes.Equal(got, jsonVector) {
		t.Errorf("AppendJSON = %x, want %x", got, jsonVector)
	}
	if jsonbVector[0] != JSONBVersion {
		t.Fatalf("the jsonb vector does not start with the version byte: %x", jsonbVector)
	}
	// Reading a jsonb as a json is the quiet failure: it returns valid-looking
	// text with a stray byte in front, which travels a long way before
	// anything notices.
	if got := DecodeJSON(jsonbVector); got[0] == '{' {
		t.Error("DecodeJSON stripped something; it must return the bytes untouched")
	}
}

func TestDecodeJSONBRefusals(t *testing.T) {
	if _, err := DecodeJSONB(nil); err == nil {
		t.Error("DecodeJSONB accepted an empty value")
	}
	// A version this package has not seen changes what follows it. Reading it
	// as version 1 would return text that is plausible and wrong.
	_, err := DecodeJSONB([]byte{2, '{', '}'})
	if err == nil || !strings.Contains(err.Error(), "version byte is 2") {
		t.Errorf("DecodeJSONB(version 2) = %v, want a refusal naming the version", err)
	}
}
