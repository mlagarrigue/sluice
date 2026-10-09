package httpstream

// How a table with no checkable algebra gets checked anyway. The Huffman code
// next door breaks the Kraft equality if one code point is wrong; this table
// would just decode into plausible wrong headers. So it is pinned three ways:
// by shape, by the one encoded example RFC 9204 itself publishes, and by a
// fixture of the field section a real client sends. A foreign peer in an
// interop run remains the missing proof, the same sentence ADR 0005 writes
// about the QUIC transport.

import (
	"strings"
	"testing"
)

// The table's shape: the count RFC 9204 Appendix A states, and the syntax
// every entry must satisfy — a transposed or duplicated line changes the
// count, and a typo in a name usually breaks the token rule.
func TestQPACKStaticTableShape(t *testing.T) {
	if len(qpackStaticTable) != 99 {
		t.Fatalf("%d entries, RFC 9204 Appendix A has 99", len(qpackStaticTable))
	}
	for i, e := range qpackStaticTable {
		name := strings.TrimPrefix(e.Name, ":")
		if !isToken([]byte(name)) {
			t.Errorf("entry %d: name %q is not a token", i, e.Name)
		}
		if strings.ToLower(e.Name) != e.Name {
			t.Errorf("entry %d: name %q is not lowercase", i, e.Name)
		}
		// RFC 9110 §5.5's field-value rule, all of it: no control octet
		// (HTAB aside) or DEL, and no whitespace at either end — a
		// transcribed "max-age=0 " decodes as a value no client sends.
		if !validFieldValue([]byte(e.Value)) {
			t.Errorf("entry %d: value %q holds a control character", i, e.Value)
		}
		if v := strings.Trim(e.Value, " \t"); v != e.Value {
			t.Errorf("entry %d: value %q has surrounding whitespace", i, e.Value)
		}
	}
}

// The indices the RFC's prose and every deployed client agree on, transcribed
// here a second time. Two transcriptions of the same source can share a
// mistake, which is what the encoded fixtures below are for; what this
// catches is the likelier slip — an off-by-one shifting the whole table.
func TestQPACKStaticTablePinnedEntries(t *testing.T) {
	pins := []struct {
		index       int
		name, value string
	}{
		{0, ":authority", ""},
		{1, ":path", "/"},
		{2, "age", "0"},
		{14, "set-cookie", ""},
		{15, ":method", "CONNECT"},
		{17, ":method", "GET"},
		{21, ":method", "PUT"},
		{22, ":scheme", "http"},
		{23, ":scheme", "https"},
		{24, ":status", "103"},
		{25, ":status", "200"},
		{28, ":status", "503"},
		{29, "accept", "*/*"},
		{31, "accept-encoding", "gzip, deflate, br"},
		{46, "content-type", "application/json"},
		{52, "content-type", "text/html; charset=utf-8"},
		{55, "range", "bytes=0-"},
		{62, "x-xss-protection", "1; mode=block"},
		{63, ":status", "100"},
		{71, ":status", "500"},
		{72, "accept-language", ""},
		{84, "authorization", ""},
		{91, "purpose", "prefetch"},
		{95, "user-agent", ""},
		{98, "x-frame-options", "sameorigin"},
	}
	for _, p := range pins {
		e := qpackStaticTable[p.index]
		if e.Name != p.name || e.Value != p.value {
			t.Errorf("entry %d = %q: %q, want %q: %q", p.index, e.Name, e.Value, p.name, p.value)
		}
	}
}

// RFC 9204 Appendix B.1's encoded field section, byte for byte. The first
// field line is a literal with a name reference to static index 1, so the RFC
// itself pins that entry to :path — the one row of the table the spec proves
// rather than lists.
func TestQPACKStaticTableAgainstRFC9204Example(t *testing.T) {
	block := []byte{
		0x00, 0x00, // prefix: required insert count 0, base 0
		0x51, 0x0b, // literal, name = static[1], value of 11 bytes
		'/', 'i', 'n', 'd', 'e', 'x', '.', 'h', 't', 'm', 'l',
	}
	got, err := newQPACKDecoder(nil, 0).Decode(nil, block)
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	if len(got) != 1 || string(got[0].Name) != ":path" || string(got[0].Value) != "/index.html" {
		t.Fatalf("fields = %v, want :path /index.html", got)
	}
}

// The field section a real client sends for a plain GET, hand-encoded from
// the representations RFC 9204 §4.5 defines. This is what shipping the table
// is for: every line here either indexes the static table or names an entry
// of it, and before the table shipped this section was refused wholesale.
func TestQPACKStaticTableResolvesAClientFieldSection(t *testing.T) {
	block := []byte{
		0x00, 0x00, // prefix
		0xd1,       // indexed, static 17: 0xc0|17     → :method: GET
		0xd7,       // indexed, static 23: 0xc0|23     → :scheme: https
		0xc1,       // indexed, static 1:  0xc0|1      → :path: /
		0x50, 0x0b, // literal, name = static[0] (:authority), 11-byte value
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0x5f, 0x50, // literal, name = static[15+80=95] (user-agent)
		0x0a, 'c', 'u', 'r', 'l', '/', '8', '.', '9', '.', '1',
		0xdd, // indexed, static 29: 0xc0|29           → accept: */*
	}

	want := []struct{ name, value string }{
		{":method", "GET"},
		{":scheme", "https"},
		{":path", "/"},
		{":authority", "example.com"},
		{"user-agent", "curl/8.9.1"},
		{"accept", "*/*"},
	}
	got, err := newQPACKDecoder(nil, 0).Decode(nil, block)
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("%d fields, want %d", len(got), len(want))
	}
	for i, w := range want {
		if string(got[i].Name) != w.name || string(got[i].Value) != w.value {
			t.Errorf("field %d = %q: %q, want %q: %q", i, got[i].Name, got[i].Value, w.name, w.value)
		}
	}
}
