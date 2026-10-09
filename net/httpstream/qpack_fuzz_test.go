package httpstream

import (
	"bytes"
	"testing"
)

// FuzzQPACKDecode covers QPACKDecoder.Decode's own wire format, which is not
// HPACK's: a two-varint section prefix precedes the field lines, indices run
// from zero into a static table with ninety-nine entries rather than HPACK's
// mixed static/dynamic space, and every dynamic-table reference is refused
// outright because this package advertises a capacity of zero. FuzzHPACKDecode
// next door does not exercise any of that.
//
// Unlike HPACK, QPACK's decoder here is stateless across calls — there is no
// dynamic table to poison — so one call per input is enough: what matters is
// that arbitrary bytes never panic and either produce a field list within
// bounds or a well-formed error.
func FuzzQPACKDecode(f *testing.F) {
	// RFC 9204 Appendix B.1's own encoded example: prefix 0,0, then a literal
	// with a name reference to static index 1 (:path), value "/index.html".
	f.Add([]byte{
		0x00, 0x00,
		0x51, 0x0b,
		'/', 'i', 'n', 'd', 'e', 'x', '.', 'h', 't', 'm', 'l',
	})
	// A plain GET built entirely from indexed field lines and name references,
	// the shape a real client sends (see qpack_static_test.go).
	f.Add([]byte{
		0x00, 0x00,
		0xd1,       // indexed, static 17 → :method: GET
		0xd7,       // indexed, static 23 → :scheme: https
		0xc1,       // indexed, static 1  → :path: /
		0x50, 0x0b, // literal, name = static[0] (:authority)
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0xdd, // indexed, static 29 → accept: */*
	})
	// A literal with a literal name, Huffman-coded value: what AppendQPACK
	// itself never writes (it only writes plain literals) but a real QPACK
	// encoder does.
	f.Add(appendQPACK(nil, []Header{
		{Name: []byte("x-trace"), Value: []byte("abc-123")},
		{Name: []byte(":method"), Value: []byte("GET")},
	}))
	// A dynamic-table reference this decoder must refuse rather than resolve,
	// since it advertises a capacity of zero.
	f.Add([]byte{0x00, 0x00, 0x81})       // indexed, T clear: dynamic
	f.Add([]byte{0x00, 0x00, 0x41, 0x00}) // literal with a dynamic name reference
	f.Add([]byte{0x00, 0x00, 0x10})       // post-base reference
	// Edge cases: nothing at all, a prefix with no base, a truncated varint,
	// and an index or length whose prefix claims more than the format's
	// smallest representable byte allows.
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	// An index past the static table's ninety-nine entries.
	f.Add([]byte{0x00, 0x00, 0xff, 0x80, 0x01})
	// A string whose declared length runs past what remains.
	f.Add([]byte{0x00, 0x00, 0x20, 0x7f, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		const maxList = 8 << 10
		d := newQPACKDecoder(nil, maxList)

		fields, err := d.Decode(nil, data)
		if err != nil {
			// A well-formed package error, not a panic. Decode may have
			// appended some fields to dst before hitting the one that failed
			// — the same partial-progress shape ParseH3Frames and the HPACK
			// decoder both have — so nothing here demands an empty result.
			return
		}
		// An empty field name is legal on the wire — QPACK does not forbid it,
		// only httpstream's own buildRequest does, one layer up — so nothing
		// here asserts on it.
		list := 0
		for _, h := range fields {
			list += len(h.Name) + len(h.Value) + 32
		}
		if list > maxList {
			t.Fatalf("a list of %d bytes was returned, over the %d bound", list, maxList)
		}
	})
}

// FuzzQPACKDecodeWithEmptyStaticTable exercises the override that refuses
// every indexed or name-referencing field line — a distinct code path (see
// TestQPACKEmptyOverrideRefusesIndexedFieldLines) that random bytes still
// must not panic or loop against.
func FuzzQPACKDecodeWithEmptyStaticTable(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0xc1})
	f.Add([]byte{0x00, 0x00, 0x51, 0x01, 'v'})
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		const maxList = 8 << 10
		d := newQPACKDecoder([]Header{}, maxList)
		fields, err := d.Decode(nil, data)
		if err != nil {
			return
		}
		// What decoded with no table at all consulted no table: every field
		// line was fully literal. The default-table decoder must therefore
		// read the same bytes to the same fields — a divergence means the
		// empty override changed the meaning of literals, not just refused
		// references.
		list := 0
		for _, h := range fields {
			list += len(h.Name) + len(h.Value) + 32
		}
		if list > maxList {
			t.Fatalf("a list of %d bytes was returned, over the %d bound", list, maxList)
		}
		again, err := newQPACKDecoder(nil, maxList).Decode(nil, data)
		if err != nil {
			t.Fatalf("decoded with an empty table but not with the default one: %v", err)
		}
		if len(again) != len(fields) {
			t.Fatalf("%d fields with an empty table, %d with the default", len(fields), len(again))
		}
		for i := range fields {
			if !bytes.Equal(fields[i].Name, again[i].Name) || !bytes.Equal(fields[i].Value, again[i].Value) {
				t.Fatalf("field %d differs between tables: %q=%q vs %q=%q",
					i, fields[i].Name, fields[i].Value, again[i].Name, again[i].Value)
			}
		}
	})
}
