package httpstream

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decoded(t *testing.T, d *hpackDecoder, block []byte) []string {
	t.Helper()
	fields, err := d.Decode(nil, block)
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, string(f.Name)+": "+string(f.Value))
	}
	return out
}

func equal(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] { //nolint:gosec // G602: the lengths are checked equal above
			t.Fatalf("field %d = %q, want %q", i, got[i], want[i]) //nolint:gosec // G602: same
		}
	}
}

// RFC 7541 Appendix C, the vectors the format is specified by. They are the
// only check that matters on the two transcribed tables: a Huffman code or a
// static index that is one off decodes into plausible wrong strings, and
// nothing but the RFC's own bytes would catch it.
func TestHPACKAppendixCVectors(t *testing.T) {
	t.Run("C.2.1 literal with incremental indexing", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		got := decoded(t, d, unhex(t, "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572"))
		equal(t, got, []string{"custom-key: custom-header"})
		// It was indexed, so the dynamic table now holds it.
		if d.n != 1 || string(d.entry(0).Name) != "custom-key" {
			t.Errorf("the dynamic table holds %d entries", d.n)
		}
	})

	t.Run("C.2.2 literal without indexing", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		got := decoded(t, d, unhex(t, "040c 2f73 616d 706c 652f 7061 7468"))
		equal(t, got, []string{":path: /sample/path"})
		if d.n != 0 {
			t.Error("a field without indexing entered the dynamic table")
		}
	})

	t.Run("C.2.3 never indexed", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		got := decoded(t, d, unhex(t, "1008 7061 7373 776f 7264 0673 6563 7265 74"))
		equal(t, got, []string{"password: secret"})
		if d.n != 0 {
			t.Error("a never-indexed field entered the dynamic table")
		}
	})

	t.Run("C.2.4 indexed", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		equal(t, decoded(t, d, unhex(t, "82")), []string{":method: GET"})
	})

	// C.3: a request sequence without Huffman, which exercises the dynamic
	// table being built and then read back.
	t.Run("C.3 request sequence", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		equal(t, decoded(t, d, unhex(t, "8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d")),
			[]string{":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com"})
		equal(t, decoded(t, d, unhex(t, "8286 84be 5808 6e6f 2d63 6163 6865")),
			[]string{":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com", "cache-control: no-cache"})
		equal(t, decoded(t, d, unhex(t, "8287 85bf 400a 6375 7374 6f6d 2d6b 6579 0c63 7573 746f 6d2d 7661 6c75 65")),
			[]string{":method: GET", ":scheme: https", ":path: /index.html", ":authority: www.example.com", "custom-key: custom-value"})
	})

	// C.4: the same, Huffman coded. This is what validates the 256-entry
	// table: www.example.com is f1e3c2e5f23a6ba0ab90f4ff and nothing else.
	t.Run("C.4 request sequence, Huffman coded", func(t *testing.T) {
		d := newHPACKDecoder(0, 0)
		equal(t, decoded(t, d, unhex(t, "8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff")),
			[]string{":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com"})
		equal(t, decoded(t, d, unhex(t, "8286 84be 5886 a8eb 1064 9cbf")),
			[]string{":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com", "cache-control: no-cache"})
		equal(t, decoded(t, d, unhex(t, "8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf")),
			[]string{":method: GET", ":scheme: https", ":path: /index.html", ":authority: www.example.com", "custom-key: custom-value"})
	})

	// C.1.2: an integer past its prefix, 1337 on five bits — the
	// continuation octets every length and index above 30 goes through.
	t.Run("C.1.2 integer 1337 on a 5-bit prefix", func(t *testing.T) {
		v, rest, err := hpackInt(unhex(t, "1f9a 0a"), 5)
		if err != nil || v != 1337 || len(rest) != 0 {
			t.Errorf("hpackInt = %d, rest %x, %v; want 1337 and nothing left", v, rest, err)
		}
	})

	// C.5 and C.6: responses through a 256-octet table, so that entries
	// are evicted — the half of the dynamic table C.3 and C.4 never reach.
	responses := []struct {
		name string
		hex  [3]string
	}{
		{"C.5 response sequence", [3]string{
			"4803 3330 3258 0770 7269 7661 7465 611d 4d6f 6e2c 2032 3120 4f63 7420 3230 3133 2032 303a 3133 3a32 3120 474d 546e 1768 7474 7073 3a2f 2f77 7777 2e65 7861 6d70 6c65 2e63 6f6d",
			"4803 3330 37c1 c0bf",
			"88c1 611d 4d6f 6e2c 2032 3120 4f63 7420 3230 3133 2032 303a 3133 3a32 3220 474d 54c0 5a04 677a 6970 7738 666f 6f3d 4153 444a 4b48 514b 425a 584f 5157 454f 5049 5541 5851 5745 4f49 553b 206d 6178 2d61 6765 3d33 3630 303b 2076 6572 7369 6f6e 3d31",
		}},
		{"C.6 response sequence, Huffman coded", [3]string{
			"4882 6402 5885 aec3 771a 4b61 96d0 7abe 9410 54d4 44a8 2005 9504 0b81 66e0 82a6 2d1b ff6e 919d 29ad 1718 63c7 8f0b 97c8 e9ae 82ae 43d3",
			"4883 640e ffc1 c0bf",
			"88c1 6196 d07a be94 1054 d444 a820 0595 040b 8166 e084 a62d 1bff c05a 839b d9ab 77ad 94e7 821d d7f2 e6c7 b335 dfdf cd5b 3960 d5af 2708 7f36 72c1 ab27 0fb5 291f 9587 3160 65c0 03ed 4ee5 b106 3d50 07",
		}},
	}
	for _, r := range responses {
		t.Run(r.name, func(t *testing.T) {
			d := newHPACKDecoder(256, 0)
			equal(t, decoded(t, d, unhex(t, r.hex[0])),
				[]string{":status: 302", "cache-control: private", "date: Mon, 21 Oct 2013 20:13:21 GMT", "location: https://www.example.com"})
			equal(t, decoded(t, d, unhex(t, r.hex[1])),
				[]string{":status: 307", "cache-control: private", "date: Mon, 21 Oct 2013 20:13:21 GMT", "location: https://www.example.com"})
			equal(t, decoded(t, d, unhex(t, r.hex[2])),
				[]string{
					":status: 200", "cache-control: private", "date: Mon, 21 Oct 2013 20:13:22 GMT", "location: https://www.example.com",
					"content-encoding: gzip", "set-cookie: foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
				})
			// The RFC's own accounting after the third response.
			if d.size != 215 || d.n != 3 {
				t.Errorf("dynamic table = %d entries, size %d; want 3 entries, size 215", d.n, d.size)
			}
		})
	}
}

// The refusals. HPACK is stateful, so a block decoded wrongly poisons every
// block after it: none of these may be tolerated.
func TestHPACKRefusals(t *testing.T) {
	tests := []struct {
		name  string
		block string
	}{
		{"index 0, which names nothing", "80"},
		{"an index past the table", "ff00"},
		{"a string longer than the block", "00 7f"},
		{"an integer that runs off the end", "00 00 ff"},
		{"a string of 2^35 bytes, longer than the block", "00 00 ffffffffff7f"},
		// RFC 9204 §4.1.1's 62 bits are decoded; 2^62 itself is not. The
		// refusal used to fall at 28 bits, on the fifth continuation octet.
		{"an integer past 62 bits", "00 00 ff81ffffffffffffff3f"},
		{"a name index past the table", "7f00 00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newHPACKDecoder(0, 0)
			if _, err := d.Decode(nil, unhex(t, tt.block)); !errors.Is(err, ErrHPACK) {
				t.Fatalf("Decode returned %v, want ErrHPACK", err)
			}
		})
	}
}

// The amplification bound: a small block naming large entries repeatedly must
// not decode into an unbounded list.
func TestHPACKBoundsTheDecodedList(t *testing.T) {
	d := newHPACKDecoder(0, 256) // a very small list ceiling
	// One literal with a long value, then many indexed references to it.
	block := append([]byte{0x40, 0x01, 'k', 0x7f, 0x01}, make([]byte, 128)...)
	for i := range 128 {
		block[5+i] = 'v'
	}
	for range 64 {
		block = append(block, 0xbe) // index 62: the entry just added
	}
	fields, err := d.Decode(nil, block)
	if !errors.Is(err, ErrTooLarge) || errors.Is(err, ErrHPACK) {
		t.Fatalf("Decode returned %v, want the list bound to refuse as too large, not as an HPACK fault", err)
	}
	if len(fields) != 0 {
		t.Errorf("Decode kept %d fields of a refused list, want none", len(fields))
	}
	// The block was read to its end: the entry it added is in the table.
	if _, err := d.Decode(nil, []byte{0xbe}); err != nil {
		t.Fatalf("the entry the refused block added is gone: %v", err)
	}
}

// A table update may not exceed what this end advertised, or a peer sizes our
// memory for us.
func TestHPACKRefusesATableUpdateOverTheLimit(t *testing.T) {
	d := newHPACKDecoder(4096, 0)
	// 0x3f is a size update with a 5-bit prefix at its maximum, continued.
	if _, err := d.Decode(nil, unhex(t, "3fe1ff03")); !errors.Is(err, ErrHPACK) {
		t.Fatalf("Decode returned %v, want ErrHPACK", err)
	}
}

// Eviction is what keeps the table at the size both ends agreed on, and both
// ends must evict identically or every later index means something different.
func TestHPACKEvictsToStayWithinTheTable(t *testing.T) {
	d := newHPACKDecoder(hpackEntrySize+8, 0) // room for exactly one small entry
	if _, err := d.Decode(nil, unhex(t, "4003 6161 6103 6262 62")); err != nil {
		t.Fatal(err) // aaa: bbb
	}
	if _, err := d.Decode(nil, unhex(t, "4003 6363 6303 6464 64")); err != nil {
		t.Fatal(err) // ccc: ddd
	}
	if d.n != 1 || string(d.entry(0).Name) != "ccc" {
		t.Errorf("the table holds %d entries, want only the newest one", d.n)
	}
	if d.size > d.maxSize {
		t.Errorf("the table is %d bytes, over its %d maximum", d.size, d.maxSize)
	}
}

// Huffman's own refusals: EOS may not be encoded, and padding must be a
// strict prefix of it.
func TestHuffmanRefusals(t *testing.T) {
	// EOS is thirty one-bits; four 0xff octets spell it and more.
	if _, err := huffmanDecode(nil, []byte{0xff, 0xff, 0xff, 0xff}); !errors.Is(err, ErrHPACK) {
		t.Errorf("EOS inside a string was accepted: %v", err)
	}
	// Padding that is not all ones.
	if _, err := huffmanDecode(nil, []byte{0x00}); !errors.Is(err, ErrHPACK) {
		t.Errorf("padding of zero bits was accepted: %v", err)
	}
}

// The transcription's algebra: every code fits in its length, and the lengths
// with EOS's thirty bits satisfy the Kraft equality — the sum of 2^-len over
// the 257 codes is exactly one — that a complete prefix code must. A single
// mistyped length breaks the sum; buildHuffman's prefix check would not see
// a code that leaves a hole in the tree.
func TestHuffmanTableIsACompletePrefixCode(t *testing.T) {
	if eosBits != 30 || eosCode != 1<<eosBits-1 {
		t.Fatalf("EOS is %#x over %d bits, want thirty one-bits", eosCode, eosBits)
	}
	sum := uint64(1) // EOS: 2^(30-30)
	for sym := range 256 {
		bits, code := huffmanBits[sym], huffmanCode[sym]
		if bits < 5 || bits > eosBits {
			t.Fatalf("symbol %d: length %d outside RFC 7541's 5..30", sym, bits)
		}
		if uint64(code) >= uint64(1)<<bits {
			t.Errorf("symbol %d: code %#x does not fit in %d bits", sym, code, bits)
		}
		sum += uint64(1) << (eosBits - bits)
	}
	if sum != 1<<eosBits {
		t.Errorf("Kraft sum = %d/2^30, want exactly 1", sum)
	}
}

// The trie is the table, so this is the table checked once more at the shape
// that decoding actually uses.
func TestHuffmanRoundTripsEveryOctet(t *testing.T) {
	for sym := range 256 {
		bits, code := huffmanBits[sym], huffmanCode[sym]
		// Encode the one symbol, padding with ones as the RFC requires.
		nbits := int(bits)
		pad := (8 - nbits%8) % 8
		// Parenthesised: | and - share a precedence level in Go and associate
		// left to right, so the obvious spelling builds (code|pad)-1 and
		// silently encodes the symbol below this one.
		v := uint64(code)<<uint(pad) | (uint64(1)<<uint(pad) - 1)
		total := (nbits + pad) / 8
		var buf []byte
		for i := total - 1; i >= 0; i-- {
			buf = append(buf, byte(v>>uint(8*i)))
		}
		got, err := huffmanDecode(nil, buf)
		if err != nil {
			t.Fatalf("symbol %d (%d bits) did not decode: %v", sym, bits, err)
		}
		if len(got) != 1 || got[0] != byte(sym) {
			t.Fatalf("symbol %d decoded to %v", sym, got)
		}
	}
}

// A table size update is bounded by SETTINGS_HEADER_TABLE_SIZE, which this
// end advertised once and does not move. Checking it against the table's
// *current* size instead refuses the legal sequence "shrink to nothing, then
// restore" — both updates within what was advertised, both from a peer doing
// nothing wrong — and there is no recovering a connection from an HPACK
// failure.
func TestHPACKTableSizeUpdateIsBoundedByWhatWasAdvertised(t *testing.T) {
	d := newHPACKDecoder(4096, 0)

	if _, err := d.Decode(nil, unhex(t, "20")); err != nil {
		t.Fatalf("shrinking the table to 0: %v", err)
	}
	if _, err := d.Decode(nil, unhex(t, "3f e1 1f")); err != nil {
		t.Fatalf("restoring the table to 4096: %v", err)
	}

	// Restored means the table takes entries again, not merely that the
	// update was tolerated.
	got := decoded(t, d, unhex(t, "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572"))
	equal(t, got, []string{"custom-key: custom-header"})
	if d.n != 1 {
		t.Errorf("the dynamic table holds %d entries after the size was restored, want 1", d.n)
	}

	// What was advertised is still the ceiling: 4097 is over it.
	if _, err := d.Decode(nil, unhex(t, "3f e2 1f")); !errors.Is(err, ErrHPACK) {
		t.Errorf("a table update to 4097 returned %v, want ErrHPACK", err)
	}
}

// A table size update that shrinks the table evicts from the oldest end until
// what remains fits (RFC 7541 §4.3), and the survivors keep their indexes.
func TestHPACKTableSizeUpdateEvictsTheOldest(t *testing.T) {
	d := newHPACKDecoder(4096, 0)
	indexed := func(name, value string) []byte {
		b := []byte{0x40, byte(len(name))}
		b = append(b, name...)
		b = append(b, byte(len(value)))
		return append(b, value...)
	}
	var block []byte
	for _, v := range []string{"one", "two", "six"} {
		block = append(block, indexed("x-k", v)...)
	}
	if _, err := d.Decode(nil, block); err != nil {
		t.Fatal(err)
	}
	if d.n != 3 || d.size != 3*(3+3+hpackEntrySize) {
		t.Fatalf("the table holds %d entries of %d bytes, want 3 of %d", d.n, d.size, 3*(3+3+hpackEntrySize))
	}

	// Room for two entries: the oldest ("one") goes, "six" stays at 62.
	two := 2 * (3 + 3 + hpackEntrySize)
	got := decoded(t, d, append(appendHPACKInt(nil, uint64(two), 5, 0x20), 0x80|62, 0x80|63))
	equal(t, got, []string{"x-k: six", "x-k: two"})
	if d.n != 2 || d.size != two {
		t.Errorf("after shrinking to %d the table holds %d entries of %d bytes", two, d.n, d.size)
	}
	if _, err := d.Decode(nil, []byte{0x80 | 64}); !errors.Is(err, ErrHPACK) {
		t.Errorf("the evicted entry's index still resolved: %v", err)
	}

	// Zero empties it.
	if _, err := d.Decode(nil, []byte{0x20}); err != nil {
		t.Fatal(err)
	}
	if d.n != 0 || d.size != 0 {
		t.Errorf("a table of size 0 still holds %d entries of %d bytes", d.n, d.size)
	}
}

// RFC 7541 §4.2: a dynamic table size update opens a header block. One that
// follows a field would resize the table between fields indexed against
// different sizes, and nghttp2 refuses it as a compression error.
func TestHPACKRefusesATableSizeUpdateAfterAField(t *testing.T) {
	t.Run("after a field", func(t *testing.T) {
		d := newHPACKDecoder(4096, 0)
		// :method GET, then a size update to 0.
		if _, err := d.Decode(nil, []byte{0x82, 0x20}); !errors.Is(err, ErrHPACK) {
			t.Fatalf("a size update after a field decoded: %v", err)
		}
	})
	t.Run("at the start, twice", func(t *testing.T) {
		// §4.2 also allows two updates in a row at the start (the smallest
		// size seen, then the final one).
		d := newHPACKDecoder(4096, 0)
		block := append(appendHPACKInt(nil, 0, 5, 0x20), appendHPACKInt(nil, 4096, 5, 0x20)...)
		equal(t, decoded(t, d, append(block, 0x82)), []string{":method: GET"})
	})
}

// A field resolved from the dynamic table is a view of the entry, not a copy;
// that is only sound if the entry's bytes are never written again. Evicting
// it, and filling the table with new entries afterwards, must leave a field
// decoded earlier untouched.
func TestHPACKDynamicFieldSurvivesEviction(t *testing.T) {
	d := newHPACKDecoder(hpackEntrySize+8, 0) // one small entry at a time
	lit := func(name, value string) []byte {
		b := []byte{0x40, byte(len(name))}
		b = append(b, name...)
		b = append(b, byte(len(value)))
		return append(b, value...)
	}
	first, err := d.Decode(nil, append(lit("aaa", "bbb"), 0x80|62))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"ccc", "ddd", "eee"} {
		if _, err := d.Decode(nil, append(lit("xxx", v), 0x80|62)); err != nil {
			t.Fatal(err)
		}
	}
	for i, f := range first {
		if string(f.Name) != "aaa" || string(f.Value) != "bbb" {
			t.Errorf("field %d of the first block now reads %s: %s", i, f.Name, f.Value)
		}
	}
}

// A dynamic-table hit costs no allocation: the field is a view of the entry.
func TestHPACKDynamicHitDoesNotAllocate(t *testing.T) {
	d := newHPACKDecoder(4096, 0)
	if _, err := d.Decode(nil, []byte{0x40, 3, 'a', 'a', 'a', 3, 'b', 'b', 'b'}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Header, 0, 4)
	block := []byte{0x80 | 62, 0x80 | 62}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := d.Decode(dst[:0], block); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("two dynamic-table hits allocated %.0f times, want 0", n)
	}
}

// RFC 7541 §7.1.3: a field carrying a credential goes out never-indexed, so
// an intermediary that re-encodes the response may not index it either.
// Everything else stays literal-without-indexing.
func TestHPACKEncodesCredentialsNeverIndexed(t *testing.T) {
	for _, tc := range []struct {
		name string
		want byte // the representation's top four bits
	}{
		{"set-cookie", 0x10},          // static name, index 55
		{"authorization", 0x10},       // static name, index 23
		{"proxy-authorization", 0x10}, // static name, index 49
		{"content-type", 0x00},
		{"x-custom", 0x00}, // literal name
	} {
		block := appendHPACK(nil, tc.name, []byte("v"))
		if got := block[0] & 0xf0; got != tc.want {
			t.Errorf("%s: representation %#x, want %#x", tc.name, got, tc.want)
		}
		equal(t, decoded(t, newHPACKDecoder(0, 0), block), []string{tc.name + ": v"})
	}
}

// The dynamic table is a ring: indexes must keep naming the right entries as
// insertions wrap around it and evictions advance its start.
func TestHPACKDynamicIndexesSurviveWrapAround(t *testing.T) {
	const entry = 3 + 3 + hpackEntrySize
	d := newHPACKDecoder(3*entry, 0) // room for exactly three
	name := func(i int) string { return fmt.Sprintf("%03d", i) }
	for i := range 40 {
		block := []byte{0x40, 3}
		block = append(block, "x-k"...)
		block = append(block, 3)
		block = append(block, name(i)...)
		if i < 2 {
			// Fewer than three entries yet: only the insertion.
			decoded(t, d, block)
			continue
		}
		got := decoded(t, d, append(block, 0x80|62, 0x80|63, 0x80|64))
		want := []string{"x-k: " + name(i), "x-k: " + name(i), "x-k: " + name(i-1), "x-k: " + name(i-2)}
		equal(t, got, want)
	}
}

// wwwExampleHuffman is "www.example.com" Huffman-coded, from RFC 7541 C.4.1.
var wwwExampleHuffman = []byte{0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff}

// The literals of one block that the dynamic table does not keep share one
// allocation, the block's arena; a literal the table keeps takes one of its
// own for name and value together. realisticHeaderBlock has two of the
// latter and five of the former, so a decode against a warm table costs
// three allocations — it cost twelve when every string was cloned on its own.
func TestHPACKDecodeAllocations(t *testing.T) {
	d := newHPACKDecoder(4096, 64<<10)
	block := realisticHeaderBlock()
	dst := make([]Header, 0, 16)
	if _, err := d.Decode(dst[:0], block); err != nil { // warm the ring
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := d.Decode(dst[:0], block); err != nil {
			t.Fatal(err)
		}
	}); n != 3 {
		t.Errorf("decoding the realistic block allocated %.0f times, want 3", n)
	}

	// Unindexed literals only, Huffman-coded and not, static names and
	// literal ones: the arena alone.
	unindexed := []byte{0x82, 0x01, 0x80 | byte(len(wwwExampleHuffman))}
	unindexed = append(unindexed, wwwExampleHuffman...)
	for _, kv := range [][2]string{{"x-a", "one"}, {"x-b", "two"}, {"x-c", ""}} {
		unindexed = append(unindexed, 0x00)
		unindexed = appendHPACKString(unindexed, kv[0])
		unindexed = appendHPACKString(unindexed, kv[1])
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := d.Decode(dst[:0], unindexed); err != nil {
			t.Fatal(err)
		}
	}); n != 1 {
		t.Errorf("decoding five unindexed fields allocated %.0f times, want 1", n)
	}
}

// Fields that are views of a shared buffer must not let an append through
// one overwrite another: every view is capped at its own length. And a
// block's arena is its own — decoding the next block leaves the fields of
// the last intact.
func TestHPACKArenaViewsAreIsolated(t *testing.T) {
	d := newHPACKDecoder(4096, 0)
	var block []byte
	for _, kv := range [][2]string{{"x-a", "one"}, {"x-b", "two"}, {"x-c", ""}} {
		block = append(block, 0x00)
		block = appendHPACKString(block, kv[0])
		block = appendHPACKString(block, kv[1])
	}
	first, err := d.Decode(nil, block)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(first[0].Value, "XXXXXXXX"...)
	_ = append(first[0].Name, "XXXXXXXX"...)
	if _, err := d.Decode(nil, block); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(first))
	for _, f := range first {
		got = append(got, string(f.Name)+": "+string(f.Value))
	}
	equal(t, got, []string{"x-a: one", "x-b: two", "x-c: "})
	if first[2].Value == nil {
		t.Error("an empty literal decoded to nil, which reads as absent")
	}
}

// A plain first string sizes the arena for plain strings; a Huffman string
// after it that does not fit regrows the arena, and the fields decoded
// before the regrowth keep reading what they read.
func TestHPACKArenaSurvivesRegrowth(t *testing.T) {
	block := []byte{0x00}
	block = appendHPACKString(block, "x-a")
	block = appendHPACKString(block, "one")
	for range 4 {
		block = append(block, 0x01, 0x80|byte(len(wwwExampleHuffman))) // :authority, Huffman value
		block = append(block, wwwExampleHuffman...)
	}
	equal(t, decoded(t, newHPACKDecoder(0, 0), block), []string{
		"x-a: one",
		":authority: www.example.com", ":authority: www.example.com",
		":authority: www.example.com", ":authority: www.example.com",
	})
}

// hpackLiteral is one literal representation with a new name and no Huffman
// coding: the first byte (0x40 indexes it, 0x00 does not), then name and
// value as length-prefixed strings.
func hpackLiteral(first byte, name, value string) []byte {
	b := []byte{first, byte(len(name))}
	b = append(b, name...)
	b = append(b, byte(len(value)))
	return append(b, value...)
}

// Past the header list bound the decoder still walks the rest of the block:
// a literal it does not keep is skipped whole, and one with incremental
// indexing still enters the table, so the next block's reference to it
// decodes. A skip that walked one byte wrong would desynchronise the table
// for the rest of the connection.
func TestHPACKSkipKeepsTheTableAfterTheBound(t *testing.T) {
	d := newHPACKDecoder(4096, 64)
	block := hpackLiteral(0x00, "x-big", strings.Repeat("v", 100))
	block = append(block, hpackLiteral(0x00, "x-skip", "skipped")...)
	block = append(block, hpackLiteral(0x40, "x-kept", "yes")...)
	if _, err := d.Decode(nil, block); !errors.Is(err, errHeadersTooLarge) {
		t.Fatalf("the oversized block: %v, want errHeadersTooLarge", err)
	}
	got, err := d.Decode(nil, []byte{0x80 | 62}) // the first dynamic entry
	if err != nil {
		t.Fatalf("the next block: %v", err)
	}
	if len(got) != 1 || string(got[0].Name) != "x-kept" || string(got[0].Value) != "yes" {
		t.Errorf("the next block decoded to %q, want x-kept: yes", got)
	}
}
