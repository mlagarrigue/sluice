package httpstream

import "testing"

// huffmanEncode packs s through the canonical code, padded with the
// most-significant bits of EOS — the shape huffmanDecode requires and the
// only one worth benchmarking it against, since that is what a peer that
// Huffman-codes actually sends.
func huffmanEncode(s string) []byte {
	var bitBuf uint64
	var nbits uint
	var out []byte
	for i := range len(s) {
		c := s[i]
		bitBuf = bitBuf<<uint(huffmanBits[c]) | uint64(huffmanCode[c])
		nbits += uint(huffmanBits[c])
		for nbits >= 8 {
			nbits -= 8
			out = append(out, byte(bitBuf>>nbits))
		}
	}
	if nbits > 0 {
		// Pad with the high bits of EOS (thirty ones), which is always
		// enough padding since nbits < 8 here.
		pad := 8 - nbits
		out = append(out, byte(bitBuf<<pad)|(1<<pad-1))
	}
	return out
}

// BenchmarkHuffmanDecode measures the trie walk over a Huffman-heavy block —
// candidate 7 of the performance pass, which only recommends a flat table if
// this justifies it. The string mixes upper and lower case and digits, which
// is what pushes real traffic (URLs, tokens) away from the table's shortest
// codes.
func BenchmarkHuffmanDecode(b *testing.B) {
	const text = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
	src := huffmanEncode(text)
	dst := make([]byte, 0, len(text))

	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for b.Loop() {
		got, err := huffmanDecode(dst[:0], src)
		if err != nil {
			b.Fatal(err)
		}
		if string(got) != text {
			b.Fatalf("decoded %q, want %q", got, text)
		}
	}
}
