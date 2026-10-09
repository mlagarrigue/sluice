package httpstream

import "fmt"

// Huffman decoding for HPACK string literals (RFC 7541 §5.2), over the
// canonical code in [huffmanCode].
//
// The decoder is a binary trie built once from the table. A trie is the shape
// that makes the two rules checkable rather than hopeful: a symbol is emitted
// only at a leaf, so no code is ever confused with a prefix of another, and
// what is left over at the end is checked against the padding rule instead of
// being discarded.
//
// A flat multi-bit table (the shape net/http's hpack uses) was considered and
// deliberately not attempted: the trie measures 301 MB/s at zero allocations
// (BenchmarkHuffmanDecode), this server's own encoder never Huffman-codes, so
// only request headers pay it, and rewriting a hostile-input decode path is
// not worth doing without the regression coverage to hold it. What would
// reopen this is a profile of real traffic showing Huffman decode dominating.

type huffNode struct {
	child [2]*huffNode
	sym   byte
	leaf  bool
}

var huffRoot = buildHuffman()

func buildHuffman() *huffNode {
	root := &huffNode{}
	for sym, bits := range huffmanBits {
		code := huffmanCode[sym]
		n := root
		for i := int(bits) - 1; i >= 0; i-- {
			b := (code >> uint(i)) & 1
			if n.child[b] == nil {
				n.child[b] = &huffNode{}
			}
			n = n.child[b]
			if n.leaf {
				// Would mean one code is a prefix of another, which a prefix
				// code cannot have. The table is checked on transcription; this
				// is the check that it was transcribed into this trie intact.
				panic("httpstream: the Huffman table is not a prefix code")
			}
		}
		if n.child[0] != nil || n.child[1] != nil {
			panic("httpstream: the Huffman table is not a prefix code")
		}
		n.sym, n.leaf = byte(sym), true
	}
	return root
}

// huffmanDecode appends the decoded octets to dst.
//
// Two refusals matter and both are silent corruption if skipped. **EOS may not
// appear**: a peer that encodes it is trying to end a string somewhere the
// length said it did not. And **padding must be a strict prefix of EOS, under
// eight bits**: anything longer is a code the encoder should have emitted, and
// anything that is not all ones is not padding at all.
func huffmanDecode(dst, src []byte) ([]byte, error) {
	n := huffRoot
	bits := 0       // how many bits walked since the last leaf
	allOnes := true // whether every one of them was a 1, as padding must be
	for _, b := range src {
		for i := 7; i >= 0; i-- {
			bit := (b >> uint(i)) & 1
			n = n.child[bit]
			if n == nil {
				// Only reachable on the EOS path, which the trie does not
				// hold: the code is complete over the 256 symbols plus EOS,
				// so a walk that leaves the trie was spelling EOS.
				return dst, fmt.Errorf("%w: EOS appears inside a Huffman string", ErrHPACK)
			}
			bits++
			allOnes = allOnes && bit == 1
			if n.leaf {
				dst = append(dst, n.sym)
				n, bits, allOnes = huffRoot, 0, true
			}
		}
	}
	if bits >= 8 {
		return dst, fmt.Errorf("%w: %d bits of padding, which is a code that was not emitted", ErrHPACK, bits)
	}
	// The leftover is padding, and RFC 7541 §5.2 says it must be the most
	// significant bits of EOS — which are all ones. Checked on the bits that
	// were walked, not by walking further: what has to be true is the path
	// already taken.
	if !allOnes {
		return dst, fmt.Errorf("%w: the padding is not a prefix of EOS", ErrHPACK)
	}
	return dst, nil
}
