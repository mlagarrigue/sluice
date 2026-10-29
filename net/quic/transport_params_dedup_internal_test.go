package quic

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// unknownIDBlock is a transport parameter block of n distinct identifiers
// from 64 up, each with an empty value: 3 bytes each while the identifier
// fits a 2-byte varint (64..16383), 5 bytes above. 19,000 of them fit the
// 64 KiB a crypto/tls handshake message may carry.
func unknownIDBlock(n int) []byte {
	var b []byte
	for i := range n {
		b = varint.Append(b, uint64(64+i))
		b = varint.Append(b, 0)
	}
	return b
}

// RFC 9000 §7.4.2 has unknown transport parameters ignored, and §7.4 a
// repeated one refused. A 64 KiB block of distinct unknown identifiers is a
// conformant block, admitted; the check for a repeat among them must cost
// what the block costs to read, not the square of its count — the peer
// pays three bytes an identifier.
func TestABlockOfDistinctUnknownParametersIsAdmitted(t *testing.T) {
	const n = 19000
	block := unknownIDBlock(n)
	if len(block) > 1<<16 {
		t.Fatalf("the block is %d bytes, over the 64 KiB a handshake message carries", len(block))
	}
	if _, err := parseParameters(block); err != nil {
		t.Fatalf("a block of %d distinct unknown identifiers (%d bytes) was refused: %v", n, len(block), err)
	}
	// The same block with its last identifier repeated at the end.
	twice := append(block, unknownIDBlock(n)[3*(n-1):]...)
	if _, err := parseParameters(twice); err == nil {
		t.Fatal("a block that repeats an unknown identifier was admitted")
	}
}

// BenchmarkParseParametersUnknownIDs measures a block of 1,000 distinct
// unknown identifiers against one of 19,000: deduplication in linear time
// scales the time by about the byte ratio (21x), a scan of every earlier
// identifier by the square of the count (about 360x).
func BenchmarkParseParametersUnknownIDs(b *testing.B) {
	for _, n := range []int{1000, 19000} {
		block := unknownIDBlock(n)
		b.Run(fmt.Sprintf("ids=%d/bytes=%d", n, len(block)), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := parseParameters(block); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
