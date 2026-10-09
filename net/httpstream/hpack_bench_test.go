package httpstream

import "testing"

// realisticHeaderBlock mixes the three representations a real browser's
// HPACK encoder actually produces: pure indexed fields for the static-table
// pseudo-headers, incremental indexing for the values the client will resend
// (:path, :authority), and literal-without-indexing for what varies or
// should not sit in a shared table (user-agent, cookie). Candidate 4 of the
// performance pass is about the cost of building each field's Header from
// this mix, not about one representation in isolation.
func realisticHeaderBlock() []byte {
	var b []byte
	b = append(b, 0x82) // :method: GET (indexed, static idx 2)
	b = append(b, 0x87) // :scheme: https (indexed, static idx 7)

	b = appendHPACKInt(b, 4, 6, 0x40) // :path, incremental indexing, name idx 4
	b = appendHPACKString(b, "/api/v2/orders/12345?expand=items")

	b = appendHPACKInt(b, 1, 6, 0x40) // :authority, incremental indexing, name idx 1
	b = appendHPACKString(b, "api.example.com")

	for _, kv := range [][2]string{
		{"user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
		{"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
		{"accept-language", "en-US,en;q=0.9"},
		{"cookie", "session_id=abc123def456; pref=dark_mode; csrf=xyz789uvw012"},
		{"x-request-id", "b3f1c2a4-9e7d-4b1a-8c3f-1234567890ab"},
	} {
		b = append(b, 0x00) // literal, name and value literal, without indexing
		b = appendHPACKString(b, kv[0])
		b = appendHPACKString(b, kv[1])
	}
	return b
}

// BenchmarkHPACKDecode measures Decode over one realistic browser header
// block — the harness candidate 4 of the performance pass asks for.
func BenchmarkHPACKDecode(b *testing.B) {
	block := realisticHeaderBlock()
	dst := make([]Header, 0, 16)

	// One decoder for the whole loop, as one connection has: the first block
	// warms the dynamic table and every later one decodes against it, and
	// the alloc signal is Decode's own rather than the constructor's.
	d := newHPACKDecoder(4096, 64<<10)
	b.ReportAllocs()
	b.SetBytes(int64(len(block)))
	for b.Loop() {
		fields, err := d.Decode(dst[:0], block)
		if err != nil {
			b.Fatal(err)
		}
		if len(fields) != 9 {
			b.Fatalf("decoded %d fields, want 9", len(fields))
		}
	}
}
