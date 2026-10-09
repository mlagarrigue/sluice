package httpstream

import "testing"

// FuzzHPACKDecode matters more than the other fuzzers here, because HPACK is
// stateful: a block decoded wrongly poisons every block after it on the same
// connection, and the damage is silent. For any bytes at all the decoder must
// refuse or return a list that respects the bounds it was given, and it must
// never panic.
func FuzzHPACKDecode(f *testing.F) {
	f.Add([]byte{0x82})
	f.Add([]byte{0x40, 0x0a, 'c', 'u', 's', 't', 'o', 'm', '-', 'k', 'e', 'y', 0x00})
	f.Add([]byte{0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff})
	f.Add([]byte{0x3f, 0xe1, 0xff, 0x03})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		const maxTable, maxList = 4096, 8 << 10
		d := newHPACKDecoder(maxTable, maxList)

		// Decoded twice on one decoder, which is what a connection does: the
		// second block is decoded against a table the first one left behind,
		// and that is where a stateful decoder goes wrong.
		for range 2 {
			fields, err := d.Decode(nil, data)
			if err != nil {
				break
			}
			list := 0
			for _, h := range fields {
				list += len(h.Name) + len(h.Value) + hpackEntrySize
			}
			if list > maxList {
				t.Fatalf("a list of %d bytes was returned, over the %d bound", list, maxList)
			}
			if d.size > d.maxSize {
				t.Fatalf("the dynamic table is %d bytes, over its %d maximum", d.size, d.maxSize)
			}
			// The table's accounted size must match what it holds, or
			// eviction drifts from the peer's and every later index means
			// something else.
			sum := 0
			for i := range d.n {
				sum += d.entry(i).size()
			}
			if sum != d.size {
				t.Fatalf("the table accounts %d bytes and holds %d", d.size, sum)
			}
		}
	})
}
