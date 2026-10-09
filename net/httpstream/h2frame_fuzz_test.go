package httpstream

import "testing"

// FuzzParseFrame holds the frame layer to the same standard as the HTTP/1.1
// parser: for any bytes at all, refuse or return something consistent, never
// panic, never claim more than it was given, and never hand out memory that
// was not in the input.
func FuzzParseFrame(f *testing.F) {
	f.Add(frame(frameData, flagEndStream, 1, []byte("x")))
	f.Add(frame(frameSettings, 0, 0, make([]byte, 6)))
	f.Add(frame(frameHeaders, flagEndHeaders, 1, []byte{0x82}))
	f.Add(frame(frameGoAway, 0, 0, make([]byte, 8)))
	f.Add([]byte{0xff, 0xff, 0xff, 0x00, 0x00, 0, 0, 0, 1})
	f.Add([]byte{})

	cfg := H2Config{}.withDefaults()
	f.Fuzz(func(t *testing.T, data []byte) {
		fr, n, err := parseFrame(data, cfg)
		if err != nil {
			if n != 0 {
				t.Errorf("a refused frame reported %d bytes consumed", n)
			}
			return
		}
		if n < frameHeaderSize || n > len(data) {
			t.Fatalf("consumed %d of %d bytes", n, len(data))
		}
		if len(fr.Payload) != n-frameHeaderSize {
			t.Errorf("payload is %d bytes for a frame of %d", len(fr.Payload), n)
		}
		if len(fr.Payload) > cfg.MaxFrameSize {
			t.Errorf("a payload of %d bytes was accepted", len(fr.Payload))
		}
		if fr.StreamID&0x80000000 != 0 {
			t.Error("the reserved bit reached the stream identifier")
		}
		// What was accepted must frame back to the same bytes — except the
		// reserved bit, which RFC 9113 §4.1 requires be ignored on receipt
		// and which therefore cannot survive a round trip. The fuzzer found
		// that exact case, and the exception is the spec's rather than a
		// weakening of the property: everything else must be identical.
		back, err := appendH2Frame(nil, fr)
		if err != nil {
			t.Fatalf("a parsed frame would not re-frame: %v", err)
		}
		want := append([]byte(nil), data[:n]...)
		want[5] &= 0x7f
		if string(back) != string(want) {
			t.Errorf("re-framing changed the bytes")
		}
	})
}
