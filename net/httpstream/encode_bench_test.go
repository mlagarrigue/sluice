package httpstream

import "testing"

// benchResponse is the response shape the encode benchmarks share: a handful
// of regular fields and a small body — the common case an API handler
// produces, not a pathological one.
func benchResponse() Response {
	return Response{
		Status: 200,
		Headers: []Header{
			{Name: []byte("content-type"), Value: []byte("application/json; charset=utf-8")},
			{Name: []byte("cache-control"), Value: []byte("no-store")},
			{Name: []byte("x-request-id"), Value: []byte("b3f1c2a4-9e7d-4b1a-8c3f-1234567890ab")},
			{Name: []byte("vary"), Value: []byte("accept-encoding")},
		},
		Body: []byte(`{"id":12345,"status":"shipped","items":[{"sku":"A-1","qty":2}]}`),
	}
}

// BenchmarkH2Encode measures appendHead — the HEADERS-frame half of the h2
// response path, HPACK encoding included. The suggestion it pins: every
// response header round-tripped through string(h.Name) / string(h.Value) on
// its way into the block.
func BenchmarkH2Encode(b *testing.B) {
	s := newBenchH2Server(H2Config{})
	r := benchResponse()
	method := []byte("GET")
	maxFrame := s.cfg.MaxFrameSize
	var dst []byte

	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = s.appendHead(dst[:0], 1, method, r, maxFrame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkH3Encode measures the responder's per-response framing —
// appendH3Response as answerH3 calls it. The suggestion it pins: the one
// response path with no buffer reuse at all (head, field list and QPACK
// block each allocated per response).
func BenchmarkH3Encode(b *testing.B) {
	r := benchResponse()
	method := []byte("GET")
	sc := &h3Scratch{} // one per responder goroutine, as ServeH3 holds it

	b.ReportAllocs()
	for b.Loop() {
		head, err := appendH3Response(sc.head[:0], r, method, 0, sc)
		if err != nil {
			b.Fatal(err)
		}
		if len(head) == 0 {
			b.Fatal("empty response frame")
		}
		sc.head = head
	}
}
