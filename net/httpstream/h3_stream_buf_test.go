package httpstream

import (
	"testing"

	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// When bodies stream, a delivery inside a DATA frame that has not ended
// (dataLeft > 0) moves its bytes to the body queue and empties st.buf. It
// emptied it in place, keeping the delivery's capacity — which the
// reassembly may make as large as the stream window — outside assembling
// for the stream's life. A buffer past h3DeliveryKept now goes, as
// keepStreamRest lets a parsed frame's go. The witness delivers the same
// bytes as a new, whole DATA frame followed by the start of a trailers
// frame: drain parses it out of the buffer and keepStreamRest, given a
// short rest, lets the capacity go.
func TestH3StreamedDeliveryReleasesBufferCapacity(t *testing.T) {
	const delta = 200 << 10
	for _, tc := range []struct {
		name     string
		declared uint64
		witness  bool
	}{
		{"witness: a new DATA frame, then a frame header", 16, true},
		{"the DATA frame goes on past the delta", 1 << 22, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := streamingAssembler(Config{MaxBodyBytes: 1 << 16, MaxStreamedBodyBytes: 1 << 24})
			head := h3Request("POST", "/up")
			head = varint.Append(head, h3Data)
			head = varint.Append(head, tc.declared)
			head = append(head, make([]byte, 16)...)
			if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, head, false)}); err != nil || len(failed) != 0 {
				t.Fatalf("the head: %v, failed %v", err, failed)
			}
			body := make([]byte, delta)
			if tc.witness {
				const payload = delta - 5 - 2 // type byte, a 4-byte length varint, two trailing bytes
				frame := varint.Append(varint.Append(nil, h3Data), payload)
				if len(frame) != 5 {
					t.Fatalf("DATA header of %d bytes", len(frame))
				}
				copy(body, frame)
				body[delta-2], body[delta-1] = byte(h3Headers), 0x40 // a HEADERS frame, its length varint cut
			}
			if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, body, false)}); err != nil || len(failed) != 0 {
				t.Fatalf("the delta: %v, failed %v", err, failed)
			}
			st := a.streams[0]
			if st == nil {
				t.Fatal("the stream left assembly before its FIN")
			}
			if spare := cap(st.buf) - len(st.buf); spare > h3BufferSlack {
				t.Errorf("st.buf keeps %d bytes of spare capacity (len %d), over h3BufferSlack %d", spare, len(st.buf), h3BufferSlack)
			}
		})
	}
}

// BenchmarkH3StreamedUploadDeliveries is a streamed upload whose one DATA
// frame arrives in 16 KiB deliveries, each inside the frame (dataLeft): the
// path whose buffer is reused from one delivery to the next.
func BenchmarkH3StreamedUploadDeliveries(b *testing.B) {
	const piece, pieces = 16 << 10, 16
	a := streamingAssembler(Config{MaxBodyBytes: 1 << 16, MaxStreamedBodyBytes: 1 << 24})
	chunk := make([]byte, piece)
	b.ReportAllocs()
	b.SetBytes(piece * pieces)
	id := quic.StreamID(0)
	for b.Loop() {
		a.inFlight.Store(0) // nobody answers: the bound on unanswered streams is not this bench's
		head := h3Request("POST", "/up")
		head = varint.Append(head, h3Data)
		head = varint.Append(head, piece*pieces)
		if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, head, false)}); err != nil || len(failed) != 0 {
			b.Fatalf("the head: %v, failed %v", err, failed)
		}
		for k := range pieces {
			if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, chunk, k == pieces-1)}); err != nil || len(failed) != 0 {
				b.Fatalf("delivery %d: %v, failed %v", k, err, failed)
			}
		}
		id += 4
	}
}
