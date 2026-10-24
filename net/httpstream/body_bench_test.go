package httpstream

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The A/B behind the default of [Config.MaxStreamedBodyBytes]
// (docs/design/adr/0009, article 4): what a small body costs on each of the
// three protocols with bodies delivered whole, the default today, against
// the same traffic with streaming turned on. A body already whole in the
// read that brought its headers is delivered in [Request.Body] and batched
// with its neighbours whichever way the bound is set, so the two sides
// should land on the same figure; the gap, if there is one, is what turning
// streaming on by default would cost every small request — the bulk of the
// traffic of an API.
//
// The pairing is by construction: the two sides are sub-benchmarks of one
// binary, run back to back in the same process on the same frames. Each
// side asserts that the bodies did arrive whole and that every request was
// handed over, so a side that quietly took the streaming path, or handed
// over nothing, cannot pass for the other.
const (
	benchBodyBytes  = 256
	benchBodyStream = 1 << 20 // MaxStreamedBodyBytes when streaming is on
)

// benchBodySides are the two configurations compared, by the names
// benchstat reads.
var benchBodySides = []struct {
	name  string
	limit int
}{
	{"whole", 0},
	{"streamed", benchBodyStream},
}

func benchBody() []byte {
	return []byte(strings.Repeat("x", benchBodyBytes))
}

// pipelinedBodies is [pipelined] with a body: depth POST requests of a
// declared length, back to back in one read.
func pipelinedBodies(depth int) []byte {
	body := benchBody()
	var b strings.Builder
	for i := range depth {
		fmt.Fprintf(&b, "POST /orders/%d HTTP/1.1\r\nHost: h\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
			i, len(body), body)
	}
	return []byte(b.String())
}

// BenchmarkReadSmallBody is the HTTP/1.1 side: the reader's own path, as
// [BenchmarkReadPipelined] measures it, with a body small enough to arrive
// in the read that brought its headers.
func BenchmarkReadSmallBody(b *testing.B) {
	for _, depth := range []int{1, 8} {
		for _, side := range benchBodySides {
			b.Run(fmt.Sprintf("depth=%d/%s", depth, side.name), func(b *testing.B) {
				data := pipelinedBodies(depth)
				const requests = 4096
				bytesPerRun := len(data) * (requests / depth)

				b.ReportAllocs()
				b.SetBytes(int64(bytesPerRun))
				for b.Loop() {
					conn := &replayConn{data: data, max: bytesPerRun}
					src := Requests(conn, Config{
						MaxRequestsPerBatch:  depth,
						MaxRequestsPerConn:   requests,
						MaxStreamedBodyBytes: side.limit,
					})
					n, whole := 0, 0
					src.Stream()(func(batch sluice.Batch[Request]) bool {
						for i := range batch.Items {
							if len(batch.Items[i].Body) == benchBodyBytes {
								whole++
							}
						}
						n += batch.Len()
						return true
					})
					if n != requests || whole != requests {
						b.Fatalf("read %d requests, %d bodies whole, want %d of each", n, whole, requests)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*requests), "ns/request")
			})
		}
	}
}

// h2BodyFrames frames one request as a browser's POST of a short document
// does: a complete header block that does not end the stream, then the
// whole body in one DATA frame that does.
func h2BodyFrames(id uint32, path string, body []byte) []h2Frame {
	block := appendHPACK(nil, ":method", "POST")
	block = appendHPACK(block, ":path", path)
	block = appendHPACK(block, ":scheme", "https")
	block = appendHPACK(block, ":authority", "example.com")
	block = appendHPACK(block, "content-length", strconv.Itoa(len(body)))
	head, err := appendH2Frame(nil, h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders, StreamID: id, Payload: block,
	})
	if err != nil {
		panic(err)
	}
	data, err := appendH2Frame(nil, h2Frame{
		Type: frameData, Flags: flagEndStream, StreamID: id, Payload: body,
	})
	if err != nil {
		panic(err)
	}
	out := make([]h2Frame, 0, 2)
	for _, raw := range [][]byte{head, data} {
		parsed, _, err := parseFrame(raw, defaultMaxFrameSize)
		if err != nil {
			panic(err)
		}
		out = append(out, parsed)
	}
	return out
}

// BenchmarkH2ConsumeBody is the HTTP/2 side: the frame-to-request path of
// [BenchmarkH2Consume], each request carrying a small body in the same
// batch of frames as its headers.
func BenchmarkH2ConsumeBody(b *testing.B) {
	for _, depth := range []int{1, 8} {
		for _, side := range benchBodySides {
			b.Run(fmt.Sprintf("depth=%d/%s", depth, side.name), func(b *testing.B) {
				body := benchBody()
				frames := make([]h2Frame, 0, 2*depth)
				for i := range depth {
					frames = append(frames, h2BodyFrames(uint32(2*i+1), fmt.Sprintf("/orders/%d", i), body)...) //nolint:gosec // G115: 2i+1 for i < 8
				}

				s := newBenchH2Server(Config{MaxStreamedBodyBytes: side.limit})
				b.ReportAllocs()
				for b.Loop() {
					resetBenchH2Server(s)
					ready, _, err := s.consume(frames)
					if err != nil {
						b.Fatal(err)
					}
					whole := 0
					for i := range ready {
						if len(ready[i].Body) == benchBodyBytes {
							whole++
						}
					}
					if len(ready) != depth || whole != depth {
						b.Fatalf("consumed %d requests, %d bodies whole, want %d of each", len(ready), whole, depth)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/request")
			})
		}
	}
}

// BenchmarkH3AssembleBody is the HTTP/3 side: the datagram-frames-to-
// requests path of [BenchmarkH3Assemble], each stream carrying a HEADERS
// frame and the whole body behind it in one delivery.
func BenchmarkH3AssembleBody(b *testing.B) {
	for _, depth := range []int{1, 8} {
		for _, side := range benchBodySides {
			b.Run(fmt.Sprintf("depth=%d/%s", depth, side.name), func(b *testing.B) {
				body := benchBody()
				frames := make([]quic.Frame, depth)
				for i := range depth {
					stream := h3Request("POST", fmt.Sprintf("/orders/%d", i),
						[2]string{"content-length", strconv.Itoa(len(body))})
					stream = appendH3Frame(stream, h3Data, body)
					frames[i] = streamFrame(uint64(4*i), stream, true) //nolint:gosec // G115: 4i for i < 8
				}

				a := testAssembler(Config{MaxStreamedBodyBytes: side.limit})
				b.ReportAllocs()
				for b.Loop() {
					a.inFlight.Store(0)
					batch, _, failed, err := a.Frames(frames)
					if err != nil {
						b.Fatal(err)
					}
					if len(failed) != 0 {
						b.Fatalf("%d streams failed", len(failed))
					}
					whole := 0
					for i := range batch.Items {
						if len(batch.Items[i].Body) == benchBodyBytes {
							whole++
						}
					}
					if batch.Len() != depth || whole != depth {
						b.Fatalf("assembled %d requests, %d bodies whole, want %d of each", batch.Len(), whole, depth)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/request")
			})
		}
	}
}
