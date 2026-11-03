package httpstream

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// replayConn serves the same bytes over and over without a syscall, so a
// benchmark measures this package rather than the loopback interface.
type replayConn struct {
	data []byte
	pos  int
	max  int // stop after this many bytes, so the stream ends
}

func (c *replayConn) Read(p []byte) (int, error) {
	if c.pos >= c.max {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.pos%len(c.data):])
	c.pos += n
	return n, nil
}

func (c *replayConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *replayConn) Close() error                     { return nil }
func (c *replayConn) LocalAddr() net.Addr              { return nil }
func (c *replayConn) RemoteAddr() net.Addr             { return nil }
func (c *replayConn) SetDeadline(time.Time) error      { return nil }
func (c *replayConn) SetReadDeadline(time.Time) error  { return nil }
func (c *replayConn) SetWriteDeadline(time.Time) error { return nil }

func pipelined(depth int) []byte {
	var b strings.Builder
	for i := range depth {
		fmt.Fprintf(&b, "GET /orders/%d HTTP/1.1\r\nHost: h\r\nAccept: application/json\r\n\r\n", i)
	}
	return []byte(b.String())
}

// What a pipelined batch is worth, measured at the reader rather than over a
// socket: the transport's own syscalls would otherwise dominate and hide the
// figure this package is about.
//
// The per-request cost falls with depth because the read, the buffer refill
// and the batch hand-off are paid once per batch and the parse once per
// request — which is the same arithmetic every other operator here makes, now
// applied to the transport.
func BenchmarkReadPipelined(b *testing.B) {
	for _, depth := range []int{1, 2, 8, 64} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			data := pipelined(depth)
			const requests = 4096
			bytesPerRun := len(data) * (requests / depth)

			b.ReportAllocs()
			b.SetBytes(int64(bytesPerRun))
			for b.Loop() {
				conn := &replayConn{data: data, max: bytesPerRun}
				src := Requests(conn, Config{MaxRequestsPerBatch: depth, MaxRequestsPerConn: requests})
				n := 0
				src.Stream()(func(batch sluice.Batch[Request]) bool {
					n += batch.Len()
					return true
				})
				if n != requests {
					b.Fatalf("read %d requests, want %d", n, requests)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*requests), "ns/request")
		})
	}
}

// What the Keep-Alive announcement costs per response: the same batch of
// eight answers written by WriteBatch, which announces nothing, and by Serve's
// writer, which appends the connection's two fields.
func BenchmarkWriteBatch(b *testing.B) {
	requests := make([]Request, 8)
	responses := make([]Response, 8)
	for i := range requests {
		requests[i] = Request{Method: []byte("GET"), KeepAlive: true}
		responses[i] = Response{Status: 200, Body: []byte(`{"id":7}`), Headers: []Header{
			{Name: []byte("Content-Type"), Value: []byte("application/json")},
		}}
	}
	cfg := Config{}
	b.Run("keep-alive", func(b *testing.B) {
		b.ReportAllocs()
		var out []byte
		for b.Loop() {
			var err error
			if out, err = WriteBatch(out[:0], cfg, requests, responses); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(responses)), "ns/response")
	})
}

// What a buffer full of small pipelined requests costs per request, as the
// buffer grows: a buffer grown once (a large body under BodyWhole, up to
// maxBuffer) stays grown, and a client that pipelines fills all of it in one
// read. The cost per request must not grow with the buffer — which it does
// if every batch moves what is left of the buffer down to its front.
func BenchmarkReadPipelinedBuffer(b *testing.B) {
	const req = "GET / HTTP/1.1\r\nHost: h\r\n\r\n"
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("buffer=%dKiB", size>>10), func(b *testing.B) {
			requests := size / len(req)
			cfg := Config{MaxRequestsPerBatch: 64, MaxRequestsPerConn: requests, BodyDelivery: BodyWhole}.mustResolve()
			if size > cfg.maxBuffer() {
				b.Skipf("buffer %d is past maxBuffer %d", size, cfg.maxBuffer())
			}
			data := []byte(strings.Repeat(req, requests))
			buf := make([]byte, size)
			b.ReportAllocs()
			for b.Loop() {
				r := &h1Conn{cfg: cfg, conn: &replayConn{data: data, max: len(data)}, buf: buf}
				n := 0
				for {
					batch, ok := r.next()
					if !ok {
						break
					}
					n += len(batch)
				}
				if n != requests {
					b.Fatalf("read %d requests, want %d: %v", n, requests, r.err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*requests), "ns/request")
		})
	}
}
