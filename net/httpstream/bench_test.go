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
