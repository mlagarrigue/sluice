package httpstream

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// BenchmarkServeH1RoundTrip is one HTTP/1.1 request answered per round trip
// on a loopback connection: a batch of one, the case where whatever the
// server does per batch weighs most. "watched" has the handler ask its
// context for Done, which is what a handler handing it to a query does, and
// which arms the watch on the client (see [Handler]).
func BenchmarkServeH1RoundTrip(b *testing.B) {
	for _, watched := range []bool{false, true} {
		name := "plain"
		if watched {
			name = "watched"
		}
		b.Run(name, func(b *testing.B) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			body := []byte("ok")
			go func() {
				defer close(done)
				_ = Serve(ctx, ln, Config{ShutdownGrace: time.Second, MaxRequestsPerConn: 1 << 30}, func(ctx context.Context, in sluice.Batch[Request]) sluice.Batch[Response] {
					if watched {
						select {
						case <-ctx.Done():
						default:
						}
					}
					out := make([]Response, len(in.Items))
					for i := range out {
						out[i] = Response{Status: 200, Body: body}
					}
					return sluice.Batch[Response]{Items: out}
				})
			}()
			defer func() { cancel(); <-done }()

			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			req := []byte("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
			r := bufio.NewReader(c)
			// The answer's length is fixed: read it once to learn it.
			if _, err := c.Write(req); err != nil {
				b.Fatal(err)
			}
			n := 0
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					b.Fatal(err)
				}
				n += len(line)
				if line == "\r\n" {
					break
				}
			}
			n += len(body)
			if _, err := r.Discard(len(body)); err != nil {
				b.Fatal(err)
			}
			buf := make([]byte, n)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.Write(req); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(r, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
