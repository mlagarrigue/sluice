package httpstream_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// A server whose handler is a stage: it receives a batch of requests and
// answers it positionally, which is the same shape parallel.Ordered takes.
func ExampleServe() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = httpstream.Serve(ctx, ln, httpstream.Config{IdleTimeout: time.Second},
			func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
				// One response per request, in order: on a pipelined
				// connection the position is the only correlation there is.
				out := make([]httpstream.Response, 0, b.Len())
				for _, r := range b.Items {
					out = append(out, httpstream.Response{
						Status: 200,
						Body:   append([]byte("you asked for "), r.Target...),
					})
				}
				return sluice.Batch[httpstream.Response]{Items: out}
			})
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()
	fmt.Fprint(c, "GET /orders/7 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	// Connection: close means the response ends with the connection, so
	// reading to EOF collects all of it however the kernel splits the reads.
	resp, err := io.ReadAll(c)
	if err != nil {
		panic(err)
	}
	// The body is the last line; the headers before it carry the length.
	fmt.Println(string(resp[len(resp)-24:]))
	// Output: you asked for /orders/7
}
