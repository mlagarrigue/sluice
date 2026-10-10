// Command interopserver is the server that third-party clients are pointed
// at: curl over HTTP/1.1 and HTTP/2, and h2spec for RFC 9113 conformance.
// scripts/interop-http.sh runs it, locally and in continuous integration.
//
// It is deliberately small. Every request is answered with its own target,
// which lets a client check that the response it got is the one it asked
// for; /stream answers with a streamed body of several batches, so that a
// streamed HTTP/2 response — the one path whose flow control is pulled a
// batch at a time — crosses a wire a real client reads.
//
// The address it listens on is printed on standard output, on its own line,
// once the listener is open: the script reads it rather than guessing a free
// port.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on; port 0 picks a free one")
	flag.Parse()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ln.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = httpstream.Serve(ctx, ln, config, handle)
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// config leaves every bound at its default: h2spec probes the protocol's
// edges, and a bound tighter than the protocol's own would be reported as a
// failure of ours. The timeouts are long enough for a slow runner.
var config = httpstream.Config{
	IdleTimeout:  10 * time.Second,
	ReadTimeout:  10 * time.Second,
	WriteTimeout: 10 * time.Second,
}

// handle answers each request with its target, and /stream with a streamed
// body: one response per request, in order, as every httpstream handler.
func handle(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, 0, b.Len())
	for _, r := range b.Items {
		if string(r.Target) == "/stream" {
			out = append(out, httpstream.Response{Status: 200, Stream: streamed})
			continue
		}
		out = append(out, httpstream.Response{
			Status: 200,
			Body:   append([]byte("you asked for "), r.Target...),
		})
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

// streamed yields three batches of one line each. The client sees the
// concatenation; what the transport sees is three pulls, each framed when
// the client's window admits it.
func streamed(yield func(sluice.Batch[[]byte]) bool) {
	for i := 1; i <= 3; i++ {
		line := fmt.Appendf(nil, "batch %d\n", i)
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{line}}) {
			return
		}
	}
}
