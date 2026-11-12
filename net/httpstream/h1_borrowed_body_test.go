package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// A body is a view of the connection's read buffer, and the requests
// pipelined behind it are views of the same buffer, handed to the same call.
// A handler that appends to the first body — the batch contract lets it
// treat the slice as its own for the duration of the call — must not write
// over the second request: each body is capped at its length, as the headers
// are.
func TestH1AppendToABodySparesTheNextRequest(t *testing.T) {
	second := "POST /b HTTP/1.1\r\nHost: h\r\nX-K: v\r\nContent-Length: 2\r\n\r\nuv"
	cases := []struct{ name, first string }{
		{"content-length", "POST /a HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nxy"},
		{"chunked", "POST /a HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nxy\r\n0\r\n\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan string, 4)
			addr, _ := serveTest(t, Config{BodyDelivery: BodyWhole}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				if b.Len() == 2 {
					first := &b.Items[0]
					first.Body = append(first.Body, bytes.Repeat([]byte("Z"), 64)...)
					r := &b.Items[1]
					var h strings.Builder
					for _, f := range r.Headers {
						fmt.Fprintf(&h, "%s=%s;", f.Name, f.Value)
					}
					seen <- fmt.Sprintf("%s %s %s %s", r.Method, r.Target, h.String(), r.Body)
				} else {
					seen <- fmt.Sprintf("a batch of %d", b.Len())
				}
				out := make([]Response, b.Len())
				for i := range out {
					out[i] = Response{Status: 200, Body: []byte("ok")}
				}
				return sluice.Batch[Response]{Items: out}
			})
			c := dial(t, addr)
			if _, err := c.Write([]byte(tc.first + second)); err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(c)
			readResponse(t, br)
			readResponse(t, br)
			want := "POST /b Host=h;X-K=v;Content-Length=2; uv"
			if got := <-seen; got != want {
				t.Fatalf("the second request, read after the handler appended to the first body:\n got %q\nwant %q", got, want)
			}
		})
	}
}
