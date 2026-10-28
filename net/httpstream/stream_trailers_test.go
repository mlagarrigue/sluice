package httpstream

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// trailersAfterStream reads the body through Stream to its end, then
// answers "<bytes>|" and the trailers StreamTrailers returns as name=value.
// A trailer read before the end is an error: there is none to read yet.
// first, when set, is closed once the first piece has been read, so a test
// can send the end of the body only then, after the handler has the request.
func trailersAfterStream(first chan<- struct{}) Handler {
	return func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			r := &b.Items[i]
			if r.StreamTrailers() != nil {
				out[i] = Response{Status: 500, Body: []byte("trailers before the body ended")}
				continue
			}
			src := r.Stream()
			n := 0
			for batch := range src.Stream() {
				if n == 0 && first != nil {
					close(first)
				}
				for _, p := range batch.Items {
					n += len(p)
				}
			}
			if err := src.Err(); err != nil {
				out[i] = Response{Status: 400, Body: []byte(err.Error())}
				continue
			}
			body := fmt.Appendf(nil, "%d|", n)
			for _, h := range r.StreamTrailers() {
				body = append(append(append(append(body, ' '), h.Name...), '='), h.Value...)
			}
			out[i] = Response{Status: 200, Body: body}
		}
		return sluice.Batch[Response]{Items: out}
	}
}

// RFC 9112 §7.1.2: the trailer section of a streamed chunked body is read
// once the body has ended — after the request was handed over — and stays
// readable after the request pipelined behind it took the buffer's front.
// The end of the body arrives alone, once the handler has read the first
// piece: what it had read is dropped, the trailers lie at the front of the
// body's region, and the next request is moved down over them.
func TestStreamTrailersHTTP1(t *testing.T) {
	first := make(chan struct{})
	addr, _ := serveTest(t, streamCfg(), trailersAfterStream(first))
	c := dial(t, addr)
	chunk := pattern(3000)
	if _, err := fmt.Fprintf(c, "POST /up HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s", len(chunk), chunk); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler got no piece of the body")
	}
	next := "POST /next HTTP/1.1\r\nHost: x\r\nX-Pad: " + strings.Repeat("p", 64) + "\r\nContent-Length: 0\r\n\r\n"
	if _, err := fmt.Fprint(c, "\r\n0\r\nX-Sum: 1\r\nX-Sig: a b\r\n\r\n"+next); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "3000| X-Sum=1 X-Sig=a b" {
		t.Fatalf("streamed body: %s %q, want 200 \"3000| X-Sum=1 X-Sig=a b\"", status, got)
	}
	if _, got := readResponse(t, br); got != "0|" {
		t.Fatalf("the request behind it: %q, want \"0|\"", got)
	}
}

// RFC 9113 §8.1: the trailing HEADERS of a streamed body is read once the
// body has ended.
func TestStreamTrailersHTTP2(t *testing.T) {
	first := make(chan struct{})
	addr, _ := serveH2(t, h2StreamCfg(), trailersAfterStream(first))
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/", [2]string{"te", "trailers"})})
	c.data(1, pattern(1000), false)
	c.flush()
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler got no piece of the body")
	}
	c.data(1, pattern(500), false)
	trailers := literal(nil, "x-sum", "1")
	trailers = literal(trailers, "x-sig", "a b")
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: trailers})
	c.flush()
	if got, want := c.bodies(1)[1], "1500| x-sum=1 x-sig=a b"; got != want {
		t.Errorf("answered %q, want %q", got, want)
	}
}

// RFC 9114 §4.1: the trailing HEADERS frame of a streamed body is read once
// the body has ended.
func TestStreamTrailersHTTP3(t *testing.T) {
	first := make(chan struct{})
	conn, _ := h3StreamServer(t, Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 1 << 20}, trailersAfterStream(first))
	s := askH3Open(t, conn, appendH3Frame(h3Request("POST", "/"), h3Data, pattern(100)))
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler got no piece of the body")
	}
	rest := appendH3Frame(nil, h3Data, pattern(50))
	rest = appendH3Frame(rest, h3Headers, appendQPACK(nil, []Header{
		{Name: []byte("x-sum"), Value: []byte("1")},
		{Name: []byte("x-sig"), Value: []byte("a b")},
	}))
	if _, err := s.WriteFinal(rest); err != nil {
		t.Fatal(err)
	}
	if body, _ := readH3Body(t, s); body != "150| x-sum=1 x-sig=a b" {
		t.Errorf("answered %q, want %q", body, "150| x-sum=1 x-sig=a b")
	}
}
