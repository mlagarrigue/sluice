package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// streamCfg streams bodies past a small whole-body bound.
func streamCfg() Config {
	return Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 1 << 20}
}

// readAllHandler answers each request with what its body held, read whole
// through ReadAll: "<length> <first byte> <last byte>", or the error.
func readAllHandler(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		body, err := r.ReadAll(nil, 1<<20)
		switch {
		case err != nil:
			out[i] = Response{Status: 400, Body: []byte(err.Error())}
		case len(body) == 0:
			out[i] = Response{Status: 200, Body: append([]byte("0 "), r.Target...)}
		default:
			out[i] = Response{Status: 200, Body: fmt.Appendf(nil, "%d %c %c", len(body), body[0], body[len(body)-1])}
		}
	}
	return sluice.Batch[Response]{Items: out}
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return b
}

// A body four times MaxBodyBytes arrives through the stream, whole and in
// order, and the request pipelined behind it is served next on the same
// connection.
func TestStreamedBodyPastMaxBodyBytes(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	body := pattern(4096)
	req := fmt.Sprintf("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", len(body))
	// Sent in pieces, so the body is not whole in the read of its head.
	for _, part := range [][]byte{[]byte(req), body[:1000], body[1000:3000], append(body[3000:], "GET /next HTTP/1.1\r\nHost: x\r\n\r\n"...)} {
		if _, err := c.Write(part); err != nil {
			t.Fatal(err)
		}
	}
	br := bufio.NewReader(c)
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "4096 a n" {
		t.Fatalf("streamed body: %s %q, want 200 \"4096 a n\"", status, got)
	}
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "0 /next" {
		t.Fatalf("the request behind it: %s %q", status, got)
	}
}

// A chunked body whose chunks exceed MaxBodyBytes streams, its trailer
// section is checked and dropped, and the connection carries on.
func TestStreamedChunkedBody(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	chunk := pattern(3000)
	var w bytes.Buffer
	w.WriteString("POST /up HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
	for range 2 {
		fmt.Fprintf(&w, "%x;ext=1\r\n%s\r\n", len(chunk), chunk)
	}
	w.WriteString("0\r\nX-Sum: 1\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n")
	raw := w.Bytes()
	for i := 0; i < len(raw); i += 700 {
		if _, err := c.Write(raw[i:min(i+700, len(raw))]); err != nil {
			t.Fatal(err)
		}
	}
	br := bufio.NewReader(c)
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "6000 a j" {
		t.Fatalf("chunked body: %s %q, want 200 \"6000 a j\"", status, got)
	}
	if _, got := readResponse(t, br); got != "0 /next" {
		t.Fatalf("the request behind it: %q", got)
	}
}

// Framing that breaks midway reaches the handler as the stream's error, and
// the connection closes after the handler's answer.
func TestStreamedChunkedBodyBrokenMidway(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	if _, err := c.Write([]byte("POST /up HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")); err != nil {
		t.Fatal(err)
	}
	// Apart, so the head is handed over before the framing breaks: in one read
	// the parser would refuse the request itself, as it should.
	time.Sleep(100 * time.Millisecond)
	if _, err := c.Write([]byte("zz\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	status, got := readResponse(t, br)
	if !strings.Contains(status, "400") || !strings.Contains(got, "not hexadecimal") {
		t.Fatalf("broken framing: %s %q, want the handler's 400 with the framing error", status, got)
	}
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("after a broken body the connection stayed open: %v", err)
	}
}

// A Content-Length past the streamed bound is refused before any body is
// read, and the client waiting for 100 Continue is not invited to send it.
func TestStreamedBodyRefusedEarly(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	if _, err := c.Write([]byte("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: 2000000\r\nExpect: 100-continue\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "413") {
		t.Fatalf("status = %q, want 413 and no 100 Continue first", status)
	}
}

// A body the handler leaves unread closes the connection after the
// response, which says so.
func TestStreamedBodyUnreadClosesTheConnection(t *testing.T) {
	ignore := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: make([]Response, b.Len())}
	}
	addr, _ := serveTest(t, streamCfg(), ignore)
	c := dial(t, addr)
	if _, err := c.Write([]byte("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: 5000\r\n\r\nabc")); err != nil {
		t.Fatal(err)
	}
	head := readFields(t, bufio.NewReader(c))
	wantLine(t, head, "Connection: close", true)
}

// A client that waits for 100 Continue is invited when the handler first
// reads the body, and its body is then received.
func TestStreamedBodyContinueOnFirstRead(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	if _, err := c.Write([]byte("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: 2000\r\nExpect: 100-continue\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "100 Continue") {
		t.Fatalf("first line = %q, %v; want the 100 Continue", line, err)
	}
	if _, err := br.ReadString('\n'); err != nil { // the blank line closing the interim response
		t.Fatal(err)
	}
	if _, err := c.Write(pattern(2000)); err != nil {
		t.Fatal(err)
	}
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "2000 a x" {
		t.Fatalf("after the 100: %s %q", status, got)
	}
}

// Small bodies whole in one read keep the whole-body contract and still
// batch together, which is what streaming must not cost.
func TestStreamingKeepsSmallBodiesBatched(t *testing.T) {
	var sawStream bool
	h := func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		for _, r := range b.Items {
			if r.body != nil || r.Body == nil {
				sawStream = true
			}
		}
		return readAllHandler(ctx, b)
	}
	addr, sizes := serveTest(t, streamCfg(), h)
	c := dial(t, addr)
	one := "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc"
	if _, err := c.Write([]byte(one + one)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	readResponse(t, br)
	readResponse(t, br)
	if sawStream {
		t.Error("a small body whole in the read was streamed")
	}
	if got := sizes(); len(got) != 1 || got[0] != 2 {
		t.Errorf("batches = %v, want one batch of two", got)
	}
}

// A stream kept past the handler's return refuses to read.
func TestStreamedBodyAfterReturn(t *testing.T) {
	b := &bodyStream{closed: true}
	if _, err := b.next(); !errors.Is(err, errBodyAfterReturn) {
		t.Errorf("read after return: %v", err)
	}
}

func TestMaxStreamedBodyBytesRefusesNegative(t *testing.T) {
	if _, err := (Config{MaxStreamedBodyBytes: -1}).resolve(); err == nil || !strings.Contains(err.Error(), "MaxStreamedBodyBytes") {
		t.Errorf("negative MaxStreamedBodyBytes: %v", err)
	}
}

// A streamed body whose last bytes share a read with the request pipelined
// behind it: the last piece is handed out before that request is moved down
// over the body's region, not after — the other order handed the handler the
// next request's bytes as the end of its body.
func TestStreamedBodyEndSharesAReadWithTheNextRequest(t *testing.T) {
	addr, _ := serveTest(t, streamCfg(), readAllHandler)
	c := dial(t, addr)
	body := pattern(2000) // over MaxBodyBytes: it streams although whole in the first read
	req := fmt.Sprintf("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%sGET /next HTTP/1.1\r\nHost: x\r\n\r\n", len(body), body)
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "2000 a x" {
		t.Fatalf("streamed body: %s %q, want 200 \"2000 a x\"", status, got)
	}
	if status, got := readResponse(t, br); !strings.Contains(status, "200") || got != "0 /next" {
		t.Fatalf("the request behind it: %s %q", status, got)
	}
}
