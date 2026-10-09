package httpstream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// serveTest starts a server on a loopback port and returns its address. The
// handler is wrapped so a test can watch the batch sizes it was given, which
// is the whole point of the package.
func serveTest(t *testing.T, cfg Config, h Handler) (addr string, sizes func() []int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var seen []int

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, ln, cfg, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			mu.Lock()
			seen = append(seen, b.Len())
			mu.Unlock()
			return h(b)
		})
	}()
	t.Cleanup(func() { cancel(); <-done })

	return ln.Addr().String(), func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), seen...)
	}
}

// echoTarget answers each request with its own target, so a test can tell
// which answer belongs to which request.
func echoTarget(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for _, r := range b.Items {
		out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
	}
	return sluice.Batch[Response]{Items: out}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

// readResponse reads one response and returns its status line and body.
func readResponse(t *testing.T, br *bufio.Reader) (status string, body string) {
	t.Helper()
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the status line: %v", err)
	}
	length := -1
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ": "); ok && strings.EqualFold(name, "content-length") {
			if _, err := fmt.Sscanf(value, "%d", &length); err != nil {
				t.Fatalf("Content-Length %q: %v", value, err)
			}
		}
	}
	if length < 0 {
		t.Fatal("the response carried no Content-Length")
	}
	buf := make([]byte, length)
	if _, err := readFull(br, buf); err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	return strings.TrimRight(status, "\r\n"), string(buf)
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := br.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func TestServeAnswersOneRequest(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /hello HTTP/1.1\r\nHost: h\r\n\r\n")
	status, body := readResponse(t, bufio.NewReader(c))

	if status != "HTTP/1.1 200 OK" {
		t.Errorf("status line = %q", status)
	}
	if body != "/hello" {
		t.Errorf("body = %q", body)
	}
}

// The thesis, and the one thing net/http cannot express: a client that
// pipelines puts several requests on the wire before reading an answer, and
// they reach the pipeline as one batch.
func TestPipelinedRequestsArriveAsOneBatch(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	const n = 8
	var out strings.Builder
	for i := range n {
		fmt.Fprintf(&out, "GET /r%d HTTP/1.1\r\nHost: h\r\n\r\n", i)
	}
	if _, err := c.Write([]byte(out.String())); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(c)
	for i := range n {
		status, body := readResponse(t, br)
		if status != "HTTP/1.1 200 OK" {
			t.Fatalf("response %d: status %q", i, status)
		}
		// Order is the correlation on a pipelined connection: the nth answer
		// belongs to the nth request and nothing on the wire says so.
		if want := fmt.Sprintf("/r%d", i); body != want {
			t.Fatalf("response %d answered %q, want %q — the order was not kept", i, body, want)
		}
	}

	got := sizes()
	if len(got) == 0 {
		t.Fatal("the handler was never called")
	}
	if got[0] < 2 {
		t.Errorf("batch sizes %v: eight pipelined requests were served one at a time, "+
			"which is what this package exists not to do", got)
	}
	t.Logf("batch sizes seen: %v", got)
}

// Keep-alive without pipelining is the ordinary case, and it produces batches
// of one — which the package documents rather than hides.
func TestKeepAliveServesSeveralRequests(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)
	br := bufio.NewReader(c)

	for i := range 3 {
		fmt.Fprintf(c, "GET /r%d HTTP/1.1\r\nHost: h\r\n\r\n", i)
		_, body := readResponse(t, br)
		if want := fmt.Sprintf("/r%d", i); body != want {
			t.Fatalf("request %d answered %q", i, body)
		}
	}
	for _, n := range sizes() {
		if n != 1 {
			t.Errorf("a client that waits for each answer produced a batch of %d", n)
		}
	}
}

// Connection: close is honoured, or a client waits on a socket nobody will
// write to again.
func TestConnectionCloseEndsTheStream(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	br := bufio.NewReader(c)
	readResponse(t, br)

	if _, err := br.ReadByte(); err == nil {
		t.Error("the connection stayed open after Connection: close")
	}
}

// A handler that answers the wrong number of requests would answer the wrong
// caller, since position is the only correlation a pipelined connection has.
func TestHandlerThatDropsARequestIsRefused(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		// The mistake the gateway documents: filtering an element out of a
		// pipeline that correlates by position.
		return sluice.Batch[Response]{Items: nil}
	})
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 500") {
		t.Errorf("status = %q, want a 500: the dropped request was answered by someone else's response", status)
	}
}

// A panic in the handler is the caller's bug and must not take the process
// down with it — the same bargain web.Recover makes over net/http.
func TestHandlerPanicBecomesA500(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(sluice.Batch[Request]) sluice.Batch[Response] {
		panic("the caller's own bug")
	})
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 500") {
		t.Errorf("status = %q, want 500", status)
	}
}

// A header value carrying CRLF would end the field early and let what follows
// be read as another response. Refused at the only place it can be.
func TestResponseSplittingIsRefused(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Headers: []Header{{
				Name:  []byte("X-Echo"),
				Value: []byte("a\r\nX-Injected: yes"),
			}},
		}}}
	})
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 500") {
		t.Errorf("status = %q: an injected header was written to the wire", status)
	}
}

func TestMalformedRequestIsAnsweredAndClosed(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"smuggling pair", "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n", "400"},
		{"transfer coding not implemented", "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n", "501"},
		{"no Host", "GET /x HTTP/1.1\r\n\r\n", "400"},
		{"absolute-form", "GET http://elsewhere/x HTTP/1.1\r\nHost: h\r\n\r\n", "400"},
		{"body over the bound", "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 999999999\r\n\r\n", "413"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{MaxBodyBytes: 1 << 10}, echoTarget)
			c := dial(t, addr)
			fmt.Fprint(c, tt.raw)
			status, _ := readResponse(t, bufio.NewReader(c))
			if !strings.Contains(status, tt.want) {
				t.Errorf("status = %q, want %s", status, tt.want)
			}
		})
	}
}

// A client that opens a connection and sends a header byte at a time is
// answered by the clock rather than tolerated.
func TestSlowClientIsTimedOut(t *testing.T) {
	addr, _ := serveTest(t, Config{ReadTimeout: 100 * time.Millisecond}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHo") // a request that never finishes
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.Contains(status, "408") {
		t.Errorf("status = %q, want 408", status)
	}
}

// newReader wraps a connection for the response reader above, so a test in
// another file can use it without importing bufio for one line.
func newReader(c net.Conn) *bufio.Reader { return bufio.NewReader(c) }

// A batch borrows the read buffer, so the buffer may not be compacted until
// the consumer has finished with it.
//
// The case that exposes it is a complete request followed by a partial one:
// the reader then has a tail to move down, and moving it before the batch was
// yielded writes it over the bodies the consumer is holding. Eight pipelined
// requests that arrive whole never show it, because there is no tail to move
// — which is why this test leaves one deliberately unfinished.
func TestABatchIsNotOverwrittenByTheNextRead(t *testing.T) {
	seen := make(chan []string, 4)
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		bodies := make([]string, 0, b.Len())
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			bodies = append(bodies, string(r.Body))
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Body...)})
		}
		seen <- bodies
		return sluice.Batch[Response]{Items: out}
	})

	c := dial(t, addr)
	// Two whole requests, then the beginning of a third: the reader parses
	// two, and holds a tail it must not shift over their bodies.
	_, _ = fmt.Fprint(c, "POST /a HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\naaaaa")
	_, _ = fmt.Fprint(c, "POST /b HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nbbbbb")
	_, _ = fmt.Fprint(c, "POST /c HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\ncc")

	select {
	case bodies := <-seen:
		for i, got := range bodies {
			want := strings.Repeat(string(rune('a'+i)), 5)
			if got != want {
				t.Errorf("request %d had body %q, want %q — the buffer moved under the batch", i, got, want)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no batch was delivered")
	}
}

// A response produced as it goes, over HTTP/1.1.
//
// This is the half of the model the package was missing: the request side was
// a stream of batches from the start, and the response side was a []byte that
// had to exist in full before a byte went out. A report from a cursor, an
// export, an event feed — none of them can be written that way, and none of
// them is unusual.
func TestStreamedResponseOverHTTP11(t *testing.T) {
	pulls := make(chan int, 8)
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range 3 {
					pulls <- i
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "part%d ", i)}}) {
						return
					}
				}
			},
		}}}
	})

	c := dial(t, addr)
	fmt.Fprint(c, "GET /report HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	chunked := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.EqualFold(line, "Transfer-Encoding: chunked") {
			chunked = true
		}
		if strings.HasPrefix(strings.ToLower(line), "content-length") {
			t.Error("a streamed response announced a length it cannot know")
		}
	}
	if !chunked {
		t.Fatal("a streamed response was not framed in chunks")
	}

	// Decoded by hand rather than with a helper: what is being checked is the
	// framing this package wrote.
	var body strings.Builder
	for {
		sizeLine, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(sizeLine), "%x", &n); err != nil {
			t.Fatalf("a chunk size that is not hexadecimal: %q", sizeLine)
		}
		if n == 0 {
			break
		}
		chunk := make([]byte, n)
		if _, err := readFull(br, chunk); err != nil {
			t.Fatal(err)
		}
		body.Write(chunk)
		if _, err := br.ReadString('\n'); err != nil { // the chunk's trailing CRLF
			t.Fatal(err)
		}
	}
	if got := body.String(); got != "part0 part1 part2 " {
		t.Errorf("body = %q", got)
	}
	if len(pulls) != 3 {
		t.Errorf("the producer ran %d times, want 3", len(pulls))
	}
}

// A streamed response answering HEAD states no length and sends no body,
// same as the buffered path does — a handler that streams an export doesn't
// get to desync a pipelined HEAD request behind it.
func TestStreamedResponseCarriesNoBodyOnHEAD(t *testing.T) {
	var headPulled atomic.Bool
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, req := range b.Items {
			method := string(req.Method)
			out = append(out, Response{
				Status: 200,
				Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if method == "HEAD" {
						headPulled.Store(true)
					}
					yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("part0 ")}})
				},
			})
		}
		return sluice.Batch[Response]{Items: out}
	})

	c := dial(t, addr)
	fmt.Fprint(c, "HEAD /export HTTP/1.1\r\nHost: h\r\n\r\nGET /next HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	br := bufio.NewReader(c)
	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.EqualFold(line, "Transfer-Encoding: chunked") {
			t.Error("a HEAD response announced a chunked body it will never send")
		}
	}
	if headPulled.Load() {
		t.Error("the producer ran for a HEAD request that will never read its body")
	}

	// The next bytes must be the second response's status line: a server
	// that wrote chunks here desynchronises the connection.
	status2 := readStatusLine(t, br)
	if !strings.HasPrefix(status2, "HTTP/1.1 200") {
		t.Fatalf("second response status = %q — the HEAD response's body ran into it", status2)
	}
}

// A response cannot say two things about its payload.
func TestResponseWithBothBodyAndStreamIsRefused(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Body:   []byte("here"),
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {},
		}}}
	})
	c := dial(t, addr)
	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")

	// The connection is closed rather than answered with something that
	// contradicts itself.
	buf := make([]byte, 64)
	if n, err := c.Read(buf); err == nil && n > 0 && !strings.Contains(string(buf[:n]), "500") {
		t.Errorf("a self-contradicting response was written: %q", buf[:n])
	}
}

// The producer runs at the socket's pace: a consumer that stops taking the
// answer stops the producer, which is what makes memory bounded by one batch
// rather than by the whole response.
func TestStreamedResponseStopsWhenTheClientLeaves(t *testing.T) {
	var produced atomic.Int64
	started, returned := make(chan struct{}), make(chan struct{})
	addr, _ := serveTest(t, Config{WriteTimeout: 300 * time.Millisecond},
		func(b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: []Response{{
				Status: 200,
				Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					defer close(returned)
					close(started)
					big := make([]byte, 64<<10)
					for range 1000 {
						produced.Add(1)
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{big}}) {
							return // the transport stopped pulling
						}
					}
				},
			}}}
		})

	c := dial(t, addr)
	fmt.Fprint(c, "GET /big HTTP/1.1\r\nHost: h\r\n\r\n")
	<-started     // the response is under way
	_ = c.Close() // the client walks away mid-response

	// The producer's return is the event: a bounded wait for it, where a
	// fixed sleep only guessed how long it takes.
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the producer was still running 5s after the client left")
	}
	if n := produced.Load(); n >= 1000 {
		t.Errorf("the producer ran %d times for a client that left: it is not being pulled", n)
	}
}

// A streamed response to an HTTP/1.0 client cannot be chunked: RFC 9112 §6.1
// forbids Transfer-Encoding in a response unless the request was HTTP/1.1,
// and a 1.0 client reads the chunk sizes as body bytes. The body is
// close-delimited instead — raw bytes to EOF, Connection: close stated even
// when the client asked for keep-alive, since EOF is the only end such a
// body has.
func TestStreamedResponseToHTTP10IsCloseDelimited(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range 3 {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "part%d ", i)}}) {
						return
					}
				}
			},
		}}}
	})

	c := dial(t, addr)
	fmt.Fprint(c, "GET /report HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")

	br := bufio.NewReader(c)
	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	headers := readHeaders(t, br)
	if te, ok := headers["transfer-encoding"]; ok {
		t.Errorf("a response to an HTTP/1.0 request carried Transfer-Encoding: %q", te)
	}
	if cl, ok := headers["content-length"]; ok {
		t.Errorf("a streamed response announced Content-Length %q it cannot know", cl)
	}
	if !strings.EqualFold(headers["connection"], "close") {
		t.Errorf("connection = %q, want close — a close-delimited body has no other end", headers["connection"])
	}

	// The body is the producer's bytes raw, to EOF: a chunk size line in
	// here means the 1.0 client would have read it as payload.
	body, err := io.ReadAll(br)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "part0 part1 part2 " {
		t.Errorf("body = %q, want the raw parts with no chunk framing", body)
	}
}

// A streamed batch whose failure strikes before any byte of the failing
// response went out answers a 500, as the buffered path does — closing with
// nothing said leaves the client to guess which request was at fault. The
// responses already owed ahead of it still go out first.
func TestStreamedBatchAnswers500WhenAResponseFailsBeforeItsFirstByte(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, req := range b.Items {
			if string(req.Target) == "/bad" {
				// Body beside Stream: refused by check before a byte of this
				// response is written.
				out = append(out, Response{
					Status: 200,
					Body:   []byte("here"),
					Stream: func(yield func(sluice.Batch[[]byte]) bool) {},
				})
				continue
			}
			out = append(out, Response{Status: 200, Body: []byte("ok")})
		}
		return sluice.Batch[Response]{Items: out}
	})

	c := dial(t, addr)
	// One write, so the two requests form one batch — the second response is
	// the streamed one that makes the batch take the streamed path.
	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /bad HTTP/1.1\r\nHost: h\r\n\r\n")

	br := bufio.NewReader(c)
	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("first status = %q, want the healthy response before the failure", status)
	}
	headers := readHeaders(t, br)
	body := make([]byte, 2)
	if _, err := readFull(br, body); err != nil || string(body) != "ok" {
		t.Fatalf("first body = %q, %v", body, err)
	}
	_ = headers

	status2 := readStatusLine(t, br)
	if !strings.HasPrefix(status2, "HTTP/1.1 500") {
		t.Fatalf("second status = %q, want the 500 the buffered path would have answered", status2)
	}
	headers2 := readHeaders(t, br)
	if !strings.EqualFold(headers2["connection"], "close") {
		t.Errorf("connection = %q, want close — the batch cannot continue past the failure", headers2["connection"])
	}
}

// Buffered responses on either side of a streamed one still come out correct
// and in order — they are coalesced into shared writes around the stream, and
// coalescing must not change what the client reads.
func TestStreamedBatchKeepsBufferedNeighboursInOrder(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, req := range b.Items {
			if string(req.Target) == "/stream" {
				out = append(out, Response{
					Status: 200,
					Stream: func(yield func(sluice.Batch[[]byte]) bool) {
						yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("flow")}})
					},
				})
				continue
			}
			out = append(out, Response{Status: 200, Body: append([]byte("got "), req.Target...)})
		}
		return sluice.Batch[Response]{Items: out}
	})

	c := dial(t, addr)
	fmt.Fprint(c, "GET /1 HTTP/1.1\r\nHost: h\r\n\r\n"+
		"GET /2 HTTP/1.1\r\nHost: h\r\n\r\n"+
		"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n"+
		"GET /3 HTTP/1.1\r\nHost: h\r\n\r\n"+
		"GET /4 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	br := bufio.NewReader(c)
	for _, want := range []string{"got /1", "got /2"} {
		readStatusLine(t, br)
		h := readHeaders(t, br)
		body := make([]byte, len(want))
		if _, err := readFull(br, body); err != nil || string(body) != want {
			t.Fatalf("body = %q (%v), want %q; headers %v", body, err, want, h)
		}
	}

	// The streamed response, chunked: one "flow" chunk and the last chunk.
	readStatusLine(t, br)
	readHeaders(t, br)
	for _, want := range []string{"4\r\n", "flow", "\r\n", "0\r\n", "\r\n"} {
		got := make([]byte, len(want))
		if _, err := readFull(br, got); err != nil || string(got) != want {
			t.Fatalf("chunk framing = %q (%v), want %q", got, err, want)
		}
	}

	for _, want := range []string{"got /3", "got /4"} {
		readStatusLine(t, br)
		readHeaders(t, br)
		body := make([]byte, len(want))
		if _, err := readFull(br, body); err != nil || string(body) != want {
			t.Fatalf("body = %q (%v), want %q", body, err, want)
		}
	}
}

// An interim 100 must not overtake the final responses of requests pipelined
// ahead of the Expect one: RFC 9112 §9.3 orders everything on the connection.
// The reader defers the 100 until the batch in front of it has been answered.
func TestExpect100WaitsForTheFinalsPipelinedAheadOfIt(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	// One write: a complete GET, then a POST whose body waits on the 100.
	fmt.Fprint(c, "GET /first HTTP/1.1\r\nHost: h\r\n\r\n"+
		"POST /second HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n")

	br := bufio.NewReader(c)
	status := readStatusLine(t, br)
	if strings.HasPrefix(status, "HTTP/1.1 100") {
		t.Fatal("the interim 100 overtook the final response of the request pipelined ahead of it")
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("first status = %q", status)
	}
	readHeaders(t, br)
	body := make([]byte, len("/first"))
	if _, err := readFull(br, body); err != nil || string(body) != "/first" {
		t.Fatalf("first body = %q, %v", body, err)
	}

	// Now the 100, then the body, then the final.
	if status := readStatusLine(t, br); !strings.HasPrefix(status, "HTTP/1.1 100") {
		t.Fatalf("status = %q, want the interim 100", status)
	}
	readHeaders(t, br)
	fmt.Fprint(c, "abcd")
	if status := readStatusLine(t, br); !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("final status = %q", status)
	}
}

// A streamed response's producer runs after the handler has returned, out of
// reach of the handler's recover. A panic there used to end the process. On
// HTTP/1.1 the header and the first chunk are already on the wire, so there
// is no status left to change: the connection closes with the chunked body
// unterminated, and the server goes on serving everyone else.
func TestStreamProducerPanicClosesOnlyItsConnection(t *testing.T) {
	addr, _ := serveTest(t, Config{}, panickingProducer)

	c := dial(t, addr)
	if _, err := io.WriteString(c, "GET /boom HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("the connection was not closed: %v (read %q)", err, got)
	}
	if !strings.Contains(string(got), "first") {
		t.Errorf("the chunk before the panic did not arrive: %q", got)
	}
	if strings.HasSuffix(string(got), "0\r\n\r\n") {
		t.Errorf("the body was terminated as if it had completed: %q", got)
	}

	// The process, and the server in it, are still up.
	c2 := dial(t, addr)
	if _, err := io.WriteString(c2, "GET /ok HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if _, body := readResponse(t, bufio.NewReader(c2)); body != "/ok" {
		t.Errorf("a fresh connection answered %q, want %q", body, "/ok")
	}
}

// cadenceProducer yields only empty batches, one per millisecond — a feed
// with nothing to say yet — until its yield returns false, which it reports
// by closing stopped, or until release closes. started closes on its first
// pull. An empty batch writes nothing, so no write failure can tell such a
// producer that the client is gone: the transport has to.
func cadenceProducer(started, stopped chan struct{}, release <-chan struct{}) sluice.Stream[[]byte] {
	return func(yield func(sluice.Batch[[]byte]) bool) {
		close(started)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			if !yield(sluice.Batch[[]byte]{}) {
				close(stopped)
				return
			}
			select {
			case <-release:
				return
			case <-tick.C:
			}
		}
	}
}

// waitStopped fails the test unless the producer was told within a bound.
func waitStopped(t *testing.T, stopped <-chan struct{}) {
	t.Helper()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("a producer yielding only empty batches was never told the client left")
	}
}

// A producer of cadence ticks learns the client left on its next batch, not
// on a next write it may never make.
func TestStreamedResponseOfEmptyBatchesStopsWhenTheClientLeaves(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200, Stream: cadenceProducer(started, stopped, release),
		}}}
	})
	// Registered after serveTest, so it runs before the server is waited
	// for: without the fix the producer would otherwise hold Serve forever.
	t.Cleanup(func() { close(release) })

	c := dial(t, addr)
	fmt.Fprint(c, "GET /feed HTTP/1.1\r\nHost: h\r\n\r\n")
	if _, err := bufio.NewReader(c).ReadString('\n'); err != nil {
		t.Fatal(err) // the status line: the response is streaming
	}
	<-started
	_ = c.Close()

	waitStopped(t, stopped)
}

// The HTTP/1.1 writer refuses the connection-specific fields HTTP/2 and
// HTTP/3 refuse: the connection is the writer's to describe, and a handler's
// Connection, Upgrade or Trailer would tell the client something about the
// following bytes that is not true.
func TestWriteBatchRefusesConnectionSpecificFields(t *testing.T) {
	for _, field := range []string{"Connection", "keep-alive", "Proxy-Connection", "Upgrade", "Trailer"} {
		res := Response{Status: 200, Headers: []Header{{Name: []byte(field), Value: []byte("x")}}}
		if _, err := WriteBatch(nil, nil, []Response{res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", field, err)
		}
	}
}

// The sniff's deadline covers the whole preface, not each read of it: a
// client dribbling the preface a byte at a time, each byte inside the idle
// timeout, is closed once the timeout has passed since the connection began
// rather than holding it for twenty-four of them.
func TestSniffDeadlineCoversTheWholePreface(t *testing.T) {
	const idle = 300 * time.Millisecond
	addr, _ := serveTest(t, Config{IdleTimeout: idle}, echoTarget)
	c := dial(t, addr)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, _ = io.Copy(io.Discard, c)
	}()
	tick := time.NewTicker(idle / 2)
	defer tick.Stop()
	start := time.Now()
	for i := range len(Preface) {
		select {
		case <-closed:
			if d := time.Since(start); d > 5*idle {
				t.Errorf("closed after %v, want about one idle timeout (%v)", d, idle)
			}
			return
		case <-tick.C:
		}
		if _, err := c.Write([]byte{Preface[i]}); err != nil {
			return // closed under the write: the same outcome
		}
	}
	t.Fatal("a client dribbling the preface byte by byte was never closed")
}

// A handler's Connection: close is honoured rather than refused: the
// responses of the batch all go out, the last one carries a single
// Connection: close written by the server, and the connection ends — where
// a refusal turned an ordinary HTTP/1.1 request to close into a 500.
func TestHandlerConnectionCloseIsHonoured(t *testing.T) {
	closer := func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for i, r := range b.Items {
			res := Response{Status: 200, Body: append([]byte(nil), r.Target...)}
			if i == 0 {
				res.Headers = []Header{{Name: []byte("Connection"), Value: []byte("Close")}}
			}
			out = append(out, res)
		}
		return sluice.Batch[Response]{Items: out}
	}
	addr, _ := serveTest(t, Config{}, closer)
	c := dial(t, addr)
	if _, err := c.Write([]byte("GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(c) // returns at EOF: the server closed
	if err != nil {
		t.Fatalf("reading until the server closes: %v", err)
	}
	got := string(all)
	if strings.Count(got, "HTTP/1.1 200") != 2 || !strings.Contains(got, "/a") || !strings.Contains(got, "/b") {
		t.Fatalf("want both responses before the close, got %q", got)
	}
	if n := strings.Count(strings.ToLower(got), "connection: close"); n != 1 {
		t.Fatalf("Connection: close appears %d times, want once on the last response: %q", n, got)
	}
	if i := strings.Index(strings.ToLower(got), "connection: close"); i < strings.Index(got, "/a") {
		t.Fatalf("Connection: close is on the first response, want the last: %q", got)
	}
}

// Connection values other than a bare close stay refused: keep-alive or a
// hop-by-hop field name promises what only the writer decides.
func TestHandlerConnectionOtherThanCloseIsRefused(t *testing.T) {
	for _, v := range []string{"keep-alive", "close, upgrade", "x"} {
		res := Response{Status: 200, Headers: []Header{{Name: []byte("Connection"), Value: []byte(v)}}}
		if _, err := WriteBatch(nil, nil, []Response{res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("Connection: %s: got %v, want ErrMalformed", v, err)
		}
	}
}
