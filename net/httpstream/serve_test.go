package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
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
		_ = Serve(ctx, ln, cfg, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			mu.Lock()
			seen = append(seen, b.Len())
			mu.Unlock()
			return h(ctx, b)
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
func echoTarget(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	logger, logged := recordingLogger(t)
	addr, _ := serveTest(t, Config{Logger: logger}, func(context.Context, sluice.Batch[Request]) sluice.Batch[Response] {
		panic("the caller's own bug")
	})
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 500") {
		t.Errorf("status = %q, want 500", status)
	}
	logged("the caller's own bug", t.Name())
}

// A header value carrying CRLF would end the field early and let what follows
// be read as another response. Refused at the only place it can be.
func TestResponseSplittingIsRefused(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
		// RFC 9112 §6.3: chunked anywhere but last is 400, not 501.
		{"chunked not the final coding", "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked, gzip\r\n\r\n", "400"},
		{"no Host", "GET /x HTTP/1.1\r\n\r\n", "400"},
		// RFC 9112 §3.2: a Host is required even with an absolute-form target.
		{"absolute-form without a Host", "GET http://h/x HTTP/1.1\r\n\r\n", "400"},
		// RFC 9110 §15.6.2: a tunnel this server does not open is 501, whatever
		// the target — the request is well formed, the feature is missing.
		{"CONNECT on an authority-form target", "CONNECT h:443 HTTP/1.1\r\nHost: h\r\n\r\n", "501"},
		{"CONNECT on an origin-form target", "CONNECT /x HTTP/1.1\r\nHost: h\r\n\r\n", "501"},
		{"a scheme that is not http", "GET ftp://h/x HTTP/1.1\r\nHost: h\r\n\r\n", "400"},
		{"body over the bound", "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 999999999\r\n\r\n", "413"},
		// RFC 9112 §3: the request-target over the bound is 414, not 413.
		{"request line over the bound", "GET /" + strings.Repeat("x", 9<<10) + " HTTP/1.1\r\nHost: h\r\n\r\n", "414"},
		// RFC 6585 §5: header fields over the bound are 431, not 413.
		{"headers over the bound", "GET /x HTTP/1.1\r\nHost: h\r\nX: " + strings.Repeat("y", 70<<10) + "\r\n\r\n", "431"},
		{"too many header fields", "GET /x HTTP/1.1\r\nHost: h\r\n" + strings.Repeat("X: y\r\n", 200) + "\r\n", "431"},
		// RFC 9110 §15.6.6: a well-formed version this server does not
		// speak is 505; a version that is not one is still 400.
		{"version not supported", "GET /x HTTP/3.0\r\nHost: h\r\n\r\n", "505"},
		{"HTTP/2 on an HTTP/1 connection", "GET /x HTTP/2.0\r\nHost: h\r\n\r\n", "505"},
		{"version that is not one", "GET /x HTTP/one\r\nHost: h\r\n\r\n", "400"},
		// The grammar allows one digit each side of the dot.
		{"two-digit minor version", "GET /x HTTP/1.10\r\nHost: h\r\n\r\n", "400"},
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

// RFC 9931 §8 (updating RFC 9112): a server rejecting a CONNECT MUST close
// the connection without processing any further request on it. A client
// that forwards a tunnel's bytes before the 2xx sends them as what follows
// the CONNECT; read as HTTP/1.1, they would be a request the client never
// made, carrying its connection's authentication. The request pipelined
// behind the CONNECT here stands for those bytes: it never reaches the
// handler, and the 501 is the connection's last answer.
func TestARejectedCONNECTEndsTheConnection(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	addr, _ := serveTest(t, Config{}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		mu.Lock()
		for i := range b.Items {
			seen = append(seen, string(b.Items[i].Target))
		}
		mu.Unlock()
		return echoTarget(ctx, b)
	})
	c := dial(t, addr)
	fmt.Fprint(c, "GET /before HTTP/1.1\r\nHost: h\r\n\r\n"+
		"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n"+
		"POST /smuggled HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n")
	br := bufio.NewReader(c)
	if status, body := readResponse(t, br); !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/before" {
		t.Fatalf("first answer = %q %q, want 200 /before", status, body)
	}
	status, headers, _ := readResponseWithHeaders(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 501") || !strings.EqualFold(headers["connection"], "close") {
		t.Errorf("the CONNECT got %q with Connection %q, want 501 and close", status, headers["connection"])
	}
	if rest, err := io.ReadAll(br); err != nil || len(rest) > 0 {
		t.Errorf("after the 501 the connection gave %q, %v; want EOF", rest, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen, []string{"/before"}) {
		t.Errorf("the handler saw %q, want /before alone", seen)
	}
}

// RFC 9110 §7.4 leaves a misdirected request to the handler, the one that
// knows which names its certificate covers (limits.md, "Misdirected
// requests are the handler's to answer"): its 421 goes out as it is, and
// the clear-text request it judges has a nil TLS to judge by.
func TestAHandlerAnswersAMisdirectedRequest(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for i := range b.Items {
			r := &b.Items[i]
			status := 200
			if r.TLS == nil && string(r.Get("Host")) != "served.example" {
				status = 421
			}
			out = append(out, Response{Status: status})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: other.example\r\n\r\n")
	if status, _ := readResponse(t, bufio.NewReader(c)); !strings.HasPrefix(status, "HTTP/1.1 421") {
		t.Errorf("status = %q, want the handler's 421", status)
	}
}

// RFC 9112 §3.2.2: an absolute-form target is served as the origin-form it
// holds, and the handler's Host is the target's authority, not the field the
// client sent beside it. The connection persists like any other.
func TestAbsoluteFormIsServedAsOriginForm(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for i := range b.Items {
			r := &b.Items[i]
			out = append(out, Response{Status: 200, Body: []byte(string(r.Get("Host")) + " " + string(r.Target))})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := bufio.NewReader(c)
	for _, tt := range []struct{ raw, want string }{
		{"GET http://h:8080/x?q=1 HTTP/1.1\r\nHost: other\r\n\r\n", "h:8080 /x?q=1"},
		{"GET HTTP://H HTTP/1.1\r\nHost: h\r\n\r\n", "H /"},
		{"GET /y HTTP/1.1\r\nHost: h\r\n\r\n", "h /y"},
	} {
		fmt.Fprint(c, tt.raw)
		status, body := readResponse(t, br)
		if !strings.HasPrefix(status, "HTTP/1.1 200") || body != tt.want {
			t.Errorf("%q: status %q, body %q; want 200 and %q", tt.raw, status, body, tt.want)
		}
	}
}

// A higher minor version of HTTP/1 is served as HTTP/1.1 rather than refused
// (RFC 9110 §6.2): the answer says HTTP/1.1, the version this server speaks,
// and the connection persists for a second request as a 1.1 one would
// (RFC 9112 §9.3).
func TestHigherMinorVersionIsServedAsHTTP11(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)
	r := bufio.NewReader(c)

	for _, target := range []string{"/a", "/b"} {
		fmt.Fprintf(c, "GET %s HTTP/1.2\r\nHost: h\r\n\r\n", target)
		status, body := readResponse(t, r)
		if !strings.HasPrefix(status, "HTTP/1.1 200") {
			t.Fatalf("%s: status = %q, want HTTP/1.1 200", target, status)
		}
		if body != target {
			t.Errorf("%s: body = %q", target, body)
		}
	}
}

// A client refused mid-upload reads its 413 rather than a reset. A server
// that closes with unread bytes in its socket makes its TCP stack answer the
// client's next segment with a reset, which fails the client's writes and can
// erase the response before its parser reads it (RFC 9112 §9.6). This server
// half-closes, reads the rest of the upload away, then closes: the client's
// whole upload is accepted by the socket and its answer is readable after.
func TestRefusedUploadReadsItsStatusInFull(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxBodyBytes: 1 << 10}, echoTarget)
	c := dial(t, addr)

	body := bytes.Repeat([]byte("u"), 8<<20)
	fmt.Fprintf(c, "POST /up HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n", len(body))
	if _, err := c.Write(body); err != nil {
		t.Fatalf("upload: %v; the server closed with the upload unread and the stack reset the client", err)
	}
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.Contains(status, "413") {
		t.Errorf("status = %q, want 413", status)
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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

// slowPipeClient asks one request over the server end of a pipe and reads the
// answer step bytes at a time, one step every pace, until the connection
// ends, the answer is whole or bound passes. It returns what arrived, how
// long it took and whether the server closed the connection. net.Pipe is what
// makes the pace the server's whole write budget: a write returns only as
// fast as the client reads, and a write that times out reports the bytes the
// client took.
func slowPipeClient(t *testing.T, cfg Config, bodyLen, step int, pace, bound time.Duration) (got []byte, took time.Duration, closed bool) {
	t.Helper()
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	body := bytes.Repeat([]byte{'p'}, bodyLen)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = server.Close() }()
		serveConn(t.Context(), &prefixConn{Conn: server}, nil, cfg.mustResolve(),
			func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				out := make([]Response, b.Len())
				for i := range out {
					out[i] = Response{Status: 200, Body: body}
				}
				return sluice.Batch[Response]{Items: out}
			})
	}()
	t.Cleanup(func() { _ = client.Close(); <-done })

	if err := client.SetDeadline(time.Now().Add(bound + 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(client, "GET /slow HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	buf := make([]byte, step)
	for time.Since(start) < bound {
		n, err := client.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			return got, time.Since(start), true
		}
		if i := bytes.Index(got, []byte("\r\n\r\n")); i >= 0 && len(got)-(i+4) >= bodyLen {
			return got, time.Since(start), false
		}
		time.Sleep(pace)
	}
	return got, time.Since(start), false
}

// A client that reads slowly but steadily is served to the end over
// HTTP/1.1: WriteTimeout is a time without progress, so a write that times
// out having put the default 4 KiB on the wire gets the timeout again from
// where it stopped. The client takes 6 KiB every 100ms, so the 64 KiB answer
// takes four timeouts and no single deadline from the start of the write
// would let it finish.
func TestH1SlowReaderAboveTheProgressFloorIsServedToTheEnd(t *testing.T) {
	const (
		timeout = 300 * time.Millisecond
		bodyLen = 64 << 10
	)
	got, took, closed := slowPipeClient(t, Config{WriteTimeout: timeout}, bodyLen, 6<<10, 100*time.Millisecond, 20*timeout)
	i := bytes.Index(got, []byte("\r\n\r\n"))
	if closed || i < 0 || len(got)-(i+4) != bodyLen {
		t.Fatalf("after %v the client had %d bytes of a %d-byte answer, closed by the server=%v: "+
			"a client reading 6 KiB per 100ms was cut by a %v write timeout", took.Round(time.Millisecond), len(got), bodyLen, closed, timeout)
	}
	if took < 3*timeout {
		t.Errorf("the answer took %v, under the three timeouts that make it a slow one", took.Round(time.Millisecond))
	}
}

// A client that reads less than the progress floor per timeout is cut at
// about the timeout over HTTP/1.1, the connection closed with the answer
// incomplete: 64 bytes every two thirds of a timeout never moves the
// deadline, however long the client keeps at it.
func TestH1DribbleReaderIsCutWithinTheTimeout(t *testing.T) {
	const (
		timeout = 300 * time.Millisecond
		bodyLen = 64 << 10
	)
	got, took, closed := slowPipeClient(t, Config{WriteTimeout: timeout}, bodyLen, 64, 2*timeout/3, 10*timeout)
	if !closed {
		t.Fatalf("after %v of 64-byte reads the connection was still open with %d bytes taken: "+
			"every read moved the write deadline", took.Round(time.Millisecond), len(got))
	}
	if i := bytes.Index(got, []byte("\r\n\r\n")); i >= 0 && len(got)-(i+4) >= bodyLen {
		t.Errorf("the whole %d-byte answer was served to a client under the progress floor", bodyLen)
	}
	if took > 3*timeout {
		t.Errorf("the cut took %v, want under %v", took.Round(time.Millisecond), 3*timeout)
	}
}

// A streamed response to an HTTP/1.0 client cannot be chunked: RFC 9112 §6.1
// forbids Transfer-Encoding in a response unless the request was HTTP/1.1,
// and a 1.0 client reads the chunk sizes as body bytes. The body is
// close-delimited instead — raw bytes to EOF, Connection: close stated even
// when the client asked for keep-alive, since EOF is the only end such a
// body has.
func TestStreamedResponseToHTTP10IsCloseDelimited(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	addr, _ := serveTest(t, Config{BodyDelivery: BodyWhole}, echoTarget)
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
	logger, logged := recordingLogger(t)
	addr, _ := serveTest(t, Config{Logger: logger}, panickingProducer)

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
	logged("producer bug", "panickingProducer")

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
// on a next write it may never make. The client leaves with a reset, which
// is what a departure looks like from the server: a plain close sends the
// same FIN as a half-close, and a half-closed client is still there (RFC
// 9112 §9.6, tested below).
func TestStreamedResponseOfEmptyBatchesStopsWhenTheClientLeaves(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
	if err := c.(*net.TCPConn).SetLinger(0); err != nil {
		t.Fatal(err)
	}
	_ = c.Close() // a reset, not a FIN

	waitStopped(t, stopped)
}

// A client that half-closes once its request is sent still wants the answer
// (RFC 9112 §9.6: a half-close neither delimits the request nor says the
// client lost interest): netcat and the simplest HTTP/1.0 clients do this.
// The streamed body goes out whole, down to its last chunk.
func TestStreamedResponseSurvivesAClientHalfClose(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range 3 {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "part%d ", i)}}) {
						return
					}
					// Pacing, not a measurement: lets the half-close reach
					// the peer watch between two batches.
					time.Sleep(50 * time.Millisecond)
				}
			},
		}}}
	})

	c := dial(t, addr).(*net.TCPConn)
	fmt.Fprint(c, "GET /report HTTP/1.1\r\nHost: h\r\n\r\n")
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("reading the response after a half-close: %v (got %d bytes)", err, len(got))
	}
	head, body, ok := bytes.Cut(got, []byte("\r\n\r\n"))
	if !ok || !bytes.HasPrefix(head, []byte("HTTP/1.1 200")) {
		t.Fatalf("response head = %q", head)
	}
	if !bytes.HasSuffix(body, []byte("0\r\n\r\n")) {
		t.Fatalf("the chunked body is unterminated after the client's half-close: %q", body)
	}
	for i := range 3 {
		if !bytes.Contains(body, fmt.Appendf(nil, "part%d ", i)) {
			t.Errorf("part%d missing from the streamed body %q", i, body)
		}
	}
}

// Past a half-close the producer is no longer watched by a blocked read, yet
// it still learns that the connection was closed under it: a producer of
// cadence ticks runs on after the half-close and stops when the shutdown
// closes the connection, without a write to fail.
func TestStreamedResponseOfEmptyBatchesSurvivesAHalfCloseUntilShutdown(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, cancel, _ := serveUntilCancelled(t, Config{ShutdownGrace: 100 * time.Millisecond},
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: []Response{{
				Status: 200, Stream: cadenceProducer(started, stopped, release),
			}}}
		})
	t.Cleanup(func() { close(release) })

	c := dial(t, addr).(*net.TCPConn)
	fmt.Fprint(c, "GET /feed HTTP/1.1\r\nHost: h\r\n\r\n")
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(c).ReadString('\n'); err != nil {
		t.Fatal(err) // the status line: the response is streaming
	}
	<-started
	select {
	case <-stopped:
		t.Fatal("the producer was stopped by the client's half-close")
	case <-time.After(200 * time.Millisecond):
		// Still running: a bound on how long the half-close had to be
		// mistaken for a departure, not a measurement.
	}
	cancel() // the shutdown closes the connection once the grace is spent
	waitStopped(t, stopped)
}

// The HTTP/1.1 writer refuses the connection-specific fields HTTP/2 and
// HTTP/3 refuse: the connection is the writer's to describe, and a handler's
// Connection, Upgrade or Trailer would tell the client something about the
// following bytes that is not true.
func TestWriteBatchRefusesConnectionSpecificFields(t *testing.T) {
	for _, field := range []string{"Connection", "keep-alive", "Proxy-Connection", "Upgrade", "Trailer"} {
		res := Response{Status: 200, Headers: []Header{{Name: []byte(field), Value: []byte("x")}}}
		if _, err := WriteBatch(nil, Config{}, nil, []Response{res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", field, err)
		}
	}
}

// The sniff's deadline covers the whole preface, not each read of it: a
// client dribbling the preface a byte at a time, each byte inside the read
// timeout (the bound before the first request, see Config.ReadTimeout), is
// closed once the timeout has passed since the connection began rather than
// holding it for twenty-four of them.
func TestSniffDeadlineCoversTheWholePreface(t *testing.T) {
	const idle = 300 * time.Millisecond
	addr, _ := serveTest(t, Config{ReadTimeout: idle}, echoTarget)
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
	closer := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
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
		if _, err := WriteBatch(nil, Config{}, nil, []Response{res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("Connection: %s: got %v, want ErrMalformed", v, err)
		}
	}
}

// Config.MaxConns caps what Serve holds open at once: the connection over
// the cap is closed as it is accepted, with nothing written, and a slot
// freed by a connection that ended admits the next one.
func TestServeRefusesConnectionsOverMaxConns(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxConns: 2}, echoTarget)

	ask := func(c net.Conn) (string, error) {
		if _, err := c.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
			return "", err
		}
		status, _ := readResponse(t, bufio.NewReader(c))
		return status, nil
	}
	first, second := dial(t, addr), dial(t, addr)
	for i, c := range []net.Conn{first, second} {
		if status, err := ask(c); err != nil || !strings.HasPrefix(status, "HTTP/1.1 200") {
			t.Fatalf("connection %d under the cap: status %q, err %v", i+1, status, err)
		}
	}

	third := dial(t, addr)
	if _, err := third.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err == nil {
		n, err := third.Read(make([]byte, 1))
		if n != 0 || err == nil {
			t.Fatalf("the third connection read %d bytes, err %v: it was served over MaxConns 2", n, err)
		}
	}

	// A slot frees when the server notices the close, which is a read on
	// its side; the fourth connection is tried until it does.
	_ = first.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(time.Second))
		status, err := askQuietly(c)
		_ = c.Close()
		if err == nil && strings.HasPrefix(status, "HTTP/1.1 200") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after one connection closed the next was still refused: status %q, err %v", status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Config.MaxConnsPerIP caps what one remote IP holds open through Serve: the
// third connection from the same address is closed as it is accepted, with
// nothing written, while MaxConns alone would have admitted it.
func TestServeRefusesConnectionsOverMaxConnsPerIP(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxConns: 10, MaxConnsPerIP: 2}, echoTarget)
	for i := range 2 {
		c := dial(t, addr)
		if _, err := c.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
			t.Fatal(err)
		}
		if status, _ := readResponse(t, bufio.NewReader(c)); !strings.HasPrefix(status, "HTTP/1.1 200") {
			t.Fatalf("connection %d under the per-IP cap: status %q", i+1, status)
		}
	}
	third := dial(t, addr)
	if _, err := third.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err == nil {
		n, err := third.Read(make([]byte, 1))
		if n != 0 || err == nil {
			t.Fatalf("the third connection read %d bytes, err %v: it was served over MaxConnsPerIP 2", n, err)
		}
	}
}

// addrConn is a net.Conn that only answers RemoteAddr, for the admission
// arithmetic of connSet.
type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c *addrConn) RemoteAddr() net.Addr { return c.remote }

// connSet counts MaxConnsPerIP by prefix: the port does not separate two
// connections, an IPv4-mapped IPv6 address counts with its IPv4 form,
// another address is admitted while one is at the cap, a removal frees its
// slot, and a remote address that is not IP is bound by the total alone. At
// the default lengths two IPv6 addresses of one /64 share a count and the
// next /64 has its own.
func TestConnSetBoundsEachIP(t *testing.T) {
	tcp := func(ip string, port int) net.Conn {
		return &addrConn{remote: &net.TCPAddr{IP: net.ParseIP(ip), Port: port}}
	}
	cfg, err := Config{MaxConnsPerIP: 2}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	s := newConnSet(cfg)
	if s.add(tcp("2001:db8:0:1::1", 1000)) == nil || s.add(tcp("2001:db8:0:1::2", 1000)) == nil {
		t.Fatal("the first two connections from 2001:db8:0:1::/64 were refused")
	}
	if s.add(tcp("2001:db8:0:1:ffff::1", 1000)) != nil {
		t.Fatal("a third connection from 2001:db8:0:1::/64 was admitted over MaxConnsPerIP 2")
	}
	if s.add(tcp("2001:db8:0:2::1", 1000)) == nil {
		t.Fatal("a connection from 2001:db8:0:2::/64 was refused while only its neighbour is at the cap")
	}
	a1, a2 := tcp("192.0.2.1", 1000), tcp("::ffff:192.0.2.1", 1001)
	if s.add(a1) == nil || s.add(a2) == nil {
		t.Fatal("the first two connections from 192.0.2.1 were refused")
	}
	if s.add(tcp("192.0.2.1", 1002)) != nil {
		t.Fatal("a third connection from 192.0.2.1 was admitted over MaxConnsPerIP 2")
	}
	if s.add(tcp("192.0.2.2", 1000)) == nil {
		t.Fatal("a connection from 192.0.2.2 was refused while only 192.0.2.1 is at the cap")
	}
	s.remove(a2)
	if s.add(tcp("192.0.2.1", 1003)) == nil {
		t.Fatal("192.0.2.1 was refused after one of its connections was removed")
	}
	for i := range 3 {
		if s.add(&addrConn{remote: &net.UnixAddr{Net: "unix"}}) == nil {
			t.Fatalf("Unix connection %d was refused: no per-IP bound applies to it", i+1)
		}
	}
	if got := s.byIP[netip.MustParsePrefix("192.0.2.1/32")]; got != 2 {
		t.Fatalf("192.0.2.1 counts %d connections, want 2", got)
	}
}

// askQuietly sends one request and returns its status line, with every
// failure reported as an error rather than ending the test, for the callers
// that retry.
func askQuietly(c net.Conn) (string, error) {
	if _, err := c.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
		return "", err
	}
	return bufio.NewReader(c).ReadString('\n')
}

// RFC 9112 §3.2: a Host that is not an authority is answered 400, by the same
// rule HTTP/2 and HTTP/3 apply to :authority — and the asterisk-form target is
// OPTIONS' alone.
func TestHostIsAnAuthorityOnHTTP1(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"an empty Host", "GET /x HTTP/1.1\r\nHost:\r\n\r\n", "400"},
		{"userinfo in Host", "GET /x HTTP/1.1\r\nHost: u@h\r\n\r\n", "400"},
		{"a path and a space in Host", "GET /x HTTP/1.1\r\nHost: evil.com/x y@z\r\n\r\n", "400"},
		{"a port that is not digits", "GET /x HTTP/1.1\r\nHost: h:8a\r\n\r\n", "400"},
		{"an unclosed IPv6 literal", "GET /x HTTP/1.1\r\nHost: [::1\r\n\r\n", "400"},
		{"an IPv6 literal with a port", "GET /x HTTP/1.1\r\nHost: [::1]:8443\r\n\r\n", "200"},
		{"brackets holding no address", "GET /x HTTP/1.1\r\nHost: [zz]\r\n\r\n", "400"},
		{"a percent not followed by hex", "GET /x HTTP/1.1\r\nHost: h%zz\r\n\r\n", "400"},
		{"an IPvFuture", "GET /x HTTP/1.1\r\nHost: [v1.x]\r\n\r\n", "200"},
		{"OPTIONS *", "OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n", "200"},
		{"GET *", "GET * HTTP/1.1\r\nHost: h\r\n\r\n", "400"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dial(t, addr)
			fmt.Fprint(c, tt.raw)
			status, _ := readResponse(t, bufio.NewReader(c))
			if !strings.Contains(status, tt.want) {
				t.Errorf("status = %q, want %s", status, tt.want)
			}
		})
	}
}

// A parse error costs the request that carries it, not the valid ones parsed
// ahead of it in the same read: those are served, in order, and the 400
// closes the connection after them (RFC 9112 §9.3 orders the responses).
func TestRequestsParsedBeforeAnErrorAreServed(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)
	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET * HTTP/1.1\r\nHost: h\r\n\r\n")
	br := bufio.NewReader(c)
	if status, body := readResponse(t, br); !strings.Contains(status, "200") || body != "/a" {
		t.Fatalf("first response %q %q, want 200 /a", status, body)
	}
	if status, _ := readResponse(t, br); !strings.Contains(status, "400") {
		t.Fatalf("second response %q, want 400", status)
	}
	if got := sizes(); len(got) != 1 || got[0] != 1 {
		t.Errorf("batch sizes %v, want the one valid request alone", got)
	}
}

// RFC 9112 §2.2: a server SHOULD ignore at least one empty line before a
// request-line — the CRLF some clients send after a body.
func TestAnEmptyLineBeforeARequestLineIsIgnored(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)
	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\n\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
	br := bufio.NewReader(c)
	for _, want := range []string{"/a", "/b"} {
		if status, body := readResponse(t, br); !strings.Contains(status, "200") || body != want {
			t.Fatalf("response %q %q, want 200 %s", status, body, want)
		}
	}
}

// RFC 9110 §15.2: a server MUST NOT send a 1xx to an HTTP/1.0 client, and
// §10.1.1 says Expect is ignored there; the request is otherwise served.
func TestNo100ContinueToAnHTTP10Client(t *testing.T) {
	addr, _ := serveTest(t, Config{BodyDelivery: BodyWhole}, echoTarget)
	c := dial(t, addr)
	fmt.Fprint(c, "POST /x HTTP/1.0\r\nHost: h\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\n")
	if err := c.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if n, err := c.Read(one[:]); err == nil {
		t.Fatalf("%d byte(s) arrived before the body was sent: an interim response to an HTTP/1.0 client", n)
	}
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(c, "ab")
	if status, body := readResponse(t, bufio.NewReader(c)); !strings.Contains(status, "200") || body != "/x" {
		t.Fatalf("response %q %q, want 200 /x", status, body)
	}
}

// readFields reads one response's status line, header lines and body — the
// body by its Content-Length, as readResponse does — and returns the header
// lines, for a test about what a response says rather than what it carries.
func readFields(t *testing.T, br *bufio.Reader) []string {
	t.Helper()
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("reading the status line: %v", err)
	}
	var head []string
	length := 0
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		head = append(head, line)
		if name, value, ok := strings.Cut(line, ": "); ok && strings.EqualFold(name, "content-length") {
			if _, err := fmt.Sscanf(value, "%d", &length); err != nil {
				t.Fatalf("Content-Length %q: %v", value, err)
			}
		}
	}
	if _, err := readFull(br, make([]byte, length)); err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	return head
}

// wantLine checks whether one exact header line is among head.
func wantLine(t *testing.T, head []string, line string, want bool) {
	t.Helper()
	got := false
	for _, h := range head {
		got = got || h == line
	}
	if got != want {
		t.Errorf("%q present = %v, want %v, in %q", line, got, want, head)
	}
}

// The idle timeout is announced on every HTTP/1.1 response that keeps the
// connection, named in Connection as RFC 9110 §7.6.1 requires of a
// hop-by-hop field, so a proxy in front with a longer idle timeout of its
// own closes first rather than reuse a connection this server is closing.
// The response that closes says only that. An HTTP/1.0 client asking for
// keep-alive gets the option back, which is what persists its connection
// (RFC 9112 §9.3). WriteBatch on its own describes no connection and
// announces nothing.
func TestKeepAliveAnnouncesIdleTimeout(t *testing.T) {
	addr, _ := serveTest(t, Config{IdleTimeout: 7 * time.Second}, echoTarget)
	c := dial(t, addr)
	br := bufio.NewReader(c)

	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\n\r\n")
	head := readFields(t, br)
	wantLine(t, head, "Connection: keep-alive", true)
	wantLine(t, head, "Keep-Alive: timeout=7", true)
	wantLine(t, head, "Connection: close", false)

	fmt.Fprint(c, "GET /b HTTP/1.0\r\nHost: h\r\nConnection: keep-alive\r\n\r\n")
	head = readFields(t, br)
	wantLine(t, head, "Connection: keep-alive", true)
	wantLine(t, head, "Keep-Alive: timeout=7", true)

	fmt.Fprint(c, "GET /c HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	head = readFields(t, br)
	wantLine(t, head, "Connection: close", true)
	wantLine(t, head, "Connection: keep-alive", false)
	wantLine(t, head, "Keep-Alive: timeout=7", false)
	if _, err := br.ReadByte(); err != io.EOF {
		t.Errorf("after Connection: close, read %v, want EOF", err)
	}
}

// A caller serving the connection itself answers as Serve would: WriteBatch
// announces the configured idle timeout on every response that does not
// close, and nothing but Connection: close on the one that does.
func TestWriteBatchAnnouncesKeepAliveAsServe(t *testing.T) {
	requests := []Request{
		{Method: []byte("GET"), KeepAlive: true},
		{Method: []byte("GET"), KeepAlive: false, http10: true}, // an HTTP/1.0 request that did not ask to stay
	}
	responses := []Response{{Status: 200}, {Status: 200}}
	out, err := WriteBatch(nil, Config{IdleTimeout: 7 * time.Second}, requests, responses)
	if err != nil {
		t.Fatal(err)
	}
	first, last, ok := strings.Cut(string(out), "\r\n\r\n")
	if !ok {
		t.Fatalf("no end of header in %q", out)
	}
	head, tail := strings.Split(first, "\r\n"), strings.Split(last, "\r\n")
	wantLine(t, head, "Connection: keep-alive", true)
	wantLine(t, head, "Keep-Alive: timeout=7", true)
	wantLine(t, tail, "Connection: close", true)
	wantLine(t, tail, "Keep-Alive: timeout=7", false)

	// The zero Config is Serve's: the default idle timeout is announced.
	out, err = WriteBatch(nil, Config{}, requests[:1], responses[:1])
	if err != nil {
		t.Fatal(err)
	}
	wantLine(t, strings.Split(string(out), "\r\n"), "Keep-Alive: timeout=120", true)

	// A negative field is refused as Serve refuses it, naming the field.
	if _, err := WriteBatch(nil, Config{IdleTimeout: -time.Second}, requests, responses); err == nil || !strings.Contains(err.Error(), "Config.IdleTimeout") {
		t.Errorf("negative IdleTimeout: err = %v, want a refusal naming the field", err)
	}
}

// A streamed response keeps the connection too, and says so the same way.
func TestStreamedResponseAnnouncesKeepAlive(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("part")}})
			},
		}}}
	})
	c := dial(t, addr)
	br := bufio.NewReader(c)
	fmt.Fprint(c, "GET /report HTTP/1.1\r\nHost: h\r\n\r\n")
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	var head []string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line = strings.TrimRight(line, "\r\n"); line == "" {
			break
		}
		head = append(head, line)
	}
	wantLine(t, head, "Connection: keep-alive", true)
	wantLine(t, head, "Keep-Alive: timeout=120", true)
	wantLine(t, head, "Transfer-Encoding: chunked", true)
}

// fieldsEcho answers each request with its header names, then its trailers
// as name=value, so a test reads where one section ends and the other
// begins.
func fieldsEcho(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		var body []byte
		for _, h := range r.Headers {
			body = append(append(body, h.Name...), ' ')
		}
		body = append(body, '|')
		for _, h := range r.Trailers {
			body = append(append(append(append(body, ' '), h.Name...), '='), h.Value...)
		}
		out = append(out, Response{Status: 200, Body: body})
	}
	return sluice.Batch[Response]{Items: out}
}

// RFC 9112 §7.1.2 and RFC 9110 §6.5: the trailer section of a chunked
// request reaches the handler in Request.Trailers, apart from its headers.
// Two requests pipelined in one write share the batch's field arena, so
// each must see its own trailers and nothing of its neighbour's fields.
func TestRequestTrailersReachTheHandler(t *testing.T) {
	addr, _ := serveTest(t, Config{}, fieldsEcho)
	c := dial(t, addr)

	fmt.Fprint(c, "POST /a HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"+
		"2\r\nhi\r\n0\r\nX-Sum: 1\r\nX-Sig: a b\r\n\r\n"+
		"POST /b HTTP/1.1\r\nHost: h\r\nX-Extra: e\r\nTransfer-Encoding: chunked\r\n\r\n"+
		"0\r\nX-Sum: 2\r\n\r\n"+
		"POST /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
	br := bufio.NewReader(c)
	for _, want := range []string{
		"Host Transfer-Encoding | X-Sum=1 X-Sig=a b",
		"Host X-Extra Transfer-Encoding | X-Sum=2",
		"Host Transfer-Encoding |",
	} {
		if _, body := readResponse(t, br); body != want {
			t.Errorf("answered %q, want %q", body, want)
		}
	}
}

// RFC 9110 §2.2 forbids a sender the elements of another role: Host, TE and
// Expect belong to a request, and a response carrying one tells its reader
// about a request it is not reading. A handler's own Date is believed but
// must be the IMF-fixdate §5.6.7 lets a sender generate: one a cache cannot
// read would misdate the entry. The one check serves the three writers;
// this is the HTTP/1.1 side of it.
func TestWriteBatchRefusesRequestFieldsAndAMalformedDate(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"Host", "example.test"},
		{"te", "trailers"},
		{"Expect", "100-continue"},
		{"Date", "yesterday"},
		{"Date", "Tue, 20 Oct 2026 20:58:44 +0000"},
	} {
		res := Response{Status: 200, Headers: []Header{{Name: []byte(tc.name), Value: []byte(tc.value)}}}
		if _, err := WriteBatch(nil, Config{}, nil, []Response{res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %s: got %v, want ErrMalformed", tc.name, tc.value, err)
		}
	}
	dated := Response{Status: 200, Headers: []Header{{Name: []byte("Date"), Value: []byte("Tue, 20 Oct 2026 20:58:44 GMT")}}}
	if _, err := WriteBatch(nil, Config{}, nil, []Response{dated}); err != nil {
		t.Errorf("an IMF-fixdate Date was refused: %v", err)
	}
}

// Two statuses carry a constraint the transport must hold for the handler:
// a 426 MUST carry Upgrade (RFC 9110 §15.5.22), which this server never
// sends, so the status is refused outright; a 205 MUST NOT carry content
// (§15.3.6), so a Body or a Stream on it is refused and a bare 205 leaves
// with Content-Length: 0, framed like any 2xx (RFC 9112 §6.3). The three
// writers go through the same two checks.
func TestWritersHoldThe205And426Constraints(t *testing.T) {
	stream := func(yield func(sluice.Batch[[]byte]) bool) {}
	refused := []struct {
		name string
		res  Response
	}{
		{"426 bare", Response{Status: 426}},
		{"426 with Upgrade", Response{Status: 426, Headers: []Header{{Name: []byte("Upgrade"), Value: []byte("HTTP/3.0")}}}},
		{"205 with a Body", Response{Status: 205, Body: []byte("x")}},
		{"205 with a Stream", Response{Status: 205, Stream: stream}},
	}
	for _, tc := range refused {
		if _, err := WriteBatch(nil, Config{}, nil, []Response{tc.res}); !errors.Is(err, ErrMalformed) {
			t.Errorf("h1: %s: got %v, want ErrMalformed", tc.name, err)
		}
		if _, err := (&h2Server{}).appendHead(nil, 1, []byte("GET"), tc.res, 16384); !errors.Is(err, ErrMalformed) {
			t.Errorf("h2: %s: got %v, want ErrMalformed", tc.name, err)
		}
		if _, err := appendH3ResponseBasic(nil, tc.res); !errors.Is(err, ErrMalformed) {
			t.Errorf("h3: %s: got %v, want ErrMalformed", tc.name, err)
		}
	}
	bare := Response{Status: 205}
	out, err := WriteBatch(nil, Config{}, nil, []Response{bare})
	if err != nil {
		t.Fatalf("h1: a bare 205 was refused: %v", err)
	}
	if !strings.HasPrefix(string(out), "HTTP/1.1 205 ") || !strings.HasSuffix(string(out), "Content-Length: 0\r\n\r\n") {
		t.Errorf("h1: a bare 205 is framed as %q, want a 205 ending in Content-Length: 0 and no body", out)
	}
	if _, err := (&h2Server{}).appendHead(nil, 1, []byte("GET"), bare, 16384); err != nil {
		t.Errorf("h2: a bare 205 was refused: %v", err)
	}
	if _, err := appendH3ResponseBasic(nil, bare); err != nil {
		t.Errorf("h3: a bare 205 was refused: %v", err)
	}
}

// RFC 9110 §6.5.1: a trailer cannot carry a field whose evaluation had to
// precede the content. The fields RFC 9110 defines in the kinds it names —
// routing, authentication, response controls, content format — are refused
// with the framing and connection fields already were, and the request's
// own fields with them; a validator is not, trailers being what an ETag
// computed on the way out is for. checkTrailer is the one point the three
// protocols' trailers go through.
func TestCheckTrailerRefusesFieldsSettledBeforeTheContent(t *testing.T) {
	for _, name := range []string{
		"content-length", "Trailer", "host", "Authorization", "www-authenticate",
		"location", "vary", "Content-Type", "content-range", "expect", "te",
	} {
		if err := checkTrailer(Header{Name: []byte(name), Value: []byte("x")}); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", name, err)
		}
	}
	for _, name := range []string{"etag", "grpc-status", "x-checksum"} {
		if err := checkTrailer(Header{Name: []byte(name), Value: []byte("x")}); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}

// RFC 9110 §8.6: a HEAD response carries a Content-Length only when it is
// the GET twin's. The writer knows that length when the handler gave one —
// a Body, or ContentLength beside a Stream — and states none for a HEAD
// answered with the metadata alone, where "0" would be a lie about the GET.
// A bare GET still states 0, and no HEAD response carries body bytes, on the
// three protocols alike.
func TestBareHeadResponseStatesNoContentLength(t *testing.T) {
	stream := func(yield func(sluice.Batch[[]byte]) bool) {
		t.Error("the producer was pulled for a HEAD answer")
	}
	cases := []struct {
		name   string
		method string
		res    Response
		length string // the Content-Length stated, "" for none
	}{
		{"bare HEAD", "HEAD", Response{Status: 200}, ""},
		{"HEAD with a Body", "HEAD", Response{Status: 200, Body: []byte("abcde")}, "5"},
		{"HEAD with a Stream and a length", "HEAD", Response{Status: 200, Stream: stream, ContentLength: 7}, "7"},
		{"HEAD with a Stream and no length", "HEAD", Response{Status: 200, Stream: stream}, ""},
		{"bare GET", "GET", Response{Status: 200}, "0"},
	}
	for _, tc := range cases {
		method := []byte(tc.method)
		noLength, noBody := noLengthAndBody(200, method)

		// HTTP/1.1: the in-hand path through WriteBatch, the streamed one
		// through the framing decision Serve makes.
		var out []byte
		var err error
		if tc.res.streamed() {
			chunked, size, _ := streamedFraming(tc.res, false, noLength, noBody)
			out, err = appendStreamedHeader(nil, true, chunked, size, nil, tc.res, dateFor(time.Now()))
		} else {
			out, err = WriteBatch(nil, Config{}, []Request{{Method: method}}, []Response{tc.res})
		}
		if err != nil {
			t.Fatalf("h1: %s: %v", tc.name, err)
		}
		head, body, _ := strings.Cut(string(out), "\r\n\r\n")
		h1 := map[string]string{}
		for _, line := range strings.Split(head, "\r\n")[1:] {
			name, value, _ := strings.Cut(line, ": ")
			h1[strings.ToLower(name)] = value
		}
		if h1["content-length"] != tc.length || h1["transfer-encoding"] != "" || body != "" {
			t.Errorf("h1: %s: Content-Length %q, Transfer-Encoding %q, body %q; want %q, none, none",
				tc.name, h1["content-length"], h1["transfer-encoding"], body, tc.length)
		}

		// HTTP/2: one HEADERS frame, and nothing after it for a HEAD.
		out, err = (&h2Server{}).appendHead(nil, 1, method, tc.res, 16384)
		if err != nil {
			t.Fatalf("h2: %s: %v", tc.name, err)
		}
		f, n, err := parseFrame(out, 16384)
		if err != nil || f.Type != frameHeaders || n != len(out) {
			t.Fatalf("h2: %s: frames %x: %v", tc.name, out, err)
		}
		if got := h2Fields(t, f.Payload)["content-length"]; strings.Join(got, ",") != tc.length {
			t.Errorf("h2: %s: content-length %q, want %q", tc.name, got, tc.length)
		}

		// HTTP/3: one HEADERS frame, no DATA.
		out, err = appendH3Response(nil, tc.res, method, h3NoFieldSectionLimit, &h3Scratch{}, dateFor(time.Now()))
		if err != nil {
			t.Fatalf("h3: %s: %v", tc.name, err)
		}
		frames, rest, _, err := parseH3Frames(nil, out, 1<<24)
		if err != nil || len(frames) != 1 || frames[0].Type != h3Headers || len(rest) != 0 {
			t.Fatalf("h3: %s: %d frames, %d bytes left, %v; want one HEADERS", tc.name, len(frames), len(rest), err)
		}
		fields, err := newQPACKDecoder(nil, 0).Decode(nil, frames[0].Payload)
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		for _, h := range fields {
			if string(h.Name) == "content-length" {
				got = string(h.Value)
			}
		}
		if got != tc.length {
			t.Errorf("h3: %s: content-length %q, want %q", tc.name, got, tc.length)
		}
	}
}

// RFC 9110 §15.5 and §15.6: the status the transport writes for an error of
// its own carries a one-line text/plain explanation, "<status> <reason>" —
// what curl prints and a browser shows where an empty 4xx is a blank page.
// A HEAD read before the refusal states the line's length and type and
// gets no bytes (§9.3.2).
func TestTransportErrorCarriesBody(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)

	c := dial(t, addr)
	fmt.Fprint(c, "GET /x HTTP/one\r\nHost: h\r\n\r\n")
	status, headers, body := readResponseWithHeaders(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 400") {
		t.Fatalf("status = %q, want 400", status)
	}
	if headers["content-type"] != "text/plain; charset=utf-8" || headers["content-length"] != "16" {
		t.Errorf("headers = %v, want text/plain; charset=utf-8 and a length of 16", headers)
	}
	if body != "400 Bad Request\n" {
		t.Errorf("body = %q, want the status line's text", body)
	}

	c = dial(t, addr)
	fmt.Fprint(c, "HEAD /x HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n")
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(status, "HTTP/1.1 400") {
		t.Fatalf("HEAD: status = %q, %v; want 400", status, err)
	}
	headers = readHeaders(t, br)
	if headers["content-type"] != "text/plain; charset=utf-8" || headers["content-length"] != "16" {
		t.Errorf("HEAD: headers = %v, want the GET twin's type and length", headers)
	}
	// The refusal closes the connection: what follows the head is its end.
	if rest, _ := io.ReadAll(br); len(rest) != 0 {
		t.Errorf("HEAD: %q followed the head, want no body", rest)
	}
}
