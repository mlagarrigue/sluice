package httpstream

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// readStatusLine reads just the status line, for a response whose framing —
// a 204, an interim 100 — means readResponse's Content-Length assumption does
// not hold.
func readStatusLine(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the status line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// readHeaders reads header lines up to the blank line, keyed lowercase.
func readHeaders(t *testing.T, br *bufio.Reader) map[string]string {
	t.Helper()
	headers := map[string]string{}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return headers
		}
		name, value, ok := strings.Cut(line, ": ")
		if !ok {
			t.Fatalf("header line %q has no colon", line)
		}
		headers[strings.ToLower(name)] = value
	}
}

// A client that sends one byte every 100ms never lets a sliding deadline
// fire, since every individual gap is under ReadTimeout. This is the
// slowloris attack the package's own documentation claims to answer; the
// deadline must be fixed at the request's first byte, not re-armed per read.
//
// The 100ms cadence is the attack, not synchronisation, so it stays. What
// the test asserts is not a wall-clock bound but an event: the trickle never
// stops — it runs until the test ends — so a sliding deadline never fires
// and no 408 arrives at all before the client's own 5s deadline, while a
// fixed one answers about ReadTimeout after the first byte.
func TestTrickleClientIsTimedOutDespiteRepeatedBytes(t *testing.T) {
	addr, _ := serveTest(t, Config{ReadTimeout: 300 * time.Millisecond}, echoTarget)
	c := dial(t, addr)

	stop := make(chan struct{})
	var trickling sync.WaitGroup
	defer func() { close(stop); trickling.Wait() }()
	trickling.Go(func() {
		// A request line, then a header name that never ends: the request
		// stays incomplete however long the trickle runs.
		line := "GET /x HTTP/1.1\r\nX-Trickle"
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for i := 0; ; i++ {
			b := byte('a')
			if i < len(line) {
				b = line[i]
			}
			if _, err := c.Write([]byte{b}); err != nil {
				return // the server closed after its 408
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	})

	status := readStatusLine(t, bufio.NewReader(c))
	if !strings.Contains(status, "408") {
		t.Errorf("status = %q, want 408 — the deadline slid with every trickled byte instead of expiring", status)
	}
}

// A client that never sends a byte is not slow about any request — there is
// none — so the connection should simply close when IdleTimeout fires,
// without a 408: that status answers a question nobody asked, which a
// strict client may read as a protocol violation rather than a close.
func TestIdleConnectionClosesWithoutAStatus(t *testing.T) {
	addr, _ := serveTest(t, Config{IdleTimeout: 200 * time.Millisecond}, echoTarget)
	c := dial(t, addr)

	buf := make([]byte, 64)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf)
	if n > 0 {
		t.Fatalf("the idle connection was answered %q, want a close with no bytes", buf[:n])
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("the idle connection ended with %v, want io.EOF", err)
	}
}

// A HEAD response states the Content-Length its GET twin would — the same
// handler answers both — but never the bytes: a client that read them would
// be reading the start of the next response.
func TestHEADResponseCarriesNoBody(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Body: []byte("hello")})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := bufio.NewReader(c)

	fmt.Fprint(c, "HEAD /x HTTP/1.1\r\nHost: h\r\n\r\nGET /y HTTP/1.1\r\nHost: h\r\n\r\n")

	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	headers := readHeaders(t, br)
	if headers["content-length"] != "5" {
		t.Errorf("content-length = %q, want 5", headers["content-length"])
	}

	// The next bytes must be the second response's status line, not "hello":
	// a server that wrote the HEAD body here desynchronises the connection.
	status2 := readStatusLine(t, br)
	if !strings.HasPrefix(status2, "HTTP/1.1 200") {
		t.Fatalf("second response status = %q — the HEAD response's body ran into it", status2)
	}
	headers2 := readHeaders(t, br)
	n, err := strconv.Atoi(headers2["content-length"])
	if err != nil {
		t.Fatalf("second response content-length %q: %v", headers2["content-length"], err)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Errorf("second response body = %q, want %q", body, "hello")
	}
}

// RFC 9110 §8.6 forbids Content-Length and a body on a 204, whatever the
// handler put in Response.Body — the same handler answers GET and HEAD and
// may legitimately set one.
func TestNoContentResponseCarriesNoLengthOrBody(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 204, Body: []byte("should never be sent")})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := bufio.NewReader(c)
	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 204") {
		t.Fatalf("status = %q", status)
	}
	headers := readHeaders(t, br)
	if _, ok := headers["content-length"]; ok {
		t.Errorf("a 204 carried Content-Length %q; RFC 9110 forbids it", headers["content-length"])
	}
	if _, err := br.ReadByte(); err == nil {
		t.Error("a 204 carried a body")
	}
}

// A conformant client sending Expect: 100-continue waits for the interim
// response before it sends the body. This package used to wait for the body
// instead — both sides blocking until ReadTimeout killed a perfectly valid
// request.
func TestExpectContinueGetsAnInterimResponse(t *testing.T) {
	addr, _ := serveTest(t, Config{ReadTimeout: 2 * time.Second}, echoTarget)
	c := dial(t, addr)
	br := bufio.NewReader(c)

	fmt.Fprint(c, "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")

	interim := readStatusLine(t, br)
	if interim != "HTTP/1.1 100 Continue" {
		t.Fatalf("interim = %q, want \"HTTP/1.1 100 Continue\"", interim)
	}
	blank, err := br.ReadString('\n')
	if err != nil || strings.TrimRight(blank, "\r\n") != "" {
		t.Fatalf("the 100 Continue was not followed by a blank line, got %q, err %v", blank, err)
	}

	fmt.Fprint(c, "hello")
	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/x" {
		t.Errorf("status=%q body=%q", status, body)
	}
}

// A pipelined batch [A: Connection: close, B] must answer A and never touch
// B: the client that sent B may never read its answer once it sees the
// connection end after A.
func TestConnectionCloseStopsBatchingFurtherPipelinedRequests(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")

	br := bufio.NewReader(c)
	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/a" {
		t.Fatalf("status=%q body=%q", status, body)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Error("a request after Connection: close in the same batch was served")
	}

	got := sizes()
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("batch sizes = %v, want a single batch of one — B must never reach the handler", got)
	}
}

// MaxRequestsPerConn bounds the whole connection, not just how many batches
// it may take: a pipelined batch that would cross the cap must be cut short,
// and the client must be told the connection is over rather than discovering
// it as a reset.
func TestMaxRequestsPerConnCapsAWholeBatch(t *testing.T) {
	cfg := Config{MaxRequestsPerConn: 2, MaxRequestsPerBatch: 8}
	addr, sizes := serveTest(t, cfg, echoTarget)
	c := dial(t, addr)

	const n = 5
	var out strings.Builder
	for i := range n {
		fmt.Fprintf(&out, "GET /r%d HTTP/1.1\r\nHost: h\r\n\r\n", i)
	}
	if _, err := c.Write([]byte(out.String())); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)

	status0, body0 := readResponse(t, br)
	if !strings.HasPrefix(status0, "HTTP/1.1 200") || body0 != "/r0" {
		t.Fatalf("response 0: status=%q body=%q", status0, body0)
	}

	status1 := readStatusLine(t, br)
	if !strings.HasPrefix(status1, "HTTP/1.1 200") {
		t.Fatalf("response 1: status=%q", status1)
	}
	headers1 := readHeaders(t, br)
	if !strings.EqualFold(headers1["connection"], "close") {
		t.Errorf("the last response under the cap had Connection: %q, want close", headers1["connection"])
	}
	n1, err := strconv.Atoi(headers1["content-length"])
	if err != nil {
		t.Fatalf("response 1 content-length %q: %v", headers1["content-length"], err)
	}
	body1 := make([]byte, n1)
	if _, err := io.ReadFull(br, body1); err != nil {
		t.Fatal(err)
	}
	if string(body1) != "/r1" {
		t.Fatalf("response 1 body = %q, want /r1", body1)
	}

	if _, err := br.ReadByte(); err == nil {
		t.Error("the connection stayed open past MaxRequestsPerConn")
	}

	served := 0
	for _, s := range sizes() {
		served += s
	}
	if served != cfg.MaxRequestsPerConn {
		t.Errorf("the handler was handed %d requests total, want the %d-request cap honoured", served, cfg.MaxRequestsPerConn)
	}
}

// The cap-reached marking must fire even when MaxRequestsPerConn is an exact
// multiple of MaxRequestsPerBatch — the case the defaults (1024 and 64) put
// every deployment in by default. Checking "this batch was cut short of
// MaxRequestsPerBatch" instead of "the connection's budget is now spent"
// misses exactly this case: the last batch runs full, so nothing marks it,
// and the connection closes without telling the client.
func TestMaxRequestsPerConnCapsAnExactMultipleOfBatches(t *testing.T) {
	cfg := Config{MaxRequestsPerConn: 4, MaxRequestsPerBatch: 2}
	addr, sizes := serveTest(t, cfg, echoTarget)
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

	for i := range cfg.MaxRequestsPerConn - 1 {
		status, body := readResponse(t, br)
		if want := fmt.Sprintf("/r%d", i); !strings.HasPrefix(status, "HTTP/1.1 200") || body != want {
			t.Fatalf("response %d: status=%q body=%q, want /r%d", i, status, body, i)
		}
	}

	last := cfg.MaxRequestsPerConn - 1
	status := readStatusLine(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("response %d: status=%q", last, status)
	}
	headers := readHeaders(t, br)
	if !strings.EqualFold(headers["connection"], "close") {
		t.Errorf("the last response under the cap had Connection: %q, want close — "+
			"a batch that happens to run exactly full must still be marked", headers["connection"])
	}
	length, err := strconv.Atoi(headers["content-length"])
	if err != nil {
		t.Fatalf("response %d content-length %q: %v", last, headers["content-length"], err)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("/r%d", last); string(body) != want {
		t.Fatalf("response %d body = %q, want %q", last, body, want)
	}

	if _, err := br.ReadByte(); err == nil {
		t.Error("the connection stayed open past MaxRequestsPerConn")
	}

	served := 0
	for _, s := range sizes() {
		served += s
	}
	if served != cfg.MaxRequestsPerConn {
		t.Errorf("the handler was handed %d requests total, want the %d-request cap honoured", served, cfg.MaxRequestsPerConn)
	}
}

// A pipelined batch mixing a HEAD, a 204 and a 304 must keep every response
// after the first framed correctly: each of the three suppresses its body (a
// 204/304 also suppresses Content-Length), and a wrong framing decision on
// any one of them would desynchronise the requests still queued behind it —
// the same class TestHEADResponseCarriesNoBody and
// TestNoContentResponseCarriesNoLengthOrBody check in isolation, checked here
// together because that is where a per-response bug in the write loop would
// actually show.
func TestPipelinedHeadNoContentNotModifiedFramingStaysSynced(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			switch string(r.Target) {
			case "/head":
				out = append(out, Response{Status: 200, Body: []byte("headbody")})
			case "/nocontent":
				out = append(out, Response{Status: 204, Body: []byte("must never be sent")})
			case "/notmodified":
				out = append(out, Response{Status: 304, Body: []byte("must never be sent either")})
			default:
				out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
			}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := bufio.NewReader(c)

	fmt.Fprint(c,
		"HEAD /head HTTP/1.1\r\nHost: h\r\n\r\n"+
			"GET /nocontent HTTP/1.1\r\nHost: h\r\n\r\n"+
			"GET /notmodified HTTP/1.1\r\nHost: h\r\n\r\n"+
			"GET /tail HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	// Response 0: HEAD states the Content-Length its GET twin would, but sends
	// no bytes of it.
	status0 := readStatusLine(t, br)
	if !strings.HasPrefix(status0, "HTTP/1.1 200") {
		t.Fatalf("response 0 (HEAD): status=%q", status0)
	}
	headers0 := readHeaders(t, br)
	if headers0["content-length"] != "8" {
		t.Errorf("response 0 (HEAD) content-length = %q, want 8", headers0["content-length"])
	}

	// Response 1: 204 carries neither Content-Length nor a body.
	status1 := readStatusLine(t, br)
	if !strings.HasPrefix(status1, "HTTP/1.1 204") {
		t.Fatalf("response 1 (204): status=%q", status1)
	}
	headers1 := readHeaders(t, br)
	if _, ok := headers1["content-length"]; ok {
		t.Errorf("response 1 (204) carried Content-Length %q", headers1["content-length"])
	}

	// Response 2: 304, same rule.
	status2 := readStatusLine(t, br)
	if !strings.HasPrefix(status2, "HTTP/1.1 304") {
		t.Fatalf("response 2 (304): status=%q", status2)
	}
	headers2 := readHeaders(t, br)
	if _, ok := headers2["content-length"]; ok {
		t.Errorf("response 2 (304) carried Content-Length %q", headers2["content-length"])
	}

	// Response 3 proves the connection never desynchronised: a wrong framing
	// decision on any of the first three would have this land somewhere other
	// than a clean status line.
	status3, body3 := readResponse(t, br)
	if !strings.HasPrefix(status3, "HTTP/1.1 200") || body3 != "/tail" {
		t.Fatalf("response 3 desynced: status=%q body=%q", status3, body3)
	}

	if _, err := br.ReadByte(); err == nil {
		t.Error("the connection stayed open after Connection: close")
	}
}

// A 1xx is not a valid final status: RFC 9110 §8.6 forbids a Content-Length
// on it just like 204/304, and unlike those there is no sense in which a
// handler's 101 is a real answer to frame.
func TestInformationalStatusIsRefused(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 101})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)

	fmt.Fprint(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	status, _ := readResponse(t, bufio.NewReader(c))
	if !strings.HasPrefix(status, "HTTP/1.1 500") {
		t.Errorf("status = %q, want 500: a 1xx handler response was written to the wire", status)
	}
}

// Methods are case-sensitive tokens (RFC 9110 §9.1): "head" is not HEAD, so
// its response carries its body like any other method's. Folding the case
// sent a Content-Length with no bytes behind it, and the client read the
// next response's head as this one's body.
func TestLowercaseHeadMethodGetsItsBody(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Body: []byte("hello")})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := newReader(c)

	fmt.Fprint(c, "head /x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	// readResponse reads Content-Length bytes of body: with the old folded
	// match the body was suppressed and this read runs into EOF.
	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	if body != "hello" {
		t.Errorf("body = %q, want %q — \"head\" is not HEAD", body, "hello")
	}
}

// A "close" buried in a Connection list, or on a second Connection line, is
// still a close (RFC 9112 §9.6 — a MUST): the whole-value comparison this
// guards against kept the connection open and served requests pipelined
// behind the close to a client that had stopped reading.
func TestConnectionCloseInsideAListEndsTheConnection(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: h\r\nConnection: foo, close\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")

	br := newReader(c)
	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/a" {
		t.Fatalf("status=%q body=%q", status, body)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Error("the connection stayed open after a close inside a Connection list")
	}
	got := sizes()
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("batch sizes = %v, want one batch of one — the request behind the close must never be served", got)
	}
}

// Expect is a list (RFC 9110 §10.1.1): a client sending
// "Expect: 100-continue, x" is still waiting for the interim response, and
// matching the value whole left it waiting out ReadTimeout for a 408.
func TestExpectContinueInsideAListGetsTheInterim(t *testing.T) {
	addr, _ := serveTest(t, Config{ReadTimeout: 2 * time.Second}, echoTarget)
	c := dial(t, addr)
	br := newReader(c)

	fmt.Fprint(c, "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue, x\r\n\r\n")

	interim := readStatusLine(t, br)
	if interim != "HTTP/1.1 100 Continue" {
		t.Fatalf("interim = %q, want \"HTTP/1.1 100 Continue\"", interim)
	}
	if blank, err := br.ReadString('\n'); err != nil || strings.TrimRight(blank, "\r\n") != "" {
		t.Fatalf("the 100 Continue was not followed by a blank line, got %q, err %v", blank, err)
	}
	fmt.Fprint(c, "hello")
	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/x" {
		t.Errorf("status=%q body=%q", status, body)
	}
}

// A chunked request body — the sole Transfer-Encoding, no Content-Length —
// reaches the handler decoded, and the request pipelined behind it is still
// found where the encoding ended.
func TestChunkedRequestBodyReachesTheHandler(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			body := append(append([]byte(nil), r.Target...), ':')
			body = append(body, r.Body...)
			out = append(out, Response{Status: 200, Body: body})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dial(t, addr)
	br := newReader(c)

	fmt.Fprint(c, "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"+
		"5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: dropped\r\n\r\n"+
		"GET /y HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	status, body := readResponse(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") || body != "/x:hello world" {
		t.Fatalf("status=%q body=%q, want the decoded chunked body", status, body)
	}
	status2, body2 := readResponse(t, br)
	if !strings.HasPrefix(status2, "HTTP/1.1 200") || body2 != "/y:" {
		t.Fatalf("the request behind the chunked body answered status=%q body=%q — framing desynced", status2, body2)
	}
}

// An HTTP/1.0 keep-alive request answered with a streamed body ends the
// connection — the body is close-delimited — so a request pipelined behind it
// in the same read must never reach the handler: it used to be handled in the
// same batch and its answer dropped, a request acted on and never answered.
func TestHTTP10StreamedResponseIsTheLastHandled(t *testing.T) {
	var mu sync.Mutex
	var handled []string
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i, r := range b.Items {
			mu.Lock()
			handled = append(handled, string(r.Target))
			mu.Unlock()
			res[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("streamed")}})
			}}
		}
		return sluice.Batch[Response]{Items: res}
	})
	c := dial(t, addr)
	fmt.Fprint(c, "GET /a HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET /b HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")

	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("the connection did not end after the close-delimited body: %v", err)
	}
	if !strings.HasPrefix(string(all), "HTTP/1.1 200") || !strings.HasSuffix(string(all), "\r\n\r\nstreamed") {
		t.Fatalf("the answer was %q", all)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(handled) != 1 || handled[0] != "/a" {
		t.Errorf("the handler saw %q; only /a can be answered on this connection", handled)
	}
}
