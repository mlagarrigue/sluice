package httpstream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// readResponseWithHeaders is readResponse with the header fields kept, for
// the tests that need to see Connection: close on the wire.
func readResponseWithHeaders(t *testing.T, br *bufio.Reader) (status string, headers map[string]string, body string) {
	t.Helper()
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the status line: %v", err)
	}
	headers = map[string]string{}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ": ")
		if !ok {
			t.Fatalf("header line %q", line)
		}
		headers[strings.ToLower(name)] = value
	}
	length := 0
	if v, ok := headers["content-length"]; ok {
		for _, d := range v {
			length = length*10 + int(d-'0')
		}
	}
	buf := make([]byte, length)
	if _, err := readFull(br, buf); err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	return strings.TrimRight(status, "\r\n"), headers, string(buf)
}

// serveUntilCancelled starts Serve on a loopback port and returns the
// address, the cancel, and a channel closed when Serve has returned.
func serveUntilCancelled(t *testing.T, cfg Config, h Handler) (addr string, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = Serve(ctx, ln, cfg, h)
	}()
	t.Cleanup(func() { cancel(); <-finished })
	return ln.Addr().String(), cancel, finished
}

// TestShutdownDrainsInFlightRequest: the context is cancelled while a handler
// is still working. The client gets its whole response, told Connection:
// close, and nothing more; a new connection is refused; Serve returns once
// the response is out, well before the grace.
func TestShutdownDrainsInFlightRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 5 * time.Second}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(ctx, b)
	})

	c := dial(t, addr)
	if _, err := io.WriteString(c, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	// The listener is closed at cancellation, not after the grace.
	deadline := time.Now().Add(2 * time.Second)
	for {
		nc, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			break
		}
		_ = nc.Close()
		if time.Now().After(deadline) {
			t.Fatal("a new connection was still accepted after the cancellation")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)

	br := bufio.NewReader(c)
	status, headers, body := readResponseWithHeaders(t, br)
	if status != "HTTP/1.1 200 OK" || body != "/slow" {
		t.Fatalf("got %q %q, want the complete answer", status, body)
	}
	if v := headers["connection"]; v != "close" {
		t.Fatalf("Connection: %q, want close on the last response", v)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("after the response: %v, want EOF", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return once the in-flight response was written")
	}
}

// TestShutdownClosesIdleConnectionsAtOnce: a keep-alive connection with no
// request on it holds nothing Serve has to wait for.
func TestShutdownClosesIdleConnectionsAtOnce(t *testing.T) {
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 5 * time.Second}, echoTarget)

	c := dial(t, addr)
	if _, err := io.WriteString(c, "GET /a HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	if _, body := readResponse(t, br); body != "/a" {
		t.Fatalf("body %q", body)
	}
	cancel()
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("the idle connection stayed open")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve waited the grace for an idle connection")
	}
}

// TestShutdownGivesUpAfterTheGrace: a handler that does not finish is not
// waited for past ShutdownGrace. Serve returns and the connection is closed
// under the handler.
func TestShutdownGivesUpAfterTheGrace(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 200 * time.Millisecond}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(ctx, b)
	})
	// Registered after serveUntilCancelled's cleanup, so it runs before it:
	// Serve is not waited for past the grace, but the handler's goroutine
	// is still released before the test ends.
	t.Cleanup(func() { close(release) })

	c := dial(t, addr)
	if _, err := io.WriteString(c, "GET /stuck HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	start := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after the grace")
	}
	if took := time.Since(start); took < 150*time.Millisecond {
		t.Fatalf("Serve returned after %v, before the grace", took)
	}
	if _, err := bufio.NewReader(c).ReadByte(); err == nil {
		t.Fatal("the connection was left open past the grace")
	}
}

// TestServeShutdownDrainsH2Stream: the context is cancelled while an HTTP/2
// handler is still working. The client reads GOAWAY NO_ERROR naming 2^31−1
// and a PING; once it acknowledges the PING, a second GOAWAY names the one
// stream taken, and a stream opened after it is refused; the response then
// arrives whole, the server closes, and Serve returns well before the grace
// (RFC 9113 §6.8, ADR 0017 §5).
func TestServeShutdownDrainsH2Stream(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 10 * time.Second}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(ctx, b)
	})

	c := dialH2(t, addr)
	c.request(1, "GET", "/slow")
	c.flush()
	<-entered
	cancel()

	c.pump(func() bool { return len(c.goAways) >= 1 && len(c.pinged) >= 1 })
	if len(c.goAways) < 1 || len(c.pinged) < 1 {
		t.Fatalf("after the cancellation: %d GOAWAY and %d PING, want one of each before anything closes", len(c.goAways), len(c.pinged))
	}
	// The first GOAWAY forbids new streams without naming the last one taken:
	// 2^31−1 is a stream the client can never have reached.
	if last, code := goAwayLast(c.goAways[0]), goAwayCode(c.goAways[0]); last != 1<<31-1 || code != errNoError {
		t.Fatalf("first GOAWAY: last stream %d, code %#x; want 2^31-1 and NO_ERROR", last, code)
	}
	if len(c.goAways) > 1 {
		t.Fatalf("%d GOAWAY before the PING was acknowledged, want one", len(c.goAways))
	}

	// A conformant client acknowledges the PING (§6.7); the round trip the
	// first GOAWAY allowed for is then over, and the second names stream 1.
	c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.pinged[0]})
	c.flush()
	c.pump(func() bool { return len(c.goAways) >= 2 })
	if len(c.goAways) < 2 {
		t.Fatal("no second GOAWAY after the PING acknowledgement")
	}
	if last, code := goAwayLast(c.goAways[1]), goAwayCode(c.goAways[1]); last != 1 || code != errNoError {
		t.Fatalf("second GOAWAY: last stream %d, code %#x; want 1 and NO_ERROR", last, code)
	}

	// A stream opened past the second GOAWAY is refused, and the connection
	// survives the refusal: the answer to stream 1 is still owed.
	c.request(3, "GET", "/late")
	c.flush()
	c.pump(func() bool { _, seen := c.resets[3]; return seen })
	if code, seen := c.resets[3]; !seen || code != errRefusedStream {
		t.Fatalf("stream 3 after GOAWAY: reset %v with %#x, want REFUSED_STREAM", seen, code)
	}

	close(release)
	c.pump(func() bool { return c.ended[1] })
	if !c.ended[1] || c.got[1] != "/slow" {
		t.Fatalf("stream 1: ended %v body %q, want the whole answer", c.ended[1], c.got[1])
	}
	// Then the server closes, with nothing more on the wire.
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	tmp := make([]byte, 64)
	for {
		n, err := c.conn.Read(tmp)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("after the answer: %v, want the server to close", err)
		}
		c.in = append(c.in, tmp[:n]...)
		c.parse()
		if len(c.goAways) > 2 {
			t.Fatalf("%d GOAWAY in all, want two", len(c.goAways))
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return once the stream was drained")
	}
}
