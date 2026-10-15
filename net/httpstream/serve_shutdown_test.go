package httpstream

import (
	"bufio"
	"context"
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
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 5 * time.Second}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(b)
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
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 200 * time.Millisecond}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(b)
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
