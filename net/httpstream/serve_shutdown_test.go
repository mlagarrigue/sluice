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

// RFC 9113 §6.8: "Endpoints MUST NOT increase the value they send in the
// last stream identifier". A stream the client opens after the drain's
// second GOAWAY is refused but still advances the highest identifier seen;
// the GOAWAY a later connection error sends must keep naming the one the
// second GOAWAY did, not the refused stream. Stream 1 is a POST whose body
// never comes: open, so the drain cannot finish, and without a handler in
// flight, so the error's GOAWAY is not held back by one.
func TestH2GoAwayLastStreamIdentifierNeverRises(t *testing.T) {
	addr, cancel, _ := serveUntilCancelled(t, Config{BodyDelivery: BodyWhole, ShutdownGrace: 10 * time.Second}, echoTarget)

	c := dialH2(t, addr)
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/slow"),
	})
	c.flush()
	c.pump(func() bool { return len(c.settings) > 0 })
	cancel()
	c.pump(func() bool { return len(c.goAways) >= 1 && len(c.pinged) >= 1 })
	if len(c.pinged) < 1 {
		t.Fatal("no drain PING after the cancellation")
	}
	c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.pinged[0]})
	c.flush()
	c.pump(func() bool { return len(c.goAways) >= 2 })
	if len(c.goAways) < 2 || goAwayLast(c.goAways[1]) != 1 {
		t.Fatalf("second GOAWAY missing or naming %d, want stream 1", goAwayLast(c.goAways[len(c.goAways)-1]))
	}

	// Stream 3, opened past the second GOAWAY: refused, identifier seen.
	c.request(3, "GET", "/late")
	c.flush()
	c.pump(func() bool { _, seen := c.resets[3]; return seen })
	if _, seen := c.resets[3]; !seen {
		t.Fatal("stream 3 was not refused after the second GOAWAY")
	}
	// Then a connection error — DATA on a stream nobody opened.
	c.frame(h2Frame{Type: frameData, StreamID: 99, Payload: []byte("x")})
	c.flush()
	c.pump(func() bool { return len(c.goAways) >= 3 })
	if len(c.goAways) < 3 {
		t.Fatal("no GOAWAY for the connection error")
	}
	if last := goAwayLast(c.goAways[2]); last != 1 {
		t.Errorf("the error's GOAWAY names stream %d, after a GOAWAY that named 1; §6.8 forbids raising it", last)
	}
}

// A client that sends the 24 bytes of the HTTP/2 preface and nothing else —
// not the SETTINGS RFC 9113 §3.4 says must follow — has opened no stream and
// can have none in flight: it is the HTTP/2 form of an accepted, silent
// connection, which ADR 0017 closes at once. It used to count as busy from
// the preface on and wait for the acknowledgement of a PING it would never
// send, so the shutdown ran to the end of its grace and reported requests in
// flight. It is now told as any connection is, given LingerTimeout to send
// its SETTINGS — a client whose SETTINGS is already in the socket gets the
// ordinary two-step drain — then told GOAWAY(0, NO_ERROR), nothing having
// been processed, and closed.
func TestShutdownClosesAPrefaceOnlyH2ConnectionAfterTheLinger(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const linger = 50 * time.Millisecond
	srv := &Server{Config: Config{ShutdownGrace: 1500 * time.Millisecond, LingerTimeout: linger}, Handler: echoTarget}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, Preface); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	// The server's SETTINGS says the preface was read and the connection is
	// HTTP/2's; the shutdown starts from there.
	if typ, _ := readRawFrame(t, c); typ != frameSettings {
		t.Fatalf("first frame from the server is type %#x, want SETTINGS", typ)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v after %v, want nil: a preface-only connection held the drain", err, time.Since(start))
	}
	if took := time.Since(start); took < linger || took > 10*linger {
		t.Errorf("Shutdown returned after %v, want about the %v linger", took, linger)
	}
	// What the client read before the close: the rest of the greeting, the
	// drain's GOAWAY and PING, and a last GOAWAY naming stream 0.
	var goAways [][]byte
	for typ, payload := readRawFrame(t, c); typ != 0xff; typ, payload = readRawFrame(t, c) {
		if typ == frameGoAway && len(payload) >= 8 {
			goAways = append(goAways, payload)
		}
	}
	if len(goAways) != 2 {
		t.Fatalf("%d GOAWAY frames before the close, want 2", len(goAways))
	}
	if last := be32(goAways[0][:4]); last != maxStreamID {
		t.Errorf("first GOAWAY names %d, want 2^31-1", last)
	}
	if last, code := be32(goAways[1][:4]), be32(goAways[1][4:8]); last != 0 || code != errNoError {
		t.Errorf("last GOAWAY names stream %d with code %#x, want 0 and NO_ERROR", last, code)
	}
	if err := <-served; err != nil {
		t.Errorf("Serve = %v", err)
	}
}

// readRawFrame reads one frame off a raw HTTP/2 connection: its type and
// payload, or type 0xff once the connection has ended.
func readRawFrame(t *testing.T, c net.Conn) (byte, []byte) {
	t.Helper()
	var hdr [9]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return 0xff, nil
	}
	payload := make([]byte, int(hdr[0])<<16|int(hdr[1])<<8|int(hdr[2]))
	if _, err := io.ReadFull(c, payload); err != nil {
		return 0xff, nil
	}
	return hdr[3], payload
}
