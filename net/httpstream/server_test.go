package httpstream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// stopClient is one client of a [Server], on one protocol, with one request
// in flight: what the two stops are tested through.
type stopClient interface {
	// ask sends GET /slow.
	ask(t *testing.T)
	// answer reads the whole response to it and returns its body.
	answer(t *testing.T) string
	// cut reports whether the connection ended without an answer.
	cut(t *testing.T) bool
}

// stopHandler answers each request with its target once release is closed.
// It signals entered when it starts, and sends what its context says when
// it ends.
type stopHandler struct {
	entered    chan struct{}
	release    chan struct{}
	handlerErr chan error
	once       sync.Once
}

func newStopHandler() *stopHandler {
	return &stopHandler{entered: make(chan struct{}), release: make(chan struct{}), handlerErr: make(chan error, 1)}
}

func (h *stopHandler) handle(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	h.once.Do(func() { close(h.entered) })
	select {
	case <-h.release:
	case <-ctx.Done():
	}
	h.handlerErr <- ctx.Err()
	return echoTarget(ctx, b)
}

// stopProtocols starts srv on one protocol each and returns a client of it,
// and a channel that receives what Serve or ServeQUIC returned.
var stopProtocols = []struct {
	name  string
	start func(t *testing.T, srv *Server) (stopClient, <-chan error)
}{
	{"HTTP/1.1", func(t *testing.T, srv *Server) (stopClient, <-chan error) {
		t.Helper()
		addr, served := serveTCP(t, srv)
		c := dial(t, addr)
		return &h1StopClient{c: c, br: bufio.NewReader(c)}, served
	}},
	{"HTTP/2", func(t *testing.T, srv *Server) (stopClient, <-chan error) {
		t.Helper()
		addr, served := serveTCP(t, srv)
		return &h2StopClient{c: dialH2(t, addr)}, served
	}},
	{"HTTP/3", func(t *testing.T, srv *Server) (stopClient, <-chan error) {
		t.Helper()
		serverPC, clientPC := dgram.Pair()
		t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
		ln, err := quic.NewListener(serverPC, h3ServerTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		served := make(chan error, 1)
		go func() { served <- srv.ServeQUIC(ln) }()
		t.Cleanup(func() { _ = srv.Close(); _ = ln.Close() })
		conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return &h3StopClient{conn: conn}, served
	}},
}

func serveTCP(t *testing.T, srv *Server) (addr string, served <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan error, 1)
	go func() { out <- srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String(), out
}

type h1StopClient struct {
	c  net.Conn
	br *bufio.Reader
}

func (c *h1StopClient) ask(t *testing.T) {
	t.Helper()
	if _, err := io.WriteString(c.c, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
}

func (c *h1StopClient) answer(t *testing.T) string {
	t.Helper()
	_, body := readResponse(t, c.br)
	return body
}

func (c *h1StopClient) cut(t *testing.T) bool {
	t.Helper()
	if err := c.c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := c.br.ReadString('\n')
	return err != nil && !isTimeout(err) && line == ""
}

type h2StopClient struct{ c *h2client }

func (c *h2StopClient) ask(*testing.T) {
	c.c.request(1, "GET", "/slow")
	c.c.flush()
}

// answer acknowledges the drain's PING, as a conformant client does, then
// reads stream 1 to its end.
func (c *h2StopClient) answer(*testing.T) string {
	c.c.pump(func() bool { return len(c.c.pinged) > 0 || c.c.ended[1] })
	if len(c.c.pinged) > 0 {
		c.c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.c.pinged[0]})
		c.c.flush()
	}
	c.c.pump(func() bool { return c.c.ended[1] })
	return c.c.got[1]
}

func (c *h2StopClient) cut(*testing.T) bool {
	c.c.pump(func() bool { return false }) // returns when the server closes
	return !c.c.ended[1]
}

type h3StopClient struct {
	conn *quic.Conn
	s    *quic.Stream
}

func (c *h3StopClient) ask(t *testing.T) {
	t.Helper()
	c.s = askH3(t, c.conn, h3Request("GET", "/slow"))
}

func (c *h3StopClient) answer(t *testing.T) string {
	t.Helper()
	body, _ := readH3Body(t, c.s)
	return body
}

func (c *h3StopClient) cut(*testing.T) bool {
	select {
	case <-c.conn.Done():
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// Server.Shutdown is the normal stop, on every protocol: the request in
// flight is answered whole, Shutdown waits for it and returns nil, the
// handler's context is never cancelled, and Serve or ServeQUIC returns nil.
func TestServerShutdownAnswersTheRequestInFlight(t *testing.T) {
	for _, p := range stopProtocols {
		t.Run(p.name, func(t *testing.T) {
			h := newStopHandler()
			srv := &Server{Config: Config{Logger: discardLogger()}, Handler: h.handle}
			c, served := p.start(t, srv)
			c.ask(t)
			<-h.entered

			shut := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				shut <- srv.Shutdown(ctx)
			}()
			select {
			case err := <-shut:
				t.Fatalf("Shutdown returned %v with a request in flight", err)
			case <-time.After(100 * time.Millisecond):
			}
			close(h.release)
			if body := c.answer(t); body != "/slow" {
				t.Fatalf("the answer was %q, want the whole of /slow", body)
			}
			if err := <-h.handlerErr; err != nil {
				t.Errorf("the handler's context ended with %v in a normal stop", err)
			}
			select {
			case err := <-shut:
				if err != nil {
					t.Errorf("Shutdown returned %v, want nil once the request was answered", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Shutdown did not return once the request was answered")
			}
			select {
			case err := <-served:
				if err != nil {
					t.Errorf("Serve returned %v, want nil for a stop", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Serve did not return after Shutdown")
			}
		})
	}
}

// Server.Close is the forced stop, on every protocol: the handler's context
// is cancelled, the connection ends without the answer, Serve or ServeQUIC
// returns nil, and the cut is reported to the Logger.
func TestServerCloseCancelsTheHandler(t *testing.T) {
	for _, p := range stopProtocols {
		t.Run(p.name, func(t *testing.T) {
			h := newStopHandler()
			defer close(h.release)
			log := &lockedWriter{}
			srv := &Server{Config: Config{Logger: slog.New(slog.NewTextHandler(log, nil))}, Handler: h.handle}
			c, served := p.start(t, srv)
			c.ask(t)
			<-h.entered

			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-h.handlerErr:
				if err == nil {
					t.Fatal("the handler's context was not cancelled by Close")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the handler's context was never cancelled")
			}
			if !c.cut(t) {
				t.Error("the connection outlived Close")
			}
			select {
			case err := <-served:
				if err != nil {
					t.Errorf("Serve returned %v, want nil for a stop", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Serve did not return after Close")
			}
			if !strings.Contains(log.String(), "connections=1") {
				t.Errorf("the Logger read %q, want the one connection cut", log.String())
			}
		})
	}
}

// A Server reaches the HTTP/2 bounds through its one Config: the stream
// window that carried an upload eleven times faster at 100 ms of RTT
// (h2rtt_test.go) is advertised as set. A Server and Serve used to be held
// to the 65535-byte default, the field being reachable through ServeH2
// alone.
func TestServerHonoursInitialWindowSize(t *testing.T) {
	srv := &Server{Config: Config{InitialWindowSize: 1 << 20}, Handler: echoTarget}
	addr, _ := serveTCP(t, srv)
	c := dialH2(t, addr)
	c.pump(func() bool { return len(c.settings) > 0 })
	if got := c.settings[settingInitialWindowSize]; got != 1<<20 {
		t.Fatalf("advertised SETTINGS_INITIAL_WINDOW_SIZE %d, want %d", got, 1<<20)
	}
}

// Serve's grace passing with a request in flight is not a clean stop: Serve
// returns ErrShutdownForced, the handler's context is cancelled, and the
// Logger says so. A drain that ends within the grace returns nil. It used to
// return nil both ways.
func TestServeReportsAForcedShutdown(t *testing.T) {
	t.Run("grace passed", func(t *testing.T) {
		h := newStopHandler()
		defer close(h.release)
		log := &lockedWriter{}
		cfg := Config{ShutdownGrace: 200 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(log, nil))}
		addr, cancel, served := serveForError(t, cfg, h.handle)
		c := &h1StopClient{c: dial(t, addr)}
		c.br = bufio.NewReader(c.c)
		c.ask(t)
		<-h.entered
		cancel()
		select {
		case err := <-served:
			if !errors.Is(err, ErrShutdownForced) {
				t.Errorf("Serve returned %v, want ErrShutdownForced", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not return after the grace")
		}
		if err := <-h.handlerErr; err == nil {
			t.Error("the handler's context was not cancelled when the grace passed")
		}
		if !strings.Contains(log.String(), "connections=1") {
			t.Errorf("the Logger read %q, want the one connection cut", log.String())
		}
	})
	t.Run("drained", func(t *testing.T) {
		h := newStopHandler()
		close(h.release)
		addr, cancel, served := serveForError(t, Config{ShutdownGrace: 5 * time.Second}, h.handle)
		c := &h1StopClient{c: dial(t, addr)}
		c.br = bufio.NewReader(c.c)
		c.ask(t)
		if body := c.answer(t); body != "/slow" {
			t.Fatalf("the answer was %q", body)
		}
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("Serve returned %v, want nil for a complete drain", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not return")
		}
	})
}

// serveForError runs Serve until cancel and hands over what it returned.
// serveH2Direct runs ServeH2 on the first connection to the address it
// returns, under a context carrying stopKey, and cancel is that context's.
func serveH2Direct(t *testing.T, cfg Config, h Handler) (addr string, cancel context.CancelFunc, served <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), stopKey{}, "carried"))
	t.Cleanup(cancel)
	out := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			out <- err
			return
		}
		defer func() { _ = c.Close() }()
		out <- ServeH2(ctx, c, cfg, h)
	}()
	return ln.Addr().String(), cancel, out
}

type stopKey struct{}

// ServeH2 called directly stops as Serve does: the cancellation of its
// context is a GOAWAY, the request in flight answered whole, the handler's
// context neither cancelled nor stripped of ctx's values, and nil. It used
// to close the socket at the cancellation, under the handler.
func TestServeH2DrainsOnCancellation(t *testing.T) {
	t.Run("request in flight", func(t *testing.T) {
		h := newStopHandler()
		values := make(chan any, 1)
		addr, cancel, served := serveH2Direct(t, Config{ShutdownGrace: 10 * time.Second, Logger: discardLogger()},
			func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				values <- ctx.Value(stopKey{})
				return h.handle(ctx, b)
			})
		c := &h2StopClient{c: dialH2(t, addr)}
		c.ask(t)
		<-h.entered
		cancel()
		c.c.pump(func() bool { return c.c.sawGoAway })
		if !c.c.sawGoAway {
			t.Fatal("no GOAWAY after the cancellation")
		}
		select {
		case err := <-served:
			t.Fatalf("ServeH2 returned %v with a request in flight", err)
		case <-time.After(100 * time.Millisecond):
		}
		close(h.release)
		if body := c.answer(t); body != "/slow" {
			t.Fatalf("the answer was %q, want the whole of /slow", body)
		}
		if err := <-h.handlerErr; err != nil {
			t.Errorf("the handler's context ended with %v in a normal stop", err)
		}
		if v := <-values; v != "carried" {
			t.Errorf("the handler's context carried %v, want ctx's value", v)
		}
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("ServeH2 returned %v, want nil for a stop", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ServeH2 did not return once the request was answered")
		}
	})
	t.Run("idle", func(t *testing.T) {
		addr, cancel, served := serveH2Direct(t, Config{ShutdownGrace: 10 * time.Second}, echoTarget)
		c := &h2StopClient{c: dialH2(t, addr)}
		c.c.pump(func() bool { return len(c.c.settings) > 0 })
		cancel()
		c.c.pump(func() bool { return len(c.c.pinged) > 0 })
		if len(c.c.pinged) == 0 {
			t.Fatal("no drain PING after the cancellation")
		}
		c.c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.c.pinged[0]})
		c.c.flush()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("ServeH2 returned %v, want nil for a stop", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("an idle ServeH2 did not return at the PING's acknowledgement")
		}
	})
}

// ServeH2's grace passing with a request in flight is a forced stop: the
// connection is closed, the handler's context cancelled, the Logger told,
// and ServeH2 returns ErrShutdownForced.
func TestServeH2ReportsAForcedShutdown(t *testing.T) {
	h := newStopHandler()
	defer close(h.release)
	log := &lockedWriter{}
	cfg := Config{ShutdownGrace: 200 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(log, nil))}
	addr, cancel, served := serveH2Direct(t, cfg, h.handle)
	c := &h2StopClient{c: dialH2(t, addr)}
	c.ask(t)
	<-h.entered
	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, ErrShutdownForced) {
			t.Errorf("ServeH2 returned %v, want ErrShutdownForced", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH2 did not return after the grace")
	}
	if err := <-h.handlerErr; err == nil {
		t.Error("the handler's context was not cancelled when the grace passed")
	}
	if !c.cut(t) {
		t.Error("the answer was written although the grace passed")
	}
	if !strings.Contains(log.String(), "connections=1") {
		t.Errorf("the Logger read %q, want the one connection cut", log.String())
	}
}

func serveForError(t *testing.T, cfg Config, h Handler) (addr string, cancel context.CancelFunc, served <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan error, 1)
	go func() { out <- Serve(ctx, ln, cfg, h) }()
	t.Cleanup(cancel)
	return ln.Addr().String(), cancel, out
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// lockedWriter lets a Logger write from the server's goroutines while the
// test reads what it wrote.
type lockedWriter struct {
	mu sync.Mutex
	w  strings.Builder
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (l *lockedWriter) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.String()
}

// serveQUICFor starts srv on a QUIC listener and returns a client dialled to
// it with clientParams. The server's end of the datagram pair is closed only
// once the client has ended, or five seconds on: a real socket's datagrams
// outlive its close, where the pair's would be lost with it — and the
// CONNECTION_CLOSE a server sends on its way out with them.
func serveQUICFor(t *testing.T, srv *Server, clientParams quic.TransportParameters) *quic.Conn {
	t.Helper()
	serverPC, clientPC := dgram.Pair()
	clientDone := make(chan struct{})
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
	ln, err := quic.NewListener(lingerPC{serverPC, clientDone}, h3ServerTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.ServeQUIC(ln) }()
	t.Cleanup(func() { _ = srv.Close(); _ = ln.Close() })
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), clientParams)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	go func() { <-conn.Done(); close(clientDone) }()
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// lingerPC closes its packet conn once until is closed, or five seconds on.
type lingerPC struct {
	net.PacketConn
	until <-chan struct{}
}

func (c lingerPC) Close() error {
	select {
	case <-c.until:
	case <-time.After(5 * time.Second):
	}
	return c.PacketConn.Close()
}

// Server.Close ends an HTTP/3 connection the way RFC 9114 §8.1 has an
// endpoint end one it has no error to signal on: an application close with
// H3_NO_ERROR, or a code from the reserved space that greases it. The
// listener's close used to win the race and end the connection with the
// transport's own NO_ERROR, which an HTTP/3 client reads as a close below
// its protocol.
func TestServerCloseEndsHTTP3WithH3NoError(t *testing.T) {
	for _, busy := range []bool{false, true} {
		name := "idle"
		if busy {
			name = "busy"
		}
		t.Run(name, func(t *testing.T) {
			h := newStopHandler()
			defer close(h.release)
			handle := h.handle
			if !busy {
				handle = echoTarget
			}
			srv := &Server{Config: Config{Logger: discardLogger()}, Handler: handle}
			c := &h3StopClient{conn: serveQUICFor(t, srv, quic.DefaultParameters())}
			c.ask(t)
			if busy {
				<-h.entered
			} else {
				// One answered request: the connection is served, not
				// merely handed to the listener.
				c.answer(t)
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !c.cut(t) {
				t.Fatal("the connection outlived Close")
			}
			ce, ok := errors.AsType[*quic.ConnectionError](c.conn.Err())
			if !ok || !ce.Application || (ce.Code != h3NoError && (ce.Code < 0x21 || (ce.Code-0x21)%0x1f != 0)) {
				t.Fatalf("the client read %v, want an application close with H3_NO_ERROR", c.conn.Err())
			}
		})
	}
}

// Config.KeepAlive keeps an HTTP/3 connection served by a Server open past
// the idle timeout its client announced, which ServeQUIC's caller could not
// do itself: ServeQUIC does not hand the *quic.Conn back.
func TestServerKeepAliveHoldsAnHTTP3ConnectionPastTheIdleTimeout(t *testing.T) {
	const idle = 300 * time.Millisecond
	params := quic.DefaultParameters()
	params.MaxIdleTimeout = idle
	srv := &Server{Config: Config{KeepAlive: 50 * time.Millisecond}, Handler: echoTarget}
	c := &h3StopClient{conn: serveQUICFor(t, srv, params)}

	c.ask(t)
	c.answer(t)
	select {
	case <-c.conn.Done():
		t.Fatalf("the connection ended after %v of silence: %v", idle, c.conn.Err())
	case <-time.After(4 * idle):
	}
	c.ask(t)
	if got := c.answer(t); got != "/slow" {
		t.Fatalf("the second request read %q, want /slow", got)
	}
}
