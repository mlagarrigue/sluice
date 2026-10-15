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
