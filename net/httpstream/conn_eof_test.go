package httpstream

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// dataThenEOF returns all of data and io.EOF from one Read, as io.Reader
// allows, then io.EOF alone.
func dataThenEOF(data string) *readConn {
	rest := []byte(data)
	return &readConn{read: func(p []byte) (int, error) {
		n := copy(p, rest)
		rest = rest[n:]
		if len(rest) > 0 {
			return n, nil
		}
		return n, io.EOF
	}}
}

// A Read that returns a whole request and io.EOF together gives the request
// first, then the clean end: the error is the next read's, judged once what
// came with it is parsed. It was judged at once, on a buffer still holding
// the request, which made it a close mid-request and a 400 for a request
// that had arrived whole.
func TestARequestReadWithEOFIsServed(t *testing.T) {
	r := &h1Conn{cfg: Config{}.mustResolve(), conn: dataThenEOF("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")}
	batch, ok := r.next()
	if !ok || len(batch) != 1 || string(batch[0].Target) != "/x" {
		t.Fatalf("a whole request read with io.EOF: %d requests, %v; want the request", len(batch), r.err)
	}
	if _, ok := r.next(); ok || r.err != io.EOF { //nolint:errorlint // the clean end is io.EOF itself
		t.Fatalf("after the request: %v, want io.EOF", r.err)
	}
}

// The control: a request short of its end, read with io.EOF, is still a
// close mid-request.
func TestAPartRequestReadWithEOFIsClosedMidRequest(t *testing.T) {
	r := &h1Conn{cfg: Config{}.mustResolve(), conn: dataThenEOF("GET /x HTTP/1.1\r\nHost: h\r\n")}
	if batch, ok := r.next(); ok || !errors.Is(r.err, errClosedMidRequest) {
		t.Fatalf("a part request read with io.EOF: %d requests, %v; want errClosedMidRequest", len(batch), r.err)
	}
}

// gatedTLSConn holds the server's first application Read until gate is
// closed, so that the client's request and its close_notify are both in the
// socket by then. Ordered by a channel, nothing timed.
type gatedTLSConn struct {
	*tls.Conn
	gate <-chan struct{}
	once bool
}

func (c *gatedTLSConn) Read(p []byte) (int, error) {
	if !c.once {
		c.once = true
		_ = c.Handshake()
		<-c.gate
	}
	return c.Conn.Read(p)
}

type gatedTLSListener struct {
	net.Listener
	gate <-chan struct{}
}

func (l gatedTLSListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &gatedTLSConn{Conn: c.(*tls.Conn), gate: l.gate}, nil
}

// End to end, the case that returns data and io.EOF together: under TLS 1.2
// crypto/tls reads the client's close_notify behind its last data record and
// returns both from one Read. Under TLS 1.3, and without the half-close, the
// EOF comes alone: the controls.
func TestARequestBeforeTheClientsCloseNotifyIsServed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		version    uint16
		closeWrite bool
	}{
		{"TLS 1.2, close_notify behind the request", tls.VersionTLS12, true},
		{"TLS 1.3, close_notify behind the request", tls.VersionTLS13, true},
		{"TLS 1.2, no close_notify", tls.VersionTLS12, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			tlsLn := gatedTLSListener{Listener: tls.NewListener(ln, &tls.Config{
				Certificates: []tls.Certificate{selfSigned(t)},
				NextProtos:   []string{"http/1.1"},
				MaxVersion:   tc.version,
			}), gate: gate}
			var calls atomic.Int64
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = Serve(ctx, tlsLn, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					calls.Add(int64(b.Len()))
					out := make([]Response, b.Len())
					for i := range out {
						out[i] = Response{Status: 200, Body: []byte("ok")}
					}
					return sluice.Batch[Response]{Items: out}
				})
			}()
			t.Cleanup(func() { cancel(); <-done })

			c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}}) //nolint:gosec // a self-signed test certificate
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(20 * time.Second))
			if _, err := io.WriteString(c, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			if tc.closeWrite {
				if err := c.CloseWrite(); err != nil {
					t.Fatal(err)
				}
			}
			close(gate)
			resp, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				t.Fatalf("no response: %v (handler calls %d)", err, calls.Load())
			}
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || calls.Load() != 1 {
				t.Fatalf("status %d, handler calls %d; want 200 and 1", resp.StatusCode, calls.Load())
			}
		})
	}
}
