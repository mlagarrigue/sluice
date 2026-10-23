package httpstream

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// serveOn runs Serve on ln until the test ends.
func serveOn(t *testing.T, ln net.Listener, handle Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, ln, Config{ShutdownGrace: time.Second}, handle)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func answerTargets(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for i := range b.Items {
		out = append(out, Response{Status: 200, Body: append([]byte(nil), b.Items[i].Target...)})
	}
	return sluice.Batch[Response]{Items: out}
}

// A client that closes its connection while the handler runs cancels the
// handler's context on HTTP/1.1, so that a query the handler started on the
// client's behalf stops with it.
func TestHTTP1ClientLeavingCancelsTheContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	seen := make(chan error, 1)
	serveOn(t, ln, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		select {
		case <-ctx.Done():
			seen <- ctx.Err()
		case <-time.After(10 * time.Second):
			seen <- nil
		}
		return answerTargets(b)
	})

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, "GET /slow HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	_ = c.Close()
	if err := <-seen; !errors.Is(err, context.Canceled) {
		t.Fatalf("the handler's context after the client closed: %v, want context.Canceled", err)
	}
}

// The watch reads the connection while the handler runs. A request the
// client pipelines meanwhile is not lost to it — it is put back for the
// request reader — and arriving data is not the client leaving.
func TestHTTP1WatchKeepsAPipelinedRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	errs := make(chan error, 2)
	serveOn(t, ln, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			errs <- ctx.Err()
		case <-time.After(200 * time.Millisecond):
			errs <- nil
		}
		return answerTargets(b)
	})

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, "GET /a HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err := io.WriteString(c, "GET /b HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	for _, want := range []string{"/a", "/b"} {
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			t.Fatalf("reading the answer to %s: %v", want, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != want {
			t.Fatalf("answer %q, want %q", body, want)
		}
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Errorf("a context was cancelled while its client was still there: %v", err)
		}
	}
}

// seenPeer records the RemoteAddr and ALPN of the first request it answers.
type seenPeer struct {
	mu   sync.Mutex
	addr netip.AddrPort
	alpn string
	tls  bool
}

func (s *seenPeer) handle(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	s.mu.Lock()
	r := b.Items[0]
	s.addr, s.tls = r.RemoteAddr, r.TLS != nil
	if r.TLS != nil {
		s.alpn = r.TLS.NegotiatedProtocol
	}
	s.mu.Unlock()
	return answerTargets(b)
}

func (s *seenPeer) check(t *testing.T, addr netip.AddrPort, alpn string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addr != addr {
		t.Errorf("RemoteAddr %v, want the client's %v", s.addr, addr)
	}
	if !s.tls {
		t.Fatalf("TLS is nil on a request that came over TLS")
	}
	if s.alpn != alpn {
		t.Errorf("NegotiatedProtocol %q, want %q", s.alpn, alpn)
	}
}

// A handler reads who it talks to — the address, and the TLS session with the
// protocol ALPN chose — on HTTP/1.1 and HTTP/2 over TLS, and on HTTP/3.
func TestRequestCarriesThePeer(t *testing.T) {
	cfg := &tls.Config{
		Certificates: []tls.Certificate{selfSigned(t)},
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	}

	for _, proto := range []string{"http/1.1", "h2"} {
		t.Run(proto, func(t *testing.T) {
			tcp, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var s seenPeer
			serveOn(t, tls.NewListener(tcp, cfg), s.handle)

			var local net.Addr
			tr := &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, //nolint:gosec // G402: a certificate the test minted
					NextProtos:         []string{proto},
				},
				ForceAttemptHTTP2: proto == "h2",
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err == nil {
						local = c.LocalAddr()
					}
					return c, err
				},
			}
			defer tr.CloseIdleConnections()
			resp, err := (&http.Client{Transport: tr}).Get("https://" + tcp.Addr().String() + "/who")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			s.check(t, addrPortOf(local), proto)
		})
	}

	t.Run("h3", func(t *testing.T) {
		serverPC, clientPC := dgram.Pair()
		defer func() { _ = serverPC.Close() }()
		defer func() { _ = clientPC.Close() }()
		var s seenPeer
		served := make(chan struct{})
		go func() {
			defer close(served)
			conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultParameters())
			if err != nil {
				return
			}
			_ = ServeH3Conn(t.Context(), conn, Config{}, s.handle)
		}()
		conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		st, err := conn.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.WriteFinal(h3Request("GET", "/who")); err != nil {
			t.Fatal(err)
		}
		if err := st.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(st)
		if !strings.Contains(string(got), "/who") {
			t.Fatalf("no answer on the request stream: %q", got)
		}
		_ = conn.Close()
		<-served
		s.check(t, addrPortOf(clientPC.LocalAddr()), "h3")
	})
}
