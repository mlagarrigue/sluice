package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// wireListener hands out connections that keep a copy of every byte the
// server writes, so a test can read the TLS records under the client's view.
type wireListener struct {
	net.Listener
	mu   sync.Mutex
	sent []byte
}

func (l *wireListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &wireConn{Conn: c, l: l}, nil
}

// recordTypes is the content type of each TLS record written so far, in
// order: 20 change_cipher_spec, 21 alert, 22 handshake, 23 application_data.
func (l *wireListener) recordTypes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	var types []byte
	for b := l.sent; len(b) >= 5; {
		n := int(b[3])<<8 | int(b[4])
		types = append(types, b[0])
		if len(b) < 5+n {
			break
		}
		b = b[5+n:]
	}
	return types
}

type wireConn struct {
	net.Conn
	l *wireListener
}

// Write records what the socket took, not what was offered: an alert
// attempted on a socket already closed under the record layer never reaches
// the wire, and must not count. The lock is held across the write itself:
// the client can read the bytes — and, on a close_notify, return its EOF to
// the test — before the write has returned here, so a recordTypes taken at
// that EOF must wait for the write in flight to be recorded. Recorded after
// the write and in the lock, it never ran ahead of the socket either.
func (c *wireConn) Write(p []byte) (int, error) {
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	n, err := c.Conn.Write(p)
	c.l.sent = append(c.l.sent, p[:n]...)
	return n, err
}

const tlsAlert = 21

// serveTLS10 serves one recording TLS 1.2 listener with a handler and returns
// a connected HTTP/1.1-only TLS client; the close-delimited tests below drive
// it with an HTTP/1.0 request. TLS 1.2 because its record header shows the
// content type in clear, so an alert can be told from application data on
// the wire; TLS 1.3 encrypts it. The wire is the judge rather than the Go
// client: crypto/tls reads an EOF without close_notify at a record boundary
// as a clean io.EOF on purpose, so the client cannot tell the two closes
// apart — stricter clients (OpenSSL 3) do.
func serveTLS10(t *testing.T, h Handler) (*tls.Conn, *wireListener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &wireListener{Listener: ln}
	tlsLn := tls.NewListener(rec, &tls.Config{ //nolint:gosec // G402: TLS 1.2 so the record type shows on the wire, see above
		Certificates: []tls.Certificate{selfSigned(t)},
		NextProtos:   []string{"http/1.1"},
		MaxVersion:   tls.VersionTLS12,
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = Serve(ctx, tlsLn, Config{}, h) }()
	t.Cleanup(func() { cancel(); <-done })

	c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: the certificate above is the test's own
		NextProtos:         []string{"http/1.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c, rec
}

// A close-delimited body (HTTP/1.0 client, streamed response without
// ContentLength) whose producer fails mid-way has no end marker but the
// close. RFC 9112 §9.8: such a body "is complete only if a valid closure
// alert has been received" — so the close must not send close_notify, or the
// client takes the cut body for whole. No alert record may follow the body.
func TestTruncatedCloseDelimitedBodyClosesWithoutCloseNotify(t *testing.T) {
	c, rec := serveTLS10(t, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("part0 ")}}) {
					return
				}
				panic("the producer failed after its first batch")
			},
		}}}
	})
	if _, err := c.Write([]byte("GET /report HTTP/1.0\r\nHost: h\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	head, body, ok := bytes.Cut(got, []byte("\r\n\r\n"))
	if !ok || !bytes.HasPrefix(head, []byte("HTTP/1.1 200")) {
		t.Fatalf("response head = %q (err %v)", head, err)
	}
	if bytes.Contains(head, []byte("Content-Length")) || bytes.Contains(head, []byte("chunked")) {
		t.Fatalf("not a close-delimited body: %q", head)
	}
	if string(body) != "part0 " {
		t.Fatalf("body = %q, want the part that went out before the failure", body)
	}
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("the client read %v, want a clean or an unexpected EOF", err)
	}
	if types := rec.recordTypes(); bytes.IndexByte(types, tlsAlert) >= 0 {
		t.Fatalf("the truncated close-delimited body was followed by a closure alert (record types %v): the client takes it for complete", types)
	}
}

// The complete close-delimited body keeps its closure alert: the producer
// ended, the close is the body's end, and close_notify says so.
func TestCompleteCloseDelimitedBodyEndsWithCloseNotify(t *testing.T) {
	c, rec := serveTLS10(t, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("whole")}})
			},
		}}}
	})
	if _, err := c.Write([]byte("GET /report HTTP/1.0\r\nHost: h\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("a complete close-delimited body ended with %v, want a clean close (close_notify then EOF)", err)
	}
	if _, body, ok := bytes.Cut(got, []byte("\r\n\r\n")); !ok || string(body) != "whole" {
		t.Fatalf("body = %q, want %q", body, "whole")
	}
	if types := rec.recordTypes(); len(types) == 0 || types[len(types)-1] != tlsAlert {
		t.Fatalf("the complete close-delimited body did not end with a closure alert (record types %v)", types)
	}
}

// selfSigned builds the throwaway certificate the TLS test terminates with.
func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "httpstream test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TLS termination stays in front of this package, and this test is that
// decision's proof rather than its apology: [Serve] takes a
// net.Listener and crypto/tls wraps one, so nothing in the package changes
// for browsers' two requirements — TLS, and ALPN choosing the protocol — to
// be met. ALPN settles what the client will speak; the preface sniff then
// sees exactly what it would have seen on a plaintext socket, because by the
// time Serve reads, the record layer has already been peeled.
func TestServeBehindATLSListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{selfSigned(t)},
		NextProtos:   []string{"h2", "http/1.1"},
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = Serve(ctx, tlsLn, Config{}, echoTarget) }()
	t.Cleanup(func() { cancel(); <-done })

	client := func(t *testing.T, proto string) *tls.Conn {
		t.Helper()
		c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // G402: the certificate above is the test's own
			NextProtos:         []string{proto},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := c.ConnectionState().NegotiatedProtocol; got != proto {
			t.Fatalf("ALPN negotiated %q, want %q", got, proto)
		}
		return c
	}

	t.Run("HTTP/1.1 under ALPN http/1.1", func(t *testing.T) {
		c := client(t, "http/1.1")
		if _, err := c.Write([]byte("GET /hello HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
			t.Fatal(err)
		}
		status, body := readResponse(t, bufio.NewReader(c))
		if status != "HTTP/1.1 200 OK" || body != "/hello" {
			t.Errorf("got %q with body %q", status, body)
		}
	})

	t.Run("HTTP/2 under ALPN h2", func(t *testing.T) {
		c := client(t, "h2")
		// The preface and an empty SETTINGS, which is how an h2 connection
		// opens; the server's own SETTINGS coming back is the proof the sniff
		// routed the decrypted stream to ServeH2Conn.
		if _, err := c.Write([]byte(Preface)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write([]byte{0, 0, 0, 0x04, 0, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		head := make([]byte, 9)
		if _, err := readFull(bufio.NewReader(c), head); err != nil {
			t.Fatal(err)
		}
		if head[3] != 0x04 {
			t.Errorf("the first frame back has type %#x, want SETTINGS", head[3])
		}
	})

	// ADR 0014, Rationale: the sniff decides, ALPN merely makes the client
	// cooperate. A client that negotiated http/1.1 and then sends the h2
	// preface is served HTTP/2, because Serve never reads the negotiated
	// protocol; this case pins that choice so that a change to it is a
	// revision of the ADR, not a drift.
	t.Run("HTTP/2 preface under ALPN http/1.1 is served HTTP/2", func(t *testing.T) {
		c := client(t, "http/1.1")
		if _, err := c.Write([]byte(Preface)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write([]byte{0, 0, 0, 0x04, 0, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		head := make([]byte, 9)
		if _, err := readFull(bufio.NewReader(c), head); err != nil {
			t.Fatal(err)
		}
		if head[3] != 0x04 {
			t.Errorf("the first frame back has type %#x, want SETTINGS", head[3])
		}
	})
}
