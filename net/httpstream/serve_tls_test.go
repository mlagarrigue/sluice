package httpstream

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

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
		// routed the decrypted stream to ServeH2.
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
