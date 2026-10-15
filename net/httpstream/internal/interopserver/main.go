// Command interopserver is the server that third-party clients are pointed
// at: curl over HTTP/1.1 and HTTP/2, and h2spec for RFC 9113 conformance.
// scripts/interop-http.sh runs it, locally and in continuous integration.
//
// It is deliberately small. Every request is answered with its own target,
// which lets a client check that the response it got is the one it asked
// for; /stream answers with a streamed body of several batches, so that a
// streamed HTTP/2 response — the one path whose flow control is pulled a
// batch at a time — crosses a wire a real client reads.
//
// With -tls it serves behind the TLS configuration docs/guide/experimental.md
// recommends — TLS 1.2 floor, AEAD suites, ALPN h2 then http/1.1 — on a
// certificate it signs for itself at start-up, so that the clients above
// also exercise ALPN, the way a browser reaches HTTP/2.
//
// The address it listens on is printed on standard output, on its own line,
// once the listener is open: the script reads it rather than guessing a free
// port.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on; port 0 picks a free one")
	withTLS := flag.Bool("tls", false, "terminate TLS with a self-signed certificate and ALPN")
	flag.Parse()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ln.Addr().String())
	if *withTLS {
		ln = tls.NewListener(ln, tlsConfig())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = httpstream.Serve(ctx, ln, config, handle)
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// config leaves every bound at its default: h2spec probes the protocol's
// edges, and a bound tighter than the protocol's own would be reported as a
// failure of ours. The timeouts are long enough for a slow runner.
var config = httpstream.Config{
	IdleTimeout:  10 * time.Second,
	ReadTimeout:  10 * time.Second,
	WriteTimeout: 10 * time.Second,
}

// tlsConfig is the configuration docs/guide/experimental.md recommends,
// verbatim, on a throwaway certificate: the script's clients skip
// verification, and what is under test is the handshake's outcome — version,
// suite, ALPN — not the trust chain.
func tlsConfig() *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "interopserver"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{ // TLS 1.2 only: TLS 1.3 picks its own, all AEAD
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		NextProtos: []string{"h2", "http/1.1"},
	}
}

// handle answers each request with its target, and /stream with a streamed
// body: one response per request, in order, as every httpstream handler.
func handle(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		if string(r.Target) == "/stream" {
			out = append(out, httpstream.Response{Status: 200, Stream: streamed})
			continue
		}
		out = append(out, httpstream.Response{
			Status: 200,
			Body:   append([]byte("you asked for "), r.Target...),
		})
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

// streamed yields three batches of one line each. The client sees the
// concatenation; what the transport sees is three pulls, each framed when
// the client's window admits it.
func streamed(yield func(sluice.Batch[[]byte]) bool) {
	for i := 1; i <= 3; i++ {
		line := fmt.Appendf(nil, "batch %d\n", i)
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{line}}) {
			return
		}
	}
}
