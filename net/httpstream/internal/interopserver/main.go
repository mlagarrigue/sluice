// Command interopserver is the server that third-party clients are pointed
// at: curl over HTTP/1.1 and HTTP/2, and h2spec for RFC 9113 conformance;
// with -h3, curl and aioquic over HTTP/3, and h3spec for RFC 9114; behind
// nginx, HAProxy and Envoy, the reverse proxies a deployment puts in front
// of it. scripts/interop-http.sh, scripts/interop-http3.sh and
// scripts/interop-proxy.sh run it, locally and in continuous integration.
//
// It is deliberately small. Every request is answered with its own target,
// which lets a client check that the response it got is the one it asked
// for; /stream answers with a streamed body of several batches, so that a
// streamed HTTP/2 response — the one path whose flow control is pulled a
// batch at a time — crosses a wire a real client reads.
//
// With -tls it serves behind the TLS configuration docs/guide/deployments.md
// recommends — TLS 1.2 floor, AEAD suites, ALPN h2 then http/1.1 — on a
// certificate it signs for itself at start-up, so that the clients above
// also exercise ALPN, the way a browser reaches HTTP/2.
//
// With -h3 it serves HTTP/3 on UDP instead, through [httpstream.Server.ServeQUIC]
// on a [quic.Listener]: TLS 1.3, ALPN h3, the same throwaway certificate.
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
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/net/quic/udp"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on; port 0 picks a free one")
	withTLS := flag.Bool("tls", false, "terminate TLS with a self-signed certificate and ALPN")
	h3 := flag.Bool("h3", false, "serve HTTP/3 over QUIC on UDP instead of TCP")
	flag.DurationVar(&config.IdleTimeout, "idle", config.IdleTimeout, "close a connection idle this long")
	flag.Parse()

	if *h3 {
		serveH3(*listen)
		return
	}

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
//
// Bodies are held whole, against the package's own default. h2spec reads a
// malformed request by the first frame that comes back, and /echo answers
// the body it was sent: both want the request finished before the response
// starts. Streaming lets a handler answer first — RFC 9113 §8.1 allows it,
// and a content-length that disagrees with the DATA sum is still a stream
// error (PROTOCOL_ERROR) when END_STREAM makes the sum final — but whether
// the response or the reset leaves first then depends on whether the DATA
// arrived with the headers, and h2spec's 8.1.2.6/2 reads one as a failure
// one run in several. The streamed form is exercised by the package's own
// tests and by the QUIC Interop Runner, not here.
var config = httpstream.Config{
	BodyDelivery: httpstream.BodyWhole,
	IdleTimeout:  10 * time.Second,
	ReadTimeout:  10 * time.Second,
	WriteTimeout: 10 * time.Second,
}

// serveH3 serves HTTP/3 on a UDP socket until SIGINT or SIGTERM, with the
// same handler and bounds as the TCP server.
func serveH3(listen string) {
	pc, err := udp.Listen("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pc.LocalAddr().String())
	cfg := &tls.Config{
		Certificates: tlsConfig().Certificates,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h3"},
	}
	ln, err := quic.NewListener(pc, cfg, quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		log.Fatal(err)
	}
	s := &httpstream.Server{Config: config, Handler: handle}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	err = s.ServeQUIC(ln)
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// tlsConfig is the configuration docs/guide/deployments.md recommends,
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

// handle answers each request with its target, /stream with a streamed
// body, /trailers with the same body and a trailer field after it, and
// /echo with the request's own body: one response per request, in order, as
// every httpstream handler.
func handle(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		switch string(r.Target) {
		case "/stream":
			out = append(out, httpstream.Response{Status: 200, Stream: streamed})
			continue
		case "/trailers":
			out = append(out, httpstream.Response{
				Status:   200,
				Headers:  []httpstream.Header{{Name: []byte("trailer"), Value: []byte("x-checksum")}},
				Stream:   streamed,
				Trailers: func() []httpstream.Header { return checksum },
			})
			continue
		case "/echo":
			// The body borrows the read buffer, which outlives this call
			// only as a copy.
			out = append(out, httpstream.Response{Status: 200, Body: append([]byte(nil), r.Body...)})
			continue
		}
		out = append(out, httpstream.Response{
			Status: 200,
			Body:   append([]byte("you asked for "), r.Target...),
		})
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

// checksum is the trailer /trailers sends after its body.
var checksum = []httpstream.Header{{Name: []byte("x-checksum"), Value: []byte("three batches")}}

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
