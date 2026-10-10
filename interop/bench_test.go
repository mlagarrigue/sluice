package interop

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
)

// These benchmarks are the measurement docs/guide/limits.md names as the
// exit condition of net/httpstream's experimental label: a client that
// shares no code with sluice — the standard library's HTTP/2 client, quic-go's
// HTTP/3 client — requests a plain response, a small streamed one and a
// bulk streamed one over loopback. ns/op is the round trip seen by the
// client; MB/s counts the body bytes it received. The package's own
// micro-benchmarks (h2bench_test.go, h3bench_test.go) measure one layer
// each with no socket in the way; these measure the whole path.
//
//	cd interop && go test -run xxx -bench 'H2|H3' -benchtime 3s -count 3

// startH2Server runs Serve behind a TLS listener that offers h2 by ALPN:
// the standard library's client then speaks HTTP/2 from the first byte,
// with no upgrade dance, and Serve routes the connection to ServeH2 on its
// preface. Serve returns when ctx ends.
func startH2Server(tb testing.TB) (base string) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tlsCfg := serverTLS(tb)
	tlsCfg.NextProtos = []string{"h2"}
	tlsLn := tls.NewListener(ln, tlsCfg)
	tb.Cleanup(func() { _ = tlsLn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	cfg := httpstream.Config{IdleTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout}
	go func() { _ = httpstream.Serve(ctx, tlsLn, cfg, h3Handle) }()
	return "https://" + ln.Addr().String()
}

// h2Client is net/http's transport pinned to HTTP/2: the TLS config offers
// only h2, so a response over anything else is a failure, not a fallback.
func h2Client(tb testing.TB) *http.Client {
	tb.Helper()
	tlsCfg := clientTLS()
	tlsCfg.NextProtos = []string{"h2"}
	tr := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	tb.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: timeout}
}

// benchGet runs b.N sequential requests on one connection and checks each
// body's length, which is what a client-side latency figure is: one round
// trip, request to last body byte, with nothing else in flight.
func benchGet(b *testing.B, client *http.Client, url string, proto int, want int) {
	b.Helper()
	get := func() {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("GET %s: %v", url, err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("GET %s: reading the body: %v", url, err)
		}
		if resp.StatusCode != http.StatusOK || resp.ProtoMajor != proto || int(n) != want {
			b.Fatalf("GET %s: %s over %s, %d body bytes, want %d", url, resp.Status, resp.Proto, n, want)
		}
	}
	get() // the handshake, outside the timer
	b.SetBytes(int64(want))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		get()
	}
}

// routes are the three bodies each protocol is measured on: the plain
// response the handler returns whole, the three-line streamed one of the
// interop tests, and the bulk one of bulkSize bytes pulled a batch at a time.
var routes = []struct {
	name, path string
	want       int
}{
	{"plain", "/hello", len("you asked for /hello")},
	{"stream", "/stream", len(h3Stream)},
	{"bulk", "/bulk", bulkSize},
}

func BenchmarkH2FromNetHTTP(b *testing.B) {
	base := startH2Server(b)
	client := h2Client(b)
	for _, r := range routes {
		b.Run(r.name, func(b *testing.B) { benchGet(b, client, base+r.path, 2, r.want) })
	}
}

func BenchmarkH3FromQuicGo(b *testing.B) {
	base, _ := startH3Server(b)
	client := h3Client(b)
	for _, r := range routes {
		b.Run(r.name, func(b *testing.B) { benchGet(b, client, base+r.path, 3, r.want) })
	}
}
