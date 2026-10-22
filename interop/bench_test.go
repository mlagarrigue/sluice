package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

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

// startTLSServer runs Serve behind a TLS listener that offers proto by
// ALPN. Offered "h2", the standard library's client speaks HTTP/2 from the
// first byte, with no upgrade dance, and Serve routes the connection to
// ServeH2Conn on its preface; offered "http/1.1", it speaks HTTP/1.1 over the
// same TLS. Serve returns when ctx ends.
func startTLSServer(tb testing.TB, proto string, handle httpstream.Handler) (base string) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tlsCfg := serverTLS(tb)
	tlsCfg.NextProtos = []string{proto}
	tlsLn := tls.NewListener(ln, tlsCfg)
	tb.Cleanup(func() { _ = tlsLn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	cfg := httpstream.Config{IdleTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout}
	go func() { _ = httpstream.Serve(ctx, tlsLn, cfg, handle) }()
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

// h1Client is net/http's transport pinned to HTTP/1.1 over TLS: the TLS
// config offers only http/1.1, and a custom TLS config already keeps the
// transport off HTTP/2. Keep-alive holds one connection across requests
// until the server's MaxRequestsPerConn ends it.
func h1Client(tb testing.TB) *http.Client {
	tb.Helper()
	tlsCfg := clientTLS()
	tlsCfg.NextProtos = []string{"http/1.1"}
	tr := &http.Transport{TLSClientConfig: tlsCfg}
	tb.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: timeout}
}

// benchGet runs b.N sequential requests on one connection and checks each
// body's length, which is what a client-side latency figure is: one round
// trip, request to last body byte, with nothing else in flight. Besides the
// mean (ns/op), it reports the distribution of two delays per request, both
// counted from the request: the first body byte (TTFB, the moment the
// client sees the response start) and the last (TTLB, the moment it has it
// all). A streamed response that keeps its first batch until the last one
// is ready shows a TTFB close to its TTLB. header, when not nil, goes out
// on every request; nil makes each request what client.Get sends.
func benchGet(b *testing.B, client *http.Client, url string, header http.Header, proto int, want int) {
	b.Helper()
	buf := make([]byte, 32<<10)
	get := func() (ttfb, ttlb time.Duration) {
		start := time.Now()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			b.Fatal(err)
		}
		if header != nil {
			req.Header = header
		}
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("GET %s: %v", url, err)
		}
		var n int
		for n == 0 && err == nil {
			n, err = resp.Body.Read(buf)
		}
		ttfb = time.Since(start)
		var rest int64
		switch {
		case err == nil:
			rest, err = io.CopyBuffer(io.Discard, resp.Body, buf)
		case errors.Is(err, io.EOF):
			err = nil
		}
		ttlb = time.Since(start)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("GET %s: reading the body: %v", url, err)
		}
		if got := int64(n) + rest; resp.StatusCode != http.StatusOK || resp.ProtoMajor != proto || got != int64(want) {
			b.Fatalf("GET %s: %s over %s, %d body bytes, want %d", url, resp.Status, resp.Proto, got, want)
		}
		return ttfb, ttlb
	}
	get() // the handshake, outside the timer
	first := make([]time.Duration, 0, b.N)
	last := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(want))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f, l := get()
		first = append(first, f)
		last = append(last, l)
	}
	b.StopTimer()
	reportPercentiles(b, "ttfb", first)
	reportPercentiles(b, "ttlb", last)
}

// reportPercentiles reports p50, p90, p99 and p99.9 of ds in nanoseconds,
// the distribution docs-fr's measurement plan asks for instead of a mean.
// A percentile is the nearest rank: below 1,000 samples, p99.9 is the
// slowest request.
func reportPercentiles(b *testing.B, name string, ds []time.Duration) {
	b.Helper()
	slices.Sort(ds)
	for _, q := range []struct {
		label string
		per   int // per thousand
	}{{"p50", 500}, {"p90", 900}, {"p99", 990}, {"p99.9", 999}} {
		i := (len(ds)*q.per + 999) / 1000 // ceil(len × q), a rank from 1
		b.ReportMetric(float64(ds[max(i, 1)-1]), name+"-"+q.label+"-ns")
	}
}

// routes are the bodies each protocol is measured on: the plain response
// the handler returns whole, the three-line streamed one of the interop
// tests, the bulk one of bulkSize bytes pulled a batch at a time, and the
// same bulk body from a producer that pauses pacedGap before each batch
// after the first, as a database reading the next rows would.
var routes = []struct {
	name, path string
	want       int
}{
	{"plain", "/hello", len("you asked for /hello")},
	{"stream", "/stream", len(h3Stream)},
	{"bulk", "/bulk", bulkSize},
	{"paced", "/paced", bulkSize},
}

func BenchmarkH2FromNetHTTP(b *testing.B) {
	base := startTLSServer(b, "h2", h3Handle)
	client := h2Client(b)
	for _, r := range routes {
		b.Run(r.name, func(b *testing.B) { benchGet(b, client, base+r.path, nil, 2, r.want) })
	}
}

// BenchmarkH3FromQuicGo also reports discarded, the datagrams the
// listener's front door refused during the run — a connection's queue
// overflowing among them. The bulk body draws back the largest burst of
// acknowledgements, the one the queue is sized for: a single refusal there
// fails the benchmark, since a dropped datagram costs the request a client
// PTO that the mean would quietly absorb.
func BenchmarkH3FromQuicGo(b *testing.B) {
	base, ln, _ := startH3Server(b, h3Handle)
	client := h3Client(b)
	for _, r := range routes {
		b.Run(r.name, func(b *testing.B) {
			before := ln.DiscardedPackets()
			benchGet(b, client, base+r.path, nil, 3, r.want)
			n := ln.DiscardedPackets() - before
			b.ReportMetric(float64(n), "discarded")
			if r.name == "bulk" && n != 0 {
				b.Errorf("the listener discarded %d datagrams during the bulk run", n)
			}
		})
	}
}
