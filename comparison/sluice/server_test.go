package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// The validation the study does once for all variants (§4): every variant
// the binary implements answers the four scenarios with the key it was asked
// for and the same normalised document as R, on a sample of keys drawn
// without repetition (§2.1), under 64 concurrent requests — the shape where
// a grouping bug returns one caller's row to another.
//
// It needs the postgres container (SLUICE_PG and friends, the tables of
// go run ./internal/bench/cmd/pgfixtures) and skips without it.

const (
	keySpace    = 100000 // bench_orders holds ids 1..100000
	sampleSize  = 192
	listLimit   = 1000
	concurrency = 64
)

// variants are what the binary implements today. R first: the others are
// compared to it.
type variant struct {
	name string
	args []string
}

var variants = []variant{
	{"R", nil},
	{"nethttp-h2", []string{"-transport", "nethttp-h2"}},
	{"httpstream-h1", []string{"-transport", "httpstream-h1"}},
	{"httpstream-h2", []string{"-transport", "httpstream-h2"}},
	{"httpstream-h3", []string{"-transport", "httpstream-h3"}},
	{"pgx", []string{"-driver", "pgx"}},
	{"gateway-16-500us", []string{"-gateway-size", "16", "-gateway-within", "500us"}},
	{"gateway-256-8ms", []string{"-gateway-size", "256", "-gateway-within", "8ms"}},
	{"gateway-eager", []string{"-gateway-eager"}},
	{"httpstream-h1-batch8-conn16", []string{"-transport", "httpstream-h1", "-max-requests-per-batch", "8", "-max-requests-per-conn", "16"}},
	{"httpstream-h1-conn-unbounded", []string{"-transport", "httpstream-h1", "-max-requests-per-conn", "-1"}},
	{"gzip1", []string{"-compression", "gzip1"}},
	{"gzip6", []string{"-compression", "gzip6"}},
	{"deflate6", []string{"-compression", "deflate6"}},
	{"httpstream-h2-gzip6", []string{"-transport", "httpstream-h2", "-compression", "gzip6"}},
	{"stream", []string{"-response", "stream"}},
	{"httpstream-h1-stream", []string{"-transport", "httpstream-h1", "-response", "stream"}},
	{"httpstream-h3-stream", []string{"-transport", "httpstream-h3", "-response", "stream"}},
	{"stream-gzip6", []string{"-response", "stream", "-compression", "gzip6"}},
	{"httpstream-h2-stream-gzip6", []string{"-transport", "httpstream-h2", "-response", "stream", "-compression", "gzip6"}},
	{"json-append", []string{"-json", "append"}},
	{"json-append-stream", []string{"-json", "append", "-response", "stream"}},
	{"json-append-gzip6", []string{"-json", "append", "-compression", "gzip6"}},
	{"httpstream-h1-json-append", []string{"-transport", "httpstream-h1", "-json", "append"}},
}

func requirePG(tb testing.TB) {
	tb.Helper()
	if os.Getenv("SLUICE_PG") == "" {
		tb.Skip("SLUICE_PG is not set")
	}
}

type server struct {
	url    string
	client *http.Client
	o      options
}

// start runs the app in-process on a free port and returns a client that
// trusts its certificate and speaks the protocol the variant names.
func start(tb testing.TB, args ...string) *server {
	tb.Helper()
	requirePG(tb)
	o, err := parseOptions(append([]string{"-listen", "127.0.0.1:0"}, args...))
	if err != nil {
		tb.Fatal(err)
	}
	a, err := newApp(o)
	if err != nil {
		tb.Fatal(err)
	}
	// One certificate for the server and the client: the listener is
	// wrapped here with a certificate the test holds, and serve told not to
	// wrap it again.
	cert, err := selfSigned()
	if err != nil {
		tb.Fatal(err)
	}
	cfg, err := tlsConfig(o)
	if err != nil {
		tb.Fatal(err)
	}
	cfg.Certificates = []tls.Certificate{cert}
	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	clientTLS := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	tb.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			tb.Errorf("serve: %v", err)
		}
		_ = a.Close()
	})

	if o.transport == transportHTTPStreamH3 {
		// Axis A: a packet socket, and quic-go's HTTP/3 client over a 1-RTT
		// handshake (sluice speaks no 0-RTT; the default dialer would try
		// early data first) — the construction interop/h3_test.go uses.
		pc, err := net.ListenPacket("udp", o.listen)
		if err != nil {
			tb.Fatal(err)
		}
		go func() { done <- serveH3(ctx, o, a, pc, cfg) }()
		tr := &http3.Transport{
			TLSClientConfig: clientTLS,
			QUICConfig:      &quicgo.Config{MaxIdleTimeout: 30 * time.Second},
			Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quicgo.Config) (*quicgo.Conn, error) {
				return quicgo.DialAddr(ctx, addr, tlsCfg, cfg)
			},
			DisableCompression: true,
		}
		tb.Cleanup(func() { _ = tr.Close() })
		return &server{
			url:    "https://" + pc.LocalAddr().String(),
			client: &http.Client{Transport: tr, Timeout: 30 * time.Second},
			o:      o,
		}
	}

	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		tb.Fatal(err)
	}
	go func() { done <- serve(ctx, o, a, ln, cfg) }()

	h2 := strings.HasSuffix(o.transport, "-h2")
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13},
		MaxIdleConnsPerHost: concurrency,
		ForceAttemptHTTP2:   h2,
		// The test sets Accept-Encoding itself and decodes itself: net/http
		// only decodes gzip, and only when it asked for it.
		DisableCompression: true,
	}
	if !h2 {
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	tb.Cleanup(tr.CloseIdleConnections)
	return &server{
		url:    "https://" + ln.Addr().String(),
		client: &http.Client{Transport: tr, Timeout: 30 * time.Second},
		o:      o,
	}
}

type reply struct {
	status int
	ctype  string
	proto  string
	body   []byte
	// streamed is what the client can see of axis E: no Content-Length
	// (chunked under HTTP/1.1, DATA frames ended by a flag under h2 and h3).
	streamed bool
}

func (s *server) do(method, path string, body []byte) (reply, error) {
	req, err := http.NewRequest(method, s.url+path, bytes.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	resp, err := s.client.Do(req)
	if err != nil {
		return reply{}, err
	}
	defer resp.Body.Close()
	var r io.Reader = resp.Body
	// Axis F: the variant must say what it encoded with, and the test
	// decodes before comparing; R must not encode at all.
	switch ce := resp.Header.Get("Content-Encoding"); ce {
	case "":
		if s.o.compression != codecNone {
			return reply{}, fmt.Errorf("-compression %s answered without Content-Encoding", s.o.compression)
		}
	case "gzip":
		if r, err = gzip.NewReader(resp.Body); err != nil {
			return reply{}, err
		}
	case "deflate":
		if r, err = zlib.NewReader(resp.Body); err != nil {
			return reply{}, err
		}
	default:
		return reply{}, fmt.Errorf("Content-Encoding %q", ce)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		if want := lookupCodec(s.o.compression); want == nil || want.token != ce {
			return reply{}, fmt.Errorf("-compression %s answered Content-Encoding %s", s.o.compression, ce)
		}
		if resp.Header.Get("Vary") != "Accept-Encoding" {
			return reply{}, errors.New("compressed answer without Vary: Accept-Encoding")
		}
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return reply{}, err
	}
	return reply{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Proto, b, resp.ContentLength < 0}, nil
}

// normalise is the comparison §4 asks for: after parsing, so that key order,
// number format and whitespace cannot fail it; the status and content type
// beside it.
func normalise(r reply) (string, error) {
	var v any
	if err := json.Unmarshal(r.body, &v); err != nil {
		return "", fmt.Errorf("status %d, body is not JSON: %w (%q)", r.status, err, truncate(r.body))
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d %s %s", r.status, r.ctype, b), nil
}

func truncate(b []byte) string {
	if len(b) > 80 {
		return string(b[:80]) + "…"
	}
	return string(b)
}

// request is one of the sample: what to send, and the key the answer must
// echo under which field.
type request struct {
	name     string
	method   string
	path     string
	body     []byte
	keyField string
	key      int64
}

// sample draws the keys without repetition from one seed, as §2.1 wants:
// every variant sees the same requests in the same order.
func sample(seed uint64) []request {
	r := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // G404: a seeded draw is the point
	perm := r.Perm(keySpace)
	var out []request
	for i := range sampleSize {
		k := int64(perm[i] + 1)
		out = append(out, request{"static", "GET", "/static", nil, "", 0})
		out = append(out, request{"unit", "GET", fmt.Sprintf("/orders/%d", k), nil, "id", k})
		// A bound in the last thousand keys is still valid: fewer rows.
		out = append(out, request{"list", "GET", fmt.Sprintf("/orders?from=%d&limit=%d", k, listLimit), nil, "from", k})
		body := fmt.Appendf(nil, `{"key":%d,"total":%d}`, k, k*3)
		out = append(out, request{"write", "POST", "/orders", body, "key", k})
	}
	// A key the table does not hold: a miss must say so, with the key.
	out = append(out, request{"unit-miss", "GET", fmt.Sprintf("/orders/%d", keySpace+7), nil, "key", keySpace + 7})
	return out
}

// checkKey verifies the response contract of §2.1: the key is in the body.
func checkKey(rq request, r reply) error {
	if rq.keyField == "" {
		return nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(r.body, &doc); err != nil {
		return fmt.Errorf("%s: body is not an object: %w", rq.name, err)
	}
	var got int64
	if err := json.Unmarshal(doc[rq.keyField], &got); err != nil {
		return fmt.Errorf("%s: field %q: %w", rq.name, rq.keyField, err)
	}
	if got != rq.key {
		return fmt.Errorf("%s: asked for key %d, answer carries %d", rq.name, rq.key, got)
	}
	if rq.name == "list" {
		var items []orderRow
		if err := json.Unmarshal(doc["items"], &items); err != nil {
			return err
		}
		prev := rq.key
		for _, it := range items {
			if it.ID <= prev {
				return fmt.Errorf("list from %d: ids not strictly increasing at %d", rq.key, it.ID)
			}
			prev = it.ID
		}
		if want := min(listLimit, keySpace-int(rq.key)); len(items) != want {
			return fmt.Errorf("list from %d: %d items, want %d", rq.key, len(items), want)
		}
	}
	return nil
}

// fire sends the sample with `concurrency` requests in flight and returns
// the normalised replies in sample order.
func fire(t *testing.T, s *server, reqs []request) []string {
	t.Helper()
	out := make([]string, len(reqs))
	errs := make([]error, len(reqs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	for i, rq := range reqs {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			r, err := s.do(rq.method, rq.path, rq.body)
			if err == nil {
				err = checkKey(rq, r)
			}
			if err != nil {
				errs[i] = err
				return
			}
			out[i], errs[i] = normalise(r)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("%s %s: %v", reqs[i].method, reqs[i].path, err)
		}
	}
	return out
}

func TestVariantsAnswerLikeR(t *testing.T) {
	requirePG(t)
	reqs := sample(20261014)
	ref := start(t, variants[0].args...)
	want := fire(t, ref, reqs)

	for _, v := range variants[1:] {
		t.Run(v.name, func(t *testing.T) {
			wrote := countWrites(t)
			s := start(t, v.args...)
			got := fire(t, s, reqs)
			for i := range reqs {
				if got[i] != want[i] {
					t.Fatalf("%s %s differs from R:\n  R: %s\n  %s: %s", reqs[i].method, reqs[i].path, want[i], v.name, got[i])
				}
			}
			r, err := s.do("GET", "/static", nil)
			if err != nil {
				t.Fatal(err)
			}
			wantProto := "HTTP/1.1"
			switch {
			case strings.HasSuffix(s.o.transport, "-h2"):
				wantProto = "HTTP/2.0"
			case s.o.transport == transportHTTPStreamH3:
				wantProto = "HTTP/3.0"
			}
			if r.proto != wantProto {
				t.Fatalf("spoke %s, want %s", r.proto, wantProto)
			}
			if n := countWrites(t); n != wrote+sampleSize {
				t.Fatalf("%d rows in comparison_writes, want %d: the writes did not all land", n, wrote+sampleSize)
			}
			// Axis E: a streamed list has no Content-Length, a buffered one
			// has; the other scenarios stay buffered whatever -response.
			for _, path := range []string{"/orders?from=1&limit=10", "/static"} {
				r, err := s.do("GET", path, nil)
				if err != nil {
					t.Fatal(err)
				}
				want := s.o.response == responseStream && strings.HasPrefix(path, "/orders?")
				if r.streamed != want {
					t.Fatalf("GET %s: streamed %v, want %v", path, r.streamed, want)
				}
			}
		})
	}
}

// countWrites reads the effect in the database (§4: the row is there).
func countWrites(t *testing.T) int64 {
	t.Helper()
	o, err := parseOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := dial(o.db)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := c.Query(context.Background(), `SELECT count(*) FROM comparison_writes`, nil, postgres.QueryConfig{BatchRows: 2})
	var n int64
	for b := range src.Stream() {
		v, _ := b.Items[0].Value(0)
		n, _ = pgtype.DecodeInt8(v)
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestUnimplementedVariantsRefuse is the second half of the brief's end
// criterion: a variant the code does not have is refused, never served as R.
func TestUnimplementedVariantsRefuse(t *testing.T) {
	refused := [][]string{
		{"-transport", "httpstream-h3", "-tls=false"},
		{"-driver", "libpq"},
		{"-response", "chunked"},
		{"-compression", "br"},
		{"-json", "fast"},
		{"-transport", "nethttp-h2", "-tls=false"},
		{"-transport", "nethttp-h1", "-max-requests-per-conn", "-1"},
		{"-transport", "nope"},
	}
	for _, args := range refused {
		_, err := parseOptions(append([]string{"-db", "x:1", "-db-user", "u"}, args...))
		if err == nil {
			t.Errorf("%v: accepted", args)
		}
	}
	if _, err := parseOptions([]string{"-db", "x:1", "-db-user", "u"}); err != nil {
		t.Errorf("R refused: %v", err)
	}
}

// --- the load for the CPU profile ----------------------------------------
//
// go test -run '^$' -bench . -cpuprofile cpu.out, then go tool pprof -top.
// Sixty-four clients over keep-alive connections against R, which is the
// shape of the generator's virtual users. The figures are a profile's
// context, never a result of the study. SLUICE_STUDY_VARIANT names one of
// `variants` to profile instead of R (the in-process pre-signal of an axis).

func benchScenario(b *testing.B, pick func(r *rand.Rand, k int64) request) {
	b.Helper()
	var args []string
	if name := os.Getenv("SLUICE_STUDY_VARIANT"); name != "" {
		i := slices.IndexFunc(variants, func(v variant) bool { return v.name == name })
		if i < 0 {
			b.Fatalf("SLUICE_STUDY_VARIANT=%s: not a variant", name)
		}
		args = variants[i].args
	}
	s := start(b, args...)
	r := rand.New(rand.NewPCG(1, 1)) //nolint:gosec // G404: same keys every run
	perm := r.Perm(keySpace)
	reqs := make([]request, len(perm))
	for i, p := range perm {
		reqs[i] = pick(r, int64(p+1))
	}
	var next int
	var mu sync.Mutex
	// RunParallel runs GOMAXPROCS × p goroutines: about `concurrency` in all.
	b.SetParallelism(max(1, concurrency/runtime.GOMAXPROCS(0)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			rq := reqs[next%len(reqs)]
			next++
			mu.Unlock()
			rp, err := s.do(rq.method, rq.path, rq.body)
			if err != nil {
				b.Error(err)
				return
			}
			if rp.status != 200 {
				b.Errorf("%s %s: %d", rq.method, rq.path, rp.status)
				return
			}
		}
	})
}

func BenchmarkStatic(b *testing.B) {
	benchScenario(b, func(_ *rand.Rand, _ int64) request {
		return request{"static", "GET", "/static", nil, "", 0}
	})
}

func BenchmarkUnit(b *testing.B) {
	benchScenario(b, func(_ *rand.Rand, k int64) request {
		return request{"unit", "GET", fmt.Sprintf("/orders/%d", k), nil, "id", k}
	})
}

func BenchmarkList1000(b *testing.B) {
	benchScenario(b, func(_ *rand.Rand, k int64) request {
		k %= keySpace - listLimit
		return request{"list", "GET", fmt.Sprintf("/orders?from=%d&limit=%d", k, listLimit), nil, "from", k}
	})
}

// BenchmarkList1000Latency is the pre-signal of axis E: one client, one
// connection, sequential list requests, and per request the two delays a
// streamed answer moves — the first body byte (TTFB) and the last (TTLB) —
// as interop/bench_test.go measures them, plus the peak of the heap while
// it runs. The heap is the process's, client included: the figure compares
// stream against buffered in the same process, nothing more. Under
// SLUICE_STUDY_VARIANT=stream against the default R.
func BenchmarkList1000Latency(b *testing.B) {
	var args []string
	if name := os.Getenv("SLUICE_STUDY_VARIANT"); name != "" {
		i := slices.IndexFunc(variants, func(v variant) bool { return v.name == name })
		if i < 0 {
			b.Fatalf("SLUICE_STUDY_VARIANT=%s: not a variant", name)
		}
		args = variants[i].args
	}
	s := start(b, args...)
	url := s.url + fmt.Sprintf("/orders?from=%d&limit=%d", 1, listLimit)
	buf := make([]byte, 32<<10)
	get := func() (ttfb, ttlb time.Duration) {
		start := time.Now()
		resp, err := s.client.Get(url)
		if err != nil {
			b.Fatalf("GET: %v", err)
		}
		var n int
		for n == 0 && err == nil {
			n, err = resp.Body.Read(buf)
		}
		ttfb = time.Since(start)
		if err == nil {
			_, err = io.CopyBuffer(io.Discard, resp.Body, buf)
		}
		ttlb = time.Since(start)
		_ = resp.Body.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatalf("GET: reading the body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("GET: %s", resp.Status)
		}
		return ttfb, ttlb
	}
	get() // the handshake, outside the timer
	first := make([]time.Duration, 0, b.N)
	last := make([]time.Duration, 0, b.N)
	var ms runtime.MemStats
	var peakInuse, peakSys uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f, l := get()
		first = append(first, f)
		last = append(last, l)
		runtime.ReadMemStats(&ms)
		peakInuse = max(peakInuse, ms.HeapInuse)
		peakSys = max(peakSys, ms.Sys)
	}
	b.StopTimer()
	reportPercentiles(b, "ttfb", first)
	reportPercentiles(b, "ttlb", last)
	b.ReportMetric(float64(peakInuse)/(1<<10), "heap-inuse-peak-KiB")
	b.ReportMetric(float64(peakSys)/(1<<10), "sys-peak-KiB")
}

// reportPercentiles reports p50, p90, p99 and p99.9 of ds in nanoseconds,
// nearest rank: below 1,000 samples, p99.9 is the slowest request.
func reportPercentiles(b *testing.B, name string, ds []time.Duration) {
	b.Helper()
	slices.Sort(ds)
	for _, p := range []struct {
		label string
		q     float64
	}{{"p50", 0.5}, {"p90", 0.9}, {"p99", 0.99}, {"p99.9", 0.999}} {
		i := min(len(ds)-1, max(0, int(float64(len(ds))*p.q+0.5)-1))
		b.ReportMetric(float64(ds[i].Nanoseconds()), name+"-"+p.label+"-ns")
	}
}

func BenchmarkMixed(b *testing.B) {
	benchScenario(b, func(r *rand.Rand, k int64) request {
		switch p := r.IntN(100); {
		case p < 80:
			return request{"unit", "GET", fmt.Sprintf("/orders/%d", k), nil, "id", k}
		case p < 95:
			k %= keySpace - listLimit
			return request{"list", "GET", fmt.Sprintf("/orders?from=%d&limit=%d", k, listLimit), nil, "from", k}
		}
		return request{"write", "POST", "/orders", fmt.Appendf(nil, `{"key":%d,"total":1}`, k), "key", k}
	})
}
