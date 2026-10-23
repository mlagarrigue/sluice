package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
)

// h3Handle mirrors net/httpstream/internal/interopserver: every request is
// answered with its own target, and /stream with a body of three batches
// pulled one at a time, so a streamed response crosses a wire a client that
// shares no code reads.
func h3Handle(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		switch string(r.Target) {
		case "/stream":
			out = append(out, httpstream.Response{Status: 200, Stream: h3Streamed})
			continue
		case "/bulk":
			out = append(out, httpstream.Response{Status: 200, Stream: h3Bulk})
			continue
		case "/paced":
			out = append(out, httpstream.Response{Status: 200, Stream: h3Paced})
			continue
		}
		out = append(out, httpstream.Response{
			Status: 200,
			Body:   append([]byte("you asked for "), r.Target...),
		})
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

func h3Streamed(yield func(sluice.Batch[[]byte]) bool) {
	for i := 1; i <= 3; i++ {
		line := fmt.Appendf(nil, "batch %d\n", i)
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{line}}) {
			return
		}
	}
}

const h3Stream = "batch 1\nbatch 2\nbatch 3\n"

// h3Bulk is the streamed body the benchmarks pull: bulkBatches batches of
// bulkChunk bytes, enough to cross several flow-control windows, so that a
// throughput figure measures the transport and not one frame.
const (
	bulkBatches = 16
	bulkChunk   = 4096
	bulkSize    = bulkBatches * bulkChunk
)

var bulkPayload = func() []byte {
	p := make([]byte, bulkChunk)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}()

func h3Bulk(yield func(sluice.Batch[[]byte]) bool) {
	for range bulkBatches {
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{bulkPayload}}) {
			return
		}
	}
}

// pacedGap is the pause h3Paced takes before every batch after the first:
// long enough beside a loopback round trip that a first batch held back
// until the last would show, short enough to keep a request under 20 ms.
const pacedGap = time.Millisecond

// h3Paced is h3Bulk from a slow producer, the shape of a response streamed
// while a database reads the next rows: the first batch is ready at once,
// the others pacedGap apart.
func h3Paced(yield func(sluice.Batch[[]byte]) bool) {
	for i := range bulkBatches {
		if i > 0 {
			time.Sleep(pacedGap)
		}
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{bulkPayload}}) {
			return
		}
	}
}

// startH3Server runs ServeH3Conn with handle behind a quic.Listener and returns
// the URL prefix to reach it, and the listener for its counters. Every accepted
// connection is served until the test ends; what ServeH3Conn returned is
// reported through served.
func startH3Server(tb testing.TB, handle httpstream.Handler) (base string, ln *quic.Listener, served <-chan error) {
	tb.Helper()
	pc := listenUDP(tb)
	cfg := httpstream.Config{IdleTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout}
	params, err := cfg.TransportParameters()
	if err != nil {
		tb.Fatal(err)
	}
	ln, err = quic.NewListener(pc, serverTLS(tb), params, quic.ListenerConfig{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ln.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	out := make(chan error, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { out <- httpstream.ServeH3Conn(ctx, c, cfg, handle) }()
		}
	}()
	return "https://" + pc.LocalAddr().String(), ln, out
}

// h3Client is quic-go's HTTP/3 client over a 1-RTT handshake: sluice speaks
// no 0-RTT, and the default dialer would try early data first.
func h3Client(tb testing.TB) *http.Client {
	tb.Helper()
	tr := &http3.Transport{
		TLSClientConfig: clientTLS(),
		QUICConfig:      &quicgo.Config{MaxIdleTimeout: timeout},
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quicgo.Config) (*quicgo.Conn, error) {
			return quicgo.DialAddr(ctx, addr, tlsCfg, cfg)
		},
	}
	tb.Cleanup(func() { _ = tr.Close() })
	return &http.Client{Transport: tr, Timeout: timeout}
}

func h3Get(tb testing.TB, client *http.Client, url string) string {
	tb.Helper()
	resp, err := client.Get(url)
	if err != nil {
		tb.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("GET %s: reading the body: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 3 {
		tb.Fatalf("GET %s: %s over %s", url, resp.Status, resp.Proto)
	}
	return string(body)
}

// TestQuicGoHTTP3ClientAgainstServeH3: a third-party HTTP/3 client gets a
// plain response, a streamed one, and several requests on one connection —
// sequentially, then two streamed bodies at once.
func TestQuicGoHTTP3ClientAgainstServeH3(t *testing.T) {
	base, _, served := startH3Server(t, h3Handle)
	client := h3Client(t)

	if got := h3Get(t, client, base+"/hello"); got != "you asked for /hello" {
		t.Fatalf("plain response: got %q", got)
	}
	if got := h3Get(t, client, base+"/stream"); got != h3Stream {
		t.Fatalf("streamed response: got %q", got)
	}
	for i := range 3 {
		target := fmt.Sprintf("/again/%d", i)
		if got := h3Get(t, client, base+target); got != "you asked for "+target {
			t.Fatalf("request %d on the same connection: got %q", i, got)
		}
	}

	var wg sync.WaitGroup
	got := make([]string, 2)
	for i := range got {
		wg.Go(func() { got[i] = h3Get(t, client, base+"/stream") })
	}
	wg.Wait()
	for i, g := range got {
		if g != h3Stream {
			t.Fatalf("parallel streamed response %d: got %q", i, g)
		}
	}

	// Closing the client's connections must end ServeH3Conn cleanly, not by
	// the idle timeout.
	client.Transport.(*http3.Transport).CloseIdleConnections()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeH3Conn returned %v once the client closed", err)
		}
	case <-time.After(timeout):
		t.Fatal("ServeH3Conn did not return after the client closed")
	}
}

// h3Fields answers every request with what the handler saw of it: method,
// target, body length, the cookie fields, the length of x-big, and the
// x-checksum trailer — so a test reads back, through a third-party client,
// what crossed the wire.
func h3Fields(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		var cookies []string
		big := 0
		for _, h := range r.Headers {
			switch strings.ToLower(string(h.Name)) {
			case "cookie":
				cookies = append(cookies, string(h.Value))
			case "x-big":
				big = len(h.Value)
			}
		}
		trailer := ""
		for _, h := range r.Trailers {
			if strings.EqualFold(string(h.Name), "x-checksum") {
				trailer = string(h.Value)
			}
		}
		out[i] = httpstream.Response{Status: 200, Body: fmt.Appendf(nil, "%s %s body=%d cookies=%s big=%d trailer=%s",
			r.Method, r.Target, len(r.Body), strings.Join(cookies, "; "), big, trailer)}
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

// h3Do sends req and returns the status and body, failing the test on a
// transport error.
func h3Do(t *testing.T, client *http.Client, req *http.Request) (int, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: reading the body: %v", req.Method, req.URL.Path, err)
	}
	return resp.StatusCode, string(body)
}

// TestQuicGoHTTP3MethodsAndFields: beyond GET, through quic-go's client — a
// POST body arrives whole, HEAD gets the status and no body (RFC 9110
// §9.3.2), OPTIONS is a request like any other, two cookies arrive (RFC 9114
// §4.2.1 lets a client split them into separate fields), a 20 KiB field
// passes under the 32 KiB bound, a 64 KiB one is refused — by the client,
// which read the bound in SETTINGS (§4.2.2), or by a 431 — and a request
// trailer reaches the handler.
func TestQuicGoHTTP3MethodsAndFields(t *testing.T) {
	base, _, served := startH3Server(t, h3Fields)
	client := h3Client(t)
	req := func(method, path string, body io.Reader) *http.Request {
		r, err := http.NewRequest(method, base+path, body)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	post := strings.Repeat("p", 100<<10)
	if status, got := h3Do(t, client, req(http.MethodPost, "/orders", strings.NewReader(post))); status != 200 || got != "POST /orders body=102400 cookies= big=0 trailer=" {
		t.Errorf("POST: %d %q", status, got)
	}
	if status, got := h3Do(t, client, req(http.MethodHead, "/orders/7", http.NoBody)); status != 200 || got != "" {
		t.Errorf("HEAD: %d %q, want 200 and no body", status, got)
	}
	if status, got := h3Do(t, client, req(http.MethodOptions, "/orders", http.NoBody)); status != 200 || got != "OPTIONS /orders body=0 cookies= big=0 trailer=" {
		t.Errorf("OPTIONS: %d %q", status, got)
	}

	withCookies := req(http.MethodGet, "/me", http.NoBody)
	withCookies.Header.Add("Cookie", "session=abc")
	withCookies.Header.Add("Cookie", "theme=dark")
	if status, got := h3Do(t, client, withCookies); status != 200 || !strings.Contains(got, "session=abc") || !strings.Contains(got, "theme=dark") {
		t.Errorf("cookies: %d %q, want both", status, got)
	}

	big := req(http.MethodGet, "/big", http.NoBody)
	big.Header.Set("X-Big", strings.Repeat("b", 20<<10))
	if status, got := h3Do(t, client, big); status != 200 || !strings.Contains(got, "big=20480") {
		t.Errorf("a 20 KiB field: %d %q", status, got)
	}
	tooBig := req(http.MethodGet, "/too-big", http.NoBody)
	tooBig.Header.Set("X-Big", strings.Repeat("b", 64<<10))
	if resp, err := client.Do(tooBig); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
			t.Errorf("a 64 KiB field was answered %d, want 431 or a client-side refusal", resp.StatusCode)
		}
	}

	withTrailer := req(http.MethodPut, "/upload", strings.NewReader("payload"))
	withTrailer.Trailer = http.Header{"X-Checksum": {"c0ffee"}}
	if status, got := h3Do(t, client, withTrailer); status != 200 || !strings.Contains(got, "trailer=c0ffee") {
		t.Errorf("request trailer: %d %q", status, got)
	}

	client.Transport.(*http3.Transport).CloseIdleConnections()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeH3Conn returned %v once the client closed", err)
		}
	case <-time.After(timeout):
		t.Fatal("ServeH3Conn did not return after the client closed")
	}
}

// TestQuicGoHTTP3ClientCancels: the client gives up on a streamed response
// halfway — its stream is reset — and the connection goes on: the next
// request on it is answered, and ServeH3Conn ends cleanly when the client
// closes.
func TestQuicGoHTTP3ClientCancels(t *testing.T) {
	base, _, served := startH3Server(t, h3Handle)
	client := h3Client(t)

	ctx, cancel := context.WithCancel(context.Background())
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/paced", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp.Body, make([]byte, bulkChunk)); err != nil {
		t.Fatalf("the first batch did not arrive: %v", err)
	}
	cancel()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("the cancelled body read to its end")
	}
	_ = resp.Body.Close()

	if got := h3Get(t, client, base+"/after"); got != "you asked for /after" {
		t.Fatalf("the request after the cancellation: %q", got)
	}
	client.Transport.(*http3.Transport).CloseIdleConnections()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeH3Conn returned %v once the client closed", err)
		}
	case <-time.After(timeout):
		t.Fatal("ServeH3Conn did not return after the client closed")
	}
}
