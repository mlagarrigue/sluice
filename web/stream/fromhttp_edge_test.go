package stream_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web/stream"
)

// The edges of the request: the asterisk, a target net/url refuses, the
// protocol from ALPN, a declared length, the default mux.
func TestFromHTTPRequestEdges(t *testing.T) {
	var proto, path string
	var length int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto, path, length = r.Proto, r.URL.Path, r.ContentLength
	})
	options := httpstream.Request{Method: []byte("OPTIONS"), Target: []byte("*"), TLS: &tls.ConnectionState{NegotiatedProtocol: "h2"}}
	answer(t, h, options)
	if proto != "HTTP/2.0" || path != "*" {
		t.Errorf("OPTIONS * over h2: %s %q", proto, path)
	}
	answer(t, h, get("/", "content-length", "12"))
	if proto != "HTTP/1.1" || length != 12 {
		t.Errorf("clear text with a declared length: %s %d", proto, length)
	}
	if r := answer(t, h, get("/%zz"))[0]; r.Status != http.StatusBadRequest {
		t.Errorf("a target net/url refuses: %d, want 400", r.Status)
	}
	registerDefault()
	out := stream.FromHTTP(nil)(context.Background(), sluice.Batch[httpstream.Request]{Items: []httpstream.Request{get("/fromhttp-default")}})
	if string(out.Items[0].Body) != "default" {
		t.Errorf("a nil handler answered %q, want DefaultServeMux's", out.Items[0].Body)
	}
}

// The status as net/http takes it: a 1xx is not sent and does not fix the
// status, a second WriteHeader changes nothing, a code out of range panics.
func TestFromHTTPWriteHeader(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/early":
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusCreated)
			w.WriteHeader(http.StatusTeapot)
		case "/invalid":
			w.WriteHeader(42)
		}
	})
	out := answer(t, h, get("/early"), get("/invalid"))
	if out[0].Status != http.StatusCreated {
		t.Errorf("status %d, want the first final one, 201", out[0].Status)
	}
	func() {
		defer func() {
			if p := recover(); p == nil || !strings.Contains(fmt.Sprint(p), "invalid WriteHeader code 42") {
				t.Errorf("an invalid code raised %v", p)
			}
		}()
		pulled(out[1])
	}()
}

// The body's edges: read twice past its end, closed by the handler, empty
// with a declared length of zero.
func TestFromHTTPBodyEdges(t *testing.T) {
	var errs []error
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		_, again := r.Body.Read(make([]byte, 1))
		errs = append(errs, err, again, r.Body.Close())
		_, afterClose := r.Body.Read(make([]byte, 1))
		errs = append(errs, afterClose)
		_, _ = w.Write(b)
	})
	r := answer(t, h, httpstream.Request{Method: []byte("POST"), Target: []byte("/"), Body: []byte("abc")})[0]
	if string(r.Body) != "abc" || errs[0] != nil || !errors.Is(errs[1], io.EOF) || errs[2] != nil || !errors.Is(errs[3], io.EOF) {
		t.Errorf("body %q, errors %v", r.Body, errs)
	}
}

// A panic after the head of an answer without a body has nowhere to go but
// the default logger; an abort after a Flush resets the stream it opened.
func TestFromHTTPLatePanics(t *testing.T) {
	logs := make(chanWriter, 1)
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	// SetDefault also points the log package at logs, and setting the
	// previous default back does not undo that when it is the built-in one
	// (slog.SetDefault's source says why): left there, the next test to log
	// through the log package fills logs and blocks on it for good — a
	// shuffled CI run timed out that way, after 10 minutes.
	defer func() { slog.SetDefault(prev); log.SetOutput(prevOut); log.SetFlags(prevFlags) }()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		if r.URL.Path == "/abort" {
			_, _ = io.WriteString(w, "x")
			panic(http.ErrAbortHandler)
		}
		panic("after a bodiless head")
	})
	out := answer(t, h, httpstream.Request{Method: []byte("HEAD"), Target: []byte("/")}, get("/abort"))
	if out[0].Stream != nil {
		t.Error("a HEAD answer carries a stream")
	}
	func() {
		defer func() {
			if p := recover(); !strings.Contains(fmt.Sprint(p), "abort Handler") {
				t.Errorf("an abort after the head raised %v; want the stream ended by it", p)
			}
		}()
		pulled(out[1])
	}()
	select {
	case line := <-logs:
		if !strings.Contains(line, "after a bodiless head") || !strings.Contains(line, "stack=") {
			t.Errorf("logged %q; want the panic and its stack", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the late panic was not logged")
	}
}

// chanWriter hands each log line to the test.
type chanWriter chan string

func (c chanWriter) Write(p []byte) (int, error) {
	c <- string(p)
	return len(p), nil
}

// A forced stop — the batch's context cancelled — releases a handler
// waiting for a client that does not read.
func TestFromHTTPForcedStop(t *testing.T) {
	stopped := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "b")
		stopped <- http.NewResponseController(w).Flush()
	})
	ctx, cancel := context.WithCancel(context.Background())
	out := stream.FromHTTP(h)(ctx, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{get("/")}})
	hold := make(chan struct{})
	defer close(hold)
	took := make(chan struct{})
	go out.Items[0].Stream(func(sluice.Batch[[]byte]) bool { close(took); <-hold; return false })
	<-took // the first chunk is out; the next waits on a client that does not read
	cancel()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Flush under a forced stop = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a forced stop did not release the handler")
	}
}

// The response's edges: a Content-Length that is not a number, an empty name
// in Trailer, a trailer set without being announced on a buffered answer.
func TestFromHTTPResponseEdges(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "many")
		w.Header().Set("Trailer", " , X-A")
		w.Header().Set("X-Plain", "1")
		_, _ = io.WriteString(w, "x")
		w.Header().Set("X-A", "a")
	})
	r := answer(t, h, get("/"))[0]
	if values(r.Headers, "content-length") != nil || strings.Join(values(r.Headers, "trailer"), "") != "X-A" {
		t.Errorf("fields %q", r.Headers)
	}
	if r.Trailers == nil || values(r.Trailers(), "x-a")[0] != "a" {
		t.Error("the announced trailer is missing")
	}
}

// ServeH3's refusals before it listens: a certificate that will not load, a
// bound the transport refuses, a TLS configuration QUIC cannot use.
func TestServeH3Refusals(t *testing.T) {
	cert := selfSignedH3(t)
	closed := &closeCounter{}
	for _, tc := range []struct {
		name string
		srv  *http.Server
		file string
	}{
		{"no certificate", &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}, "missing.pem"},
		{"a bound refused", &http.Server{
			Addr: "127.0.0.1:0", MaxHeaderBytes: 1 << 40, ReadHeaderTimeout: time.Second,
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		}, ""},
		{"a leaf QUIC refuses", &http.Server{
			Addr: "127.0.0.1:0", ReadTimeout: -1, WriteTimeout: -1, IdleTimeout: -1, ReadHeaderTimeout: time.Second,
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{certWithUsage(t, x509.KeyUsageKeyEncipherment)}},
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := stream.ServeH3(context.Background(), tc.srv, -1, tc.file, tc.file, closed); err == nil {
				t.Error("nil, want the refusal")
			}
		})
	}
}

// registerDefault puts one route on http.DefaultServeMux, once however many
// times the test runs.
var registerDefault = sync.OnceFunc(func() {
	http.DefaultServeMux.HandleFunc("GET /fromhttp-default", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "default")
	})
})

// A trailer set under TrailerPrefix alone, before the head, still makes the
// answer ask for trailers and stays out of its fields; a second Flush of a
// bodiless answer does nothing.
func TestFromHTTPPrefixedTrailerAndBodilessFlush(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.(http.Flusher).Flush()
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("a second Flush of a HEAD answer: %v", err)
			}
			return
		}
		w.Header().Set(http.TrailerPrefix+"X-Digest", "d")
		_, _ = io.WriteString(w, "body")
	})
	out := answer(t, h, get("/"), httpstream.Request{Method: []byte("HEAD"), Target: []byte("/")})
	if out[0].Trailers == nil || values(out[0].Trailers(), "x-digest")[0] != "d" {
		t.Error("the prefixed trailer is missing")
	}
	if len(values(out[0].Headers, http.TrailerPrefix+"x-digest")) != 0 {
		t.Error("the prefixed trailer went out in the head")
	}
}

// A long write after a forced stop fails as a Flush does.
func TestFromHTTPLongWriteAfterAForcedStop(t *testing.T) {
	failed := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		_, err := w.Write(make([]byte, 128<<10))
		failed <- err
	})
	ctx, cancel := context.WithCancel(context.Background())
	stream.FromHTTP(h)(ctx, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{get("/")}})
	cancel()
	select {
	case err := <-failed:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Write = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the write waited past a forced stop")
	}
}

// A streamed body brings its trailers to Request.Trailer, and one past the
// transport's bound fails the read with the transport's error.
func TestFromHTTPStreamedBodyEdges(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		fmt.Fprintf(w, "%d %s", len(b), r.Trailer.Get("X-Sum")) //nolint:gosec // G705: an echo the test alone reads
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &httpstream.Server{Config: httpstream.Config{MaxBodyBytes: 16 << 10, MaxStreamedBodyBytes: 32 << 10}, Handler: stream.FromHTTP(h)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	send := func(n int, trailer http.Header) (int, string) {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(make([]byte, n))
			_ = pw.Close()
		}()
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/", pr)
		req.Trailer = trailer
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if status, body := send(1<<10, http.Header{"X-Sum": {"7"}}); status != 200 || body != "1024 7" {
		t.Errorf("a body with trailers: %d %q", status, body)
	}
	if status, _ := send(64<<10, nil); status != http.StatusRequestEntityTooLarge {
		t.Errorf("a body past the bound: %d, want the read to fail", status)
	}
}

// ServeH3 loads the certificate from files as ServeTLS does, and a zero
// grace is web's default.
func TestServeH3CertificateFiles(t *testing.T) {
	cert := selfSignedH3(t)
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	if err := stream.ServeH3(ctx, srv, 0, certFile, keyFile); err != nil {
		t.Errorf("ServeH3 with certificate files = %v", err)
	}
}
