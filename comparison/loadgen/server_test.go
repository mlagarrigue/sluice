package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// testServer is the sleep server under one protocol, and what it saw of
// the generator: how many connections it accepted and the most requests
// one of them carried at once — the ceiling of §3.5 seen from the other
// end, and the proof that no two virtual clients share a connection.
type testServer struct {
	URL         string
	conns       atomic.Int64
	maxInFlight atomic.Int64

	// closeEvery, past zero, ends every connection after that many
	// requests: the answer carries `Connection: close`, which net/http's
	// server honours under HTTP/1.1 by closing and turns into a GOAWAY
	// under HTTP/2. closes counts the connections so ended.
	closeEvery atomic.Int64
	closes     atomic.Int64

	// stream sends every answer in two writes with a flush between, so
	// that it goes out with no Content-Length (chunked under HTTP/1.1).
	stream atomic.Bool
}

// A fault is what the test server does to one key instead of answering it.
type fault uint8

const (
	wrongKey    fault = iota + 1 // answer another key: the contract broken
	silent                       // never answer, until the request is given up on
	notFound                     // 404
	unavailable                  // 503
	inClear                      // ignore Accept-Encoding: the document in clear
	truncated                    // half the encoded body, under a Content-Length that says so
	unasked                      // encoded in a coding the request did not ask for
	rawDeflate                   // deflate without its zlib wrapper
)

// connState is one connection's view in the test server: the requests in
// flight on it and how many it has carried.
type connState struct {
	inFlight, served atomic.Int64
}

// sleepServer answers every path after exactly `d` — a sleep that wakes
// early, then a spin to the instant, since time.Sleep alone overshoots by
// ~0.2 ms here and the calibration wants the reference exact — with a
// document that honours the response contract for the key the path
// carries, so that the generator's checks all pass and only the delay is
// under test. The document is encoded in the coding the request asks for,
// gzip or deflate. A key listed in `faults` gets the fault instead:
// another key, a 404, a 503, no answer, or a body in clear, cut, in
// another coding or in raw deflate. Under h1 and h2 it is net/http's
// server; under h3, quic-go's, on a UDP socket, with the same certificate.
func sleepServer(tb testing.TB, proto string, d time.Duration, faults map[int64]fault) *testServer {
	tb.Helper()
	ts := &testServer{}
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs, _ := r.Context().Value(connKey{}).(*connState)
		if cs != nil {
			if every := ts.closeEvery.Load(); every > 0 && cs.served.Add(1)%every == 0 {
				w.Header().Set("Connection", "close")
				ts.closes.Add(1)
			}
			n := cs.inFlight.Add(1)
			defer cs.inFlight.Add(-1)
			for {
				m := ts.maxInFlight.Load()
				if n <= m || ts.maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
		}
		sleepExactly(d)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/static":
			ts.write(w, r, []byte(`{"status":"ok"}`), 0)
		case strings.HasPrefix(r.URL.Path, "/orders/"):
			var id int64
			id, _ = strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/orders/"), 10, 64)
			switch faults[id] {
			case wrongKey:
				id++
			case silent:
				select {
				case <-r.Context().Done():
				case <-release:
				}
				return
			case notFound:
				http.Error(w, "no such order", http.StatusNotFound)
				return
			case unavailable:
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			ts.write(w, r, fmt.Appendf(nil, `{"id":%d,"total":%d,"label":"order-%d"}`, id, id*7, id), faults[id])
		default:
			http.Error(w, "no such route", http.StatusNotFound)
		}
	})
	newConn := func(ctx context.Context) context.Context {
		ts.conns.Add(1)
		return context.WithValue(ctx, connKey{}, new(connState))
	}

	srv := httptest.NewUnstartedServer(handler)
	srv.Config.ConnContext = func(ctx context.Context, _ net.Conn) context.Context { return newConn(ctx) }
	srv.EnableHTTP2 = proto == protoH2
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	if proto == protoH1 {
		srv.TLS.NextProtos = []string{"http/1.1"}
	}
	srv.StartTLS()
	tb.Cleanup(srv.Close)
	ts.URL = srv.URL
	if proto != protoH3 {
		tb.Cleanup(func() { close(release) })
		return ts
	}

	// The TCP server above lends its certificate; nothing talks to it.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	h3 := &http3.Server{
		Handler:     handler,
		TLSConfig:   http3.ConfigureTLSConfig(&tls.Config{Certificates: srv.TLS.Certificates, MinVersion: tls.VersionTLS13}),
		ConnContext: func(ctx context.Context, _ *quic.Conn) context.Context { return newConn(ctx) },
	}
	go func() { _ = h3.Serve(pc) }()
	tb.Cleanup(func() { _ = h3.Close() })
	ts.URL = "https://" + pc.LocalAddr().String()
	// Registered last, so run first: a silent handler returns before
	// the servers wait for it.
	tb.Cleanup(func() { close(release) })
	return ts
}

// write sends doc in the coding the request asks for, the generator
// asking for one at most, or as the fault says.
func (ts *testServer) write(w http.ResponseWriter, r *http.Request, doc []byte, f fault) {
	coding := r.Header.Get("Accept-Encoding")
	switch f {
	case inClear:
		coding = ""
	case unasked:
		coding = map[string]string{"": codingGzip, codingGzip: codingDeflate, codingDeflate: codingGzip}[coding]
	}
	body := doc
	if coding != "" {
		var buf bytes.Buffer
		var zw io.WriteCloser
		switch {
		case coding == codingGzip:
			zw = gzip.NewWriter(&buf)
		case f == rawDeflate:
			zw, _ = flate.NewWriter(&buf, flate.DefaultCompression)
		default:
			zw = zlib.NewWriter(&buf)
		}
		_, _ = zw.Write(doc)
		_ = zw.Close()
		body = buf.Bytes()
		w.Header().Set("Content-Encoding", coding)
	}
	if f == truncated {
		body = body[:len(body)/2]
	}
	if !ts.stream.Load() {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
		return
	}
	half := len(body) / 2
	_, _ = w.Write(body[:half])
	http.NewResponseController(w).Flush() //nolint:errcheck // all three servers flush
	_, _ = w.Write(body[half:])
}

// connKey carries one connection's connState in its requests'
// context: net/http's and quic-go's servers both derive a request's
// context from the one ConnContext returned.
type connKey struct{}

func sleepExactly(d time.Duration) {
	until := time.Now().Add(d)
	if d > time.Millisecond {
		time.Sleep(d - time.Millisecond)
	}
	for time.Now().Before(until) { //nolint:revive // the spin is the point
	}
}

// protos runs a test under the three protocols.
func protos(t *testing.T, f func(t *testing.T, proto string)) {
	t.Helper()
	for _, p := range []string{protoH1, protoH2, protoH3} {
		t.Run(p, func(t *testing.T) { f(t, p) })
	}
}
