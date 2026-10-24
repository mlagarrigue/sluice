package main

import (
	"context"
	"crypto/tls"
	"fmt"
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
}

// A fault is what the test server does to one key instead of answering it.
type fault uint8

const (
	wrongKey    fault = iota + 1 // answer another key: the contract broken
	silent                       // never answer, until the request is given up on
	notFound                     // 404
	unavailable                  // 503
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
// under test. A key listed in `faults` gets the fault instead: another
// key, a 404, a 503 or no answer. Under h1 and h2 it is net/http's
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
			fmt.Fprint(w, `{"status":"ok"}`)
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
			fmt.Fprintf(w, `{"id":%d,"total":%d,"label":"order-%d"}`, id, id*7, id)
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
