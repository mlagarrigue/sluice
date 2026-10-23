package httpstream_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A server whose handler is a stage: it receives a batch of requests and
// answers it positionally, which is the same shape parallel.Ordered takes.
func ExampleServe() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = httpstream.Serve(ctx, ln, httpstream.Config{IdleTimeout: time.Second},
			func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
				// One response per request, in order: on a pipelined
				// connection the position is the only correlation there is.
				out := make([]httpstream.Response, 0, b.Len())
				for i := range b.Items {
					r := &b.Items[i]
					out = append(out, httpstream.Response{
						Status: 200,
						Body:   append([]byte("you asked for "), r.Target...),
					})
				}
				return sluice.Batch[httpstream.Response]{Items: out}
			})
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()
	fmt.Fprint(c, "GET /orders/7 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	// Connection: close means the response ends with the connection, so
	// reading to EOF collects all of it however the kernel splits the reads.
	resp, err := io.ReadAll(c)
	if err != nil {
		panic(err)
	}
	// The body is the last line; the headers before it carry the length.
	fmt.Println(string(resp[len(resp)-24:]))
	// Output: you asked for /orders/7
}

// The production form: a Server on a listener, stopped in two steps.
// Shutdown takes nothing new and waits, within its context, for the
// requests in flight; Close, after it, would cut what is left.
func ExampleServer() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := &httpstream.Server{
		Config: httpstream.Config{MaxConns: 1000},
		Handler: func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
			out := make([]httpstream.Response, b.Len())
			for i := range out {
				out[i] = httpstream.Response{Status: 200, Body: []byte("ok")}
			}
			return sluice.Batch[httpstream.Response]{Items: out}
		},
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		panic(err)
	}
	fmt.Fprint(c, "GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	resp, err := io.ReadAll(c)
	if err != nil {
		panic(err)
	}
	_ = c.Close()
	fmt.Println(string(resp[:15]))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close() // the wait ran out: cut what is left
	}
	fmt.Println("serve returned:", <-served)
	// Output:
	// HTTP/1.1 200 OK
	// serve returned: <nil>
}

// HTTP/2 on a connection the caller accepted, once TLS has negotiated h2.
// Compiled, not run: it needs a certificate and an HTTP/2 client.
func ExampleServeH2Conn() {
	var (
		tcp       net.Listener // a TCP listener on port 443
		tlsConfig *tls.Config  // the server's certificate, NextProtos: []string{"h2", "http/1.1"}
		handle    httpstream.Handler
	)
	ctx := context.Background()
	for {
		raw, err := tcp.Accept()
		if err != nil {
			return
		}
		go func() {
			c := tls.Server(raw, tlsConfig)
			if err := c.HandshakeContext(ctx); err != nil || c.ConnectionState().NegotiatedProtocol != "h2" {
				_ = c.Close()
				return
			}
			_ = httpstream.ServeH2Conn(ctx, c, httpstream.Config{}, handle)
		}()
	}
}

// HTTP/3 on each connection of a QUIC listener built on the parameters the
// Config derives, so the transport's windows fit the bounds above it.
// Compiled, not run: it needs a certificate and an HTTP/3 client.
func ExampleServeH3Conn() {
	var (
		udp       net.PacketConn // a UDP socket on port 443
		tlsConfig *tls.Config    // the server's certificate, NextProtos: []string{"h3"}
		handle    httpstream.Handler
	)
	ctx := context.Background()
	cfg := httpstream.Config{}
	params, err := cfg.TransportParameters()
	if err != nil {
		panic(err)
	}
	ln, err := quic.NewListener(udp, tlsConfig, params, quic.ListenerConfig{})
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() { _ = httpstream.ServeH3Conn(ctx, conn, cfg, handle) }()
	}
}
