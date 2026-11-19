package stream_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/web/stream"
)

// freePort is a port no listener holds now, in TCP and in UDP: ServeTLS
// listens on srv.Addr itself, so the test cannot ask it for port zero and
// read back what it got.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// orderCloser records, when it is stopped, whether both servers had let go
// of their address by then.
type orderCloser struct {
	addr     string
	mu       sync.Mutex
	n        int
	tcpGone  bool
	udpFreed bool
}

func (c *orderCloser) Shutdown(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if conn, err := net.Dial("tcp", c.addr); err == nil {
		_ = conn.Close()
	} else {
		c.tcpGone = true
	}
	if pc, err := net.ListenPacket("udp", c.addr); err == nil {
		_ = pc.Close()
		c.udpFreed = true
	}
	return nil
}

// One call serves TCP and UDP on one address: the TCP responses announce
// HTTP/3 on the UDP port without overriding a handler's own Alt-Svc, both
// servers drain on one stop, and the closer runs once, after both have let
// go of the address.
func TestServeTLSAndH3(t *testing.T) {
	cert := selfSignedH3(t)
	addr := freePort(t)
	_, port, _ := net.SplitHostPort(addr)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/own", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Alt-Svc", "clear")
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Second,
		Handler:           mux,
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13},
	}
	closer := &orderCloser{addr: addr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- stream.ServeTLSAndH3(ctx, srv, time.Second, "", "", closer) }()

	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // G402: the test's own self-signed certificate
	}}
	var resp *http.Response
	var err error
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if resp, err = client.Get("https://" + addr + "/"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the TCP server never came up: %v", err)
		}
	}
	_ = resp.Body.Close()
	if got, want := resp.Header.Get("Alt-Svc"), `h3=":`+port+`"`; got != want {
		t.Errorf("Alt-Svc %q, want %q", got, want)
	}
	own, err := client.Get("https://" + addr + "/own")
	if err != nil {
		t.Fatal(err)
	}
	_ = own.Body.Close()
	if got := own.Header.Get("Alt-Svc"); got != "clear" {
		t.Errorf("the handler's own Alt-Svc read %q, want clear", got)
	}
	if pc, err := net.ListenPacket("udp", addr); err == nil {
		_ = pc.Close()
		t.Error("the UDP port was free while serving: HTTP/3 is not listening")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ServeTLSAndH3 = %v, want nil after a normal stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeTLSAndH3 did not return once its context ended")
	}
	closer.mu.Lock()
	defer closer.mu.Unlock()
	if closer.n != 1 {
		t.Errorf("closer stopped %d times, want once", closer.n)
	}
	if !closer.tcpGone || !closer.udpFreed {
		t.Errorf("when the closer ran, TCP gone %v, UDP freed %v: want both servers down first", closer.tcpGone, closer.udpFreed)
	}
}

// A TCP port already taken fails the call, stops HTTP/3 and still runs the
// closer, once.
func TestServeTLSAndH3TakenPort(t *testing.T) {
	cert := selfSignedH3(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Second,
		Handler:           http.NotFoundHandler(),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13},
	}
	closer := &orderCloser{addr: addr}
	done := make(chan error, 1)
	go func() { done <- stream.ServeTLSAndH3(context.Background(), srv, time.Second, "", "", closer) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("ServeTLSAndH3 on a taken TCP port returned nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeTLSAndH3 kept serving HTTP/3 after its TCP listener failed")
	}
	if pc, err := net.ListenPacket("udp", addr); err != nil {
		t.Errorf("the UDP port is still held after the TCP listener failed: %v", err)
	} else {
		_ = pc.Close()
	}
	if closer.n != 1 {
		t.Errorf("closer stopped %d times, want once", closer.n)
	}
	if err := stream.ServeTLSAndH3(context.Background(), nil, 0, "", ""); err == nil {
		t.Error("ServeTLSAndH3 without a server returned nil")
	}
}
