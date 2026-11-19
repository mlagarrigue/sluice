package stream_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/web/stream"
)

// dialH3 completes a QUIC handshake to addr from a socket of its own.
func dialH3(addr string) (*quic.Conn, error) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	to, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	//nolint:gosec // G402: the test's own self-signed certificate
	c, err := quic.Dial(ctx, pc, to, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}, MinVersion: tls.VersionTLS13}, quic.DefaultConfig())
	if err != nil {
		_ = pc.Close()
	}
	return c, err
}

// The QUIC listener's bounds are within reach of ServeH3 and ServeTLSAndH3
// through H3: with MaxConnsPerIP at one, two connections from the same
// address are never both kept. The package function, H3's zero value, leaves
// it unbounded and keeps both.
//
// Which of the two goes depends on timing, not on order: the listener counts
// a connection once its handshake proves the address, so a second one that
// arrives before the first is counted is admitted, and the first is refused
// when it proves the address in turn (CI, 2026-11-19, saw the second dial
// succeed). The test reads what is alive after both dials.
func TestH3ListenerSettingsReachServeH3(t *testing.T) {
	cert := selfSignedH3(t)
	for _, tc := range []struct {
		name    string
		h3      stream.H3
		refused bool
		all     bool // ServeTLSAndH3 rather than ServeH3
	}{
		{"MaxConnsPerIP 1", stream.H3{Listener: quic.ListenerConfig{MaxConnsPerIP: 1}}, true, false},
		{"MaxConnsPerIP 1, ServeTLSAndH3", stream.H3{Listener: quic.ListenerConfig{MaxConnsPerIP: 1}}, true, true},
		{"zero value", stream.H3{}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := freePort(t)
			srv := &http.Server{
				Addr:              addr,
				ReadHeaderTimeout: time.Second,
				Handler:           http.NotFoundHandler(),
				TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13},
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			serve := tc.h3.ServeH3
			if tc.all {
				serve = tc.h3.ServeTLSAndH3
			}
			go func() { done <- serve(ctx, srv, time.Second, "", "") }()
			defer func() { cancel(); <-done }()

			var first *quic.Conn
			var err error
			for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				if first, err = dialH3(addr); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the first connection never completed: %v", err)
				}
			}
			defer func() { _ = first.Close() }()
			second, err := dialH3(addr)
			if err == nil {
				defer func() { _ = second.Close() }()
			}
			both := err == nil && alive(t, first) && alive(t, second)
			if refused := !both; refused != tc.refused {
				t.Errorf("second connection from the same address: dial err %v, one refused %v, want %v", err, refused, tc.refused)
			}
		})
	}
}

// alive reports whether c still answers a PING. A connection the listener
// refused after its handshake is closed by then, and its Ping fails as soon
// as the close arrives.
func alive(t *testing.T, c *quic.Conn) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	return c.Ping(ctx) == nil
}
