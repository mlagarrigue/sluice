package httpstream

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"
)

// recordingListener hands each connection it accepts to accepted too.
type recordingListener struct {
	net.Listener
	accepted chan net.Conn
}

func (l recordingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted <- c
	}
	return c, err
}

// tcpKeepAlive reads the socket's keep-alive switch and its idle and
// interval periods, in seconds.
func tcpKeepAlive(t *testing.T, c net.Conn) (on bool, idle, interval int) {
	t.Helper()
	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var keep int
	var errs [3]error
	if err := raw.Control(func(fd uintptr) {
		keep, errs[0] = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
		idle, errs[1] = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE)
		interval, errs[2] = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL)
	}); err != nil {
		t.Fatal(err)
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	return keep != 0, idle, interval
}

// Config.KeepAlive is the TCP keep-alive period of every connection a
// Server accepts, and of the one ServeH2Conn is handed. A listener from
// net.Listen sets 15s, which is what the socket kept before.
func TestKeepAliveSetsTheTCPPeriod(t *testing.T) {
	const period = 7 * time.Second
	cfg := Config{KeepAlive: period}
	for _, tc := range []struct {
		name  string
		serve func(ln net.Listener)
	}{
		{"Server", func(ln net.Listener) {
			srv := &Server{Config: cfg, Handler: echoTarget}
			go func() { _ = srv.Serve(ln) }()
			t.Cleanup(func() { _ = srv.Close() })
		}},
		{"ServeH2Conn", func(ln net.Listener) {
			go func() {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				defer func() { _ = c.Close() }()
				_ = ServeH2Conn(t.Context(), c, cfg, echoTarget)
			}()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln := recordingListener{inner, make(chan net.Conn, 1)}
			t.Cleanup(func() { _ = ln.Close() })
			tc.serve(ln)
			dial(t, ln.Addr().String())
			server := <-ln.accepted
			// The period is set right after Accept; the request a client
			// sends is not needed for it, only the moment to pass.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for {
				on, idle, interval := tcpKeepAlive(t, server)
				if on && idle == int(period/time.Second) && interval == int(period/time.Second) {
					return
				}
				select {
				case <-ctx.Done():
					t.Fatalf("keep-alive %v, idle %ds, interval %ds; want on, %v and %v", on, idle, interval, period, period)
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	}
}
