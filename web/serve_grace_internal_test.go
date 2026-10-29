package web

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// pipeListener hands out the server ends of in-memory connections, so a test
// can run a real http.Server inside a synctest bubble, where a TCP socket
// would never count as durably blocked.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{} }

// dial opens a connection to the listener and returns the client end.
func (l *pipeListener) dial() net.Conn {
	client, server := net.Pipe()
	l.conns <- server
	return client
}

// A zero grace is DefaultShutdownGrace, as httpstream's Config.ShutdownGrace
// reads it, and no longer "no bound": with a request that never finishes,
// Serve gives up on the drain DefaultShutdownGrace after its context ends.
func TestServeZeroGraceIsTheDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln := newPipeListener()
		release := make(chan struct{})
		entered := make(chan struct{})
		srv := &http.Server{ReadHeaderTimeout: DefaultReadHeaderTimeout, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		})}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- serve(ctx, srv, 0, func() error { return srv.Serve(ln) }) }()

		client := ln.dial()
		go func() {
			_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
			_, _ = bufio.NewReader(client).ReadString('\n')
		}()
		<-entered

		cancel()
		start := time.Now()
		var err error
		select {
		case err = <-done:
		case <-time.After(2 * DefaultShutdownGrace):
			close(release)
			t.Fatalf("Serve still draining %v after its context ended: a zero grace is no bound", 2*DefaultShutdownGrace)
		}
		if took := time.Since(start); took != DefaultShutdownGrace {
			t.Errorf("Serve returned %v after its context ended, want the %v default grace", took, DefaultShutdownGrace)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Serve = %v, want the drain's deadline", err)
		}
		close(release)
		_ = client.Close()
		synctest.Wait()
	})
}

// A listener that breaks after it has served still has connections behind
// it: the drain that follows is bounded by grace like the one a cancelled
// context starts, not left to wait for a request that never finishes.
func TestServeListenerFailureDrainIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln := newPipeListener()
		release := make(chan struct{})
		entered := make(chan struct{})
		srv := &http.Server{ReadHeaderTimeout: DefaultReadHeaderTimeout, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		})}
		const grace = 3 * time.Second
		done := make(chan error, 1)
		go func() { done <- serve(t.Context(), srv, grace, func() error { return srv.Serve(ln) }) }()

		client := ln.dial()
		go func() {
			_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
			_, _ = bufio.NewReader(client).ReadString('\n')
		}()
		<-entered

		// The accept loop breaks: Serve returns an error that is not
		// ErrServerClosed, with the request still running.
		_ = ln.Close()
		start := time.Now()
		select {
		case err := <-done:
			if took := time.Since(start); took != grace {
				t.Errorf("Serve returned %v after the listener broke, want the %v grace", took, grace)
			}
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve = %v, want the listener's error", err)
			}
		case <-time.After(2 * grace):
			close(release)
			t.Fatalf("Serve still draining %v after the listener broke: the drain is unbounded", 2*grace)
		}
		close(release)
		_ = client.Close()
		synctest.Wait()
	})
}
