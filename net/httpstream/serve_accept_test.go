package httpstream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// flakyListener is a loopback listener whose Accept fails a chosen number
// of times, with a chosen error, after a chosen number of successful
// accepts; every other call is the real listener's. It is the kernel on a
// bad day: descriptors run out for a moment, then come back.
type flakyListener struct {
	net.Listener
	mu       sync.Mutex
	after    int   // successful accepts before the failures start
	fails    int   // failures still to report
	err      error // what each failure reports
	accepted int
}

func (l *flakyListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.accepted >= l.after && l.fails > 0 {
		l.fails--
		l.mu.Unlock()
		return nil, l.err
	}
	l.accepted++
	l.mu.Unlock()
	return l.Listener.Accept()
}

// temporaryError is what the kernel's EMFILE looks like once net has
// wrapped it: a [net.Error] whose Temporary reports true.
type temporaryError struct{}

func (temporaryError) Error() string   { return "accept: too many open files" }
func (temporaryError) Timeout() bool   { return false }
func (temporaryError) Temporary() bool { return true }

// serveFlaky runs Serve over the listener and returns what it returned once
// it has, or fails the test if it has not within the deadline.
func serveFlaky(t *testing.T, ln net.Listener) (addr string, wait func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, Config{}, echoTarget) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after the cancellation")
		}
	})
	return ln.Addr().String(), func() error {
		t.Helper()
		select {
		case err := <-done:
			done <- err // for the cleanup
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not return")
			return nil
		}
	}
}

func listenFlaky(t *testing.T, after, fails int, err error) *flakyListener {
	t.Helper()
	ln, lerr := net.Listen("tcp", "127.0.0.1:0")
	if lerr != nil {
		t.Fatal(lerr)
	}
	return &flakyListener{Listener: ln, after: after, fails: fails, err: err}
}

func ask(t *testing.T, c net.Conn, target string) {
	t.Helper()
	fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: h\r\n\r\n", target)
	if _, body := readResponse(t, bufio.NewReader(c)); body != target {
		t.Errorf("body = %q, want %q", body, target)
	}
}

// A burst of temporary Accept failures is weathered, not fatal: the
// connection accepted before it still answers, and the one dialled after it
// is accepted once the failures stop.
func TestServeBacksOffOnTemporaryAcceptError(t *testing.T) {
	ln := listenFlaky(t, 1, 3, temporaryError{})
	addr, _ := serveFlaky(t, ln)

	before := dial(t, addr)
	ask(t, before, "/before")

	after := dial(t, addr)
	ask(t, after, "/after")
	ask(t, before, "/before-again")

	ln.mu.Lock()
	left := ln.fails
	ln.mu.Unlock()
	if left != 0 {
		t.Errorf("%d temporary failures were never reported, so the backoff was not exercised", left)
	}
}

// A failure that is not temporary is the listener failing, and Serve
// returns it.
func TestServeReturnsOnPermanentAcceptError(t *testing.T) {
	permanent := errors.New("accept: the listener is gone")
	ln := listenFlaky(t, 0, 1, permanent)
	_, wait := serveFlaky(t, ln)

	if err := wait(); !errors.Is(err, permanent) {
		t.Fatalf("Serve returned %v, want %v", err, permanent)
	}
}

// A listener closed under Serve, by someone other than Serve, is reported
// as net.ErrClosed: the caller asked nothing, so this is not the clean
// return a cancellation earns.
func TestServeReportsAListenerClosedUnderIt(t *testing.T) {
	ln := listenFlaky(t, 0, 0, nil)
	_, wait := serveFlaky(t, ln)

	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wait(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Serve returned %v, want net.ErrClosed", err)
	}
}
