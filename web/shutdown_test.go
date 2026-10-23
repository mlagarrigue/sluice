package web_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// The order of ADR 0017 §1, end to end and across two fronts: an http.Server
// and an httpstream.Serve drain in one Shutdown, the gateway closes once the
// last of them has returned, and no in-flight request finds it closed.
func TestShutdownDrainsEveryServerBeforeTheGateway(t *testing.T) {
	gw := &closerFunc{}
	started := make(chan struct{})
	release := make(chan struct{})
	var handlerFinished atomic.Value
	var callFailed atomic.Bool

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h3 := web.Start(ctx, func(ctx context.Context) error {
		return httpstream.Serve(ctx, ln, httpstream.Config{ShutdownGrace: 5 * time.Second},
			func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
				close(started)
				<-release
				// The in-flight request uses the gateway after Shutdown began.
				if closed, _ := gw.state(); closed {
					callFailed.Store(true)
				}
				handlerFinished.Store(time.Now())
				out := make([]httpstream.Response, 0, b.Len())
				for range b.Items {
					out = append(out, httpstream.Response{Status: 200, Body: []byte("done")})
				}
				return sluice.Batch[httpstream.Response]{Items: out}
			})
	})
	srv := &http.Server{ReadHeaderTimeout: time.Second}
	srvLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(srvLn) }()

	respCh := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			resp = nil
		}
		respCh <- resp
	}()
	<-started

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- web.Shutdown(context.Background(), web.Servers(srv, h3), gw) }()

	// Both servers close their listener before they wait for a handler: once
	// both refuse a dial, the drain has begun under the running request.
	waitRefused(t, srvLn.Addr().String())
	waitRefused(t, ln.Addr().String())
	if closed, _ := gw.state(); closed {
		t.Fatal("the gateway was closed while a request was still running")
	}
	select {
	case <-h3.Done():
		t.Fatal("Serve returned under a running handler")
	default:
	}
	close(release)

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown returned %v", err)
	}
	resp := <-respCh
	if resp == nil {
		t.Fatal("the in-flight request was not answered")
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "done" {
		t.Errorf("the in-flight request answered %d %q (%v) during a clean stop", resp.StatusCode, body, err)
	}
	if callFailed.Load() {
		t.Error("the handler found the gateway already closed")
	}
	select {
	case <-h3.Done():
	default:
		t.Error("Shutdown returned before Serve did")
	}
	closed, closedAt := gw.state()
	if !closed {
		t.Fatal("the gateway was never closed")
	}
	finished, ok := handlerFinished.Load().(time.Time)
	if !ok {
		t.Fatal("the handler never recorded when it finished")
	}
	if closedAt.Before(finished) {
		t.Errorf("the gateway closed at %v, before the handler finished at %v", closedAt, finished)
	}
}

// A handle's Shutdown is bounded by ctx, not by the server it waits for: a
// serve that never returns must not turn a deploy into a hang, and the
// closers still run.
func TestStartedShutdownGivesUpWithItsContext(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	s := web.Start(context.Background(), func(context.Context) error {
		<-stuck
		return nil
	})
	gw := &closerFunc{}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := web.Shutdown(ctx, s, gw)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the drain's deadline", err)
	}
	if closed, _ := gw.state(); !closed {
		t.Error("the closers were abandoned because the drain timed out")
	}
}

// A serve that fails on its own — a port in use — says so through Done, and
// its reason comes back from Shutdown rather than being lost on a goroutine.
func TestStartedReportsAServeThatEndedOnItsOwn(t *testing.T) {
	boom := errors.New("listen: address in use")
	s := web.Start(context.Background(), func(context.Context) error { return boom })
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("Done never closed")
	}
	if err := s.Shutdown(context.Background()); !errors.Is(err, boom) {
		t.Errorf("Shutdown returned %v, want the serve's error", err)
	}
}

// Several servers drain together, not one after the other: the second must
// have been asked to stop before the first has finished.
func TestServersDrainAtOnce(t *testing.T) {
	var asked atomic.Int32
	bothAsked := make(chan struct{})
	serve := func(ctx context.Context) error {
		<-ctx.Done()
		if asked.Add(1) == 2 {
			close(bothAsked)
		}
		select {
		case <-bothAsked:
			return nil
		case <-time.After(time.Second):
			return errors.New("the other server was never asked to stop")
		}
	}
	a := web.Start(context.Background(), serve)
	b := web.Start(context.Background(), serve)
	boom := errors.New("gateway")
	if err := web.Shutdown(context.Background(), web.Servers(a, nil, b), &closerFunc{err: boom}); !errors.Is(err, boom) {
		t.Errorf("err = %v, want a clean drain and the closer's failure", err)
	}
}

// The forced half of ADR 0017's stop: a handler that does not return within
// Shutdown's ctx is cut by the server's Close, and its context learns it.
func TestShutdownClosesAServerItCouldNotDrain(t *testing.T) {
	started := make(chan struct{})
	handlerCtx := make(chan error, 1)
	srv := &httpstream.Server{Handler: func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		close(started)
		<-ctx.Done()
		handlerCtx <- ctx.Err()
		return sluice.Batch[httpstream.Response]{}
	}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String()); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	gw := &closerFunc{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = web.Shutdown(ctx, srv, gw)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want the deadline", err)
	}
	if closed, _ := gw.state(); !closed {
		t.Fatal("the gateway was not closed")
	}
	select {
	case err := <-handlerCtx:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's context was not cancelled")
	}
}

// The same with net/http: its Close cuts the connection the handler holds.
func TestShutdownClosesAnHTTPServerItCouldNotDrain(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	got := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err == nil {
			_ = resp.Body.Close()
		}
		got <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := web.Shutdown(ctx, srv); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want the deadline", err)
	}
	select {
	case err := <-got:
		if err == nil {
			t.Fatal("the request was answered; its connection should have been cut")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection was not cut")
	}
}

// waitRefused waits until addr refuses a connection: the observable sign that
// a server's shutdown has closed its listener. A test that slept instead
// would assert on a shutdown that may not have begun, and pass for nothing.
func waitRefused(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s still accepts connections: the shutdown never began", addr)
}

// stuckServer never finishes draining: its Shutdown waits for ctx, and its
// Close records the forced stop.
type stuckServer struct{ closed atomic.Bool }

func (s *stuckServer) Shutdown(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
func (s *stuckServer) Close() error                       { s.closed.Store(true); return nil }

// When ctx ends before several servers have drained, Shutdown's forced stop
// reaches every one of them through Servers, not only the first.
func TestShutdownForcesEveryServerOfServers(t *testing.T) {
	a, b := &stuckServer{}, &stuckServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := web.Shutdown(ctx, web.Servers(a, b)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown returned %v, want context.DeadlineExceeded", err)
	}
	if !a.closed.Load() || !b.closed.Load() {
		t.Errorf("forced stop: closed %v and %v, want both", a.closed.Load(), b.closed.Load())
	}
}

// A handler under Recover still reaches the connection's controls: the
// wrapper unwraps, so SetWriteDeadline is supported rather than refused.
func TestRecoverKeepsTheResponseController(t *testing.T) {
	got := make(chan error, 1)
	h := web.Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		got <- http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Second))
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	resp, err := http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if err := <-got; err != nil {
		t.Errorf("SetWriteDeadline under Recover: %v", err)
	}
}
