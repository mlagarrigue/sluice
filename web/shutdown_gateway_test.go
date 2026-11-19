package web_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/web"
)

// releasingBackend is the connection a gateway's pipeline waits on: closing
// it is what lets the pipeline return, as closing a socket fails the read a
// back-end call is blocked in.
type releasingBackend struct {
	once    sync.Once
	release chan struct{}
}

func (b *releasingBackend) Close() error {
	b.once.Do(func() { close(b.release) })
	return nil
}

// A gateway whose pipeline is blocked on the back end does not hold
// Shutdown past ctx: the gateway is shut down under ctx, Shutdown goes on to
// close the back end, which releases the pipeline. Until 2026-10-28 the
// gateway was closed with Close, which waited for the pipeline without a
// bound, so the back end's Close — the one that would have released it —
// never ran and Shutdown never returned.
func TestShutdownIsNotHeldByAGatewayBlockedOnItsBackEnd(t *testing.T) {
	backend := &releasingBackend{release: make(chan struct{})}
	entered := make(chan struct{})
	var once sync.Once
	gw := gateway.New(gateway.Config{Size: 4, Within: time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int, int]]) {
			calls(func(b sluice.Batch[*gateway.Call[int, int]]) bool {
				once.Do(func() { close(entered) })
				<-backend.release // a back-end call that answers only once its connection closes
				for _, c := range b.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	called := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := gw.Do(ctx, 1)
		called <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- web.Shutdown(ctx, idleServer{}, gw, web.CloseFunc(backend.Close)) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown returned %v, want the gateway's context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown is still waiting on the gateway 5 s after its 100 ms deadline")
	}
	select {
	case <-backend.release:
	default:
		t.Fatal("the back end after the gateway was not closed")
	}
	// Released, the pipeline answers the call it held.
	select {
	case err := <-called:
		if err != nil {
			t.Errorf("the call the pipeline held ended with %v, want its answer", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the call the pipeline held was never answered")
	}
	if err := gw.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after the pipeline returned: %v", err)
	}
}

type idleServer struct{}

func (idleServer) Shutdown(context.Context) error { return nil }

// A gateway handed to Serve as a closer is drained within grace, with
// nothing to wrap: it is a web.Drainer. Its pipeline is blocked on a back end
// that only a later closer releases, so the drain cannot finish by itself;
// Serve stops waiting for it at grace, runs the back end's close through
// web.CloseFunc, and returns. Before the closers were Drainers, a gateway
// entered the list by its Close, which waited without a bound.
func TestServeDrainsAGatewayWithinGrace(t *testing.T) {
	backend := &releasingBackend{release: make(chan struct{})}
	entered := make(chan struct{})
	var once sync.Once
	gw := gateway.New(gateway.Config{Size: 4, Within: time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int, int]]) {
			calls(func(b sluice.Batch[*gateway.Call[int, int]]) bool {
				once.Do(func() { close(entered) })
				<-backend.release
				for _, c := range b.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = gw.Do(ctx, 1)
	}()
	<-entered

	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	const grace = 100 * time.Millisecond
	go func() { done <- web.Serve(ctx, srv, grace, gw, web.CloseFunc(backend.Close)) }()
	cancel()
	start := time.Now()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Serve returned %v, want the gateway's drain cut at grace", err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("Serve took %v to return, for a grace of %v", took, grace)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve is still waiting on the gateway 5 s after a grace of 100 ms")
	}
	select {
	case <-backend.release:
	default:
		t.Fatal("the back end after the gateway was not closed")
	}
	if _, err := gw.Do(context.Background(), 2); !errors.Is(err, gateway.ErrClosed) {
		t.Errorf("a call after Serve returned: %v, want gateway.ErrClosed", err)
	}
}
