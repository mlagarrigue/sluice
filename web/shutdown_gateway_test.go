package web_test

import (
	"context"
	"errors"
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
// never ran and Shutdown never returned (RP-20261027-api-01).
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
	go func() { done <- web.Shutdown(ctx, idleServer{}, gw, backend) }()
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
	if err := gw.Close(); err != nil {
		t.Errorf("Close after the pipeline returned: %v", err)
	}
}

type idleServer struct{}

func (idleServer) Shutdown(context.Context) error { return nil }
