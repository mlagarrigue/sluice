package nats

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Close is how a connection is stopped, and a call whose context has
// neither deadline nor cancellation, on a server that says nothing, cannot
// stop it: Close closes the socket before it takes the lock the call holds,
// and the call's read fails. Close waited for that lock, so for the server.
func TestCloseEndsACallWithoutADeadline(t *testing.T) {
	c, err := Connect(t.Context(), silentServer(t), Config{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Request(context.Background(), "orders.get", []byte("x"))
		done <- err
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if !c.mu.TryLock() {
			break
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the call never took the connection")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		_ = c.nc.Close() // release the call so the test ends
		t.Fatal("Close waited for a call without a deadline")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnBroken) {
			t.Errorf("the call ended with %v, want ErrConnBroken", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the call outlived Close")
	}
}
