package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A connect that fails gives back both things lend took before it: the
// slot, which TestPoolWaitsWithinTheContext reads, and the count Close
// waits on, which no test read. Without the count back, Close after one
// refused connect waited for ever.
func TestPoolCloseAfterAFailedConnect(t *testing.T) {
	p := NewPool(1, func(context.Context) (*Conn, error) { return nil, errors.New("refused") })
	if _, err := p.lend(t.Context()); err == nil {
		t.Fatal("a failed connect lent something")
	}
	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close still waits for a connection a failed connect never lent")
	}
}
