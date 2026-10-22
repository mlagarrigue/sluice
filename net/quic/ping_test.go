package quic_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// Ping waits for the peer's acknowledgement, not for its own send: while
// every client datagram is dropped it is still waiting when its context
// ends, and once the path carries again a fresh Ping returns nil. On a
// closed connection, or with a context already done, it returns at once.
func TestPingWaitsForTheAcknowledgement(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()
	hooked := &hookedConn{PacketConn: clientPC}

	connCh, errCh := startEchoServer(t, serverPC)
	conn, err := quic.Dial(context.Background(), hooked, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	select {
	case serverConn := <-connCh:
		defer serverConn.Close()
	case err := <-errCh:
		t.Fatalf("the server: %v", err)
	}

	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("Ping on a live path: %v", err)
	}

	hooked.mu.Lock()
	hooked.filterWrite = func([]byte) [][]byte { return nil }
	hooked.mu.Unlock()
	const wait = 150 * time.Millisecond
	// The clock is read before the context starts its own: read after, it
	// missed the moments between the two, and Windows measured 149.9 ms for
	// a 150 ms deadline that had fired on time.
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	err = conn.Ping(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ping with every datagram dropped: %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < wait {
		t.Fatalf("Ping with every datagram dropped returned after %v, before its context ended", elapsed)
	}

	hooked.mu.Lock()
	hooked.filterWrite = nil
	hooked.mu.Unlock()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping once the path carries again: %v", err)
	}

	done, stop := context.WithCancel(t.Context())
	stop()
	if err := conn.Ping(done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping with a cancelled context: %v, want context.Canceled", err)
	}

	_ = conn.Close()
	if err := conn.Ping(t.Context()); err == nil {
		t.Fatal("Ping on a closed connection returned nil")
	}
}
