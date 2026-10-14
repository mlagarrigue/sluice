package quic_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// Shutdown holds the CONNECTION_CLOSE until the peer has acknowledged every
// byte: a 64 KiB stream written and closed just before Shutdown arrives
// whole, and neither end records an error.
func TestShutdownDeliversTheTailBeforeClosing(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	const size = 64 << 10
	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	if _, err := cs.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := cs.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(ss)
	if err != nil {
		t.Fatalf("reading the stream after Shutdown: %v (got %d bytes)", err, len(got))
	}
	if len(got) != size {
		t.Fatalf("the server read %d bytes, want %d", len(got), size)
	}
	for i, b := range got {
		if b != byte(i) {
			t.Fatalf("byte %d is %#x, want %#x", i, b, byte(i))
		}
	}
	if err := client.Err(); err != nil {
		t.Fatalf("client Err after Shutdown: %v", err)
	}
	if err := server.Err(); err != nil {
		t.Fatalf("server Err after the client's Shutdown: %v", err)
	}
}

// A context already done does not wait: Shutdown reports the context's
// error at once and the connection is closed all the same — the peer's
// AcceptStream learns it.
func TestShutdownGivesUpWithTheContext(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write(make([]byte, 16<<10)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 0)
	defer cancel()
	start := time.Now()
	err = client.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown with an expired context: %v; want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Shutdown took %v with an expired context", elapsed)
	}
	if err := client.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v; want nil", err)
	}

	actx, acancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer acancel()
	for {
		s, err := server.AcceptStream(actx)
		if err == nil {
			_, _ = io.Copy(io.Discard, s) // the stream that was open when the client closed
			continue
		}
		if !strings.Contains(err.Error(), "the connection is closed") {
			t.Fatalf("server AcceptStream after the client's Shutdown: %v", err)
		}
		break
	}
}
