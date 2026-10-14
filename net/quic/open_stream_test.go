package quic_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// oneStreamParams lets the client open a single bidirectional stream at a
// time: the second OpenStream has to wait for the first to retire.
func oneStreamParams() quic.TransportParameters {
	p := quic.DefaultParameters()
	p.InitialMaxStreamsBidi = 1
	return p
}

// With the peer's grant spent, OpenStream parks until the peer retires a
// stream and raises MAX_STREAMS, then hands out the next stream.
func TestOpenStreamWaitsForMaxStreams(t *testing.T) {
	client, server, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneStreamParams())

	first, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := first.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	type opened struct {
		s   *quic.Stream
		err error
	}
	second := make(chan opened, 1)
	go func() {
		s, err := client.OpenStream(t.Context())
		second <- opened{s, err}
	}()
	select {
	case got := <-second:
		t.Fatalf("second OpenStream came back at once with %v, %v; the grant is spent", got.s, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	// The server finishes the first stream in both directions, which
	// retires it and grants the slot back.
	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	if err := ss.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-second:
		if got.err != nil {
			t.Fatalf("second OpenStream after the grant: %v", got.err)
		}
		if got.s.ID() != first.ID()+4 {
			t.Fatalf("second stream is %d, want %d", got.s.ID(), first.ID()+4)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second OpenStream still parked 2 s after the peer granted a stream")
	}
}

// The wait is the caller's to bound: with the grant spent and a context
// that expires, OpenStream reports the context's error and leaves the
// connection as it was.
func TestOpenStreamGivesUpWithTheContext(t *testing.T) {
	client, _, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneStreamParams())

	if _, err := client.OpenStream(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s, err := client.OpenStream(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStream with the grant spent: stream %v, err %v; want context.DeadlineExceeded", s, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("OpenStream took %v to notice the deadline", elapsed)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("the connection ended with %v; the context should only end the wait", err)
	}
}
