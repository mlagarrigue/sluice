package quic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// AcceptStream gives up when its context does: the caller's error, verbatim,
// well before the connection ends, and no stream.
func TestAcceptStreamReturnsOnContextCancel(t *testing.T) {
	_, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	s, err := server.AcceptStream(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcceptStream after cancel: stream %v, err %v; want context.Canceled", s, err)
	}
	if s != nil {
		t.Fatalf("AcceptStream handed out %v with a cancelled context", s)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("AcceptStream took %v to notice the cancellation", elapsed)
	}
}

// A live context changes nothing: the stream the peer opens comes out.
func TestAcceptStreamStillHandsOutStreams(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ss.ID() != cs.ID() {
		t.Fatalf("accepted stream %d, the peer opened %d", ss.ID(), cs.ID())
	}
}

// Once the peer has closed the connection, AcceptStream reports the
// connection closed rather than the context — the context is still live.
func TestAcceptStreamReturnsErrClosedAfterTheConnectionEnds(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, err := server.AcceptStream(ctx)
	if err == nil || ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcceptStream on an ended connection: stream %v, err %v; want the connection closed", s, err)
	}
	if !strings.Contains(err.Error(), "the connection is closed") {
		t.Fatalf("AcceptStream on an ended connection: %v; want the connection closed", err)
	}
}
